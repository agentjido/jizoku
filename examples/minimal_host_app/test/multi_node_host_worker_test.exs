defmodule MinimalHostApp.MultiNodeHostWorkerTest do
  use MinimalHostApp.DataCase, async: false

  alias Jizoku.ReadModel.Explanation.Diagnostic
  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto, as: JournalStorage

  @moduletag timeout: 20_000

  @controller_registry __MODULE__.ControllerRegistry

  defmodule Workflow do
    use Jizoku.Workflow

    workflow do
      trigger :probe do
        manual()

        payload do
          field :probe_id, :string
        end
      end

      step :probe, MinimalHostApp.MultiNodeHostWorkerTest.Probe
      transition :probe, on: :ok, to: :complete
    end
  end

  defmodule Probe do
    use Jizoku.Step,
      name: "multi_node_host_worker_probe",
      input_schema: [probe_id: [type: :string, required: true]]

    @impl Jizoku.Step
    def run(%{probe_id: probe_id}, context) do
      claim_id = context.claim_id

      case Registry.lookup(
             MinimalHostApp.MultiNodeHostWorkerTest.ControllerRegistry,
             context.run_id
           ) do
        [] ->
          {:error,
           %{
             code: "probe_controller_missing",
             message: "multi-node probe controller is unavailable",
             retryable?: false
           }}

        [{controller, nil}] ->
          send(controller, {:probe_started, self(), claim_id, probe_id})

          receive do
            {:finish_probe, ^claim_id, outcome} -> outcome
          after
            5_000 ->
              {:error,
               %{
                 code: "probe_timed_out",
                 message: "multi-node probe timed out",
                 retryable?: false
               }}
          end
      end
    end
  end

  setup do
    start_supervised!({Registry, keys: :unique, name: @controller_registry})
    {:ok, task_supervisor: start_supervised!(Task.Supervisor)}
  end

  test "concurrent workers execute and apply one visible attempt once", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("concurrent-claim")

    node_a = execute_gated_async(task_supervisor, "node-a", lease_for: 30)
    node_b = execute_gated_async(task_supervisor, "node-b", lease_for: 30)

    assert_receive {:worker_ready, node_a_pid}, 1_000
    assert_receive {:worker_ready, node_b_pid}, 1_000

    send(node_a_pid, :drain)
    send(node_b_pid, :drain)

    assert_receive {:probe_started, winner_pid, winner_claim_id, "concurrent-claim"}, 1_000
    finish_probe(winner_pid, winner_claim_id, success("concurrent-claim"))

    results = [Task.await(node_a, 5_000), Task.await(node_b, 5_000)]

    assert Enum.all?(results, &valid_concurrent_result?(&1, run.run_id))
    assert Enum.any?(results, &match?({:ok, %Snapshot{status: :completed}}, &1))
    refute_receive {:probe_started, _pid, _claim_id, "concurrent-claim"}, 50

    assert {:ok, %Snapshot{status: :completed, terminal?: true}} =
             Jizoku.inspect_run(run.run_id)

    dispatch_entries = dispatch_entries!()
    assert count_entries(dispatch_entries, :attempt_claimed) == 1
    assert count_entries(dispatch_entries, :attempt_completed) == 1

    run_entries = run_entries!(run.run_id)
    assert count_entries(run_entries, :runnable_applied) == 1
    assert count_entries(run_entries, :run_terminal) == 1
  end

  test "a renewed claim stays owned by one node past its original lease", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("heartbeat")

    node_a =
      execute_async(task_supervisor, "node-a", lease_for: 1, heartbeat_interval_ms: 50)

    assert_receive {:probe_started, node_a_pid, node_a_claim_id, "heartbeat"}, 1_000

    entries = await_dispatch_entry(:attempt_heartbeat)
    claimed = find_entry!(entries, :attempt_claimed, node_a_claim_id)
    heartbeat = find_entry!(entries, :attempt_heartbeat, node_a_claim_id)

    assert claimed.data.owner_id == "node-a"
    assert DateTime.compare(heartbeat.data.lease_until, claimed.data.lease_until) == :gt

    after_original_lease = DateTime.add(claimed.data.lease_until, 1, :millisecond)

    assert {:ok, :none} =
             Jizoku.execute_next(owner_id: "node-b", now: after_original_lease)

    assert {:ok,
            %Snapshot{
              reason: :attempt_claimed,
              expired_claims: [],
              attempts: [
                %{status: :claimed, owner_id: "node-a", claim_id: ^node_a_claim_id}
              ]
            }} = Jizoku.inspect_run(run.run_id, now: after_original_lease)

    finish_probe(node_a_pid, node_a_claim_id, success("heartbeat"))

    assert {:ok, %Snapshot{status: :completed} = completed} = Task.await(node_a, 5_000)
    assert completed.applied_runnable_keys == completed.planned_runnable_keys
    assert [%{status: :completed, applied?: true}] = completed.attempts

    entries = dispatch_entries!()
    assert count_entries(entries, :attempt_claimed) == 1
    assert count_entries(entries, :attempt_completed) == 1
    refute_receive {:probe_started, _pid, _claim_id, "heartbeat"}, 50
  end

  test "an expired claim is taken over and stale completion is rejected", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("stale-completion")

    node_a = execute_async(task_supervisor, "node-a", lease_for: 1)

    assert_receive {:probe_started, node_a_pid, node_a_claim_id, "stale-completion"}, 1_000

    claimed =
      :attempt_claimed
      |> await_dispatch_entry()
      |> find_entry!(:attempt_claimed, node_a_claim_id)

    takeover_at = DateTime.add(claimed.data.lease_until, 1, :second)

    assert_expired_claim_evidence(run.run_id, node_a_claim_id, takeover_at)

    node_b = execute_async(task_supervisor, "node-b", now: takeover_at, lease_for: 30)

    assert_receive {:probe_started, node_b_pid, node_b_claim_id, "stale-completion"}, 1_000
    refute node_b_claim_id == node_a_claim_id

    finish_probe(node_a_pid, node_a_claim_id, success("stale-completion-old"))
    assert {:error, :stale_claim} = Task.await(node_a, 5_000)

    assert_current_owner(run.run_id, node_b_claim_id, takeover_at)

    finish_probe(node_b_pid, node_b_claim_id, success("stale-completion"))
    assert {:ok, %Snapshot{status: :completed}} = Task.await(node_b, 5_000)

    entries = dispatch_entries!()
    assert count_entries(entries, :attempt_claimed) == 2
    assert count_entries(entries, :attempt_completed) == 1
    assert count_entries(entries, :attempt_failed) == 0

    run_entries = run_entries!(run.run_id)
    assert count_entries(run_entries, :runnable_applied) == 1
    assert count_entries(run_entries, :run_terminal) == 1

    assert %{probe: %{id: "stale-completion", status: "completed"}} =
             find_entry!(run_entries, :runnable_applied).data.result
  end

  test "an expired claim is taken over and stale failure is rejected", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("stale-failure")

    node_a = execute_async(task_supervisor, "node-a", lease_for: 1)

    assert_receive {:probe_started, node_a_pid, node_a_claim_id, "stale-failure"}, 1_000

    claimed =
      :attempt_claimed
      |> await_dispatch_entry()
      |> find_entry!(:attempt_claimed, node_a_claim_id)

    takeover_at = DateTime.add(claimed.data.lease_until, 1, :second)
    node_b = execute_async(task_supervisor, "node-b", now: takeover_at, lease_for: 30)

    assert_receive {:probe_started, node_b_pid, node_b_claim_id, "stale-failure"}, 1_000

    finish_probe(node_a_pid, node_a_claim_id, failure("stale-owner-failure"))
    assert {:error, :stale_claim} = Task.await(node_a, 5_000)

    assert_current_owner(run.run_id, node_b_claim_id, takeover_at)

    finish_probe(node_b_pid, node_b_claim_id, success("stale-failure"))
    assert {:ok, %Snapshot{status: :completed}} = Task.await(node_b, 5_000)

    entries = dispatch_entries!()
    assert count_entries(entries, :attempt_completed) == 1
    assert count_entries(entries, :attempt_failed) == 0
  end

  test "cancellation fences a claimed worker before it can complete", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("cancellation")
    node_a = execute_async(task_supervisor, "node-a", lease_for: 30)

    assert_receive {:probe_started, node_a_pid, node_a_claim_id, "cancellation"}, 1_000

    assert {:ok, %Snapshot{status: :cancelled, terminal?: true}} =
             Jizoku.cancel(run.run_id)

    finish_probe(node_a_pid, node_a_claim_id, success("cancellation"))
    assert {:error, :terminal_run} = Task.await(node_a, 5_000)

    assert {:ok, :none} = Jizoku.execute_next(owner_id: "node-b")

    assert {:ok,
            %Diagnostic{
              reason: :terminal,
              next_actions: [:inspect_terminal_run],
              evidence: %{terminal_status: :cancelled}
            }} = Jizoku.explain_run(run.run_id)

    entries = dispatch_entries!()
    assert count_entries(entries, :attempt_completed) == 0
    assert count_entries(entries, :attempt_failed) == 0

    run_entries = run_entries!(run.run_id)
    assert count_entries(run_entries, :runnable_applied) == 0
    assert count_entries(run_entries, :run_terminal) == 1
  end

  test "terminal failure fences a competing node from later work", %{
    task_supervisor: task_supervisor
  } do
    run = start_probe_run!("terminal-failure")
    node_a = execute_async(task_supervisor, "node-a", lease_for: 30)

    assert_receive {:probe_started, node_a_pid, node_a_claim_id, "terminal-failure"}, 1_000
    assert {:ok, :none} = Jizoku.execute_next(owner_id: "node-b")

    finish_probe(node_a_pid, node_a_claim_id, failure("terminal-failure"))

    assert {:ok, %Snapshot{status: :failed, terminal?: true}} = Task.await(node_a, 5_000)
    assert {:ok, :none} = Jizoku.execute_next(owner_id: "node-b")

    assert {:ok,
            %Diagnostic{
              reason: :terminal,
              next_actions: [:inspect_terminal_run],
              evidence: %{terminal_status: :failed}
            }} = Jizoku.explain_run(run.run_id)

    entries = dispatch_entries!()
    assert count_entries(entries, :attempt_claimed) == 1
    assert count_entries(entries, :attempt_failed) == 1
    assert count_entries(entries, :attempt_completed) == 0
  end

  defp start_probe_run!(probe_id) do
    assert {:ok, %Snapshot{} = run} = Jizoku.start(Workflow, %{probe_id: probe_id})
    assert {:ok, _owner} = Registry.register(@controller_registry, run.run_id, nil)
    run
  end

  defp execute_async(task_supervisor, owner_id, opts) do
    Task.Supervisor.async_nolink(task_supervisor, fn ->
      Jizoku.execute_next(Keyword.put(opts, :owner_id, owner_id))
    end)
  end

  defp execute_gated_async(task_supervisor, owner_id, opts) do
    parent = self()

    Task.Supervisor.async_nolink(task_supervisor, fn ->
      send(parent, {:worker_ready, self()})

      receive do
        :drain -> Jizoku.execute_next(Keyword.put(opts, :owner_id, owner_id))
      end
    end)
  end

  defp finish_probe(pid, claim_id, outcome) do
    send(pid, {:finish_probe, claim_id, outcome})
  end

  defp success(probe_id), do: {:ok, %{probe: %{id: probe_id, status: "completed"}}}

  defp failure(code) do
    {:error, %{code: code, message: "multi-node probe failed", retryable?: false}}
  end

  defp assert_expired_claim_evidence(run_id, claim_id, takeover_at) do
    assert {:ok,
            %Snapshot{
              reason: :expired_claim,
              expired_claims: [
                %{status: :claimed, owner_id: "node-a", claim_id: ^claim_id}
              ]
            }} = Jizoku.inspect_run(run_id, now: takeover_at)

    assert {:ok,
            %Diagnostic{
              reason: :expired_claim,
              next_actions: [:recover_expired_claim],
              details: %{expired_claim_count: 1, oldest_lease_until: %DateTime{}}
            }} = Jizoku.explain_run(run_id, now: takeover_at)
  end

  defp assert_current_owner(run_id, claim_id, at) do
    assert {:ok,
            %Snapshot{
              reason: :attempt_claimed,
              expired_claims: [],
              attempts: [%{status: :claimed, owner_id: "node-b", claim_id: ^claim_id}]
            }} = Jizoku.inspect_run(run_id, now: at)
  end

  defp await_dispatch_entry(type, attempts_left \\ 100)

  defp await_dispatch_entry(type, attempts_left) when attempts_left > 0 do
    entries = dispatch_entries!()

    if Enum.any?(entries, &(&1.type == type)) do
      entries
    else
      receive do
      after
        10 -> await_dispatch_entry(type, attempts_left - 1)
      end
    end
  end

  defp await_dispatch_entry(type, 0) do
    flunk("timed out waiting for #{inspect(type)} dispatch entry")
  end

  defp dispatch_entries! do
    storage = {JournalStorage, repo: Repo}
    queue = Application.fetch_env!(:jizoku, :queue)
    assert {:ok, entries} = Journal.load_entries(storage, {:dispatch, queue})
    entries
  end

  defp run_entries!(run_id) do
    storage = {JournalStorage, repo: Repo}
    assert {:ok, entries} = Journal.load_entries(storage, {:run, run_id})
    entries
  end

  defp valid_concurrent_result?({:ok, :none}, _run_id), do: true
  defp valid_concurrent_result?({:error, :conflict}, _run_id), do: true

  defp valid_concurrent_result?({:ok, %Snapshot{run_id: run_id}}, expected_run_id) do
    run_id == expected_run_id
  end

  defp valid_concurrent_result?(_result, _run_id), do: false

  defp find_entry!(entries, type, claim_id) do
    Enum.find(entries, fn entry ->
      entry.type == type and Map.get(entry.data, :claim_id) == claim_id
    end) || flunk("missing #{inspect(type)} entry for claim #{inspect(claim_id)}")
  end

  defp find_entry!(entries, type) do
    Enum.find(entries, &(&1.type == type)) || flunk("missing #{inspect(type)} entry")
  end

  defp count_entries(entries, type), do: Enum.count(entries, &(&1.type == type))
end
