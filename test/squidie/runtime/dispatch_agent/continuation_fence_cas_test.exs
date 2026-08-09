defmodule Squidie.Runtime.DispatchAgent.ContinuationFenceCASTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Journal

  defmodule CASBarrierStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.get_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.put_checkpoint(key, data, delegate_opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.load_thread(thread_id, delegate_opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      wait_at_barrier(thread_id, entries, opts)
      {adapter, delegate_opts} = delegate(opts)

      adapter.append_thread(
        thread_id,
        entries,
        Keyword.merge(delegate_opts, Keyword.take(opts, [:expected_rev]))
      )
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp wait_at_barrier(thread_id, entries, opts) do
      target = Keyword.fetch!(opts, :barrier_thread_id)
      ref = Keyword.fetch!(opts, :barrier_ref)

      kind =
        entries
        |> List.first()
        |> Map.fetch!(:kind)

      key = {__MODULE__, ref, kind}

      if thread_id == target and kind in Keyword.fetch!(opts, :barrier_kinds) and
           is_nil(Process.get(key)) do
        Process.put(key, true)
        send(Keyword.fetch!(opts, :test_pid), {:append_blocked, ref, kind, self()})

        receive do
          {:append_release, ^ref} -> :ok
        after
          5_000 -> raise "append barrier timed out"
        end
      end
    end

    defp delegate(opts) do
      case Keyword.fetch!(opts, :delegate) do
        {adapter, delegate_opts} -> {adapter, delegate_opts}
        adapter when is_atom(adapter) -> {adapter, []}
      end
    end
  end

  @storage {ETS, table: :squidie_continuation_fence_cas_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @other_run_id "33333333-3333-5333-8333-333333333333"
  @runnable_key "#{@run_id}:monitor:1"
  @now ~U[2026-08-09 16:00:00Z]
  @lease_until ~U[2026-08-09 16:05:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "persists a quiescent continuation fence and reuses it idempotently" do
    agent = queued_agent()

    assert {:ok, %{agent: fenced_agent, fence: fence, created?: true}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert fence.run_id == @run_id
    assert fence.queue == "default"
    assert fenced_agent.state.thread_rev == agent.state.thread_rev + 1
    before_duplicate = dispatch_entries()

    assert {:ok, %{agent: ^fenced_agent, fence: ^fence, created?: false}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               fenced_agent,
               continuation_fence(trace: %{@trace | span_id: "b7ad6b7169203331"}),
               now: DateTime.add(@now, 1, :second)
             )

    assert dispatch_entries() == before_duplicate
  end

  test "reuses the first fence after checkpoint restart" do
    agent = queued_agent()

    assert {:ok, %{agent: fenced_agent, fence: fence}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert :ok = DispatchAgent.put_checkpoint(@storage, fenced_agent)
    assert {:ok, rebuilt_agent} = DispatchAgent.rebuild(@storage, "default")
    before_duplicate = dispatch_entries()

    assert {:ok, %{fence: ^fence, created?: false}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               rebuilt_agent,
               continuation_fence(trace: %{@trace | span_id: "b7ad6b7169203331"}),
               now: DateTime.add(@now, 1, :second)
             )

    assert dispatch_entries() == before_duplicate
  end

  test "rejects conflicting fence identity without writing" do
    agent = queued_agent()

    assert {:ok, %{agent: fenced_agent}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    before_conflict = dispatch_entries()

    assert {:error, :conflicting_continuation_fence} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               fenced_agent,
               continuation_fence(input: %{cursor: "page-99"}),
               now: @now
             )

    assert dispatch_entries() == before_conflict
  end

  test "rejects malformed fence data before writing" do
    agent = queued_agent()
    before_fence = dispatch_entries()

    assert {:error, {:invalid_continuation_fence, :invalid}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(input: "page-42"),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "rejects a successor that reuses the predecessor run id before writing" do
    agent = queued_agent()
    before_fence = dispatch_entries()

    assert {:error, {:invalid_continuation_fence, :invalid}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(successor_run_id: @run_id),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "rejects caller-supplied queue metadata before writing" do
    agent = queued_agent()
    before_fence = dispatch_entries()

    assert {:error, {:invalid_continuation_fence, :invalid}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               Map.put(continuation_fence(), :queue, "priority"),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "preserves invalid lifecycle time errors without writing" do
    agent = queued_agent()
    before_fence = dispatch_entries()

    assert {:error, {:invalid_option, :now}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: "tomorrow"
             )

    assert dispatch_entries() == before_fence
  end

  test "rejects every unsettled attempt state before writing" do
    scenarios = [
      {[%{reason: :available_attempt, runnable_key: @runnable_key}],
       [entry!(:attempt_scheduled, scheduled_attrs())]},
      {[%{reason: :claimed_attempt, runnable_key: @runnable_key}],
       [
         entry!(:attempt_scheduled, scheduled_attrs()),
         entry!(:attempt_claimed, claimed_attrs())
       ]},
      {[%{reason: :pending_result, runnable_key: @runnable_key}],
       [
         entry!(:attempt_scheduled, scheduled_attrs()),
         entry!(:attempt_claimed, claimed_attrs()),
         entry!(:attempt_completed, completed_attrs())
       ]},
      {[%{reason: :pending_result, runnable_key: @runnable_key}],
       [
         entry!(:attempt_scheduled, scheduled_attrs()),
         entry!(:attempt_claimed, claimed_attrs()),
         entry!(:attempt_failed, failed_attrs())
       ]},
      {[
         %{reason: :pending_result, runnable_key: @runnable_key},
         %{reason: :retry_attempt, runnable_key: "#{@run_id}:monitor:2"}
       ],
       [
         entry!(:attempt_scheduled, scheduled_attrs()),
         entry!(:attempt_claimed, claimed_attrs()),
         entry!(
           :attempt_failed,
           failed_attrs(
             retry_runnable_key: "#{@run_id}:monitor:2",
             retry_visible_at: @now
           )
         )
       ]}
    ]

    for {expected_blockers, entries} <- scenarios do
      cleanup_storage()
      assert {:ok, _thread} = Journal.append_entries(@storage, entries)
      assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
      before_fence = dispatch_entries()

      assert {:error, {:unsafe_continuation, ^expected_blockers}} =
               DispatchAgent.fence_run_for_continuation(
                 @storage,
                 agent,
                 continuation_fence(),
                 now: @now
               )

      assert dispatch_entries() == before_fence
    end
  end

  test "permits completed and failed attempts already applied to the run" do
    scenarios = [
      {[entry!(:attempt_completed, completed_attrs())], %{}},
      {[entry!(:attempt_failed, failed_attrs())], %{transition: %{kind: :failed}}}
    ]

    for {result_entries, applied_attrs} <- scenarios do
      cleanup_storage()

      assert {:ok, _thread} =
               Journal.append_entries(
                 @storage,
                 [
                   entry!(:attempt_scheduled, scheduled_attrs()),
                   entry!(:attempt_claimed, claimed_attrs())
                 ] ++ result_entries
               )

      assert {:ok, _thread} = Journal.append_entries(@storage, [applied_entry(applied_attrs)])
      assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

      assert {:ok, %{created?: true}} =
               DispatchAgent.fence_run_for_continuation(
                 @storage,
                 agent,
                 continuation_fence(),
                 now: @now
               )
    end
  end

  test "rejects a terminal run before writing" do
    agent = queued_agent()

    assert {:ok, terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :completed,
               occurred_at: @now
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [terminal])
    before_fence = dispatch_entries()

    expected_error = {:unsafe_continuation, [%{reason: :terminal_run}]}

    assert {:error, ^expected_error} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "blocks an integrity anomaly belonging to the target run" do
    conflicting_attrs = scheduled_attrs(idempotency_key: "conflicting-monitor")

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs()),
               entry!(:attempt_scheduled, conflicting_attrs),
               entry!(:attempt_claimed, claimed_attrs()),
               entry!(:attempt_completed, completed_attrs())
             ])

    assert {:ok, _thread} = Journal.append_entries(@storage, [applied_entry()])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    before_fence = dispatch_entries()

    assert {:error,
            {:unsafe_continuation,
             [%{reason: :dispatch_anomaly, anomaly: %{reason: :conflicting_runnable_intent}}]}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "blocks the incoming run in a cross-run runnable-key collision" do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs()),
               entry!(:attempt_scheduled, scheduled_attrs(run_id: @other_run_id))
             ])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    before_fence = dispatch_entries()

    assert {:error,
            {:unsafe_continuation,
             [
               %{
                 reason: :dispatch_anomaly,
                 anomaly: %{reason: :conflicting_runnable_intent, run_id: @other_run_id}
               }
             ]}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(run_id: @other_run_id),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "blocks the incoming run when its retry key collides with another run" do
    shared_retry_key = "shared-retry:1"
    source_key = "#{@other_run_id}:source:1"

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(
                 :attempt_scheduled,
                 scheduled_attrs(
                   runnable_key: shared_retry_key,
                   idempotency_key: "existing-owner"
                 )
               ),
               entry!(
                 :attempt_scheduled,
                 scheduled_attrs(
                   run_id: @other_run_id,
                   runnable_key: source_key,
                   idempotency_key: "retry-source"
                 )
               ),
               entry!(
                 :attempt_claimed,
                 Map.merge(claimed_attrs(), %{run_id: @other_run_id, runnable_key: source_key})
               ),
               entry!(
                 :attempt_failed,
                 failed_attrs(
                   run_id: @other_run_id,
                   runnable_key: source_key,
                   retry_runnable_key: shared_retry_key,
                   retry_visible_at: @now
                 )
               )
             ])

    assert {:ok, _thread} =
             Journal.append_entries(
               @storage,
               [
                 applied_entry(%{
                   run_id: @other_run_id,
                   runnable_key: source_key,
                   transition: %{kind: :failed}
                 })
               ]
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    before_fence = dispatch_entries()

    assert {:error,
            {:unsafe_continuation,
             [
               %{
                 reason: :dispatch_anomaly,
                 anomaly: %{reason: :conflicting_runnable_intent, run_id: @other_run_id}
               }
             ]}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(run_id: @other_run_id),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "blocks a malformed target-run dispatch fact" do
    malformed = %Entry{
      type: :run_continuation_fenced,
      thread: {:dispatch, "default"},
      data:
        Map.merge(continuation_fence(), %{
          input: "bad-input",
          queue: "default",
          occurred_at: @now
        }),
      occurred_at: @now
    }

    assert {:ok, _thread} = Journal.append_entries(@storage, [malformed])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    before_fence = dispatch_entries()

    assert {:error,
            {:unsafe_continuation,
             [%{reason: :dispatch_anomaly, anomaly: %{reason: :malformed_entry}}]}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert dispatch_entries() == before_fence
  end

  test "blocks claim result and application facts whose scheduled intent is missing" do
    scenarios = [
      {:attempt_claimed, entry!(:attempt_claimed, claimed_attrs())},
      {:attempt_completed, entry!(:attempt_completed, completed_attrs())},
      {:attempt_failed, entry!(:attempt_failed, failed_attrs())},
      {:runnable_applied, applied_entry()}
    ]

    for {entry_type, entry} <- scenarios do
      cleanup_storage()

      if entry_type == :runnable_applied do
        queued_agent()
      end

      assert {:ok, _thread} = Journal.append_entries(@storage, [entry])
      assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
      before_fence = dispatch_entries()

      assert {:error,
              {:unsafe_continuation,
               [
                 %{
                   reason: :dispatch_anomaly,
                   anomaly: %{reason: :unknown_runnable_intent, entry_type: ^entry_type}
                 }
               ]}} =
               DispatchAgent.fence_run_for_continuation(
                 @storage,
                 agent,
                 continuation_fence(),
                 now: @now
               )

      assert dispatch_entries() == before_fence
    end
  end

  test "does not treat an unknown heartbeat as missing executable intent" do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [entry!(:attempt_heartbeat, heartbeat_attrs())])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{created?: true}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )
  end

  test "does not permanently block on a rejected stale lifecycle fact" do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs()),
               entry!(:attempt_claimed, claimed_attrs()),
               entry!(:attempt_completed, completed_attrs()),
               entry!(:attempt_heartbeat, heartbeat_attrs())
             ])

    assert {:ok, _thread} = Journal.append_entries(@storage, [applied_entry()])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{created?: true}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )
  end

  test "ignores anomalies belonging to another run in the shared queue" do
    other_key = "#{@other_run_id}:monitor:1"

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(
                 :attempt_scheduled,
                 scheduled_attrs(
                   run_id: @other_run_id,
                   runnable_key: other_key,
                   idempotency_key: "other-monitor"
                 )
               ),
               entry!(
                 :attempt_scheduled,
                 scheduled_attrs(
                   run_id: @other_run_id,
                   runnable_key: other_key,
                   idempotency_key: "conflicting-other-monitor"
                 )
               )
             ])

    agent = queued_agent()

    assert {:ok, %{created?: true}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )
  end

  test "derives and writes a non-default queue only" do
    agent = queued_agent("priority")

    assert {:ok, %{fence: %{queue: "priority"}, created?: true}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert Enum.map(dispatch_entries(@storage, "priority"), & &1.type) == [
             :run_queued,
             :run_continuation_fenced
           ]

    assert dispatch_entries() == []
  end

  test "rejects a mismatched storage partition without writing either partition" do
    agent = queued_agent()
    partitioned_storage = partitioned_storage("tenant-a")
    before_default = dispatch_entries()
    before_partition = dispatch_entries(partitioned_storage)

    assert {:error, {:partition_mismatch, :dispatch_agent}} =
             DispatchAgent.fence_run_for_continuation(
               partitioned_storage,
               agent,
               continuation_fence(),
               now: @now
             )

    assert dispatch_entries() == before_default
    assert dispatch_entries(partitioned_storage) == before_partition
  end

  test "fence wins the shared revision before scheduling" do
    assert_race(:fence)
  end

  test "scheduling wins the shared revision before the fence" do
    assert_race(:schedule)
  end

  test "complete rejects a rebuilt fenced claim before writing" do
    {agent, claim_id, claim_token, before_call} = claimed_then_forced_fence()

    assert {:error, :continuation_fenced} =
             DispatchAgent.complete(
               @storage,
               agent,
               @runnable_key,
               claim_id,
               claim_token,
               %{cursor: "page-42"},
               now: @now
             )

    assert dispatch_entries() == before_call
  end

  test "exact completed-result retry remains a no-write success after fencing" do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [entry!(:attempt_scheduled, scheduled_attrs())])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker-1",
               now: @now,
               lease_for: 300,
               claim_id: "claim-1",
               claim_token: "claim-token-1"
             )

    result = %{cursor: "page-42"}

    assert {:ok, %{agent: completed_agent}} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               result,
               now: @now
             )

    assert {:ok, _thread} = Journal.append_entries(@storage, [applied_entry()])
    assert {:ok, applied_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: fenced_agent}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               applied_agent,
               continuation_fence(),
               now: @now
             )

    before_retry = dispatch_entries()

    assert {:ok, %{agent: ^fenced_agent}} =
             DispatchAgent.complete(
               @storage,
               fenced_agent,
               @runnable_key,
               claim_id,
               claim_token,
               result,
               now: @now
             )

    assert completed_agent.state.thread_rev < fenced_agent.state.thread_rev
    assert dispatch_entries() == before_retry
  end

  test "failure rejects a rebuilt fenced claim before writing or retrying" do
    {agent, claim_id, claim_token, before_call} = claimed_then_forced_fence()

    assert {:error, :continuation_fenced} =
             DispatchAgent.fail(
               @storage,
               agent,
               @runnable_key,
               claim_id,
               claim_token,
               %{reason: "failed"},
               now: @now,
               retry_runnable_key: "#{@run_id}:monitor:2",
               retry_visible_at: @now,
               retry_trace: %{@trace | span_id: "b7ad6b7169203331"}
             )

    assert dispatch_entries() == before_call
  end

  test "heartbeat rejects a rebuilt fenced claim before writing" do
    {agent, claim_id, claim_token, before_call} = claimed_then_forced_fence()

    assert {:error, :continuation_fenced} =
             DispatchAgent.heartbeat(
               @storage,
               agent,
               @runnable_key,
               claim_id,
               claim_token,
               now: @now,
               lease_for: 60
             )

    assert dispatch_entries() == before_call
  end

  test "scheduling rejects a rebuilt fenced run before writing" do
    agent = fenced_agent()
    before_call = dispatch_entries()

    assert {:error, :continuation_fenced} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [scheduled_runnable()],
               now: @now
             )

    assert dispatch_entries() == before_call
  end

  test "exact scheduling retry remains a no-write success after fencing" do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs()),
               forced_fence_entry()
             ])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    before_call = dispatch_entries()

    assert {:ok, %{runnables: []}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [scheduled_runnable()],
               now: @now
             )

    assert dispatch_entries() == before_call
  end

  test "queue repair remains a no-write success after fencing" do
    agent = fenced_agent()
    before_call = dispatch_entries()

    assert {:ok, %{queued?: false}} =
             DispatchAgent.ensure_run_queued(@storage, agent, @run_id, now: @now)

    assert dispatch_entries() == before_call
  end

  defp assert_race(winner) do
    agent = queued_agent()
    ref = make_ref()

    barrier_storage =
      {CASBarrierStorage,
       delegate: @storage,
       barrier_thread_id: Journal.thread_id({:dispatch, "default"}),
       barrier_ref: ref,
       barrier_kinds: [:run_continuation_fenced, :attempt_scheduled],
       test_pid: self()}

    fence_task =
      Task.async(fn ->
        DispatchAgent.fence_run_for_continuation(
          barrier_storage,
          agent,
          continuation_fence(),
          now: @now
        )
      end)

    schedule_task =
      Task.async(fn ->
        DispatchAgent.schedule_attempts(
          barrier_storage,
          agent,
          @run_id,
          [scheduled_runnable()],
          now: @now
        )
      end)

    assert_receive {:append_blocked, ^ref, :run_continuation_fenced, fence_pid}
    assert_receive {:append_blocked, ^ref, :attempt_scheduled, schedule_pid}
    assert fence_pid == fence_task.pid
    assert schedule_pid == schedule_task.pid

    {winner_task, winner_pid, loser_task, loser_pid, winner_kind} =
      case winner do
        :fence -> {fence_task, fence_pid, schedule_task, schedule_pid, :run_continuation_fenced}
        :schedule -> {schedule_task, schedule_pid, fence_task, fence_pid, :attempt_scheduled}
      end

    send(winner_pid, {:append_release, ref})
    assert {:ok, _update} = Task.await(winner_task)
    send(loser_pid, {:append_release, ref})
    assert {:error, :conflict} = Task.await(loser_task)

    assert Enum.map(dispatch_entries(), & &1.type) == [:run_queued, winner_kind]
  end

  defp queued_agent(queue \\ "default") do
    assert {:ok, queued} =
             DispatchProtocol.new_entry(:run_queued, %{
               run_id: @run_id,
               queue: queue,
               occurred_at: @now
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [queued])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, queue)
    agent
  end

  defp claimed_then_forced_fence do
    assert {:ok, _thread} =
             Journal.append_entries(@storage, [entry!(:attempt_scheduled, scheduled_attrs())])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker-1",
               now: @now,
               lease_for: 300,
               claim_id: "claim-1",
               claim_token: "claim-token-1"
             )

    assert {:ok, _thread} = Journal.append_entries(@storage, [forced_fence_entry()])
    assert {:ok, fenced_agent} = DispatchAgent.rebuild(@storage, "default")
    {fenced_agent, claim_id, claim_token, dispatch_entries()}
  end

  defp fenced_agent do
    agent = queued_agent()

    assert {:ok, %{agent: fenced_agent}} =
             DispatchAgent.fence_run_for_continuation(
               @storage,
               agent,
               continuation_fence(),
               now: @now
             )

    fenced_agent
  end

  defp continuation_fence(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        successor_run_id: "22222222-2222-5222-8222-222222222222",
        continuation_key: "page-42",
        workflow: "MonitoringWorkflow",
        trigger: "continue",
        input: %{cursor: "page-42"},
        definition: :current,
        definition_version: nil,
        definition_fingerprint: "definition-fingerprint-v1",
        trace: @trace
      },
      Map.new(attrs)
    )
  end

  defp scheduled_runnable do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      idempotency_key: "monitor-page-42",
      attempt_number: 1,
      step: "monitor",
      input: %{cursor: "page-42"},
      queue: "default",
      visible_at: @now
    }
  end

  defp scheduled_attrs(attrs \\ []) do
    scheduled_runnable()
    |> Map.put(:occurred_at, @now)
    |> Map.merge(Map.new(attrs))
  end

  defp claimed_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim-1",
      claim_token_hash: "claim-token-hash",
      owner_id: "worker-1",
      queue: "default",
      lease_until: @lease_until,
      occurred_at: @now
    }
  end

  defp completed_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim-1",
      claim_token_hash: "claim-token-hash",
      queue: "default",
      result: %{cursor: "page-42"},
      occurred_at: @now
    }
  end

  defp heartbeat_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim-1",
      claim_token_hash: "claim-token-hash",
      queue: "default",
      lease_until: @lease_until,
      occurred_at: @now
    }
  end

  defp failed_attrs(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: "claim-1",
        claim_token_hash: "claim-token-hash",
        queue: "default",
        error: %{reason: "failed"},
        occurred_at: @now
      },
      Map.new(attrs)
    )
  end

  defp applied_entry(attrs \\ %{}) do
    entry!(
      :runnable_applied,
      Map.merge(
        %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          occurred_at: @now
        },
        attrs
      )
    )
  end

  defp forced_fence_entry do
    entry!(
      :run_continuation_fenced,
      Map.merge(continuation_fence(), %{queue: "default", occurred_at: @now})
    )
  end

  defp partitioned_storage(partition) do
    assert {:ok, storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, partition)

    storage
  end

  defp dispatch_entries(storage \\ @storage, queue \\ "default") do
    case Journal.load_entries(storage, {:dispatch, queue}) do
      {:ok, entries} -> entries
      {:error, :not_found} -> []
    end
  end

  defp cleanup_storage do
    for table <- [
          :squidie_continuation_fence_cas_test_checkpoints,
          :squidie_continuation_fence_cas_test_threads,
          :squidie_continuation_fence_cas_test_thread_meta
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end
  rescue
    ArgumentError -> :ok
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
