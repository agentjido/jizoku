defmodule Squidie.Runtime.Journal.NativeContinuationRecoveryTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.ContinuationRecovery
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.ContinuationIntent
  alias Squidie.Runtime.WorkflowAgent

  defmodule RecoveryStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      delegate(:get_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      delegate(:put_checkpoint, [key, data], opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      delegate(:delete_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      delegate(:load_thread, [thread_id], opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      kinds = Enum.map(entries, & &1.kind)
      failure_key = {__MODULE__, Keyword.fetch!(opts, :failure_ref)}

      case {kinds, Keyword.fetch!(opts, :mode), Process.get(failure_key)} do
        {[:runnable_applied, :run_continuation_requested, :run_terminal], :fail_before, nil} ->
          Process.put(failure_key, true)
          {:error, :injected_native_run_failure}

        {[:runnable_applied, :run_continuation_requested, :run_terminal], :commit_then_conflict,
         nil} ->
          Process.put(failure_key, true)
          assert_delegate_then_conflict(thread_id, entries, opts)

        {[:runnable_applied], :fail_source_apply, nil} ->
          Process.put(failure_key, true)
          {:error, :injected_source_apply_failure}

        {[:runnable_applied], :commit_source_then_conflict, nil} ->
          Process.put(failure_key, true)
          assert_delegate_then_conflict(thread_id, entries, opts)

        {[:run_continuation_aborted], :fail_abort, nil} ->
          Process.put(failure_key, true)
          {:error, :injected_abort_failure}

        _other ->
          delegate(:append_thread, [thread_id, entries], opts)
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      delegate(:delete_thread, [thread_id], opts)
    end

    defp assert_delegate_then_conflict(thread_id, entries, opts) do
      case delegate(:append_thread, [thread_id, entries], opts) do
        {:ok, _thread} -> {:error, :conflict}
        {:error, _reason} = error -> error
      end
    end

    defp delegate(callback, args, opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      apply(
        adapter,
        callback,
        Enum.concat(args, [delegate_opts ++ Keyword.take(opts, [:expected_rev])])
      )
    end
  end

  defmodule AdvancePage do
    use Squidie.Step, name: :advance_page

    @impl Squidie.Step
    def run(%{cursor: cursor}, _context) do
      {:ok, %{cursor: cursor + 1}}
    end
  end

  defmodule PagingWorkflow do
    use Squidie.Workflow

    workflow do
      version "2026-08-09.native-recovery"

      trigger :page do
        manual()

        payload do
          field :cursor, :integer
        end
      end

      step :advance_page, AdvancePage
    end
  end

  @storage {ETS, table: :squidie_native_continuation_recovery_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @now ~U[2026-08-09 23:00:00Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "repairs the native source and predecessor in one ordered run append" do
    %{intent: intent} = seed_native_fence()

    assert {:ok, {:repaired, %{successor: successor}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert successor.run_id == intent.successor_run_id
    assert successor.input == %{cursor: 1}

    assert Enum.take(run_types(@run_id), -3) == [
             :runnable_applied,
             :run_continuation_requested,
             :run_terminal
           ]

    assert terminal_entry(@run_id).data.status == :continued
    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1

    before_retry = journal_state(intent.successor_run_id)

    assert {:ok, {:repaired, %{successor: same_successor}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: DateTime.add(@now, 1, :second)
             )

    assert same_successor.run_id == successor.run_id
    assert journal_state(intent.successor_run_id) == before_retry
  end

  test "a failed predecessor append leaves the native fence retryable" do
    %{intent: intent} = seed_native_fence()
    fault_storage = recovery_storage(:fail_before)

    assert {:error, :injected_native_run_failure} =
             ContinuationRecovery.resolve_fenced_run(
               fault_storage,
               @run_id,
               "default",
               now: @now
             )

    refute :runnable_applied in run_types(@run_id)
    refute :run_continuation_requested in run_types(@run_id)
    assert Journal.load_entries(@storage, {:run, intent.successor_run_id}) == {:error, :not_found}

    assert {:ok, {:repaired, %{successor: successor}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: DateTime.add(@now, 1, :second)
             )

    assert successor.run_id == intent.successor_run_id
    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
  end

  test "an unknown successful predecessor append converges without duplicate facts" do
    %{intent: intent} = seed_native_fence()
    fault_storage = recovery_storage(:commit_then_conflict)

    assert {:ok, {:repaired, %{successor: successor}}} =
             ContinuationRecovery.resolve_fenced_run(
               fault_storage,
               @run_id,
               "default",
               now: @now
             )

    assert successor.run_id == intent.successor_run_id
    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
    assert Enum.count(run_types(@run_id), &(&1 == :run_continuation_requested)) == 1
    assert Enum.count(run_types(@run_id), &(&1 == :run_terminal)) == 1
  end

  test "a dynamic winner consumes the native source before aborting" do
    %{intent: intent} = seed_native_fence()
    append_dynamic_work()

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    predecessor_types = run_types(@run_id)
    assert Enum.count(predecessor_types, &(&1 == :runnable_applied)) == 1
    assert :dynamic_work_recorded in predecessor_types
    refute :run_continuation_requested in predecessor_types
    refute :run_terminal in predecessor_types

    assert :run_continuation_aborted in dispatch_types()
    assert Journal.load_entries(@storage, {:run, intent.successor_run_id}) == {:error, :not_found}
  end

  test "a directive mismatch remains fenced without mutating either run" do
    mismatched_request = %{
      input: %{cursor: 1},
      continuation_key: "other-page",
      definition: :current
    }

    %{intent: intent} = seed_native_fence(execution_request: mismatched_request)
    before_repair = journal_state(intent.successor_run_id)

    assert {:error, {:unsafe_continuation, :source_directive_mismatch}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state(intent.successor_run_id) == before_repair
    refute :runnable_applied in run_types(@run_id)
    refute :run_continuation_requested in run_types(@run_id)
  end

  test "a missing durable source attempt fails closed without mutation" do
    %{intent: intent} = seed_missing_source_fence()
    append_dynamic_work()
    before_repair = journal_state(intent.successor_run_id)

    assert {:error, {:invalid_continuation, :missing_source_attempt}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state(intent.successor_run_id) == before_repair
    refute :runnable_applied in run_types(@run_id)
    refute :run_continuation_aborted in dispatch_types()
  end

  test "a source-application failure remains fenced and retryable" do
    %{intent: intent} = seed_native_fence()
    append_dynamic_work()

    assert {:error, :injected_source_apply_failure} =
             ContinuationRecovery.resolve_fenced_run(
               recovery_storage(:fail_source_apply),
               @run_id,
               "default",
               now: @now
             )

    refute :runnable_applied in run_types(@run_id)
    refute :run_continuation_aborted in dispatch_types()

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: DateTime.add(@now, 1, :second)
             )

    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
    assert Journal.load_entries(@storage, {:run, intent.successor_run_id}) == {:error, :not_found}
  end

  test "an unknown successful source application converges before abort" do
    seed_native_fence()
    append_dynamic_work()

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}}}} =
             ContinuationRecovery.resolve_fenced_run(
               recovery_storage(:commit_source_then_conflict),
               @run_id,
               "default",
               now: @now
             )

    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
    assert Enum.count(dispatch_types(), &(&1 == :run_continuation_aborted)) == 1
  end

  test "a crash after source application retries the abort without duplicate application" do
    seed_native_fence()
    append_dynamic_work()

    assert {:error, :injected_abort_failure} =
             ContinuationRecovery.resolve_fenced_run(
               recovery_storage(:fail_abort),
               @run_id,
               "default",
               now: @now
             )

    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
    refute :run_continuation_aborted in dispatch_types()

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: DateTime.add(@now, 1, :second)
             )

    assert Enum.count(run_types(@run_id), &(&1 == :runnable_applied)) == 1
    assert Enum.count(dispatch_types(), &(&1 == :run_continuation_aborted)) == 1
  end

  defp seed_native_fence(opts \\ []) do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, claim} =
             DispatchAgent.claim_next(@storage, dispatch_agent, "native-worker",
               now: @now,
               lease_for: 300,
               claim_id: "native-claim-1",
               claim_token: "native-token-1"
             )

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    request = %{input: %{cursor: 1}, continuation_key: "page-1", definition: :current}
    execution_request = Keyword.get(opts, :execution_request, request)

    assert {:ok, intent} =
             ContinuationIntent.prepare_current(
               @storage,
               workflow_agent,
               request.input,
               request.continuation_key,
               "default",
               @now,
               parent_trace: claim.attempt.trace
             )

    fence =
      ContinuationIntent.fence_attrs(intent, request.input, %{
        source_runnable_key: claim.attempt.runnable_key
      })

    assert {:ok, %{created?: true}} =
             DispatchAgent.complete_with_continuation_fence(
               @storage,
               claim.agent,
               %{
                 runnable_key: claim.attempt.runnable_key,
                 claim_id: claim.claim_id,
                 claim_token: claim.claim_token,
                 result: %{},
                 fence: fence
               },
               now: @now,
               execution_opts: [continue_as_new: execution_request]
             )

    %{intent: intent}
  end

  defp seed_missing_source_fence do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert {:ok, intent} =
             ContinuationIntent.prepare_current(
               @storage,
               workflow_agent,
               %{cursor: 1},
               "page-1",
               "default",
               @now
             )

    fence_data =
      intent
      |> ContinuationIntent.fence_attrs(%{cursor: 1}, %{
        source_runnable_key: "#{@run_id}:missing:1"
      })
      |> Map.merge(%{queue: "default", occurred_at: @now})

    assert {:ok, fence_entry} =
             DispatchProtocol.new_entry(:run_continuation_fenced, fence_data)

    assert {:ok, _thread} = Journal.append_entries(@storage, [fence_entry])
    %{intent: intent}
  end

  defp append_dynamic_work do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:dynamic_work_recorded, %{
               run_id: @run_id,
               dynamic_key: "post-fence-work",
               origin: %{runnable_key: "external"},
               nodes: [],
               occurred_at: DateTime.add(@now, 1, :second)
             })

    assert {:ok, thread} = Journal.load_thread(@storage, {:run, @run_id})
    assert {:ok, _thread} = Journal.append_entries(@storage, [entry], expected_rev: thread.rev)
  end

  defp recovery_storage(mode) do
    {RecoveryStorage, delegate: @storage, failure_ref: make_ref(), mode: mode}
  end

  defp run_types(run_id) do
    run_id
    |> run_entries()
    |> Enum.map(& &1.type)
  end

  defp terminal_entry(run_id) do
    run_id
    |> run_entries()
    |> Enum.find(&(&1.type == :run_terminal))
  end

  defp run_entries(run_id) do
    assert {:ok, entries} = Journal.load_entries(@storage, {:run, run_id})
    entries
  end

  defp dispatch_types do
    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    Enum.map(entries, & &1.type)
  end

  defp journal_state(successor_run_id) do
    %{
      predecessor: Journal.load_entries(@storage, {:run, @run_id}),
      successor: Journal.load_entries(@storage, {:run, successor_run_id}),
      dispatch: Journal.load_entries(@storage, {:dispatch, "default"}),
      catalog: Journal.load_entries(@storage, {:run_catalog, "all"}),
      index: Journal.load_entries(@storage, {:run_index, Atom.to_string(PagingWorkflow)})
    }
  end

  defp cleanup_storage do
    for table <- [
          :squidie_native_continuation_recovery_test_checkpoints,
          :squidie_native_continuation_recovery_test_threads,
          :squidie_native_continuation_recovery_test_thread_meta
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end
  rescue
    ArgumentError -> :ok
  end
end
