defmodule Jizoku.Runtime.Journal.NativeContinuationTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Commands.Continuation
  alias Jizoku.Runtime.Journal.Commands.Starter
  alias Jizoku.Runtime.Journal.ContinuationIntent
  alias Jizoku.Runtime.Journal.Executor
  alias Jizoku.Runtime.WorkflowAgent

  defmodule FaultStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts), do: delegate(:get_checkpoint, [key], opts)

    @impl Jido.Storage
    def put_checkpoint(key, data, opts), do: delegate(:put_checkpoint, [key, data], opts)

    @impl Jido.Storage
    def delete_checkpoint(key, opts), do: delegate(:delete_checkpoint, [key], opts)

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      fail_load_run_id = Keyword.get(opts, :fail_load_run_id)
      fail_load_phase = Keyword.get(opts, :fail_load_phase)
      load_key = {__MODULE__, Keyword.fetch!(opts, :failure_ref), fail_load_phase}

      if is_binary(fail_load_run_id) and String.ends_with?(thread_id, fail_load_run_id) and
           failure_phase_in_stack?(fail_load_phase) and is_nil(Process.get(load_key)) do
        Process.put(load_key, true)
        send(Keyword.fetch!(opts, :test_pid), {:native_run_load_failed, fail_load_phase})
        {:error, :injected_native_read_failure}
      else
        delegate(:load_thread, [thread_id], opts)
      end
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      failure_key = {__MODULE__, Keyword.fetch!(opts, :failure_ref)}
      kinds = Enum.map(entries, & &1.kind)

      case native_append_mode(kinds, opts, failure_key) do
        :fail_before_pair ->
          Process.put(failure_key, true)
          {:error, :injected_native_pair_failure}

        :commit_pair_then_conflict ->
          Process.put(failure_key, true)
          {:ok, _thread} = delegate(:append_thread, [thread_id, entries], opts)
          {:error, :conflict}

        :fail_run ->
          Process.put(failure_key, true)
          send(Keyword.fetch!(opts, :test_pid), {:native_run_append_failed, kinds})
          {:error, :injected_native_run_failure}

        :delegate ->
          result = delegate(:append_thread, [thread_id, entries], opts)
          maybe_run_after_native_fence(kinds, result, opts)
          result
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts), do: delegate(:delete_thread, [thread_id], opts)

    defp delegate(callback, args, opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      apply(
        adapter,
        callback,
        Enum.concat(args, [delegate_opts ++ Keyword.take(opts, [:expected_rev])])
      )
    end

    defp maybe_run_after_native_fence(
           [:attempt_completed, :run_continuation_fenced],
           {:ok, _thread},
           opts
         ) do
      hook_key = {__MODULE__, Keyword.fetch!(opts, :failure_ref), :after_fence}

      case {Keyword.get(opts, :after_native_fence), Process.get(hook_key)} do
        {hook, nil} when is_function(hook, 0) ->
          Process.put(hook_key, true)
          hook.()

        _missing_or_called ->
          :ok
      end
    end

    defp maybe_run_after_native_fence(_kinds, _result, _opts) do
      :ok
    end

    defp native_append_mode(kinds, opts, failure_key) do
      cond do
        Process.get(failure_key) ->
          :delegate

        kinds == [:attempt_completed, :run_continuation_fenced] and
            Keyword.get(opts, :fail_native_pair_before?, false) ->
          :fail_before_pair

        kinds == [:attempt_completed, :run_continuation_fenced] and
            Keyword.get(opts, :commit_native_pair_then_conflict?, false) ->
          :commit_pair_then_conflict

        Keyword.get(opts, :fail_run_append?, true) and :runnable_applied in kinds ->
          :fail_run

        true ->
          :delegate
      end
    end

    defp failure_phase_in_stack?(:validation) do
      stack_includes?(
        Jizoku.Runtime.Journal.Commands.Continuation,
        :validate_native_intent
      )
    end

    defp failure_phase_in_stack?(:active_run) do
      stack_includes?(Jizoku.Runtime.DispatchAgent, :active_run)
    end

    defp failure_phase_in_stack?(_phase) do
      false
    end

    defp stack_includes?(module, function) do
      {:current_stacktrace, stacktrace} = Process.info(self(), :current_stacktrace)

      Enum.any?(stacktrace, fn {stack_module, stack_function, _arity, _location} ->
        stack_module == module and stack_function == function
      end)
    end
  end

  defmodule AdvancePage do
    use Jizoku.Step, name: :advance_page

    @impl Jizoku.Step
    def run(%{cursor: cursor}, _context) when cursor < 1 do
      count_key = {__MODULE__, :invocations}
      :persistent_term.put(count_key, :persistent_term.get(count_key, 0) + 1)

      {:continue_as_new, %{cursor: cursor + 1}, key: "page-#{cursor + 1}", definition: :current}
    end

    def run(%{cursor: cursor}, _context) do
      {:ok, %{completed_cursor: cursor}}
    end
  end

  defmodule PagingWorkflow do
    use Jizoku.Workflow

    workflow do
      version "2026-08-09.native-continuation"

      trigger :page do
        manual()

        payload do
          field :cursor, :integer
        end
      end

      step :advance_page, AdvancePage
      transition :advance_page, on: :ok, to: :complete
    end
  end

  defmodule AllowAudit do
    @spec validate_guardrail(map(), map()) :: {:ok, map()}
    def validate_guardrail(_value, context) do
      {:ok, %{placement: context.placement, step: context.step}}
    end
  end

  defmodule GuardedPagingWorkflow do
    use Jizoku.Workflow

    workflow do
      version "2026-08-09.native-continuation-guarded"

      trigger :page do
        manual()

        payload do
          field :cursor, :integer
        end
      end

      step :advance_page, AdvancePage,
        guardrails: [input: ["native.audit"], action: ["native.audit"]]

      transition :advance_page, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :jizoku_native_continuation_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @now ~U[2026-08-09 23:00:00Z]

  setup do
    previous_activation = Application.fetch_env(:jizoku, :continuation_fences)
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    cleanup_storage()
    :persistent_term.put({AdvancePage, :invocations}, 0)

    on_exit(fn ->
      restore_activation(previous_activation)
      :persistent_term.erase({AdvancePage, :invocations})
      cleanup_storage()
    end)

    :ok
  end

  test "restart repairs a committed dispatch pair without rerunning the native step" do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    failure_ref = make_ref()

    fault_storage =
      {FaultStorage, delegate: @storage, failure_ref: failure_ref, test_pid: self()}

    assert {:error, :injected_native_run_failure} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: fault_storage,
               now: @now,
               finished_at: DateTime.add(@now, 1, :second),
               owner_id: "native-worker",
               claim_id: "native-claim-1",
               claim_token: "native-token-1"
             )

    assert_receive {:native_run_append_failed,
                    [:runnable_applied, :run_continuation_requested, :run_terminal]}

    assert :persistent_term.get({AdvancePage, :invocations}) == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    assert Enum.take(Enum.map(dispatch_entries, & &1.type), -2) == [
             :attempt_completed,
             :run_continuation_fenced
           ]

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(predecessor_entries, &(&1.type == :run_continuation_requested))

    Application.delete_env(:jizoku, :continuation_fences)

    assert {:ok, successor} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               now: DateTime.add(@now, 2, :second),
               owner_id: "recovery-worker"
             )

    assert successor.input == %{cursor: 1}
    assert :persistent_term.get({AdvancePage, :invocations}) == 1
  end

  test "a storage failure before the dispatch pair remains retryable" do
    assert {:ok, _started} = start_predecessor()

    failure_ref = make_ref()

    fault_storage =
      {FaultStorage,
       delegate: @storage,
       failure_ref: failure_ref,
       fail_native_pair_before?: true,
       fail_run_append?: false,
       test_pid: self()}

    assert {:error, :injected_native_pair_failure} =
             execute_predecessor(fault_storage, "native-claim-failed")

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    refute :attempt_completed in dispatch_types
    refute :attempt_failed in dispatch_types
    refute :run_continuation_fenced in dispatch_types

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor_types = Enum.map(predecessor_entries, & &1.type)
    refute :runnable_applied in predecessor_types
    refute :run_continuation_requested in predecessor_types
    refute :run_terminal in predecessor_types

    retry_at = DateTime.add(@now, 301, :second)
    assert {:ok, successor} = execute_predecessor(@storage, "native-claim-retry", retry_at)
    assert successor.input == %{cursor: 1}
    assert :persistent_term.get({AdvancePage, :invocations}) == 2
  end

  test "a storage read failure during native validation remains retryable" do
    assert {:ok, _started} = start_predecessor()

    failure_ref = make_ref()

    fault_storage =
      {FaultStorage,
       delegate: @storage,
       failure_ref: failure_ref,
       fail_load_run_id: @run_id,
       fail_load_phase: :validation,
       fail_run_append?: false,
       test_pid: self()}

    assert {:error, :injected_native_read_failure} =
             execute_predecessor(fault_storage, "native-read-failed")

    assert_receive {:native_run_load_failed, :validation}
    assert_native_continuation_not_persisted()
  end

  test "a storage read failure during active-run admission remains retryable" do
    assert {:ok, _started} = start_predecessor()

    failure_ref = make_ref()

    fault_storage =
      {FaultStorage,
       delegate: @storage,
       failure_ref: failure_ref,
       fail_load_run_id: @run_id,
       fail_load_phase: :active_run,
       fail_run_append?: false,
       test_pid: self()}

    assert {:error, :injected_native_read_failure} =
             execute_predecessor(fault_storage, "native-active-run-read-failed")

    assert_receive {:native_run_load_failed, :active_run}
    assert_native_continuation_not_persisted()
  end

  test "an unknown successful dispatch pair is repaired without rerunning the step" do
    assert {:ok, _started} = start_predecessor()

    failure_ref = make_ref()

    fault_storage =
      {FaultStorage,
       delegate: @storage,
       failure_ref: failure_ref,
       commit_native_pair_then_conflict?: true,
       fail_run_append?: false,
       test_pid: self()}

    assert {:ok, successor} = execute_predecessor(fault_storage, "native-claim-unknown")
    assert successor.input == %{cursor: 1}
    assert :persistent_term.get({AdvancePage, :invocations}) == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    assert Enum.count(dispatch_types, &(&1 == :attempt_completed)) == 1
    assert Enum.count(dispatch_types, &(&1 == :run_continuation_fenced)) == 1

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor_types = Enum.map(predecessor_entries, & &1.type)
    assert Enum.count(predecessor_types, &(&1 == :runnable_applied)) == 1
    assert Enum.count(predecessor_types, &(&1 == :run_continuation_requested)) == 1
    assert Enum.count(predecessor_types, &(&1 == :run_terminal)) == 1
  end

  test "disabled activation durably fails the native attempt without fencing" do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    Application.delete_env(:jizoku, :continuation_fences)

    assert {:ok, failed} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               now: @now,
               finished_at: DateTime.add(@now, 1, :second),
               owner_id: "native-worker",
               claim_id: "native-claim-1",
               claim_token: "native-token-1"
             )

    assert failed.status == :failed
    assert failed.terminal? == true

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    assert :attempt_failed in dispatch_types
    refute :run_continuation_fenced in dispatch_types
  end

  test "dynamic work winning after the fence consumes the source before aborting" do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    hook_ref = make_ref()

    hook_storage =
      {FaultStorage,
       delegate: @storage,
       failure_ref: hook_ref,
       fail_run_append?: false,
       after_native_fence: fn -> append_dynamic_work() end,
       test_pid: self()}

    assert {:error, {:continuation_aborted, :predecessor_changed}} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: hook_storage,
               now: @now,
               finished_at: DateTime.add(@now, 1, :second),
               owner_id: "native-worker",
               claim_id: "native-claim-1",
               claim_token: "native-token-1"
             )

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor_types = Enum.map(predecessor_entries, & &1.type)
    assert Enum.count(predecessor_types, &(&1 == :runnable_applied)) == 1
    assert :dynamic_work_recorded in predecessor_types
    refute :run_continuation_requested in predecessor_types
    refute :run_terminal in predecessor_types

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    assert :run_continuation_aborted in dispatch_types

    fence = Enum.find(dispatch_entries, &(&1.type == :run_continuation_fenced))

    assert Journal.load_entries(@storage, {:run, fence.data.successor_run_id}) ==
             {:error, :not_found}
  end

  test "a planned sibling rejects native continuation before the dispatch pair" do
    assert {:ok, _started} = start_predecessor()
    append_sibling_plan()

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
             DispatchAgent.claim_next(@storage, dispatch_agent, "native-sibling-worker",
               now: @now,
               lease_for: 300,
               claim_id: "native-sibling-claim",
               claim_token: "native-sibling-token"
             )

    assert attempt.step == "advance_page"
    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert {:ok, intent} =
             ContinuationIntent.prepare_current(
               @storage,
               workflow_agent,
               %{cursor: 1},
               "page-1",
               "default",
               @now,
               parent_trace: attempt.trace
             )

    before_run = Journal.load_entries(@storage, {:run, @run_id})
    before_dispatch = Journal.load_entries(@storage, {:dispatch, "default"})

    assert {:error, {:unsafe_continuation, :unapplied_runnables}} =
             Continuation.validate_native_intent(
               @storage,
               claimed_agent,
               workflow_agent,
               intent,
               attempt.runnable_key
             )

    assert Journal.load_entries(@storage, {:run, @run_id}) == before_run
    assert Journal.load_entries(@storage, {:dispatch, "default"}) == before_dispatch
  end

  test "native continuation preserves source trace and guardrail audit data" do
    assert {:ok, _started} =
             Starter.start_run(GuardedPagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               run_id: @run_id,
               now: @now
             )

    guardrail_registry = %{"native.audit" => AllowAudit}

    assert {:ok, successor} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               now: @now,
               finished_at: DateTime.add(@now, 1, :second),
               owner_id: "native-guarded-worker",
               claim_id: "native-guarded-claim",
               claim_token: "native-guarded-token",
               guardrail_registry: guardrail_registry
             )

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    claimed = Enum.find(dispatch_entries, &(&1.type == :attempt_claimed))
    completed = Enum.find(dispatch_entries, &(&1.type == :attempt_completed))
    fence = Enum.find(dispatch_entries, &(&1.type == :run_continuation_fenced))

    assert completed.data.guardrails == [
             %{
               key: "native.audit",
               placement: :input,
               policy: :block_run_start,
               status: :passed,
               result: %{placement: :input, step: :advance_page}
             },
             %{
               key: "native.audit",
               placement: :action,
               policy: :route_error,
               status: :passed,
               result: %{placement: :action, step: :advance_page}
             }
           ]

    assert fence.data.trace.trace_id == claimed.data.trace.trace_id
    assert fence.data.trace.parent_span_id == claimed.data.trace.span_id
    assert fence.data.trace.causation_id == "continuation:#{successor.run_id}"
  end

  test "native result atomically continues and the successor executes normally" do
    assert {:ok, _started} =
             Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
               journal_storage: @storage,
               queue: "priority",
               run_id: @run_id,
               now: @now
             )

    finished_at = DateTime.add(@now, 1, :second)

    assert {:ok, successor} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: "priority",
               now: @now,
               finished_at: finished_at,
               owner_id: "native-worker",
               claim_id: "native-claim-1",
               claim_token: "native-token-1"
             )

    assert successor.run_id != @run_id
    assert successor.input == %{cursor: 1}
    assert successor.terminal? == false

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})

    assert Enum.take(Enum.map(predecessor_entries, & &1.type), -3) == [
             :runnable_applied,
             :run_continuation_requested,
             :run_terminal
           ]

    assert %{data: %{status: :continued}} =
             Enum.find(predecessor_entries, &(&1.type == :run_terminal))

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "priority"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    completion_index = Enum.find_index(dispatch_types, &(&1 == :attempt_completed))

    assert Enum.slice(dispatch_types, completion_index, 2) == [
             :attempt_completed,
             :run_continuation_fenced
           ]

    assert {:ok, completed} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: "priority",
               now: finished_at,
               finished_at: DateTime.add(finished_at, 1, :second),
               owner_id: "native-worker",
               claim_id: "native-claim-2",
               claim_token: "native-token-2"
             )

    assert completed.run_id == successor.run_id
    assert completed.status == :completed
    assert completed.terminal? == true
  end

  defp restore_activation({:ok, value}) do
    Application.put_env(:jizoku, :continuation_fences, value)
  end

  defp restore_activation(:error) do
    Application.delete_env(:jizoku, :continuation_fences)
  end

  defp append_dynamic_work do
    assert {:ok, entry} =
             Jizoku.Runtime.DispatchProtocol.new_entry(:dynamic_work_recorded, %{
               run_id: @run_id,
               dynamic_key: "post-fence-work",
               origin: %{runnable_key: "external"},
               nodes: [],
               occurred_at: DateTime.add(@now, 2, :second)
             })

    assert {:ok, thread} = Journal.load_thread(@storage, {:run, @run_id})
    assert {:ok, _thread} = Journal.append_entries(@storage, [entry], expected_rev: thread.rev)
  end

  defp append_sibling_plan do
    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    planned = Enum.find(entries, &(&1.type == :runnables_planned))
    [source] = planned.data.runnables

    sibling = %{
      source
      | step: "sibling_work",
        runnable_key: "#{@run_id}:sibling_work:1",
        idempotency_key: "#{@run_id}:sibling_work:1"
    }

    assert {:ok, entry} =
             Jizoku.Runtime.DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [sibling],
               occurred_at: DateTime.add(@now, 1, :second)
             })

    assert {:ok, thread} = Journal.load_thread(@storage, {:run, @run_id})
    assert {:ok, _thread} = Journal.append_entries(@storage, [entry], expected_rev: thread.rev)
  end

  defp start_predecessor do
    Starter.start_run(PagingWorkflow, :page, %{cursor: 0},
      journal_storage: @storage,
      run_id: @run_id,
      now: @now
    )
  end

  defp execute_predecessor(storage, claim_id, now \\ @now) do
    Executor.execute_next(
      runtime: :journal,
      journal_storage: storage,
      now: now,
      finished_at: DateTime.add(now, 1, :second),
      owner_id: "native-worker",
      claim_id: claim_id,
      claim_token: claim_id <> "-token"
    )
  end

  defp assert_native_continuation_not_persisted do
    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    dispatch_types = Enum.map(dispatch_entries, & &1.type)
    refute :attempt_completed in dispatch_types
    refute :attempt_failed in dispatch_types
    refute :run_continuation_fenced in dispatch_types

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor_types = Enum.map(predecessor_entries, & &1.type)
    refute :runnable_applied in predecessor_types
    refute :run_continuation_requested in predecessor_types
    refute :run_terminal in predecessor_types
  end

  defp cleanup_storage do
    for table <- [
          :jizoku_native_continuation_test_checkpoints,
          :jizoku_native_continuation_test_threads,
          :jizoku_native_continuation_test_thread_meta
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end
  rescue
    ArgumentError -> :ok
  end
end
