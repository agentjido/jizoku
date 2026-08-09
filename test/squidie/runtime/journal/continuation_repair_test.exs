defmodule Squidie.Runtime.Journal.ContinuationRepairTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Continuation
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition

  defmodule FaultStorage do
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
      case Keyword.fetch!(opts, :append_hook).(thread_id, entries, opts) do
        :continue ->
          delegate_append(thread_id, entries, opts)

        {:return, result} ->
          result

        {:delegate_then_return, result} ->
          with {:ok, _thread} <- delegate_append(thread_id, entries, opts) do
            result
          end
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp delegate(opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)
      {adapter, delegate_opts ++ Keyword.drop(opts, [:append_hook, :delegate])}
    end

    defp delegate_append(thread_id, entries, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.append_thread(thread_id, entries, delegate_opts)
    end
  end

  defmodule RecordCursor do
    use Jido.Action,
      name: "record_repaired_continuation_cursor",
      description: "Records the successor cursor",
      schema: [cursor: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{cursor: cursor}, _context) do
      {:ok, %{cursor: cursor}}
    end
  end

  defmodule CursorWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :continue do
        manual()

        payload do
          field :cursor, :string
        end
      end

      step :record_cursor, RecordCursor
      transition :record_cursor, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :squidie_continuation_repair_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @successor_run_id "22222222-2222-5222-8222-222222222222"
  @now ~U[2026-08-09 21:00:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "repairs a fenced predecessor through one fresh executable successor and receipt" do
    fence = seed_fenced_predecessor()

    assert {:ok,
            %{
              predecessor: %{created?: true},
              successor: %{run_id: @successor_run_id},
              receipt_created?: true
            }} = Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor = Projection.rebuild(predecessor_entries)
    assert predecessor.status == :continued
    assert predecessor.continuation_request == continuation_request(fence)

    assert {:ok, successor_entries} =
             Journal.load_entries(@storage, {:run, @successor_run_id})

    successor = Projection.rebuild(successor_entries)
    assert successor.input == %{cursor: "next"}

    assert Projection.continuation(successor).continued_from == %{
             run_id: @run_id,
             continuation_key: "page-42"
           }

    assert %{data: %{trace: @trace}} =
             Enum.find(successor_entries, &(&1.type == :run_started))

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.pending_continuation_fences(dispatch_agent) == []

    assert {:ok, completed} =
             Squidie.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               owner_id: "successor-worker",
               now: @now
             )

    assert completed.run_id == @successor_run_id
    assert completed.status == :completed
    assert completed.context.cursor == "next"
  end

  test "returns the same successor and performs no journal writes on exact retry" do
    _fence = seed_fenced_predecessor()

    assert {:ok, %{successor: first}} =
             Continuation.repair_fenced_run(@storage, @run_id, "default")

    before_retry = journal_state()

    assert {:ok,
            %{
              predecessor: %{created?: false},
              successor: second,
              receipt_created?: false
            }} = Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert second.run_id == first.run_id
    assert journal_state() == before_retry
  end

  test "repairs a crash after predecessor commit and before successor start" do
    _fence = seed_fenced_predecessor()

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})

    assert {:ok,
            %{
              predecessor: %{created?: false},
              successor: %{run_id: @successor_run_id},
              receipt_created?: true
            }} = Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert_repair_complete()
  end

  test "does not acknowledge repair until successor exposure is complete" do
    _fence = seed_fenced_predecessor()
    failure_ref = make_ref()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        kinds = Enum.map(entries, & &1.kind)

        if kinds == [:run_indexed] and is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:return, {:error, :injected_index_failure}}
        else
          :continue
        end
      end)

    assert {:error, {:journal_start_committed, @successor_run_id, _reason}} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert {:ok, successor_entries} =
             Journal.load_entries(@storage, {:run, @successor_run_id})

    assert Projection.rebuild(successor_entries).run_id == @successor_run_id
    assert_pending_repair()

    assert {:ok, %{successor: %{run_id: @successor_run_id}, receipt_created?: true}} =
             Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert_repair_complete()
  end

  test "leaves a failed initial successor append retryable without a receipt" do
    _fence = seed_fenced_predecessor()
    failure_ref = make_ref()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        kinds = Enum.map(entries, & &1.kind)

        if kinds == [:run_signal_received, :run_started, :run_continued_from, :runnables_planned] and
             is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:return, {:error, :injected_successor_failure}}
        else
          :continue
        end
      end)

    assert {:error, :injected_successor_failure} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
    assert_pending_repair()

    assert {:ok, %{successor: %{run_id: @successor_run_id}, receipt_created?: true}} =
             Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert_repair_complete()
  end

  test "rejects an incompatible preexisting successor without acknowledging repair" do
    _fence = seed_fenced_predecessor()

    assert {:ok, _snapshot} =
             Starter.start_run(CursorWorkflow, :continue, %{cursor: "collision"},
               journal_storage: @storage,
               queue: "default",
               run_id: @successor_run_id,
               now: @now
             )

    assert {:error, :conflict} =
             Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert {:ok, successor_entries} =
             Journal.load_entries(@storage, {:run, @successor_run_id})

    successor = Projection.rebuild(successor_entries)
    assert successor.input == %{cursor: "collision"}
    assert Projection.continuation(successor).continued_from == nil
    assert_pending_repair()
  end

  test "repairs a crash after successor exposure and before the repair receipt" do
    _fence = seed_fenced_predecessor()
    failure_ref = make_ref()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        kinds = Enum.map(entries, & &1.kind)

        if kinds == [:run_continuation_repaired] and is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:return, {:error, :injected_receipt_failure}}
        else
          :continue
        end
      end)

    assert {:error, :injected_receipt_failure} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert {:ok, successor_entries} =
             Journal.load_entries(@storage, {:run, @successor_run_id})

    assert Projection.rebuild(successor_entries).run_id == @successor_run_id
    assert_pending_repair()

    before_retry = successor_state_without_receipt()

    assert {:ok,
            %{
              predecessor: %{created?: false},
              successor: %{run_id: @successor_run_id},
              receipt_created?: true
            }} = Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert successor_state_without_receipt() == before_retry
    assert_repair_complete()
  end

  test "recovers a committed repair receipt after an unknown append outcome" do
    _fence = seed_fenced_predecessor()
    expose_successor_without_receipt()
    failure_ref = make_ref()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        if Enum.map(entries, & &1.kind) == [:run_continuation_repaired] and
             is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:delegate_then_return, {:error, :conflict}}
        else
          :continue
        end
      end)

    assert {:ok, %{receipt_created?: false}} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(entries, &(&1.type == :run_continuation_repaired)) == 1
    assert_repair_complete()
  end

  test "bounds receipt conflicts and leaves the durable fence retryable" do
    _fence = seed_fenced_predecessor()
    expose_successor_without_receipt()
    test_pid = self()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        if Enum.map(entries, & &1.kind) == [:run_continuation_repaired] do
          send(test_pid, :receipt_append_attempt)
          {:return, {:error, :conflict}}
        else
          :continue
        end
      end)

    before_conflicts = successor_state_without_receipt()

    assert {:error, :conflict} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    for _attempt <- 1..25 do
      assert_receive :receipt_append_attempt
    end

    refute_receive :receipt_append_attempt
    assert successor_state_without_receipt() == before_conflicts
    assert_pending_repair()

    assert {:ok, %{receipt_created?: true}} =
             Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert_repair_complete()
  end

  test "acknowledges an existing successor after its workflow module is unavailable" do
    workflow = compile_ephemeral_workflow()
    _fence = seed_fenced_predecessor(workflow: workflow)
    failure_ref = make_ref()

    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        if Enum.map(entries, & &1.kind) == [:run_indexed] and
             is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:return, {:error, :injected_index_failure}}
        else
          :continue
        end
      end)

    assert {:error, {:journal_start_committed, @successor_run_id, _reason}} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert {:ok, _entries} = Journal.load_entries(@storage, {:run, @successor_run_id})

    assert {:ok, index_entries} =
             Journal.load_entries(
               @storage,
               {:run_index, Definition.serialize_workflow(workflow)}
             )

    refute Enum.any?(index_entries, &(&1.data.run_id == @successor_run_id))

    :code.purge(workflow)
    :code.delete(workflow)
    refute Code.ensure_loaded?(workflow)

    assert {:ok,
            %{
              predecessor: %{created?: false},
              successor: %{run_id: @successor_run_id},
              receipt_created?: true
            }} = Continuation.repair_fenced_run(@storage, @run_id, "default")

    assert {:ok, index_entries} =
             Journal.load_entries(
               @storage,
               {:run_index, Definition.serialize_workflow(workflow)}
             )

    assert Enum.any?(index_entries, &(&1.data.run_id == @successor_run_id))

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert [%{run_id: @successor_run_id}] =
             DispatchAgent.visible_attempts(dispatch_agent, @now)

    assert_repair_complete()
  end

  test "preserves a non-default queue and isolates the same run IDs by partition" do
    assert {:ok, acme_storage} = Storage.scope(@storage, "tenant_acme")
    assert {:ok, globex_storage} = Storage.scope(@storage, "tenant_globex")

    _acme_fence = seed_fenced_predecessor(storage: acme_storage, queue: "priority")
    _globex_fence = seed_fenced_predecessor(storage: globex_storage, queue: "priority")

    assert {:ok, %{successor: %{run_id: @successor_run_id}}} =
             Continuation.repair_fenced_run(acme_storage, @run_id, "priority")

    assert {:ok, acme_entries} =
             Journal.load_entries(acme_storage, {:run, @successor_run_id})

    assert [%{queue: "priority"}] =
             acme_entries
             |> Projection.rebuild()
             |> Projection.planned_runnables()

    assert_pending_repair(globex_storage, "priority")
    assert {:error, :not_found} = Journal.load_entries(acme_storage, {:dispatch, "default"})

    assert {:error, :not_found} =
             Journal.load_entries(globex_storage, {:run, @successor_run_id})

    assert {:ok, %{successor: %{run_id: @successor_run_id}}} =
             Continuation.repair_fenced_run(globex_storage, @run_id, "priority")

    assert_repair_complete(acme_storage, "priority")
    assert_repair_complete(globex_storage, "priority")
  end

  defp seed_fenced_predecessor(opts \\ []) do
    storage = Keyword.get(opts, :storage, @storage)
    queue = Keyword.get(opts, :queue, "default")
    workflow = Keyword.get(opts, :workflow, CursorWorkflow)

    {:ok, _snapshot} =
      Starter.start_run(workflow, :continue, %{cursor: "current"},
        journal_storage: storage,
        queue: queue,
        run_id: @run_id,
        now: @now
      )

    {:ok, dispatch_agent} = DispatchAgent.rebuild(storage, queue)

    {:ok, %{agent: claimed_agent, attempt: attempt}} =
      DispatchAgent.claim_next(storage, dispatch_agent, "worker-1",
        claim_id: "claim-1",
        claim_token: "token-1",
        now: @now
      )

    {:ok, %{agent: _completed_agent}} =
      DispatchAgent.complete(
        storage,
        claimed_agent,
        attempt.runnable_key,
        "claim-1",
        "token-1",
        %{cursor: "current"},
        now: @now
      )

    append_applied(storage, attempt.runnable_key)
    {:ok, definition} = Definition.load(workflow)

    fence = %{
      run_id: @run_id,
      successor_run_id: @successor_run_id,
      continuation_key: "page-42",
      workflow: Definition.serialize_workflow(workflow),
      trigger: "continue",
      input: %{cursor: "next"},
      definition: :current,
      definition_version: definition.definition_version,
      definition_fingerprint: Definition.fingerprint(definition),
      trace: @trace
    }

    {:ok, rebuilt_dispatch_agent} = DispatchAgent.rebuild(storage, queue)

    assert {:ok, %{fence: persisted_fence}} =
             DispatchAgent.fence_run_for_continuation(
               storage,
               rebuilt_dispatch_agent,
               fence,
               now: @now
             )

    persisted_fence
  end

  defp append_applied(storage, runnable_key) do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: runnable_key,
               result: %{cursor: "current"},
               occurred_at: @now
             })

    assert {:ok, thread} = Journal.load_thread(storage, {:run, @run_id})
    assert {:ok, _thread} = Journal.append_entries(storage, [entry], expected_rev: thread.rev)
  end

  defp continuation_request(fence) do
    Map.take(fence, [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_version,
      :definition_fingerprint
    ])
  end

  defp assert_pending_repair(storage \\ @storage, queue \\ "default") do
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(storage, queue)
    assert [%{run_id: @run_id}] = DispatchAgent.pending_continuation_fences(dispatch_agent)
  end

  defp assert_repair_complete(storage \\ @storage, queue \\ "default") do
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(storage, queue)
    assert DispatchAgent.pending_continuation_fences(dispatch_agent) == []
  end

  defp journal_state(storage \\ @storage, queue \\ "default") do
    Map.new(
      [
        {:run, @run_id},
        {:run, @successor_run_id},
        {:run_index, Definition.serialize_workflow(CursorWorkflow)},
        {:run_catalog, "all"},
        {:dispatch, queue}
      ],
      fn thread -> {thread, Journal.load_entries(storage, thread)} end
    )
  end

  defp successor_state_without_receipt do
    journal_state()
    |> Map.delete({:run, @run_id})
    |> Map.update!({:dispatch, "default"}, fn {:ok, entries} ->
      {:ok, Enum.reject(entries, &(&1.type == :run_continuation_repaired))}
    end)
  end

  defp expose_successor_without_receipt do
    storage =
      fault_storage(fn _thread_id, entries, _opts ->
        if Enum.map(entries, & &1.kind) == [:run_continuation_repaired] do
          {:return, {:error, :injected_receipt_failure}}
        else
          :continue
        end
      end)

    assert {:error, :injected_receipt_failure} =
             Continuation.repair_fenced_run(storage, @run_id, "default")

    assert_pending_repair()
  end

  defp compile_ephemeral_workflow do
    workflow = Squidie.Runtime.Journal.ContinuationRepairTest.EphemeralWorkflow
    action = RecordCursor

    Code.compile_quoted(
      quote do
        defmodule unquote(workflow) do
          use Squidie.Workflow

          workflow do
            trigger :continue do
              manual()

              payload do
                field :cursor, :string
              end
            end

            step :record_cursor, unquote(action)
            transition :record_cursor, on: :ok, to: :complete
          end
        end
      end
    )

    workflow
  end

  defp fault_storage(append_hook) do
    {FaultStorage, delegate: @storage, append_hook: append_hook}
  end

  defp cleanup_storage do
    for table <- [
          :squidie_continuation_repair_test_checkpoints,
          :squidie_continuation_repair_test_threads,
          :squidie_continuation_repair_test_thread_meta
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end
  rescue
    ArgumentError -> :ok
  end
end
