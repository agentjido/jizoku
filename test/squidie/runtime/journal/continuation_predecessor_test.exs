defmodule Squidie.Runtime.Journal.ContinuationPredecessorTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Continuation
  alias Squidie.Runtime.Journal.Commands.ContinuationRecovery
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.Spec

  defmodule RecordingStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.get_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:storage_checkpoint_put, key})

      case run_checkpoint_hook(opts, key, data) do
        :continue ->
          {adapter, delegate_opts} = delegate(opts)
          adapter.put_checkpoint(key, data, delegate_opts)

        {:return, result} ->
          result
      end
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
      kinds = Enum.map(entries, & &1.kind)

      send(Keyword.fetch!(opts, :test_pid), {
        :storage_append,
        thread_id,
        kinds
      })

      case run_append_hook(opts, thread_id, entries, kinds) do
        :continue ->
          {adapter, delegate_opts} = delegate(opts)
          adapter.append_thread(thread_id, entries, delegate_opts)

        {:return, result} ->
          result
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp delegate(opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      {adapter,
       delegate_opts ++
         Keyword.drop(opts, [:append_hook, :checkpoint_hook, :delegate, :test_pid])}
    end

    defp run_append_hook(opts, thread_id, entries, kinds) do
      case Keyword.get(opts, :append_hook) do
        hook when is_function(hook, 3) -> hook.(thread_id, entries, opts)
        hook when is_function(hook, 2) -> hook.(thread_id, kinds)
        _none -> :continue
      end
    end

    defp run_checkpoint_hook(opts, key, data) do
      case Keyword.get(opts, :checkpoint_hook) do
        hook when is_function(hook, 3) -> hook.(key, data, opts)
        _none -> :continue
      end
    end
  end

  defmodule RecordCursor do
    use Jido.Action,
      name: "record_cursor_for_continuation",
      description: "Records a cursor before continuation",
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

  @storage {ETS, table: :squidie_continuation_predecessor_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @successor_run_id "22222222-2222-5222-8222-222222222222"
  @now ~U[2026-08-09 17:00:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "commits continuation request before the continued terminal in one predecessor append" do
    fence = seed_fenced_predecessor()
    before_exposure = successor_exposure_state()
    recording_storage = {RecordingStorage, delegate: @storage, test_pid: self()}

    assert {:ok, %{created?: true, intent: intent}} =
             Continuation.commit_predecessor(recording_storage, @run_id, "default")

    assert Map.from_struct(intent) == fence

    assert_receive {
      :storage_append,
      "squidie:run:#{@run_id}",
      [:run_continuation_requested, :run_terminal]
    }

    assert_receive {
      :storage_checkpoint_put,
      {"squidie", :checkpoint, "squidie:run:#{@run_id}"}
    }

    refute_receive {:storage_append, "squidie:run:#{@run_id}", _kinds}

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})

    assert Enum.take(Enum.map(entries, & &1.type), -2) == [
             :run_continuation_requested,
             :run_terminal
           ]

    assert Enum.count(entries, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(entries, &(&1.type == :run_terminal)) == 1

    [request_entry, terminal_entry] = Enum.take(entries, -2)
    assert request_entry.occurred_at == @now
    assert request_entry.data.occurred_at == @now
    assert terminal_entry.occurred_at == @now
    assert terminal_entry.data.occurred_at == @now
    assert terminal_entry.data.trace == @trace

    projection = Projection.rebuild(entries)

    assert projection.status == :continued

    assert projection.continuation_request ==
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

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
    assert successor_exposure_state() == before_exposure
  end

  test "returns an exact committed continuation without another write" do
    fence = seed_fenced_predecessor()

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    before_duplicate = journal_state()
    recording_storage = {RecordingStorage, delegate: @storage, test_pid: self()}

    assert {:ok, %{created?: false, intent: intent}} =
             Continuation.commit_predecessor(recording_storage, @run_id, "default")

    assert Map.from_struct(intent) == fence

    assert journal_state() == before_duplicate
    refute_receive {:storage_append, _thread_id, _kinds}
    refute_receive {:storage_checkpoint_put, _key}
  end

  test "revalidates the selected queue for an exact committed predecessor" do
    fence = seed_fenced_predecessor()

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    priority_fence = Map.put(fence, :queue, "priority")

    assert {:ok, priority_entry} =
             DispatchProtocol.new_entry(:run_continuation_fenced, priority_fence)

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [priority_entry], expected_rev: 0)

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :multiple_queues}} =
             Continuation.commit_predecessor(@storage, @run_id, "priority")

    assert journal_state() == before_rejection
  end

  test "commits a quiescent predecessor on a non-default queue" do
    _fence = seed_fenced_predecessor(%{}, queue: "priority")

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "priority")

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Projection.rebuild(entries).status == :continued
    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "isolates predecessor commits for the same run identity by partition" do
    assert {:ok, acme_storage} = Storage.scope(@storage, "tenant_acme")
    assert {:ok, globex_storage} = Storage.scope(@storage, "tenant_globex")

    _acme_fence = seed_fenced_predecessor(%{}, storage: acme_storage)
    _globex_fence = seed_fenced_predecessor(%{}, storage: globex_storage)

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(acme_storage, @run_id, "default")

    assert {:ok, acme_entries} = Journal.load_entries(acme_storage, {:run, @run_id})
    assert {:ok, globex_entries} = Journal.load_entries(globex_storage, {:run, @run_id})
    assert Projection.rebuild(acme_entries).status == :continued
    refute Projection.terminal?(Projection.rebuild(globex_entries))

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(globex_storage, @run_id, "default")

    assert {:ok, globex_entries} = Journal.load_entries(globex_storage, {:run, @run_id})
    assert Projection.rebuild(globex_entries).status == :continued
  end

  test "rebuilds a current-revision legacy checkpoint before classifying a duplicate" do
    _fence = seed_fenced_predecessor()

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    refute Projection.checkpoint_compatible?(%{})

    refute workflow_agent.state.projection
           |> Map.delete(:terminal_status)
           |> Projection.checkpoint_compatible?()

    legacy_projection =
      workflow_agent.state.projection
      |> Map.delete(:continued_from_run_id)
      |> Map.delete(:continued_from_key)
      |> Map.delete(:continued_to_run_id)
      |> Map.delete(:continued_to_key)
      |> Map.delete(:continuation_request)
      |> Map.delete(:continuation_origin)

    refute Projection.checkpoint_compatible?(legacy_projection)

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:run, @run_id},
               legacy_projection,
               workflow_agent.state.thread_rev,
               updated_at: @now
             )

    recording_storage = {RecordingStorage, delegate: @storage, test_pid: self()}

    assert {:ok, %{created?: false}} =
             Continuation.commit_predecessor(recording_storage, @run_id, "default")

    refute_receive {:storage_append, _thread_id, _kinds}
    refute_receive {:storage_checkpoint_put, _key}

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(entries, &(&1.type == :run_terminal)) == 1
  end

  test "rejects a conflicting predecessor request without terminalizing" do
    fence = seed_fenced_predecessor()

    append_run_entry(request_entry(fence, input: %{cursor: "different"}))

    before_conflict = journal_state()

    assert {:error, :conflicting_continuation} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_conflict
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "rejects every one-field continuation request identity conflict" do
    overrides = [
      successor_run_id: "33333333-3333-5333-8333-333333333333",
      continuation_key: "page-43",
      workflow: "Elixir.OtherWorkflow",
      trigger: "other",
      input: %{cursor: "different"},
      definition_version: "other-version",
      definition_fingerprint: "other-fingerprint"
    ]

    Enum.each(overrides, fn {field, value} ->
      cleanup_storage()
      fence = seed_fenced_predecessor()
      append_run_entry(request_entry(fence, [{field, value}]))
      before_conflict = journal_state()

      assert {:error, :conflicting_continuation} =
               Continuation.commit_predecessor(@storage, @run_id, "default")

      assert journal_state() == before_conflict
    end)
  end

  test "rejects a fence for a different source workflow without mutation" do
    _fence = seed_fenced_predecessor(%{workflow: "Elixir.OtherWorkflow"})
    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :workflow_mismatch}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a fence for a different source trigger without mutation" do
    _fence = seed_fenced_predecessor(%{trigger: "other"})
    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :trigger_mismatch}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects target definition version drift before terminalizing" do
    _fence = seed_fenced_predecessor(%{definition_version: "other-version"})
    before_rejection = journal_state()

    assert {:error, {:invalid_continuation_target, :definition_version}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects target definition fingerprint drift before terminalizing" do
    _fence = seed_fenced_predecessor(%{definition_fingerprint: "other-fingerprint"})
    before_rejection = journal_state()

    assert {:error, {:invalid_continuation_target, :definition_fingerprint}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects undeclared target input before terminalizing" do
    _fence = seed_fenced_predecessor(%{input: %{cursor: "next", private: "discard"}})
    before_rejection = journal_state()

    assert {:error, {:invalid_payload, %{unknown_fields: [:private]}}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a request-only partial predecessor commit" do
    fence = seed_fenced_predecessor()
    append_run_entry(request_entry(fence))
    before_repair = journal_state()

    assert {:error, {:incomplete_continuation_commit, @run_id}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_repair
  end

  test "rejects a terminal-only partial predecessor commit" do
    _fence = seed_fenced_predecessor()

    append_run_entry(
      Squidie.Runtime.Journal.EntryBuilder.traced_run_terminal!(
        @run_id,
        :continued,
        @trace,
        @now
      )
    )

    before_repair = journal_state()

    assert {:error, {:incomplete_continuation_commit, @run_id}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_repair
  end

  test "returns conflict after exhausting the bounded append retry budget" do
    _fence = seed_fenced_predecessor()
    before_conflict = journal_state()

    conflicting_storage =
      recording_storage(fn thread_id, kinds ->
        if thread_id == Journal.thread_id({:run, @run_id}) and
             kinds == [:run_continuation_requested, :run_terminal] do
          {:return, {:error, :conflict}}
        else
          :continue
        end
      end)

    assert {:error, :conflict} =
             Continuation.commit_predecessor(conflicting_storage, @run_id, "default")

    for _attempt <- 1..25 do
      assert_receive {
        :storage_append,
        "squidie:run:#{@run_id}",
        [:run_continuation_requested, :run_terminal]
      }
    end

    refute_receive {:storage_append, "squidie:run:#{@run_id}", _kinds}
    refute_receive {:storage_checkpoint_put, _key}
    assert journal_state() == before_conflict
  end

  test "rejects a competing terminal state without exposing a successor" do
    _fence = seed_fenced_predecessor()

    append_run_entry(
      Squidie.Runtime.Journal.EntryBuilder.traced_run_terminal!(
        @run_id,
        :completed,
        @trace,
        @now
      )
    )

    before_conflict = journal_state()

    assert {:error, {:conflicting_continuation_terminal, :completed}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_conflict
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "retries safely after the predecessor append fails before commit" do
    _fence = seed_fenced_predecessor()
    failure_ref = make_ref()

    failing_storage =
      recording_storage(fn thread_id, kinds ->
        if thread_id == Journal.thread_id({:run, @run_id}) and
             kinds == [:run_continuation_requested, :run_terminal] and
             is_nil(Process.get(failure_ref)) do
          Process.put(failure_ref, true)
          {:return, {:error, :injected_failure}}
        else
          :continue
        end
      end)

    assert {:error, :injected_failure} =
             Continuation.commit_predecessor(failing_storage, @run_id, "default")

    assert {:ok, entries_before_retry} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries_before_retry, &(&1.type == :run_continuation_requested))
    refute Enum.any?(entries_before_retry, &(&1.type == :run_terminal))

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert {:ok, entries_after_retry} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries_after_retry, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(entries_after_retry, &(&1.type == :run_terminal)) == 1
  end

  test "loses the predecessor revision race to a competing terminal without partial commit" do
    _fence = seed_fenced_predecessor()
    race_ref = make_ref()

    terminal_entry =
      Squidie.Runtime.Journal.EntryBuilder.traced_run_terminal!(
        @run_id,
        :cancelled,
        @trace,
        @now
      )

    racing_storage =
      recording_storage(fn thread_id, kinds ->
        if thread_id == Journal.thread_id({:run, @run_id}) and
             kinds == [:run_continuation_requested, :run_terminal] and
             is_nil(Process.get(race_ref)) do
          Process.put(race_ref, true)
          append_run_entry(terminal_entry)
        end

        :continue
      end)

    assert {:error, {:conflicting_continuation_terminal, :cancelled}} =
             Continuation.commit_predecessor(racing_storage, @run_id, "default")

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries, &(&1.type == :run_continuation_requested))
    assert Enum.count(entries, &(&1.type == :run_terminal)) == 1
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "recovers an unknown successful predecessor append without duplicating facts" do
    _fence = seed_fenced_predecessor()
    unknown_outcome_ref = make_ref()

    storage =
      recording_storage(
        append_hook: fn thread_id, entries, opts ->
          if thread_id == Journal.thread_id({:run, @run_id}) and
               Enum.map(entries, & &1.kind) ==
                 [:run_continuation_requested, :run_terminal] and
               is_nil(Process.get(unknown_outcome_ref)) do
            Process.put(unknown_outcome_ref, true)
            {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

            assert {:ok, _thread} =
                     adapter.append_thread(
                       thread_id,
                       entries,
                       delegate_opts ++ Keyword.take(opts, [:expected_rev])
                     )

            {:return, {:error, :conflict}}
          else
            :continue
          end
        end
      )

    assert {:ok, %{created?: false}} =
             Continuation.commit_predecessor(storage, @run_id, "default")

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(entries, &(&1.type == :run_terminal)) == 1
  end

  test "treats a failed checkpoint write as recoverable after the journal commit" do
    _fence = seed_fenced_predecessor()
    checkpoint_ref = make_ref()

    storage =
      recording_storage(
        checkpoint_hook: fn _key, _data, _opts ->
          if is_nil(Process.get(checkpoint_ref)) do
            Process.put(checkpoint_ref, true)
            {:return, {:error, :injected_checkpoint_failure}}
          else
            :continue
          end
        end
      )

    assert {:ok, %{created?: true}} =
             Continuation.commit_predecessor(storage, @run_id, "default")

    before_retry = journal_state()

    assert {:ok, %{created?: false}} =
             Continuation.commit_predecessor(storage, @run_id, "default")

    assert journal_state() == before_retry
    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(entries, &(&1.type == :run_terminal)) == 1
  end

  test "rejects an unapplied planned runnable without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:runnables_planned, %{
      run_id: @run_id,
      runnables: [planned_runnable("late-work", "default")],
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :unapplied_runnables}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects unresolved manual state without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:manual_step_paused, %{
      run_id: @run_id,
      step: "operator_review",
      kind: :approval,
      metadata: %{},
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :manual_state}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a mixed-queue runnable plan without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:runnables_planned, %{
      run_id: @run_id,
      runnables: [planned_runnable("priority-work", "priority")],
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :multiple_queues}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects recorded dynamic work without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :dynamic_work}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects graph mutation history without a dynamic-work receipt" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_graph_mutated, %{
      run_id: @run_id,
      mutation_id: "mutation-add",
      expected_version: 0,
      result_version: 1,
      origin: "record_cursor",
      additions: [],
      removals: [],
      runnable_intent_fingerprints: %{},
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :dynamic_work}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "recovery aborts a fence after a competing predecessor terminal wins" do
    _fence = seed_fenced_predecessor()

    append_run_entry(
      Squidie.Runtime.Journal.EntryBuilder.traced_run_terminal!(
        @run_id,
        :cancelled,
        @trace,
        @now
      )
    )

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_terminal}, created?: true}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert {:ok, predecessor_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert predecessor_agent.state.projection.status == :cancelled
    assert predecessor_agent.state.projection.continuation_request == nil
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "recovery aborts a fence after durable dynamic work changes the predecessor" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}, created?: true}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.active_continuation_fence(dispatch_agent, @run_id) == nil
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "recovery aborts a fence after a durable graph mutation changes the predecessor" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_graph_mutated, %{
      run_id: @run_id,
      mutation_id: "mutation-add",
      expected_version: 0,
      result_version: 1,
      origin: "record_cursor",
      additions: [],
      removals: [],
      runnable_intent_fingerprints: %{},
      occurred_at: @now
    })

    assert {:ok, {:aborted, %{abort: %{abort_reason: :predecessor_changed}, created?: true}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.active_continuation_fence(dispatch_agent, @run_id) == nil
    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @successor_run_id})
  end

  test "recovery does not abort a trigger-mismatched fence after dynamic work" do
    _fence = seed_fenced_predecessor(%{trigger: "other"})

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    before_resolution = journal_state()

    assert {:error, {:unsafe_continuation, :trigger_mismatch}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state() == before_resolution
  end

  test "recovery does not abort a trigger-mismatched fence after a terminal wins" do
    _fence = seed_fenced_predecessor(%{trigger: "other"})

    append_run_entry(
      Squidie.Runtime.Journal.EntryBuilder.traced_run_terminal!(
        @run_id,
        :cancelled,
        @trace,
        @now
      )
    )

    before_resolution = journal_state()

    assert {:error, {:conflicting_continuation_terminal, :cancelled}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state() == before_resolution
  end

  test "recovery bounds abort CAS conflicts and remains retryable" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    storage =
      recording_storage(fn _thread_id, kinds ->
        if kinds == [:run_continuation_aborted] do
          {:return, {:error, :conflict}}
        else
          :continue
        end
      end)

    assert {:error, :conflict} =
             ContinuationRecovery.resolve_fenced_run(
               storage,
               @run_id,
               "default",
               now: @now
             )

    for _attempt <- 1..25 do
      assert_receive {:storage_append, _thread_id, [:run_continuation_aborted]}
    end

    refute_receive {:storage_append, _thread_id, [:run_continuation_aborted]}

    assert {:ok, {:aborted, %{created?: true}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )
  end

  test "recovery leaves dynamic work fenced when the workflow projection is anomalous" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    append_run_entry(%Entry{
      type: :dynamic_work_recorded,
      thread: {:run, @run_id},
      data: %{
        run_id: @run_id,
        dynamic_key: "malformed-dynamic",
        origin: %{},
        nodes: :not_a_list,
        occurred_at: @now
      },
      occurred_at: @now
    })

    before_resolution = journal_state()

    assert {:error, {:unsafe_continuation, {:workflow_anomalies, [_anomaly]}}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state() == before_resolution
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.active_continuation_fence(dispatch_agent, @run_id)
  end

  test "recovery leaves dynamic work fenced when the dispatch projection is anomalous" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    append_dispatch_entry(%Entry{
      type: :run_continuation_fenced,
      thread: {:dispatch, "default"},
      data: %{run_id: @run_id},
      occurred_at: @now
    })

    before_resolution = journal_state()

    assert {:error, {:unsafe_continuation, :dynamic_work}} =
             ContinuationRecovery.resolve_fenced_run(
               @storage,
               @run_id,
               "default",
               now: @now
             )

    assert journal_state() == before_resolution
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.active_continuation_fence(dispatch_agent, @run_id)
  end

  test "rejects a persisted compensation runnable without mutation" do
    _fence = seed_fenced_predecessor()

    compensation =
      planned_runnable("compensate-record-cursor", "default")
      |> Map.put(:dynamic?, true)
      |> Map.put(:dynamic_work, %{
        kind: :compensation,
        failure_runnable_key: "failed-work"
      })

    append_run_fact(:runnables_planned, %{
      run_id: @run_id,
      runnables: [compensation],
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :compensation}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a child lineage whose child thread is missing without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_fact(:child_run_started, %{
      run_id: @run_id,
      child_run_id: "44444444-4444-5444-8444-444444444444",
      child_workflow: Definition.serialize_workflow(CursorWorkflow),
      child_trigger: "continue",
      child_key: "missing-child",
      origin: %{runnable_key: "origin", step: "record_cursor", attempt: 1},
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :child_starting}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a workflow projection integrity anomaly without mutation" do
    _fence = seed_fenced_predecessor()

    append_run_entry(%Entry{
      type: :dynamic_work_recorded,
      thread: {:run, @run_id},
      data: %{
        run_id: @run_id,
        dynamic_key: "malformed-dynamic",
        origin: %{},
        nodes: :not_a_list,
        occurred_at: @now
      },
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, {:workflow_anomalies, [_anomaly]}}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a post-fence dispatch integrity anomaly without mutation" do
    _fence = seed_fenced_predecessor()

    append_dispatch_entry(%Entry{
      type: :run_continuation_fenced,
      thread: {:dispatch, "default"},
      data: %{run_id: @run_id},
      occurred_at: @now
    })

    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, {:dispatch_blockers, [%{reason: :dispatch_anomaly}]}}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  test "rejects a runtime-authored workflow before terminalizing" do
    {:ok, definition} = Definition.load(CursorWorkflow)
    spec = Spec.from_definition(CursorWorkflow, definition)

    assert {:ok, _snapshot} =
             Starter.start_spec_run(spec, :continue, %{cursor: "current"},
               journal_storage: @storage,
               queue: "default",
               run_id: @run_id,
               now: @now
             )

    _fence = fence_started_predecessor(@storage, "default", %{})
    before_rejection = journal_state()

    assert {:error, {:unsafe_continuation, :runtime_spec}} =
             Continuation.commit_predecessor(@storage, @run_id, "default")

    assert journal_state() == before_rejection
  end

  defp seed_fenced_predecessor(fence_overrides \\ %{}, opts \\ []) do
    storage = Keyword.get(opts, :storage, @storage)
    queue = Keyword.get(opts, :queue, "default")

    {:ok, _snapshot} =
      Starter.start_run(CursorWorkflow, :continue, %{cursor: "current"},
        journal_storage: storage,
        queue: queue,
        run_id: @run_id,
        now: @now
      )

    fence_started_predecessor(storage, queue, fence_overrides)
  end

  defp fence_started_predecessor(storage, queue, fence_overrides) do
    {:ok, available_agent} = DispatchAgent.rebuild(storage, queue)

    {:ok, %{agent: claimed_agent, attempt: attempt}} =
      DispatchAgent.claim_next(storage, available_agent, "worker-1",
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

    {:ok, definition} = Definition.load(CursorWorkflow)

    fence =
      Map.merge(
        %{
          run_id: @run_id,
          successor_run_id: @successor_run_id,
          continuation_key: "page-42",
          workflow: Definition.serialize_workflow(CursorWorkflow),
          trigger: "continue",
          input: %{cursor: "next"},
          definition: :current,
          definition_version: definition.definition_version,
          definition_fingerprint: Definition.fingerprint(definition),
          trace: @trace
        },
        fence_overrides
      )

    {:ok, fence_agent} = DispatchAgent.rebuild(storage, queue)

    assert {:ok, %{fence: persisted_fence}} =
             DispatchAgent.fence_run_for_continuation(
               storage,
               fence_agent,
               fence,
               now: @now
             )

    persisted_fence
  end

  defp request_entry(fence, overrides \\ []) do
    attrs =
      fence
      |> Map.take([
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
      |> Map.merge(Map.new(overrides))
      |> Map.put(:occurred_at, @now)

    assert {:ok, entry} =
             DispatchProtocol.new_entry(:run_continuation_requested, attrs)

    entry
  end

  defp append_applied(storage, runnable_key) do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: runnable_key,
               result: %{cursor: "current"},
               occurred_at: @now
             })

    append_run_entry(storage, @run_id, entry)
  end

  defp append_run_entry(entry) do
    append_run_entry(@storage, @run_id, entry)
  end

  defp append_run_entry(storage, run_id, entry) do
    assert {:ok, thread} = Journal.load_thread(storage, {:run, run_id})

    assert {:ok, _thread} =
             Journal.append_entries(storage, [entry], expected_rev: thread.rev)
  end

  defp append_run_fact(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    append_run_entry(entry)
  end

  defp append_dispatch_entry(entry) do
    case Journal.load_thread(@storage, {:dispatch, "default"}) do
      {:ok, thread} ->
        assert {:ok, _thread} =
                 Journal.append_entries(@storage, [entry], expected_rev: thread.rev)

      {:error, :not_found} ->
        assert {:ok, _thread} = Journal.append_entries(@storage, [entry], expected_rev: 0)
    end
  end

  defp planned_runnable(label, queue) do
    %{
      run_id: @run_id,
      runnable_key: "#{@run_id}:#{label}:1",
      idempotency_key: "#{@run_id}:#{label}:1",
      attempt_number: 1,
      queue: queue,
      step: label,
      input: %{},
      visible_at: @now
    }
  end

  defp journal_state do
    Map.new(
      [
        {:run, @run_id},
        {:run, @successor_run_id},
        {:run_index, Definition.serialize_workflow(CursorWorkflow)},
        {:run_catalog, "all"},
        {:dispatch, "default"},
        {:dispatch, "priority"}
      ],
      fn thread -> {thread, Journal.load_entries(@storage, thread)} end
    )
  end

  defp successor_exposure_state do
    Map.new(
      [
        {:run, @successor_run_id},
        {:run_index, Definition.serialize_workflow(CursorWorkflow)},
        {:run_catalog, "all"},
        {:dispatch, "default"},
        {:dispatch, "priority"}
      ],
      fn thread -> {thread, Journal.load_entries(@storage, thread)} end
    )
  end

  defp recording_storage(append_hook) when is_function(append_hook, 2) do
    recording_storage(append_hook: append_hook)
  end

  defp recording_storage(opts) when is_list(opts) do
    {RecordingStorage, [delegate: @storage, test_pid: self()] ++ opts}
  end

  defp cleanup_storage do
    for table <- [
          :squidie_continuation_predecessor_test_checkpoints,
          :squidie_continuation_predecessor_test_threads,
          :squidie_continuation_predecessor_test_thread_meta
        ] do
      delete_table(table)
    end
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
