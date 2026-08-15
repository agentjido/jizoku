defmodule Jizoku.ContinueAsNewTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Commands.Starter
  alias Jizoku.Runtime.Journal.Storage
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  defmodule TestStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      call_or_probe(:get_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      record(opts, {:storage_checkpoint_put, key})
      call_or_probe(:put_checkpoint, [key, data], opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      call_or_probe(:delete_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      record(opts, {:storage_load, thread_id})
      call_or_probe(:load_thread, [thread_id], opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      maybe_block_fence(entries, opts)
      record(opts, {:storage_append, thread_id, Enum.map(entries, & &1.kind)})
      call_or_probe(:append_thread, [thread_id, entries], opts)
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      call_or_probe(:delete_thread, [thread_id], opts)
    end

    defp maybe_block_fence(entries, opts) do
      case Keyword.get(opts, :barrier_ref) do
        nil ->
          :ok

        ref ->
          barrier_key = {__MODULE__, ref}

          if Enum.map(entries, & &1.kind) == [:run_continuation_fenced] and
               is_nil(Process.get(barrier_key)) do
            Process.put(barrier_key, true)
            send(Keyword.fetch!(opts, :test_pid), {:fence_append_blocked, ref, self()})

            receive do
              {:release_fence_append, ^ref} -> :ok
            after
              5_000 -> raise "timed out waiting to release continuation fence append"
            end
          end
      end
    end

    defp call_or_probe(callback, args, opts) do
      case Keyword.fetch(opts, :delegate) do
        {:ok, {adapter, delegate_opts}} ->
          callback_opts = delegate_opts ++ forwarded_opts(opts)
          apply(adapter, callback, Enum.concat(args, [callback_opts]))

        :error ->
          send(Keyword.fetch!(opts, :test_pid), {:storage_call, callback})
          {:error, :unexpected_storage_call}
      end
    end

    defp record(opts, message) do
      if Keyword.get(opts, :record?, false) do
        send(Keyword.fetch!(opts, :test_pid), message)
      end
    end

    defp forwarded_opts(opts) do
      Keyword.drop(opts, [:barrier_ref, :delegate, :record?, :test_pid])
    end
  end

  defmodule RecordCursor do
    use Jido.Action,
      name: "record_public_continuation_cursor",
      description: "Records a cursor before public continuation",
      schema: [cursor: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{cursor: cursor}, _context) do
      {:ok, %{cursor: cursor}}
    end
  end

  defmodule CursorWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :continue do
        manual()

        payload do
          field :cursor, :string
          field :batch_size, :integer, default: 100
        end
      end

      step :record_cursor, RecordCursor
      transition :record_cursor, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :jizoku_public_continue_as_new_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @now ~U[2026-08-09 22:00:00Z]

  setup do
    previous_activation = Application.fetch_env(:jizoku, :continuation_fences)
    Application.delete_env(:jizoku, :continuation_fences)
    cleanup_storage()

    on_exit(fn ->
      restore_activation(previous_activation)
      cleanup_storage()
    end)

    :ok
  end

  test "checks host activation before touching configured storage" do
    probe_storage = {TestStorage, test_pid: self()}

    assert Jizoku.continue_as_new(@run_id,
             input: %{cursor: "next"},
             continuation_key: "page-42",
             journal_storage: probe_storage
           ) == {:error, :continuation_fence_not_activated}

    refute_receive {:storage_call, _callback}
  end

  test "rejects malformed public command input before touching storage" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    probe_storage = {TestStorage, test_pid: self()}
    base = [runtime: :journal, journal_storage: probe_storage]

    cases = [
      {"invalid opts", :invalid, {:error, {:invalid_option, {:opts, :invalid}}}},
      {"unsupported option",
       Keyword.merge(base, input: %{}, continuation_key: "page", trace: %{}),
       {:error, {:invalid_option, {:option, :trace}}}},
      {"invalid run id", Keyword.merge(base, input: %{}, continuation_key: "page"),
       {:error, :invalid_run_id}, "not-a-uuid"},
      {"missing input", Keyword.put(base, :continuation_key, "page"),
       {:error, {:invalid_option, {:input, :missing}}}},
      {"invalid input", Keyword.merge(base, input: "bad", continuation_key: "page"),
       {:error, {:invalid_payload, :expected_map}}},
      {"missing key", Keyword.put(base, :input, %{}),
       {:error, {:invalid_option, {:continuation_key, :missing}}}},
      {"invalid key", Keyword.merge(base, input: %{}, continuation_key: "bad key"),
       {:error, {:invalid_option, {:continuation_key, :invalid}}}}
    ]

    for test_case <- cases do
      {label, opts, expected, run_id} = normalize_validation_case(test_case)

      assert Jizoku.continue_as_new(run_id, opts) == expected, label
      refute_receive {:storage_call, _callback}
    end
  end

  test "rejects trigger-invalid input before writing a fence" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()
    before_rejection = predecessor_state()

    assert {:error, {:invalid_payload, _details}} =
             Jizoku.continue_as_new(@run_id, continuation_opts(%{batch_size: 10}))

    assert predecessor_state() == before_rejection
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.continuation_fence(dispatch_agent, @run_id) == nil
  end

  test "rejects available work before writing a fence" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    attempt = seed_started_predecessor()
    append_applied(attempt.runnable_key)
    before_rejection = predecessor_state()

    assert {:error, {:unsafe_continuation, {:dispatch_blockers, blockers}}} =
             Jizoku.continue_as_new(
               @run_id,
               continuation_opts(%{cursor: "next", batch_size: 100})
             )

    assert Enum.any?(blockers, &(&1.reason == :available_attempt))
    assert predecessor_state() == before_rejection
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.continuation_fence(dispatch_agent, @run_id) == nil
  end

  test "terminalizes the predecessor and returns one fresh successor" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    previous_history = Application.fetch_env(:jizoku, :continuation_history)

    Application.put_env(:jizoku, :continuation_history,
      run_warning_threshold: 4,
      run_critical_threshold: 100
    )

    on_exit(fn -> restore_history(previous_history) end)
    seed_quiescent_predecessor()

    assert {:ok, successor} =
             Jizoku.continue_as_new(@run_id,
               input: %{cursor: "next"},
               continuation_key: "page-42",
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               now: @now
             )

    assert successor.run_id != @run_id
    assert successor.input == %{cursor: "next", batch_size: 100}
    assert successor.terminal? == false

    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    predecessor = Projection.rebuild(predecessor_entries)

    assert predecessor.terminal_status == :continued
    assert predecessor.continued_to_run_id == successor.run_id

    assert {:ok, successor_entries} = Journal.load_entries(@storage, {:run, successor.run_id})
    successor_started = Enum.find(successor_entries, &(&1.type == :run_started))

    assert successor_started.data.trace.trace_id == predecessor.trace.trace_id
    assert successor_started.data.trace.parent_span_id == predecessor.trace.span_id
    assert successor_started.data.trace.causation_id == "continuation:#{successor.run_id}"

    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.pending_continuation_fences(dispatch_agent) == []

    assert successor.continuation == %{
             continued_from: %{run_id: @run_id, continuation_key: "page-42"},
             continued_to: nil
           }

    assert successor.history.thread_revision == 4
    assert successor.history.level == :warning

    assert {:ok, predecessor_snapshot} =
             Jizoku.inspect_run(@run_id, journal_storage: @storage, now: @now)

    assert predecessor_snapshot.continuation.continued_to == %{
             run_id: successor.run_id,
             continuation_key: "page-42"
           }

    assert {:ok, summaries} =
             Jizoku.list_runs([], journal_storage: @storage, now: @now)

    predecessor_summary = Enum.find(summaries, &(&1.run_id == @run_id))
    successor_summary = Enum.find(summaries, &(&1.run_id == successor.run_id))
    assert predecessor_summary.continuation == predecessor_snapshot.continuation
    assert successor_summary.continuation == successor.continuation
    assert predecessor_summary.history.thread_revision == predecessor_summary.thread_revision
    assert successor_summary.history.level == :warning

    assert {:ok, graph} =
             Jizoku.inspect_run_graph(successor.run_id, journal_storage: @storage, now: @now)

    assert graph.continuation == successor.continuation

    assert graph.continuation_links == [
             %{
               id: Enum.join([@run_id, "continuation", successor.run_id], ":"),
               from: @run_id,
               to: successor.run_id,
               type: :continuation,
               continuation_key: "page-42"
             }
           ]

    assert {:ok, predecessor_graph} =
             Jizoku.inspect_run_graph(@run_id, journal_storage: @storage, now: @now)

    assert predecessor_graph.continuation_links == graph.continuation_links

    assert {:ok, predecessor_timeline} =
             Jizoku.inspect_run_timeline(@run_id, journal_storage: @storage, now: @now)

    assert %{details: %{run_id: successor_run_id, continuation_key: "page-42"}} =
             Enum.find(predecessor_timeline.events, &(&1.type == :run_continued_to))

    assert successor_run_id == successor.run_id

    assert {:ok, successor_timeline} =
             Jizoku.inspect_run_timeline(successor.run_id,
               journal_storage: @storage,
               now: @now
             )

    assert %{details: %{run_id: @run_id, continuation_key: "page-42"}} =
             Enum.find(successor_timeline.events, &(&1.type == :run_continued_from))

    assert {:ok, explanation} =
             Jizoku.explain_run(@run_id, journal_storage: @storage, now: @now)

    assert explanation.summary ==
             "The run continued as a fresh successor with durable lineage."

    assert :inspect_continuation_successor in explanation.next_actions
    assert explanation.evidence.continuation == predecessor_snapshot.continuation

    assert explanation.details.continued_to == %{
             run_id: successor.run_id,
             continuation_key: "page-42"
           }
  end

  test "continuation chain inspection remains bounded and reports large chains" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    previous_history = Application.fetch_env(:jizoku, :continuation_history)

    Application.put_env(:jizoku, :continuation_history,
      chain_warning_hops: 2,
      max_chain_hops: 10
    )

    on_exit(fn -> restore_history(previous_history) end)

    seed_quiescent_predecessor()

    run_ids =
      Enum.reduce(1..5, [@run_id], fn cursor, [current_run_id | _rest] = run_ids ->
        assert {:ok, successor} =
                 Jizoku.continue_as_new(current_run_id,
                   input: %{cursor: "page-#{cursor}"},
                   continuation_key: "page-#{cursor}",
                   runtime: :journal,
                   journal_storage: @storage,
                   queue: "default",
                   now: DateTime.add(@now, cursor, :second)
                 )

        if cursor < 5 do
          quiesce_existing_run(successor.run_id, cursor)
        end

        [successor.run_id | run_ids]
      end)

    [latest_run_id | _rest] = run_ids
    recording_storage = {TestStorage, delegate: @storage, record?: true, test_pid: self()}

    assert {:ok, chain} =
             Jizoku.inspect_continuation_chain(latest_run_id,
               journal_storage: recording_storage,
               direction: :backward,
               max_hops: 2
             )

    assert chain.hops == 2
    assert chain.truncated? == true
    assert [_latest, _previous, _oldest] = chain.runs
    assert Enum.map(chain.runs, & &1.run_id) == Enum.take(run_ids, 3)
    assert chain.warnings == [%{code: :large_continuation_chain, hops: 2, threshold: 2}]
    assert_receive {:storage_load, _first_run}
    assert_receive {:storage_load, _second_run}
    assert_receive {:storage_load, _third_run}
    refute_receive {:storage_load, _unbounded_run}

    assert {:ok, forward_chain} =
             Jizoku.inspect_continuation_chain(@run_id,
               journal_storage: recording_storage,
               direction: :forward,
               max_hops: 10
             )

    assert forward_chain.hops == 5
    assert forward_chain.truncated? == false
    assert Enum.map(forward_chain.runs, & &1.run_id) == Enum.reverse(run_ids)

    for _load <- 1..6 do
      assert_receive {:storage_load, _run}
    end

    refute_receive {:storage_load, _run_after_natural_end}
  end

  test "continuation chain inspection rejects malformed option lists" do
    assert Jizoku.inspect_continuation_chain(@run_id, [:bad]) ==
             {:error, {:invalid_option, {:opts, :invalid}}}
  end

  test "returns the same successor on exact retry and rejects changed input" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()
    opts = continuation_opts(%{cursor: "next"})

    assert {:ok, first} = Jizoku.continue_as_new(@run_id, opts)
    before_retry = journal_state(first.run_id)

    recording_storage =
      {TestStorage, delegate: @storage, record?: true, test_pid: self()}

    assert {:ok, duplicate} =
             Jizoku.continue_as_new(
               @run_id,
               opts
               |> Keyword.put(:journal_storage, recording_storage)
               |> Keyword.put(:now, DateTime.add(@now, 60, :second))
             )

    assert duplicate.run_id == first.run_id
    assert journal_state(first.run_id) == before_retry
    refute_receive {:storage_append, _thread_id, _kinds}
    refute_receive {:storage_checkpoint_put, _key}

    assert {:error, :conflicting_continuation_fence} =
             Jizoku.continue_as_new(
               @run_id,
               Keyword.put(opts, :input, %{cursor: "different", batch_size: 100})
             )

    assert journal_state(first.run_id) == before_retry

    assert {:error, :conflicting_continuation_fence} =
             Jizoku.continue_as_new(
               @run_id,
               Keyword.put(opts, :continuation_key, "page-43")
             )

    assert journal_state(first.run_id) == before_retry
  end

  test "exact retry does not depend on the current workflow module" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    workflow = compile_ephemeral_workflow()
    seed_quiescent_predecessor(@storage, "default", @run_id, workflow)
    opts = continuation_opts(%{cursor: "next"})

    assert {:ok, first} = Jizoku.continue_as_new(@run_id, opts)
    before_retry = journal_state(first.run_id, workflow)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert :ok = DispatchAgent.put_checkpoint(@storage, dispatch_agent, updated_at: @now)
    :code.purge(workflow)
    :code.delete(workflow)
    refute Code.ensure_loaded?(workflow)

    recording_storage =
      {TestStorage, delegate: @storage, record?: true, test_pid: self()}

    assert {:ok, duplicate} =
             Jizoku.continue_as_new(
               @run_id,
               Keyword.put(opts, :journal_storage, recording_storage)
             )

    assert duplicate.run_id == first.run_id
    assert journal_state(first.run_id, workflow) == before_retry
    refute_receive {:storage_append, _thread_id, _kinds}
    refute_receive {:storage_checkpoint_put, _key}
  end

  test "concurrent exact commands converge on one successor" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()
    ref = make_ref()
    barrier_storage = barrier_storage(ref)
    opts = continuation_opts(%{cursor: "next"}, barrier_storage)

    first_task = Task.async(fn -> Jizoku.continue_as_new(@run_id, opts) end)
    second_task = Task.async(fn -> Jizoku.continue_as_new(@run_id, opts) end)

    assert_receive {:fence_append_blocked, ^ref, first_blocked_pid}
    assert_receive {:fence_append_blocked, ^ref, second_blocked_pid}

    assert MapSet.new([first_blocked_pid, second_blocked_pid]) ==
             MapSet.new([first_task.pid, second_task.pid])

    send(first_task.pid, {:release_fence_append, ref})
    assert {:ok, first} = Task.await(first_task)
    send(second_task.pid, {:release_fence_append, ref})
    assert {:ok, second} = Task.await(second_task)

    assert first.run_id == second.run_id
    assert_single_continuation(first.run_id)
  end

  test "a concurrent conflicting command cannot create another successor" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()
    ref = make_ref()
    barrier_storage = barrier_storage(ref)

    first_task =
      Task.async(fn ->
        Jizoku.continue_as_new(
          @run_id,
          continuation_opts(%{cursor: "next"}, barrier_storage)
        )
      end)

    conflicting_task =
      Task.async(fn ->
        Jizoku.continue_as_new(
          @run_id,
          Keyword.put(
            continuation_opts(%{cursor: "next"}, barrier_storage),
            :continuation_key,
            "page-43"
          )
        )
      end)

    assert_receive {:fence_append_blocked, ^ref, first_blocked_pid}
    assert_receive {:fence_append_blocked, ^ref, conflicting_blocked_pid}

    assert MapSet.new([first_blocked_pid, conflicting_blocked_pid]) ==
             MapSet.new([first_task.pid, conflicting_task.pid])

    send(first_task.pid, {:release_fence_append, ref})
    assert {:ok, first} = Task.await(first_task)
    send(conflicting_task.pid, {:release_fence_append, ref})
    assert {:error, :conflicting_continuation_fence} = Task.await(conflicting_task)

    assert_single_continuation(first.run_id)
  end

  test "a dispatch schedule that wins the fence CAS leaves the predecessor untouched" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()
    ref = make_ref()
    barrier_storage = barrier_storage(ref)
    before_state = scoped_state(@storage, nil)

    task =
      Task.async(fn ->
        Jizoku.continue_as_new(
          @run_id,
          continuation_opts(%{cursor: "next"}, barrier_storage)
        )
      end)

    assert_receive {:fence_append_blocked, ^ref, task_pid}
    assert task_pid == task.pid
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    late_runnable = %{
      run_id: @run_id,
      runnable_key: "#{@run_id}:late:1",
      idempotency_key: "#{@run_id}:late:1",
      attempt_number: 1,
      step: "late",
      input: %{},
      queue: "default",
      visible_at: @now
    }

    assert {:ok, _update} =
             DispatchAgent.schedule_attempts(
               @storage,
               dispatch_agent,
               @run_id,
               [late_runnable],
               now: @now
             )

    send(task_pid, {:release_fence_append, ref})

    assert {:error, {:unsafe_continuation, {:dispatch_blockers, blockers}}} =
             Task.await(task)

    assert Enum.any?(blockers, &(&1.reason == :available_attempt))
    after_state = scoped_state(@storage, nil)

    assert Map.drop(after_state, [{:dispatch, "default"}]) ==
             Map.drop(before_state, [{:dispatch, "default"}])

    assert {:ok, before_dispatch} = before_state[{:dispatch, "default"}]
    assert {:ok, after_dispatch} = after_state[{:dispatch, "default"}]

    assert [%{type: :attempt_scheduled, data: %{runnable_key: late_key}}] =
             Enum.drop(after_dispatch, length(before_dispatch))

    assert late_key == late_runnable.runnable_key
    assert {:ok, rebuilt} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.continuation_fence(rebuilt, @run_id) == nil
  end

  test "rejects workflow-side dynamic state before writing a fence" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor()

    append_run_fact(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "dynamic-1",
      origin: %{runnable_key: "origin"},
      nodes: [],
      occurred_at: @now
    })

    before_rejection = predecessor_state()

    assert {:error, {:unsafe_continuation, :dynamic_work}} =
             Jizoku.continue_as_new(
               @run_id,
               continuation_opts(%{cursor: "next", batch_size: 100})
             )

    assert predecessor_state() == before_rejection
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert DispatchAgent.continuation_fence(dispatch_agent, @run_id) == nil
  end

  test "routes continuation to the configured queue and partition only" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    assert {:ok, acme_storage} = Storage.scope(@storage, "tenant_acme")
    assert {:ok, globex_storage} = Storage.scope(@storage, "tenant_globex")

    seed_quiescent_predecessor(acme_storage, "priority", @run_id)
    seed_quiescent_predecessor(globex_storage, "priority", @run_id)
    globex_before = scoped_state(globex_storage, nil)

    assert {:ok, successor} =
             Jizoku.continue_as_new(@run_id,
               input: %{cursor: "next", batch_size: 100},
               continuation_key: "page-42",
               runtime: :journal,
               journal_storage: @storage,
               partition: "tenant_acme",
               queue: "priority",
               now: @now
             )

    assert successor.partition == "tenant_acme"
    assert {:ok, acme_dispatch} = DispatchAgent.rebuild(acme_storage, "priority")
    assert DispatchAgent.continuation_repair(acme_dispatch, @run_id)
    assert {:error, :not_found} = Journal.load_entries(acme_storage, {:dispatch, "default"})
    assert scoped_state(globex_storage, nil) == globex_before
    assert {:error, :not_found} = Journal.load_entries(globex_storage, {:run, successor.run_id})
  end

  test "rejects a queue override that does not match the durable plan" do
    Application.put_env(:jizoku, :continuation_fences, :enabled)
    seed_quiescent_predecessor(@storage, "priority", @run_id)
    before_rejection = scoped_state(@storage, nil)

    assert {:error, {:unsafe_continuation, :queue_mismatch}} =
             Jizoku.continue_as_new(@run_id,
               input: %{cursor: "next", batch_size: 100},
               continuation_key: "page-42",
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               now: @now
             )

    assert scoped_state(@storage, nil) == before_rejection
    assert {:ok, priority_dispatch} = DispatchAgent.rebuild(@storage, "priority")
    assert DispatchAgent.continuation_fence(priority_dispatch, @run_id) == nil
  end

  defp seed_quiescent_predecessor(
         storage \\ @storage,
         queue \\ "default",
         run_id \\ @run_id,
         workflow \\ CursorWorkflow
       ) do
    attempt = seed_started_predecessor(storage, queue, run_id, workflow)

    assert {:ok, available_agent} = DispatchAgent.rebuild(storage, queue)

    assert {:ok, %{agent: claimed_agent, attempt: claimed_attempt}} =
             DispatchAgent.claim_next(storage, available_agent, "worker-1",
               claim_id: "claim-1",
               claim_token: "token-1",
               now: @now
             )

    assert {:ok, %{agent: _completed_agent}} =
             DispatchAgent.complete(
               storage,
               claimed_agent,
               claimed_attempt.runnable_key,
               "claim-1",
               "token-1",
               %{cursor: "current"},
               now: @now
             )

    append_applied(storage, run_id, attempt.runnable_key)
  end

  defp seed_started_predecessor(
         storage \\ @storage,
         queue \\ "default",
         run_id \\ @run_id,
         workflow \\ CursorWorkflow
       ) do
    assert {:ok, _snapshot} =
             Starter.start_run(workflow, :continue, %{cursor: "current"},
               journal_storage: storage,
               queue: queue,
               run_id: run_id,
               now: @now
             )

    assert {:ok, available_agent} = DispatchAgent.rebuild(storage, queue)

    [attempt] = DispatchAgent.visible_attempts(available_agent, @now)
    attempt
  end

  defp append_applied(runnable_key) do
    append_applied(@storage, @run_id, runnable_key)
  end

  defp append_applied(storage, run_id, runnable_key) do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: run_id,
               runnable_key: runnable_key,
               result: %{cursor: "current"},
               occurred_at: @now
             })

    assert {:ok, thread} = Journal.load_thread(storage, {:run, run_id})

    assert {:ok, _thread} =
             Journal.append_entries(storage, [entry], expected_rev: thread.rev)
  end

  defp quiesce_existing_run(run_id, cursor) do
    assert {:ok, available_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, attempt: claimed_attempt}} =
             DispatchAgent.claim_next(@storage, available_agent, "chain-worker-#{cursor}",
               claim_id: "chain-claim-#{cursor}",
               claim_token: "chain-token-#{cursor}",
               now: DateTime.add(@now, cursor, :second)
             )

    assert claimed_attempt.run_id == run_id

    assert {:ok, %{agent: _completed_agent}} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               claimed_attempt.runnable_key,
               "chain-claim-#{cursor}",
               "chain-token-#{cursor}",
               %{cursor: "page-#{cursor}"},
               now: DateTime.add(@now, cursor, :second)
             )

    append_applied(@storage, run_id, claimed_attempt.runnable_key)
  end

  defp normalize_validation_case({label, opts, expected, run_id}) do
    {label, opts, expected, run_id}
  end

  defp normalize_validation_case({label, opts, expected}) do
    {label, opts, expected, @run_id}
  end

  defp continuation_opts(input) do
    continuation_opts(input, @storage)
  end

  defp continuation_opts(input, storage) do
    [
      input: input,
      continuation_key: "page-42",
      runtime: :journal,
      journal_storage: storage,
      queue: "default",
      now: @now
    ]
  end

  defp journal_state(successor_run_id, workflow \\ CursorWorkflow) do
    Map.new(
      [
        {:run, @run_id},
        {:run, successor_run_id},
        {:run_index, Definition.serialize_workflow(workflow)},
        {:dispatch, "default"},
        {:run_catalog, "all"}
      ],
      fn thread -> {thread, Journal.load_entries(@storage, thread)} end
    )
  end

  defp append_run_fact(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    assert {:ok, thread} = Journal.load_thread(@storage, {:run, @run_id})
    assert {:ok, _thread} = Journal.append_entries(@storage, [entry], expected_rev: thread.rev)
  end

  defp barrier_storage(ref) do
    {TestStorage, delegate: @storage, barrier_ref: ref, test_pid: self()}
  end

  defp assert_single_continuation(successor_run_id) do
    assert {:ok, predecessor_entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(predecessor_entries, &(&1.type == :run_continuation_requested)) == 1
    assert Enum.count(predecessor_entries, &(&1.type == :run_terminal)) == 1

    assert {:ok, successor_entries} = Journal.load_entries(@storage, {:run, successor_run_id})
    assert Enum.count(successor_entries, &(&1.type == :run_continued_from)) == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(dispatch_entries, &(&1.type == :run_continuation_fenced)) == 1
    assert Enum.count(dispatch_entries, &(&1.type == :run_continuation_repaired)) == 1

    assert {:ok, catalog_entries} = Journal.load_entries(@storage, {:run_catalog, "all"})

    cataloged_run_ids =
      catalog_entries
      |> Enum.filter(&(&1.type == :run_cataloged))
      |> Enum.map(& &1.data.run_id)
      |> Enum.sort()

    assert cataloged_run_ids == Enum.sort([@run_id, successor_run_id])

    assert {:ok, index_entries} =
             Journal.load_entries(
               @storage,
               {:run_index, Definition.serialize_workflow(CursorWorkflow)}
             )

    indexed_run_ids =
      index_entries
      |> Enum.filter(&(&1.type == :run_indexed))
      |> Enum.map(& &1.data.run_id)
      |> Enum.sort()

    assert indexed_run_ids == Enum.sort([@run_id, successor_run_id])
  end

  defp compile_ephemeral_workflow do
    workflow = Jizoku.ContinueAsNewTest.EphemeralWorkflow
    action = RecordCursor
    :code.purge(workflow)
    :code.delete(workflow)

    Code.compile_quoted(
      quote do
        defmodule unquote(workflow) do
          use Jizoku.Workflow

          workflow do
            trigger :continue do
              manual()

              payload do
                field :cursor, :string
                field :batch_size, :integer, default: 100
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

  defp predecessor_state do
    Map.new(
      [
        {:run, @run_id},
        {:dispatch, "default"},
        {:dispatch, "priority"},
        {:run_index, Definition.serialize_workflow(CursorWorkflow)},
        {:run_catalog, "all"}
      ],
      fn thread -> {thread, Journal.load_entries(@storage, thread)} end
    )
  end

  defp scoped_state(storage, successor_run_id) do
    threads =
      maybe_add_successor(
        [
          {:run, @run_id},
          {:dispatch, "default"},
          {:dispatch, "priority"},
          {:run_index, Definition.serialize_workflow(CursorWorkflow)},
          {:run_catalog, "all"}
        ],
        successor_run_id
      )

    Map.new(threads, fn thread -> {thread, Journal.load_entries(storage, thread)} end)
  end

  defp maybe_add_successor(threads, nil) do
    threads
  end

  defp maybe_add_successor(threads, successor_run_id) do
    [{:run, successor_run_id} | threads]
  end

  defp restore_activation({:ok, value}) do
    Application.put_env(:jizoku, :continuation_fences, value)
  end

  defp restore_activation(:error) do
    Application.delete_env(:jizoku, :continuation_fences)
  end

  defp restore_history({:ok, value}) do
    Application.put_env(:jizoku, :continuation_history, value)
  end

  defp restore_history(:error) do
    Application.delete_env(:jizoku, :continuation_history)
  end

  defp cleanup_storage do
    for table <- [
          :jizoku_public_continue_as_new_test_checkpoints,
          :jizoku_public_continue_as_new_test_threads,
          :jizoku_public_continue_as_new_test_thread_meta
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
