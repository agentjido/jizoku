defmodule Jizoku.Runtime.Journal.GraphMutation.ReconcilerTest do
  use ExUnit.Case, async: false

  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.GraphMutation.Reconciler
  alias Jizoku.Runtime.WorkflowAgent

  defmodule FaultInjectingStorage do
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
      cond do
        thread_id == Keyword.get(opts, :reject_conflict_thread_id) ->
          {:error, :conflict}

        thread_id == Keyword.get(opts, :conflict_thread_id) ->
          append_before_conflict(thread_id, entries, opts)

        thread_id == Keyword.get(opts, :fail_append_thread_id) ->
          {:error, :append_failed}

        true ->
          append_to_delegate(thread_id, entries, opts)
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp append_before_conflict(thread_id, entries, opts) do
      case append_to_delegate(thread_id, entries, opts) do
        {:ok, _thread} -> {:error, :conflict}
        {:error, _reason} = error -> error
      end
    end

    defp append_to_delegate(thread_id, entries, opts) do
      {adapter, delegate_opts} = delegate(opts)

      adapter.append_thread(
        thread_id,
        entries,
        Keyword.merge(delegate_opts, Keyword.take(opts, [:expected_rev]))
      )
    end

    defp delegate(opts) do
      case Keyword.fetch!(opts, :delegate) do
        {adapter, delegate_opts} -> {adapter, delegate_opts}
        adapter when is_atom(adapter) -> {adapter, []}
      end
    end
  end

  @storage {Jido.Storage.ETS, table: :jizoku_graph_mutation_reconciler_test}
  @run_id "0190a4f1-0a7c-7cb1-80c5-b4f8b1d23008"
  @ready_queue "graph-ready"
  @blocked_queue "graph-blocked"
  @removed_queue "graph-removed"
  @now ~U[2026-07-18 09:00:00Z]

  setup do
    cleanup_storage()
    append_run_entries()
    on_exit(&cleanup_storage/0)
    :ok
  end

  test "repairs queue markers and schedules only active ready nodes" do
    assert {:ok, reconciliation} = Reconciler.reconcile(@storage, @run_id, now: @now)

    assert Enum.map(reconciliation.queues, & &1.queue) == [
             @blocked_queue,
             @ready_queue,
             @removed_queue
           ]

    assert Enum.all?(reconciliation.queues, & &1.run_queued?)
    assert scheduled_steps(@ready_queue) == ["ready"]
    assert scheduled_steps(@blocked_queue) == []
    assert scheduled_steps(@removed_queue) == []

    revisions = dispatch_revisions()

    assert {:ok, repeated} = Reconciler.reconcile(@storage, @run_id, now: @now)
    refute Enum.any?(repeated.queues, & &1.run_queued?)
    assert Enum.all?(repeated.queues, &(&1.scheduled_runnables == []))
    assert dispatch_revisions() == revisions
  end

  test "repairs an attempt after a marker-only partial dispatch failure" do
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, @ready_queue)

    assert {:ok, %{queued?: true}} =
             DispatchAgent.ensure_run_queued(@storage, dispatch_agent, @run_id, now: @now)

    failing_storage =
      {FaultInjectingStorage,
       delegate: @storage, fail_append_thread_id: Journal.thread_id({:dispatch, @ready_queue})}

    assert {:error, :append_failed} =
             Reconciler.reconcile(failing_storage, @run_id, now: @now)

    assert scheduled_steps(@ready_queue) == []

    assert {:ok, repaired} = Reconciler.reconcile(@storage, @run_id, now: @now)

    ready = Enum.find(repaired.queues, &(&1.queue == @ready_queue))
    refute ready.run_queued?
    assert Enum.map(ready.scheduled_runnables, & &1.step) == ["ready"]
    assert scheduled_steps(@ready_queue) == ["ready"]
  end

  test "recovers committed conflict results and remains idempotent after checkpoint loss" do
    conflicting_storage =
      {FaultInjectingStorage,
       delegate: @storage, conflict_thread_id: Journal.thread_id({:dispatch, @ready_queue})}

    assert {:ok, reconciliation} =
             Reconciler.reconcile(conflicting_storage, @run_id, now: @now)

    ready = Enum.find(reconciliation.queues, &(&1.queue == @ready_queue))
    refute ready.run_queued?
    assert ready.scheduled_runnables == []
    assert scheduled_steps(@ready_queue) == ["ready"]

    put_checkpoints()
    delete_checkpoints()
    revisions = dispatch_revisions()

    assert {:ok, repeated} = Reconciler.reconcile(@storage, @run_id, now: @now)
    assert Enum.all?(repeated.queues, &(&1.scheduled_runnables == []))
    assert dispatch_revisions() == revisions
  end

  test "rejects invalid reconciliation options before reading durable state" do
    assert Reconciler.reconcile(@storage, @run_id, now: :invalid) ==
             {:error, {:invalid_option, {:now, :invalid}}}

    assert Reconciler.reconcile(@storage, @run_id, queue: "invalid queue") ==
             {:error, {:invalid_option, {:queue, :invalid}}}
  end

  test "rejects an invalid durable runnable queue without writing dispatch state" do
    invalid_runnable = runnable("invalid-queue", "invalid queue")

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [runnables_planned_entry([invalid_runnable])])

    assert Reconciler.reconcile(@storage, @run_id, now: @now) ==
             {:error, {:invalid_option, {:queue, :invalid}}}
  end

  test "returns missing runs and bounded non-committing conflicts unchanged" do
    assert Reconciler.reconcile(@storage, "missing-run", now: @now) == {:error, :not_found}

    conflicting_storage =
      {FaultInjectingStorage,
       delegate: @storage, reject_conflict_thread_id: Journal.thread_id({:dispatch, @ready_queue})}

    assert Reconciler.reconcile(conflicting_storage, @run_id, now: @now) ==
             {:error, :conflict}
  end

  defp append_run_entries do
    entries = [
      entry!(:run_started, %{
        run_id: @run_id,
        workflow: "GraphMutationReconcilerWorkflow",
        occurred_at: @now
      }),
      runnables_planned_entry([runnable("origin", @ready_queue)]),
      entry!(:runnable_applied, %{
        run_id: @run_id,
        runnable_key: runnable_key("origin"),
        result: %{origin: true},
        occurred_at: @now
      }),
      mutation_entry("mutation-add", 0, 1, additions(), []),
      mutation_entry(
        "mutation-remove",
        1,
        2,
        [],
        [%{kind: :edge, id: "origin-removed"}, %{kind: :node, id: "removed"}]
      ),
      runnables_planned_entry([
        runnable("ready", @ready_queue, "mutation-add"),
        runnable("blocked", @blocked_queue, "mutation-add"),
        runnable("removed", @removed_queue, "mutation-add")
      ])
    ]

    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp additions do
    [
      node("ready", @ready_queue),
      node("blocked", @blocked_queue),
      node("removed", @removed_queue),
      edge("origin-ready", "origin", "ready"),
      edge("ready-blocked", "ready", "blocked"),
      edge("origin-removed", "origin", "removed")
    ]
  end

  defp mutation_entry(mutation_id, expected_version, result_version, additions, removals) do
    fingerprints =
      additions
      |> Enum.filter(&(&1.kind == :node))
      |> Map.new(&{&1.id, "intent-#{&1.id}"})

    entry!(:dynamic_graph_mutated, %{
      run_id: @run_id,
      mutation_id: mutation_id,
      expected_version: expected_version,
      result_version: result_version,
      origin: "origin",
      additions: additions,
      removals: removals,
      runnable_intent_fingerprints: fingerprints,
      occurred_at: @now
    })
  end

  defp runnables_planned_entry(runnables) do
    entry!(:runnables_planned, %{run_id: @run_id, runnables: runnables, occurred_at: @now})
  end

  defp runnable(step, queue, mutation_id \\ nil) do
    runnable = %{
      run_id: @run_id,
      runnable_key: runnable_key(step),
      idempotency_key: runnable_key(step),
      attempt_number: 1,
      queue: queue,
      step: step,
      input: %{},
      visible_at: @now
    }

    if mutation_id do
      Map.put(runnable, :graph_mutation, %{
        mutation_id: mutation_id,
        node_id: step,
        intent_fingerprint: "intent-#{step}"
      })
    else
      runnable
    end
  end

  defp runnable_key(step) do
    "#{@run_id}:#{step}:1"
  end

  defp node(id, queue) do
    %{kind: :node, id: id, action: "dynamic", input: %{}, queue: queue}
  end

  defp edge(id, from, to) do
    %{kind: :edge, id: id, from: from, to: to}
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp scheduled_steps(queue) do
    {:ok, entries} = Journal.load_entries(@storage, {:dispatch, queue})

    entries
    |> Enum.filter(&(&1.type == :attempt_scheduled))
    |> Enum.map(& &1.data.step)
  end

  defp dispatch_revisions do
    Map.new([@ready_queue, @blocked_queue, @removed_queue], fn queue ->
      {:ok, thread} = Journal.load_thread(@storage, {:dispatch, queue})
      {queue, thread.rev}
    end)
  end

  defp put_checkpoints do
    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert :ok = WorkflowAgent.put_checkpoint(@storage, workflow_agent, updated_at: @now)

    Enum.each([@ready_queue, @blocked_queue, @removed_queue], fn queue ->
      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, queue)
      assert :ok = DispatchAgent.put_checkpoint(@storage, dispatch_agent, updated_at: @now)
    end)
  end

  defp delete_checkpoints do
    table = :jizoku_graph_mutation_reconciler_test_checkpoints

    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  end

  defp cleanup_storage do
    Enum.each(storage_tables(), fn table ->
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end)
  rescue
    ArgumentError -> :ok
  end

  defp storage_tables do
    [
      :jizoku_graph_mutation_reconciler_test_checkpoints,
      :jizoku_graph_mutation_reconciler_test_threads,
      :jizoku_graph_mutation_reconciler_test_thread_meta
    ]
  end
end
