defmodule Squidie.GraphMutationApplyTest do
  use ExUnit.Case, async: false

  alias Squidie.GraphMutation.Reconciliation
  alias Squidie.GraphMutation.Report
  alias Squidie.Runtime.Journal

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
      if thread_id == Keyword.get(opts, :fail_append_thread_id) do
        {:error, :append_failed}
      else
        {adapter, delegate_opts} = delegate(opts)

        adapter.append_thread(
          thread_id,
          entries,
          Keyword.merge(delegate_opts, Keyword.take(opts, [:expected_rev]))
        )
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp delegate(opts) do
      case Keyword.fetch!(opts, :delegate) do
        {adapter, delegate_opts} -> {adapter, delegate_opts}
        adapter when is_atom(adapter) -> {adapter, []}
      end
    end
  end

  defmodule OriginAction do
    use Squidie.Step, name: :origin

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{origin: true}}
    end
  end

  defmodule HoldAction do
    use Squidie.Step, name: :hold

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{hold: true}}
    end
  end

  defmodule AddedAction do
    use Squidie.Step,
      name: :added,
      input_schema: [account_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{added: true}}
    end
  end

  defmodule Workflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :origin, OriginAction
      step :hold, HoldAction

      transition :origin, on: :ok, to: :hold
      transition :hold, on: :ok, to: :complete
    end
  end

  @storage {Jido.Storage.ETS, table: :squidie_graph_mutation_apply_test}
  @run_id "0190a4f1-0a7c-7cb1-80c5-b4f8b1d23009"
  @queue "default"
  @dynamic_queue "dynamic"
  @now ~U[2026-07-18 10:00:00Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)

    assert {:ok, _snapshot} =
             Squidie.start(Workflow, %{},
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               run_id: @run_id,
               now: @now
             )

    assert {:ok, _snapshot} =
             Squidie.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               owner_id: "graph-mutation-setup",
               now: @now
             )

    :ok
  end

  test "atomically commits mutation intent and returns exact duplicates" do
    assert {:ok, %Report{} = report} =
             Squidie.apply_graph_mutation(@run_id, mutation(), apply_options())

    assert report.status == :committed
    assert report.base_version == 0
    assert report.result_version == 1
    assert report.reconciliation == :completed
    assert scheduled_steps() == ["child"]

    assert {:ok, run_thread} = Journal.load_thread(@storage, {:run, @run_id})

    assert Enum.map(Enum.take(run_thread.entries, -2), & &1.type) == [
             :dynamic_graph_mutated,
             :runnables_planned
           ]

    revisions = thread_revisions()
    duplicate_options = Keyword.put(apply_options(), :action_registry, %{})

    assert {:ok, %Report{} = duplicate} =
             Squidie.apply_graph_mutation(@run_id, mutation(), duplicate_options)

    assert duplicate.status == :duplicate
    assert duplicate.duplicate?
    assert duplicate.result_version == 1
    assert thread_revisions() == revisions

    conflicting = put_in(mutation(), [:additions, Access.at(0), :input], %{account_id: "other"})

    assert Squidie.apply_graph_mutation(@run_id, conflicting, apply_options()) ==
             {:error, {:invalid_graph_mutation, {:mutation_id, {:conflict, "mutation-apply"}}}}

    stale = %{mutation() | mutation_id: "mutation-stale"}

    assert Squidie.apply_graph_mutation(@run_id, stale, apply_options()) ==
             {:error, {:invalid_graph_mutation, {:expected_version, {:stale, 1}}}}
  end

  test "reports committed dispatch failure and public reconciliation repairs it" do
    failing_storage =
      {FaultInjectingStorage,
       delegate: @storage, fail_append_thread_id: Journal.thread_id({:dispatch, @dynamic_queue})}

    options = Keyword.put(apply_options(), :journal_storage, failing_storage)

    assert {:ok, %Report{} = report} =
             Squidie.apply_graph_mutation(@run_id, mutation(), options)

    assert report.status == :committed_needs_reconciliation
    assert report.reconciliation == :required
    assert report.warnings == [:reconciliation_required]
    assert scheduled_steps() == []

    assert {:ok, pending_snapshot} =
             Squidie.inspect_run(@run_id,
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               now: @now
             )

    assert pending_snapshot.graph_version == 1
    assert pending_snapshot.active_node_ids == ["child"]
    assert pending_snapshot.ready_node_ids == ["child"]
    assert pending_snapshot.blocked_node_ids == []
    assert pending_snapshot.reconciliation_status == :required

    assert [
             %{
               mutation_id: "mutation-apply",
               origin: "origin",
               expected_version: 0,
               result_version: 1,
               added_node_ids: ["child"],
               added_edge_ids: ["origin-child"],
               removed_node_ids: [],
               removed_edge_ids: []
             }
           ] = pending_snapshot.mutation_history

    refute inspect(pending_snapshot.mutation_history) =~ "account-123"

    assert {:ok, pending_graph} =
             Squidie.inspect_run_graph(@run_id,
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               now: @now
             )

    pending_payload = Squidie.Runs.GraphInspection.to_map(pending_graph)

    assert pending_payload.graph_version == 1

    assert pending_payload.graph_provenance.nodes == [
             %{id: "child", provenance: :dependency_ordered}
           ]

    assert pending_payload.reconciliation_status == :required
    refute inspect(pending_payload) =~ "account-123"

    assert {:ok, %Reconciliation{} = reconciliation} =
             Squidie.reconcile_dynamic_graph(@run_id,
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               now: @now
             )

    assert reconciliation.graph_version == 1
    assert reconciliation.status == :reconciled
    assert reconciliation.repaired_queue_ids == [@dynamic_queue]
    assert reconciliation.scheduled_node_ids == ["child"]
    assert scheduled_steps() == ["child"]

    assert {:ok, reconciled_snapshot} =
             Squidie.inspect_run(@run_id,
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               now: @now
             )

    assert reconciled_snapshot.reconciliation_status == :completed

    assert {:ok, %Reconciliation{} = repeated} =
             Squidie.reconcile_dynamic_graph(@run_id,
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               now: @now
             )

    assert repeated.repaired_queue_ids == []
    assert repeated.scheduled_node_ids == []
  end

  test "failed run append creates no mutation or dynamic dispatch facts" do
    failing_storage =
      {FaultInjectingStorage,
       delegate: @storage, fail_append_thread_id: Journal.thread_id({:run, @run_id})}

    options = Keyword.put(apply_options(), :journal_storage, failing_storage)

    assert Squidie.apply_graph_mutation(@run_id, mutation(), options) ==
             {:error, :append_failed}

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries, &(&1.type == :dynamic_graph_mutated))
    assert Journal.load_entries(@storage, {:dispatch, @dynamic_queue}) == {:error, :not_found}
  end

  test "rejects unsupported and malformed public options" do
    assert Squidie.apply_graph_mutation(@run_id, mutation(), unknown: true) ==
             {:error, {:invalid_option, {:option, :unknown}}}

    assert Squidie.apply_graph_mutation(@run_id, mutation(), :invalid) ==
             {:error, {:invalid_option, {:opts, :invalid}}}

    assert Squidie.reconcile_dynamic_graph(@run_id, limits: %{}) ==
             {:error, {:invalid_option, {:option, :limits}}}

    assert Squidie.reconcile_dynamic_graph(@run_id, :invalid) ==
             {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp mutation do
    %{
      mutation_id: "mutation-apply",
      expected_version: 0,
      origin: "origin",
      additions: [
        %{
          kind: :node,
          id: "child",
          action: "added",
          input: %{account_id: "account-123"},
          queue: @dynamic_queue
        },
        %{kind: :edge, id: "origin-child", from: "origin", to: "child"}
      ],
      removals: []
    }
  end

  defp apply_options do
    [
      runtime: :journal,
      journal_storage: @storage,
      queue: @queue,
      now: @now,
      limits: %{
        max_nodes_per_mutation: 10,
        max_edges_per_mutation: 10,
        max_active_nodes_per_run: 10,
        max_active_edges_per_run: 10
      },
      action_registry: %{"added" => AddedAction}
    ]
  end

  defp scheduled_steps do
    case Journal.load_entries(@storage, {:dispatch, @dynamic_queue}) do
      {:ok, entries} ->
        entries
        |> Enum.filter(&(&1.type == :attempt_scheduled))
        |> Enum.map(& &1.data.step)

      {:error, :not_found} ->
        []
    end
  end

  defp thread_revisions do
    {:ok, run_thread} = Journal.load_thread(@storage, {:run, @run_id})
    {:ok, dispatch_thread} = Journal.load_thread(@storage, {:dispatch, @dynamic_queue})
    {run_thread.rev, dispatch_thread.rev}
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
      :squidie_graph_mutation_apply_test_checkpoints,
      :squidie_graph_mutation_apply_test_threads,
      :squidie_graph_mutation_apply_test_thread_meta
    ]
  end
end
