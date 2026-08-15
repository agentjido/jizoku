defmodule Jizoku.Runtime.WorkflowAgent.LegacyGraphProjectionTest do
  use ExUnit.Case, async: false

  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @storage {Jido.Storage.ETS, table: :jizoku_legacy_graph_projection_test}
  @run_id "legacy_graph_run"
  @workflow "LegacyGraphWorkflow"
  @started_at ~U[2026-07-15 00:00:00Z]
  @recorded_at ~U[2026-07-15 00:00:10Z]

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "declared workflow facts keep the semantic graph version at zero" do
    runnables_planned =
      entry!(:runnables_planned, %{
        run_id: @run_id,
        runnables: [
          %{runnable_key: "legacy_graph_run:charge_card:1", step: "charge_card"}
        ],
        occurred_at: @recorded_at
      })

    projection = Projection.rebuild([run_started_entry(), runnables_planned])

    assert projection.graph.version == 0
    assert projection.graph.provenance == %{nodes: %{}, edges: %{}}
    assert projection.graph.active_node_ids == MapSet.new()
    assert projection.graph.active_edge_ids == MapSet.new()
  end

  test "accepted legacy records advance graph state while duplicates and conflicts do not" do
    entries = legacy_entries()
    projection = Projection.rebuild(entries)

    assert projection.graph.version == 2

    assert projection.graph.provenance == %{
             nodes: %{
               "deliver_email" => :legacy_eager,
               "deliver_sms" => :legacy_eager
             },
             edges: %{
               "charge_card:dynamic:deliver_email" => :legacy_eager,
               "charge_card:dynamic:deliver_sms" => :legacy_eager
             }
           }

    assert projection.graph.active_node_ids == MapSet.new(["deliver_email", "deliver_sms"])

    assert projection.graph.active_edge_ids ==
             MapSet.new([
               "charge_card:dynamic:deliver_email",
               "charge_card:dynamic:deliver_sms"
             ])

    assert projection.graph.reserved_node_ids == projection.graph.active_node_ids
    assert projection.graph.reserved_edge_ids == projection.graph.active_edge_ids
    assert projection.graph.tombstoned_node_ids == MapSet.new()
    assert projection.graph.tombstoned_edge_ids == MapSet.new()
    assert projection.graph.mutation_history == %{}

    assert Enum.all?(Projection.dynamic_work(projection), fn work ->
             work.provenance == :legacy_eager
           end)

    assert [%{reason: :conflicting_dynamic_work}] = Projection.anomalies(projection)
  end

  test "checkpoint restoration preserves the legacy graph projection" do
    entries = legacy_entries()
    assert {:ok, %{rev: 5}} = Journal.append_entries(@storage, entries)

    assert {:ok, full_replay_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    expected = graph_state(full_replay_agent.state.projection)
    assert expected.graph.version == 2

    full_projection = Projection.rebuild(entries)

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, full_projection, 5,
               updated_at: @recorded_at
             )

    assert {:ok, current_checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    stale_projection = Projection.rebuild(Enum.take(entries, 2))

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, stale_projection, 2,
               updated_at: @recorded_at
             )

    assert {:ok, stale_checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    legacy_checkpoint = legacy_checkpoint(full_projection)

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, legacy_checkpoint, 5,
               updated_at: @recorded_at
             )

    assert {:ok, upgraded_checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    :ets.delete(checkpoint_table())
    assert {:ok, deleted_checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert graph_state(current_checkpoint_agent.state.projection) == expected
    assert graph_state(stale_checkpoint_agent.state.projection) == expected
    assert graph_state(upgraded_checkpoint_agent.state.projection) == expected
    assert graph_state(deleted_checkpoint_agent.state.projection) == expected
  end

  defp legacy_entries do
    first =
      dynamic_work_entry("fanout-email", "deliver_email", occurred_at: @recorded_at)

    duplicate =
      dynamic_work_entry("fanout-email", "deliver_email",
        occurred_at: DateTime.add(@recorded_at, 1, :second)
      )

    conflict =
      dynamic_work_entry("fanout-email", "conflicting_email",
        occurred_at: DateTime.add(@recorded_at, 2, :second)
      )

    second =
      dynamic_work_entry("fanout-sms", "deliver_sms",
        occurred_at: DateTime.add(@recorded_at, 3, :second)
      )

    [run_started_entry(), first, duplicate, conflict, second]
  end

  defp run_started_entry do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      occurred_at: @started_at
    })
  end

  defp dynamic_work_entry(dynamic_key, node_id, opts) do
    entry!(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: dynamic_key,
      origin: %{
        runnable_key: "legacy_graph_run:charge_card:1",
        step: "charge_card",
        attempt: 1
      },
      nodes: [%{id: node_id}],
      occurred_at: Keyword.fetch!(opts, :occurred_at)
    })
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp graph_state(projection) do
    %{
      graph: projection.graph,
      dynamic_work: Projection.dynamic_work(projection)
    }
  end

  defp legacy_checkpoint(projection) do
    dynamic_work =
      Enum.map(projection.dynamic_work, fn work ->
        Map.delete(work, :provenance)
      end)

    projection
    |> Map.delete(:graph)
    |> Map.put(:dynamic_work, dynamic_work)
  end

  defp checkpoint_table do
    :jizoku_legacy_graph_projection_test_checkpoints
  end

  defp cleanup_storage do
    Enum.each(storage_tables(), &delete_table/1)
  end

  defp storage_tables do
    [
      :jizoku_legacy_graph_projection_test_checkpoints,
      :jizoku_legacy_graph_projection_test_threads,
      :jizoku_legacy_graph_projection_test_thread_meta
    ]
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
