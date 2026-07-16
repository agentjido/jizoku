defmodule Squidie.Runtime.WorkflowAgent.MutationReplayTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  @storage {Jido.Storage.ETS, table: :squidie_mutation_replay_test}
  @run_id "mutation_replay_run"
  @workflow "MutationReplayWorkflow"
  @started_at ~U[2026-07-16 00:00:00Z]
  @mutated_at ~U[2026-07-16 00:00:10Z]

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "replays additions and removals into versioned permanent graph state" do
    added = Projection.rebuild([add_mutation_entry()])

    assert added.graph.version == 1

    assert added.graph.topology == %{
             nodes: %{
               "deliver_digest" => %{
                 kind: :node,
                 id: "deliver_digest",
                 action: "deliver",
                 input: %{"account_id" => "acct_123"},
                 queue: "priority"
               }
             },
             edges: %{
               "charge_to_digest" => %{
                 kind: :edge,
                 id: "charge_to_digest",
                 from: "charge_card",
                 to: "deliver_digest"
               }
             }
           }

    assert added.graph.provenance == %{
             nodes: %{"deliver_digest" => :dependency_ordered},
             edges: %{"charge_to_digest" => :dependency_ordered}
           }

    assert added.graph.active_node_ids == MapSet.new(["deliver_digest"])
    assert added.graph.active_edge_ids == MapSet.new(["charge_to_digest"])
    assert Map.has_key?(added.graph.mutation_history, "mutation-add")

    removed = Projection.replay(added, [remove_mutation_entry()])

    assert removed.graph.version == 2
    assert removed.graph.topology == %{nodes: %{}, edges: %{}}
    assert removed.graph.active_node_ids == MapSet.new()
    assert removed.graph.active_edge_ids == MapSet.new()
    assert removed.graph.reserved_node_ids == MapSet.new(["deliver_digest"])
    assert removed.graph.reserved_edge_ids == MapSet.new(["charge_to_digest"])
    assert removed.graph.tombstoned_node_ids == MapSet.new(["deliver_digest"])
    assert removed.graph.tombstoned_edge_ids == MapSet.new(["charge_to_digest"])

    assert Enum.sort(Map.keys(removed.graph.mutation_history)) == [
             "mutation-add",
             "mutation-remove"
           ]
  end

  test "treats an exact mutation duplicate as a replay no-op" do
    duplicate =
      mutation_entry(add_mutation_attrs(%{occurred_at: DateTime.add(@mutated_at, 1, :second)}))

    projection = Projection.rebuild([add_mutation_entry(), duplicate])

    assert projection.graph.version == 1
    assert map_size(projection.graph.mutation_history) == 1
    assert Projection.anomalies(projection) == []
  end

  test "records deterministic mutation replay anomalies without changing graph state" do
    base = Projection.rebuild([add_mutation_entry()])

    cases = [
      {mutation_entry(add_mutation_attrs(%{origin: "refund_card"})), :conflicting_graph_mutation},
      {mutation_entry(
         add_mutation_attrs(%{
           mutation_id: "mutation-gap",
           expected_version: 4,
           result_version: 5
         })
       ), :discontinuous_graph_version},
      {mutation_entry(
         add_mutation_attrs(%{
           mutation_id: "mutation-malformed",
           expected_version: 1,
           result_version: 2,
           additions: [%{kind: :node, id: "missing-action"}]
         })
       ), :malformed_graph_operations}
    ]

    Enum.each(cases, fn {entry, reason} ->
      projection = Projection.replay(base, [entry])

      assert projection.graph == base.graph
      assert [%{reason: ^reason}] = Projection.anomalies(projection)
    end)
  end

  test "records a runnable intent fingerprint mismatch" do
    runnable = %{
      runnable_key: "mutation_replay_run:deliver_digest:1",
      step: "deliver_digest",
      graph_mutation: %{
        mutation_id: "mutation-add",
        node_id: "deliver_digest",
        intent_fingerprint: "unexpected-intent"
      }
    }

    projection =
      Projection.rebuild([
        add_mutation_entry(),
        entry!(:runnables_planned, %{
          run_id: @run_id,
          runnables: [runnable],
          occurred_at: DateTime.add(@mutated_at, 1, :second)
        })
      ])

    assert [
             %{
               reason: :runnable_intent_fingerprint_mismatch,
               mutation_id: "mutation-add",
               runnable_key: "mutation_replay_run:deliver_digest:1"
             }
           ] = Projection.anomalies(projection)
  end

  test "checkpoint suffix replay and checkpoint loss rebuild identical graph state" do
    entries = [
      run_started_entry(),
      add_mutation_entry(),
      matching_runnables_entry(),
      remove_mutation_entry()
    ]

    assert {:ok, %{rev: 4}} = Journal.append_entries(@storage, entries)
    assert {:ok, full_replay_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    expected = replay_state(full_replay_agent)

    stale_projection =
      entries
      |> Enum.take(2)
      |> Projection.rebuild()
      |> legacy_graph_checkpoint()

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, stale_projection, 2,
               updated_at: @mutated_at
             )

    assert {:ok, stale_checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    :ets.delete(:squidie_mutation_replay_test_checkpoints)
    assert {:ok, checkpoint_loss_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert replay_state(stale_checkpoint_agent) == expected
    assert replay_state(checkpoint_loss_agent) == expected
  end

  defp add_mutation_entry do
    mutation_entry(add_mutation_attrs())
  end

  defp remove_mutation_entry do
    mutation_entry(%{
      run_id: @run_id,
      mutation_id: "mutation-remove",
      expected_version: 1,
      result_version: 2,
      origin: "charge_card",
      additions: [],
      removals: [
        %{kind: :edge, id: "charge_to_digest"},
        %{kind: :node, id: "deliver_digest"}
      ],
      runnable_intent_fingerprints: %{},
      occurred_at: DateTime.add(@mutated_at, 2, :second)
    })
  end

  defp add_mutation_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        mutation_id: "mutation-add",
        expected_version: 0,
        result_version: 1,
        origin: "charge_card",
        additions: [
          %{
            kind: :node,
            id: "deliver_digest",
            action: "deliver",
            input: %{"account_id" => "acct_123"},
            queue: "priority"
          },
          %{
            kind: :edge,
            id: "charge_to_digest",
            from: "charge_card",
            to: "deliver_digest"
          }
        ],
        removals: [],
        runnable_intent_fingerprints: %{"deliver_digest" => "intent-1"},
        occurred_at: @mutated_at
      },
      overrides
    )
  end

  defp mutation_entry(attrs) do
    entry!(:dynamic_graph_mutated, attrs)
  end

  defp matching_runnables_entry do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: [
        %{
          runnable_key: "mutation_replay_run:deliver_digest:1",
          step: "deliver_digest",
          graph_mutation: %{
            mutation_id: "mutation-add",
            node_id: "deliver_digest",
            intent_fingerprint: "intent-1"
          }
        }
      ],
      occurred_at: DateTime.add(@mutated_at, 1, :second)
    })
  end

  defp run_started_entry do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      occurred_at: @started_at
    })
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp replay_state(agent) do
    %{
      graph: agent.state.projection.graph,
      planned_runnables: agent.state.projection.planned_runnables,
      anomalies: Projection.anomalies(agent.state.projection)
    }
  end

  defp legacy_graph_checkpoint(projection) do
    Map.put(projection, :graph, Map.delete(projection.graph, :topology))
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
      :squidie_mutation_replay_test_checkpoints,
      :squidie_mutation_replay_test_threads,
      :squidie_mutation_replay_test_thread_meta
    ]
  end
end
