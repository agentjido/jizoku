defmodule Squidie.Runtime.Journal.GraphMutation.TopologyTest do
  use ExUnit.Case, async: true

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Limits
  alias Squidie.Runtime.Journal.GraphMutation.Topology
  alias Squidie.Runtime.Journal.GraphMutation.Topology.Result
  alias Squidie.Runtime.Journal.GraphMutation.ValidationContext
  alias Squidie.Runtime.WorkflowAgent.Projection.GraphState

  test "computes deterministic readiness for a chain and fan-in" do
    candidate =
      mutation(
        additions: [
          node_operation("left"),
          node_operation("right"),
          node_operation("join"),
          edge("origin-left", "origin", "left"),
          edge("origin-right", "origin", "right"),
          edge("left-join", "left", "join"),
          edge("right-join", "right", "join")
        ]
      )

    assert {:ok, %Result{} = result} = Topology.evaluate(candidate, context())
    assert result.predecessors["left"] == MapSet.new(["origin"])
    assert result.predecessors["right"] == MapSet.new(["origin"])
    assert result.predecessors["join"] == MapSet.new(["left", "right"])
    assert result.ready_node_ids == MapSet.new(["right"])
    assert result.blocked_node_ids == MapSet.new(["join"])
    refute MapSet.member?(result.ready_node_ids, "left")
  end

  test "rejects an unknown edge endpoint" do
    candidate = mutation(additions: [edge("missing-edge", "origin", "missing")])

    assert Topology.evaluate(candidate, context()) ==
             {:error,
              {:invalid_graph_mutation,
               {:topology, {:unknown_endpoint, "missing-edge", "missing"}}}}
  end

  test "rejects an edge targeting a declared node" do
    candidate = mutation(additions: [edge("declared-edge", "origin", "other")])

    assert Topology.evaluate(candidate, context(declared_node_ids: ["origin", "other"])) ==
             {:error,
              {:invalid_graph_mutation, {:topology, {:invalid_target, "declared-edge", "other"}}}}
  end

  test "rejects a cycle" do
    candidate =
      mutation(
        additions: [
          node_operation("a"),
          node_operation("b"),
          edge("origin-a", "origin", "a"),
          edge("a-b", "a", "b"),
          edge("b-a", "b", "a")
        ]
      )

    assert Topology.evaluate(candidate, context()) ==
             {:error, {:invalid_graph_mutation, {:topology, :cycle}}}
  end

  test "rejects an unreachable node" do
    candidate = mutation(additions: [node_operation("orphan")])

    assert Topology.evaluate(candidate, context()) ==
             {:error, {:invalid_graph_mutation, {:topology, {:unreachable_nodes, ["orphan"]}}}}
  end

  test "requires an applied declared origin" do
    candidate = child_mutation()

    assert Topology.evaluate(candidate, context(applied_runnable_keys: [])) ==
             {:error, {:invalid_graph_mutation, {:origin, {:unapplied_declared, "origin"}}}}
  end

  test "rejects an origin outside the declared graph" do
    candidate = child_mutation()

    assert Topology.evaluate(candidate, context(declared_node_ids: ["other"])) ==
             {:error, {:invalid_graph_mutation, {:origin, {:unknown_declared, "origin"}}}}
  end

  test "edge removal can make a fan-in target ready" do
    candidate = mutation(expected_version: 2, removals: [%{kind: :edge, id: "right-join"}])

    assert {:ok, %Result{} = result} =
             Topology.evaluate(candidate, context(graph: fan_in_graph()))

    assert result.predecessors["join"] == MapSet.new(["left"])
    assert result.ready_node_ids == MapSet.new(["join", "right"])
    assert result.blocked_node_ids == MapSet.new()
    refute MapSet.member?(result.ready_node_ids, "left")
    refute MapSet.member?(result.active_edge_ids, "right-join")
  end

  test "accepts exact limits and rejects max plus one before topology evaluation" do
    candidate = child_mutation()

    exact_limits = limits(max_nodes_per_mutation: 1, max_edges_per_mutation: 1)
    assert {:ok, %Result{}} = Topology.evaluate(candidate, context(limits: exact_limits))

    exceeded =
      mutation(
        additions: [
          node_operation("child"),
          node_operation("second"),
          edge("origin-child", "origin", "child"),
          edge("origin-second", "origin", "second")
        ]
      )

    assert Topology.evaluate(exceeded, context(limits: exact_limits)) ==
             {:error, {:invalid_graph_mutation, {:limits, {:max_nodes_per_mutation, 2, 1}}}}
  end

  defp context(overrides \\ []) do
    graph = Keyword.get(overrides, :graph, %GraphState{})
    configured_limits = Keyword.get(overrides, :limits, limits())

    attrs =
      Keyword.merge(
        [
          declared_node_ids: ["origin"],
          node_runnable_keys: %{
            "origin" => ["run:origin:1"],
            "left" => ["run:left:1"],
            "right" => ["run:right:1"],
            "join" => ["run:join:1"]
          },
          applied_runnable_keys: ["run:origin:1", "run:left:1"]
        ],
        Keyword.drop(overrides, [:graph, :limits])
      )

    ValidationContext.new(graph, configured_limits, attrs)
  end

  defp fan_in_graph do
    nodes = Map.new(["left", "right", "join"], &{&1, %{kind: :node, id: &1}})

    edges = %{
      "origin-left" => edge("origin-left", "origin", "left"),
      "origin-right" => edge("origin-right", "origin", "right"),
      "left-join" => edge("left-join", "left", "join"),
      "right-join" => edge("right-join", "right", "join")
    }

    %GraphState{
      version: 2,
      topology: %{nodes: nodes, edges: edges},
      provenance: %{
        nodes: Map.new(Map.keys(nodes), &{&1, :dependency_ordered}),
        edges: Map.new(Map.keys(edges), &{&1, :dependency_ordered})
      },
      active_node_ids: MapSet.new(Map.keys(nodes)),
      active_edge_ids: MapSet.new(Map.keys(edges)),
      reserved_node_ids: MapSet.new(Map.keys(nodes)),
      reserved_edge_ids: MapSet.new(Map.keys(edges))
    }
  end

  defp mutation(overrides) do
    attrs = %{
      mutation_id: Keyword.get(overrides, :mutation_id, "mutation-topology"),
      expected_version: Keyword.get(overrides, :expected_version, 0),
      origin: "origin",
      additions: Keyword.get(overrides, :additions, []),
      removals: Keyword.get(overrides, :removals, [])
    }

    {:ok, mutation} = GraphMutation.normalize(attrs)
    mutation
  end

  defp child_mutation do
    mutation(additions: [node_operation("child"), edge("origin-child", "origin", "child")])
  end

  defp node_operation(id) do
    %{kind: :node, id: id, action: "test.action", input: %{}}
  end

  defp edge(id, from, to) do
    %{kind: :edge, id: id, from: from, to: to}
  end

  defp limits(overrides \\ []) do
    struct!(
      Limits,
      Keyword.merge(
        [
          max_nodes_per_mutation: 10,
          max_edges_per_mutation: 10,
          max_active_nodes_per_run: 10,
          max_active_edges_per_run: 10
        ],
        overrides
      )
    )
  end
end
