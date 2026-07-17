defmodule Squidie.Runtime.Journal.GraphMutation.ValidatorTest do
  use ExUnit.Case, async: true

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Limits
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal.GraphMutation.ValidationContext
  alias Squidie.Runtime.Journal.GraphMutation.Validator
  alias Squidie.Runtime.WorkflowAgent.Projection.GraphState

  test "builds a pure context with derived legacy identities and lifecycle evidence" do
    context =
      context(
        declared_node_ids: ["declared"],
        applied_runnable_keys: ["run:blocked:1"],
        node_runnable_keys: %{"blocked" => ["run:blocked:1"]},
        dispatch_attempts: [attempt("blocked", :claimed)],
        retry_node_ids: ["blocked"],
        compensation_node_ids: ["blocked"]
      )

    assert context.declared_node_ids == MapSet.new(["declared"])
    assert context.legacy_node_ids == MapSet.new(["legacy"])
    assert context.legacy_edge_ids == MapSet.new(["legacy-edge"])
    assert context.node_runnable_keys["blocked"] == MapSet.new(["run:blocked:1"])
    assert [%ActionAttempt{status: :claimed}] = context.dispatch_attempts
    assert context.compensation_node_ids == MapSet.new(["blocked"])
  end

  test "accepts an exact duplicate before stale-version and terminal checks" do
    mutation = mutation(expected_version: 1)
    fingerprint = GraphMutation.fingerprint(mutation)
    graph = put_in(graph().mutation_history[mutation.mutation_id], %{fingerprint: fingerprint})

    assert Validator.validate(mutation, context(graph: graph, terminal?: true)) ==
             {:ok, :duplicate}
  end

  test "rejects conflicting IDs, stale versions, terminal runs, identity reuse, and limits" do
    cases = [
      {mutation(), context(history: %{"mutation-1" => %{fingerprint: "different"}}),
       {:mutation_id, {:conflict, "mutation-1"}}},
      {mutation(expected_version: 2), context(), {:expected_version, {:stale, 3}}},
      {mutation(), context(terminal?: true), {:run, :terminal}},
      {mutation(additions: [node_operation("blocked")]), context(),
       {:additions, {:reserved_identity, :node, "blocked"}}},
      {mutation(removals: [%{kind: :node, id: "declared"}]),
       context(declared_node_ids: ["declared"]),
       {:removals, {:declared_identity, :node, "declared"}}},
      {mutation(removals: [%{kind: :edge, id: "legacy-edge"}]), context(),
       {:removals, {:legacy_identity, :edge, "legacy-edge"}}},
      {mutation(additions: [node_operation("new-a"), node_operation("new-b")]),
       context(limits: limits(max_nodes_per_mutation: 1)),
       {:limits, {:max_nodes_per_mutation, 2, 1}}},
      {mutation(additions: [node_operation("new")]),
       context(limits: limits(max_active_nodes_per_run: 3)),
       {:limits, {:max_active_nodes_per_run, 4, 3}}}
    ]

    Enum.each(cases, fn {candidate, validation_context, reason} ->
      assert Validator.validate(candidate, validation_context) ==
               {:error, {:invalid_graph_mutation, reason}}
    end)
  end

  test "allows only blocked, untouched node removal with every incident edge removed" do
    removable =
      mutation(
        removals: [
          %{kind: :edge, id: "origin-to-blocked"},
          %{kind: :node, id: "blocked"}
        ]
      )

    assert Validator.validate(removable, context()) == {:ok, :valid}

    cases = [
      {context(blocked_node_ids: []), :not_blocked},
      {context(applied_runnable_keys: ["run:blocked:1"]), :applied},
      {context(dispatch_attempts: [attempt("blocked", :available)]), :dispatched},
      {context(dispatch_attempts: [attempt("blocked", :claimed)]), :claimed},
      {context(dispatch_attempts: [attempt("blocked", :completed)]), :completed},
      {context(dispatch_attempts: [attempt("blocked", :failed)]), :failed},
      {context(retry_node_ids: ["blocked"]), :retry_pending},
      {context(compensation_node_ids: ["blocked"]), :compensation_pending}
    ]

    Enum.each(cases, fn {validation_context, reason} ->
      assert Validator.validate(removable, validation_context) ==
               {:error,
                {:invalid_graph_mutation, {:removals, {:node_not_removable, "blocked", reason}}}}
    end)

    assert Validator.validate(
             mutation(removals: [%{kind: :node, id: "blocked"}]),
             context()
           ) ==
             {:error,
              {:invalid_graph_mutation,
               {:removals,
                {:node_not_removable, "blocked", {:incident_edges_active, ["origin-to-blocked"]}}}}}
  end

  test "rejects new predecessors for existing nodes past readiness" do
    candidate = mutation(additions: [edge("new-edge", "origin", "blocked")])

    assert Validator.validate(candidate, context()) == {:ok, :valid}

    cases = [
      {context(ready_node_ids: ["blocked"]), :ready},
      {context(dispatch_attempts: [attempt("blocked", :available)]), :dispatched},
      {context(dispatch_attempts: [attempt("blocked", :claimed)]), :claimed},
      {context(dispatch_attempts: [attempt("blocked", :completed)]), :completed},
      {context(dispatch_attempts: [attempt("blocked", :failed)]), :failed}
    ]

    Enum.each(cases, fn {validation_context, reason} ->
      assert Validator.validate(candidate, validation_context) ==
               {:error,
                {:invalid_graph_mutation,
                 {:additions, {:predecessor_for_started_node, "new-edge", "blocked", reason}}}}
    end)

    declared_target = mutation(additions: [edge("declared-edge", "origin", "declared")])

    assert Validator.validate(
             declared_target,
             context(declared_node_ids: ["declared"], ready_node_ids: ["declared"])
           ) ==
             {:error,
              {:invalid_graph_mutation,
               {:additions, {:predecessor_for_started_node, "declared-edge", "declared", :ready}}}}
  end

  defp context(overrides \\ []) do
    graph = Keyword.get(overrides, :graph, graph(Keyword.get(overrides, :history, %{})))
    configured_limits = Keyword.get(overrides, :limits, limits())

    attrs =
      overrides
      |> Keyword.drop([:graph, :history, :limits])
      |> Keyword.put_new(:blocked_node_ids, ["blocked"])
      |> Keyword.put_new(:node_runnable_keys, %{"blocked" => ["run:blocked:1"]})

    ValidationContext.new(graph, configured_limits, attrs)
  end

  defp graph(history \\ %{}) do
    %GraphState{
      version: 3,
      topology: %{
        nodes: %{
          "blocked" => %{kind: :node, id: "blocked"},
          "legacy" => %{kind: :node, id: "legacy"},
          "ready" => %{kind: :node, id: "ready"}
        },
        edges: %{
          "origin-to-blocked" => %{
            kind: :edge,
            id: "origin-to-blocked",
            from: "origin",
            to: "blocked"
          },
          "legacy-edge" => %{kind: :edge, id: "legacy-edge", from: "origin", to: "legacy"}
        }
      },
      provenance: %{
        nodes: %{
          "blocked" => :dependency_ordered,
          "legacy" => :legacy_eager,
          "ready" => :dependency_ordered
        },
        edges: %{
          "origin-to-blocked" => :dependency_ordered,
          "legacy-edge" => :legacy_eager
        }
      },
      active_node_ids: MapSet.new(["blocked", "legacy", "ready"]),
      active_edge_ids: MapSet.new(["origin-to-blocked", "legacy-edge"]),
      reserved_node_ids: MapSet.new(["blocked", "legacy", "ready"]),
      reserved_edge_ids: MapSet.new(["origin-to-blocked", "legacy-edge"]),
      mutation_history: history
    }
  end

  defp mutation(overrides \\ []) do
    attrs = %{
      mutation_id: Keyword.get(overrides, :mutation_id, "mutation-1"),
      expected_version: Keyword.get(overrides, :expected_version, 3),
      origin: "origin",
      additions: Keyword.get(overrides, :additions, []),
      removals: Keyword.get(overrides, :removals, [])
    }

    {:ok, mutation} = GraphMutation.normalize(attrs)
    mutation
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

  defp attempt(step, status) do
    %ActionAttempt{
      run_id: "run",
      runnable_key: "run:#{step}:1",
      idempotency_key: "run:#{step}:1",
      attempt_number: 1,
      step: step,
      input: %{},
      visible_at: ~U[2026-07-16 00:00:00Z],
      status: status
    }
  end
end
