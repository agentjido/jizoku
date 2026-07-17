defmodule Squidie.Runtime.Journal.GraphMutation.MaterializerTest do
  use ExUnit.Case, async: true

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Limits
  alias Squidie.Runtime.Journal.GraphMutation.Materializer
  alias Squidie.Runtime.Journal.GraphMutation.Materializer.Result
  alias Squidie.Runtime.Journal.GraphMutation.ValidationContext
  alias Squidie.Runtime.WorkflowAgent.Projection.GraphState

  defmodule DeliverAction do
    use Squidie.Step,
      name: :deliver,
      input_schema: [account_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{delivered?: true}}
    end

    @spec persisted_action_opts(keyword()) :: keyword()
    def persisted_action_opts(opts) do
      Keyword.take(opts, [:policy])
    end
  end

  defmodule IncompatibleAction do
    def run(_input, _context) do
      {:ok, %{}}
    end
  end

  @now ~U[2026-07-17 10:00:00Z]

  test "materializes backend-neutral mutation data and resolved runnable intents" do
    mutation = mutation(queue: "priority")
    registry = registry(action_opts: [policy: "strict", credential: "secret"])

    assert {:ok, %Result{} = result} =
             Materializer.evaluate(
               "run-1",
               mutation,
               context(),
               registry,
               "default",
               @now
             )

    assert result.mutation_attrs.result_version == 1
    assert result.mutation_attrs.additions == Enum.map(mutation.additions, &operation_map/1)
    assert result.mutation_attrs.removals == []
    refute inspect(result.mutation_attrs) =~ inspect(DeliverAction)
    refute inspect(result.mutation_attrs) =~ "secret"

    assert [runnable] = result.runnables
    assert runnable.run_id == "run-1"
    assert runnable.runnable_key == "run-1:child:1"
    assert runnable.idempotency_key == runnable.runnable_key
    assert runnable.queue == "priority"
    assert runnable.step == "child"
    assert runnable.input == %{account_id: "acct-1"}
    assert runnable.visible_at == @now
    assert runnable.dynamic_work.module == DeliverAction
    assert runnable.dynamic_work.action_opts == [policy: "strict"]

    fingerprint = result.runnable_intent_fingerprints["child"]
    assert runnable.graph_mutation.intent_fingerprint == fingerprint
    assert result.mutation_attrs.runnable_intent_fingerprints == %{"child" => fingerprint}
  end

  test "intent fingerprints are stable across evaluation times" do
    mutation = mutation()
    registry = registry()

    assert {:ok, first} =
             Materializer.evaluate("run-1", mutation, context(), registry, "default", @now)

    later = DateTime.add(@now, 60, :second)

    assert {:ok, second} =
             Materializer.evaluate("run-1", mutation, context(), registry, "default", later)

    assert first.runnable_intent_fingerprints == second.runnable_intent_fingerprints
    refute hd(first.runnables).visible_at == hd(second.runnables).visible_at
  end

  test "returns exact duplicates before resolving the current registry" do
    mutation = mutation(mutation_id: "duplicate")
    fingerprint = GraphMutation.fingerprint(mutation)

    graph = %GraphState{
      mutation_history: %{"duplicate" => %{fingerprint: fingerprint}}
    }

    assert Materializer.evaluate("run-1", mutation, context(graph: graph), %{}, "default", @now) ==
             {:ok, :duplicate}
  end

  test "rejects an unknown action key" do
    assert_materialization_error(%{}, {:action, :unknown_action_key})
  end

  test "rejects a disabled action key" do
    registry = registry(enabled?: false)
    assert_materialization_error(registry, {:action, :disabled_action_key})
  end

  test "rejects an incompatible action module" do
    registry = %{"deliver" => IncompatibleAction}
    assert_materialization_error(registry, {:action, :incompatible_action_module})
  end

  test "redacts invalid action input failures" do
    secret = "credential-value"
    invalid = mutation(input: %{account_id: 123, credential: secret})

    assert Materializer.evaluate(
             "run-1",
             invalid,
             context(),
             registry(),
             "default",
             @now
           ) ==
             {:error,
              {:invalid_graph_mutation, {:additions, {:node, "child", {:input, :invalid}}}}}

    refute inspect(
             Materializer.evaluate(
               "run-1",
               invalid,
               context(),
               registry(),
               "default",
               @now
             )
           ) =~ secret
  end

  defp assert_materialization_error(registry, reason) do
    assert Materializer.evaluate(
             "run-1",
             mutation(),
             context(),
             registry,
             "default",
             @now
           ) ==
             {:error, {:invalid_graph_mutation, {:additions, {:node, "child", reason}}}}
  end

  defp mutation(overrides \\ []) do
    input = Keyword.get(overrides, :input, %{account_id: "acct-1"})
    queue = Keyword.get(overrides, :queue)

    node = maybe_put(%{kind: :node, id: "child", action: "deliver", input: input}, :queue, queue)

    attrs = %{
      mutation_id: Keyword.get(overrides, :mutation_id, "mutation-materializer"),
      expected_version: 0,
      origin: "origin",
      additions: [node, %{kind: :edge, id: "origin-child", from: "origin", to: "child"}],
      removals: []
    }

    {:ok, mutation} = GraphMutation.normalize(attrs)
    mutation
  end

  defp context(overrides \\ []) do
    graph = Keyword.get(overrides, :graph, %GraphState{})

    ValidationContext.new(graph, limits(),
      declared_node_ids: ["origin"],
      node_runnable_keys: %{"origin" => ["run-1:origin:1"]},
      applied_runnable_keys: ["run-1:origin:1"]
    )
  end

  defp limits do
    %Limits{
      max_nodes_per_mutation: 10,
      max_edges_per_mutation: 10,
      max_active_nodes_per_run: 10,
      max_active_edges_per_run: 10
    }
  end

  defp registry(overrides \\ []) do
    entry = Keyword.merge([module: DeliverAction, enabled?: true, action_opts: []], overrides)

    %{"deliver" => entry}
  end

  defp operation_map(operation) do
    Squidie.GraphMutation.Operation.to_map(operation)
  end

  defp maybe_put(map, _key, nil) do
    map
  end

  defp maybe_put(map, key, value) do
    Map.put(map, key, value)
  end
end
