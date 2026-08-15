defmodule Jizoku.GraphMutation.ContractsTest do
  use ExUnit.Case, async: true

  alias Jizoku.GraphMutation
  alias Jizoku.GraphMutation.Limits
  alias Jizoku.GraphMutation.Preview
  alias Jizoku.GraphMutation.Report

  test "preview stores and serializes redacted operation summaries" do
    mutation = mutation()
    [_edge, node] = mutation.additions

    preview =
      Preview.new(mutation,
        base_version: 4,
        result_version: 5,
        status: :applicable,
        applied_operations: [{:add, node}],
        warnings: [:edge_removal_unblocks_node]
      )

    assert Preview.to_map(preview) == %{
             mutation_id: "mutation-1",
             expected_version: 4,
             base_version: 4,
             result_version: 5,
             duplicate?: false,
             status: :applicable,
             active_node_ids: [],
             ready_node_ids: [],
             blocked_node_ids: [],
             tombstoned_node_ids: [],
             applied_operations: [
               %{
                 operation: :add,
                 kind: :node,
                 id: "node-a",
                 action: "billing.capture",
                 queue: "critical"
               }
             ],
             reconciliation: :not_required,
             warnings: [:edge_removal_unblocks_node]
           }

    refute inspect(preview) =~ "secret-token"
  end

  test "report exposes committed reconciliation state without sensitive input" do
    mutation = mutation()

    report =
      Report.new(mutation,
        base_version: 4,
        result_version: 5,
        status: :committed_needs_reconciliation,
        applied_operations: [{:remove, List.first(mutation.removals)}],
        reconciliation: :required
      )

    assert Report.to_map(report) == %{
             mutation_id: "mutation-1",
             expected_version: 4,
             base_version: 4,
             result_version: 5,
             duplicate?: false,
             status: :committed_needs_reconciliation,
             applied_operations: [
               %{operation: :remove, kind: :edge, id: "obsolete-edge"}
             ],
             reconciliation: :required,
             warnings: []
           }

    refute inspect(report) =~ "secret-token"
  end

  test "normalizes atom and string keyed host-owned graph limits" do
    attrs = %{
      max_nodes_per_mutation: 20,
      max_edges_per_mutation: 40,
      max_active_nodes_per_run: 200,
      max_active_edges_per_run: 400
    }

    string_attrs =
      Map.new(attrs, fn {key, value} ->
        {Atom.to_string(key), value}
      end)

    assert {:ok, limits} = Limits.normalize(attrs)
    assert {:ok, ^limits} = Limits.normalize(string_attrs)
    assert Limits.to_map(limits) == attrs
  end

  test "rejects missing, invalid, and unsupported graph limits" do
    assert Limits.normalize(:invalid) ==
             {:error, {:invalid_graph_mutation_limits, {:attrs, :invalid}}}

    assert Limits.normalize(%{}) ==
             {:error, {:invalid_graph_mutation_limits, {:max_nodes_per_mutation, :invalid}}}

    assert Limits.normalize(limits_attrs(%{max_edges_per_mutation: 0})) ==
             {:error, {:invalid_graph_mutation_limits, {:max_edges_per_mutation, :invalid}}}

    assert Limits.normalize(Map.put(limits_attrs(), :timeout, 10)) ==
             {:error, {:invalid_graph_mutation_limits, {:timeout, :unsupported}}}
  end

  defp mutation do
    {:ok, mutation} =
      GraphMutation.normalize(%{
        mutation_id: "mutation-1",
        expected_version: 4,
        origin: "approve",
        additions: [
          %{kind: :edge, id: "edge-a", from: "approve", to: "node-a"},
          %{
            kind: :node,
            id: "node-a",
            action: "billing.capture",
            input: %{credential: "secret-token"},
            queue: "critical"
          }
        ],
        removals: [%{kind: :edge, id: "obsolete-edge"}]
      })

    mutation
  end

  defp limits_attrs(overrides \\ %{}) do
    Map.merge(
      %{
        max_nodes_per_mutation: 20,
        max_edges_per_mutation: 40,
        max_active_nodes_per_run: 200,
        max_active_edges_per_run: 400
      },
      overrides
    )
  end
end
