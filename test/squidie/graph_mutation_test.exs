defmodule Squidie.GraphMutationTest do
  use ExUnit.Case, async: true

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Operation

  test "normalizes atom and string keyed mutation input" do
    atom_input = mutation_input()

    string_input = %{
      "mutation_id" => atom_input.mutation_id,
      "expected_version" => atom_input.expected_version,
      "origin" => atom_input.origin,
      "additions" => Enum.map(atom_input.additions, &stringify_operation/1),
      "removals" => Enum.map(atom_input.removals, &stringify_operation/1)
    }

    assert {:ok, mutation} = GraphMutation.normalize(atom_input)
    assert {:ok, ^mutation} = GraphMutation.normalize(string_input)

    assert Enum.map(mutation.additions, &{&1.kind, &1.id}) == [
             {:edge, "edge-a"},
             {:node, "node-a"},
             {:node, "node-b"}
           ]
  end

  test "keeps shape normalization separate from semantic graph validation" do
    input =
      mutation_input(%{
        origin: "not-yet-known",
        additions: [
          %{kind: :edge, id: "self-edge", from: "missing", to: "missing"}
        ]
      })

    assert {:ok, %GraphMutation{} = mutation} = GraphMutation.normalize(input)
    assert mutation.origin == "not-yet-known"
    assert [%Operation{from: "missing", to: "missing"}] = mutation.additions
  end

  test "rejects malformed mutation and operation shapes" do
    assert GraphMutation.normalize(:invalid) ==
             {:error, {:invalid_graph_mutation, {:attrs, :invalid}}}

    assert GraphMutation.normalize(mutation_input(%{mutation_id: ""})) ==
             {:error, {:invalid_graph_mutation, {:mutation_id, :invalid}}}

    assert GraphMutation.normalize(mutation_input(%{expected_version: -1})) ==
             {:error, {:invalid_graph_mutation, {:expected_version, :invalid}}}

    assert GraphMutation.normalize(Map.put(mutation_input(), :metadata, %{})) ==
             {:error, {:invalid_graph_mutation, {:metadata, :unsupported}}}

    assert GraphMutation.normalize(mutation_input(%{additions: [%{kind: :node, id: "n"}]})) ==
             {:error,
              {:invalid_graph_mutation, {:additions, {:operation, 0, {:action, :invalid}}}}}

    assert GraphMutation.normalize(
             mutation_input(%{removals: [%{kind: :edge, id: "e", from: "a"}]})
           ) ==
             {:error,
              {:invalid_graph_mutation, {:removals, {:operation, 0, {:from, :unsupported}}}}}

    invalid_input = [
      %{kind: :node, id: "n", action: "action", input: %{owner: self()}}
    ]

    assert GraphMutation.normalize(mutation_input(%{additions: invalid_input})) ==
             {:error,
              {:invalid_graph_mutation, {:additions, {:operation, 0, {:input, :invalid}}}}}
  end

  test "accepts stable scalar values that resemble internal error markers" do
    input = [
      %{kind: :node, id: "n", action: "action", input: %{status: :invalid}}
    ]

    assert {:ok, %GraphMutation{}} =
             GraphMutation.normalize(mutation_input(%{additions: input}))
  end

  test "canonical content and fingerprint are stable across operation and map ordering" do
    first = mutation_input()

    second =
      Map.update!(first, :additions, fn additions ->
        Enum.map(Enum.reverse(additions), fn
          %{id: "node-a"} = operation ->
            Map.put(operation, :input, %{amount: 10, account: "acct-1"})

          operation ->
            operation
        end)
      end)

    assert {:ok, first} = GraphMutation.normalize(first)
    assert {:ok, second} = GraphMutation.normalize(second)

    assert GraphMutation.canonical_content(first) == GraphMutation.canonical_content(second)
    assert GraphMutation.fingerprint(first) == GraphMutation.fingerprint(second)

    assert GraphMutation.canonical_content(first) ==
             {:squidie_graph_mutation, 1, "mutation-1", 4, "approve",
              [
                {:edge, "edge-a", "approve", "node-a"},
                {:node, "node-a", "billing.capture", nil,
                 {:map, [account: "acct-1", amount: 10]}},
                {:node, "node-b", "billing.notify", "critical", {:map, []}}
              ], [{:node, "obsolete"}]}

    assert GraphMutation.fingerprint(first) ==
             "1512905653502e7397ad813aafee460d6a023686961d27e4e47cbe4c045d14f0"
  end

  test "serializes mutation deterministically" do
    assert {:ok, mutation} = GraphMutation.normalize(mutation_input())

    assert GraphMutation.to_map(mutation) == %{
             mutation_id: "mutation-1",
             expected_version: 4,
             origin: "approve",
             additions: [
               %{kind: :edge, id: "edge-a", from: "approve", to: "node-a"},
               %{
                 kind: :node,
                 id: "node-a",
                 action: "billing.capture",
                 input: %{account: "acct-1", amount: 10},
                 queue: nil
               },
               %{
                 kind: :node,
                 id: "node-b",
                 action: "billing.notify",
                 input: %{},
                 queue: "critical"
               }
             ],
             removals: [%{kind: :node, id: "obsolete"}]
           }
  end

  test "normalizes, serializes, and canonicalizes edge removals" do
    input = mutation_input(%{removals: [%{kind: :edge, id: "obsolete-edge"}]})

    assert {:ok, mutation} = GraphMutation.normalize(input)
    assert mutation.removals == [%Operation{kind: :edge, id: "obsolete-edge"}]
    assert GraphMutation.to_map(mutation).removals == [%{kind: :edge, id: "obsolete-edge"}]

    assert {:squidie_graph_mutation, 1, _mutation_id, _version, _origin, _additions,
            [{:edge, "obsolete-edge"}]} = GraphMutation.canonical_content(mutation)
  end

  defp mutation_input(overrides \\ %{}) do
    Map.merge(
      %{
        mutation_id: "mutation-1",
        expected_version: 4,
        origin: "approve",
        additions: [
          %{kind: :node, id: "node-b", action: "billing.notify", input: %{}, queue: "critical"},
          %{
            kind: :node,
            id: "node-a",
            action: "billing.capture",
            input: %{amount: 10, account: "acct-1"}
          },
          %{kind: :edge, id: "edge-a", from: "approve", to: "node-a"}
        ],
        removals: [%{kind: :node, id: "obsolete"}]
      },
      overrides
    )
  end

  defp stringify_operation(operation) do
    Map.new(operation, fn {key, value} ->
      {Atom.to_string(key), value}
    end)
  end
end
