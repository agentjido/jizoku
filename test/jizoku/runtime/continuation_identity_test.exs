defmodule Jizoku.Runtime.ContinuationIdentityTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.ContinuationIdentity

  @predecessor_run_id "11111111-1111-5111-8111-111111111111"

  test "derives one stable successor UUID from durable continuation identity" do
    attrs = identity_attrs()

    assert {:ok, successor_run_id = "477d4022-b304-57e6-950e-0491d3c69960"} =
             ContinuationIdentity.successor_run_id(attrs)

    assert {:ok, ^successor_run_id} = ContinuationIdentity.successor_run_id(attrs)
    assert {:ok, ^successor_run_id} = Ecto.UUID.cast(successor_run_id)
    refute successor_run_id == @predecessor_run_id
  end

  test "changes identity for every durable continuation identity field" do
    assert {:ok, original_run_id} =
             ContinuationIdentity.successor_run_id(identity_attrs())

    changes = [
      predecessor_run_id: "22222222-2222-5222-8222-222222222222",
      continuation_key: "page-43",
      workflow: "Example.OtherWorkflow",
      trigger: "resume",
      definition_version: "v2",
      definition_fingerprint: String.duplicate("b", 64),
      partition: "tenant_globex"
    ]

    for {field, value} <- changes do
      assert {:ok, changed_run_id} =
               identity_attrs()
               |> Map.put(field, value)
               |> ContinuationIdentity.successor_run_id()

      refute changed_run_id == original_run_id
    end
  end

  test "distinguishes legacy partition and unversioned definition identities" do
    assert {:ok, legacy_run_id} =
             ContinuationIdentity.successor_run_id(
               identity_attrs(partition: nil, definition_version: nil)
             )

    assert legacy_run_id == "60a6b1e1-cbdf-551c-bc54-472773dcdb54"

    assert {:ok, partitioned_run_id} =
             ContinuationIdentity.successor_run_id(identity_attrs(definition_version: nil))

    assert {:ok, versioned_run_id} =
             ContinuationIdentity.successor_run_id(identity_attrs(partition: nil))

    refute legacy_run_id == partitioned_run_id
    refute legacy_run_id == versioned_run_id
    refute partitioned_run_id == versioned_run_id
  end

  test "keeps nil tags distinct from literal values" do
    assert {:ok, nil_run_id} =
             ContinuationIdentity.successor_run_id(
               identity_attrs(partition: nil, definition_version: nil)
             )

    assert {:ok, literal_run_id} =
             ContinuationIdentity.successor_run_id(
               identity_attrs(partition: "nil", definition_version: "nil")
             )

    assert {:ok, tagged_literal_run_id} =
             ContinuationIdentity.successor_run_id(
               identity_attrs(partition: nil, definition_version: "value:nil")
             )

    refute nil_run_id == literal_run_id
    refute nil_run_id == tagged_literal_run_id
  end

  test "ignores insertion order and non-identity metadata" do
    attrs = identity_attrs()

    reversed_attrs =
      attrs
      |> Enum.reverse()
      |> Map.new()

    with_metadata =
      Map.merge(attrs, %{
        input: %{cursor: "page-99"},
        queue: "priority",
        trace: %{trace_id: "ignored"},
        occurred_at: ~U[2026-08-09 18:00:00Z],
        future_metadata: "ignored"
      })

    assert {:ok, run_id} = ContinuationIdentity.successor_run_id(attrs)
    assert {:ok, ^run_id} = ContinuationIdentity.successor_run_id(reversed_attrs)
    assert {:ok, ^run_id} = ContinuationIdentity.successor_run_id(with_metadata)
  end

  test "rejects missing or malformed identity fields" do
    invalid_values = [
      predecessor_run_id: "not-a-uuid",
      continuation_key: "",
      workflow: nil,
      trigger: "",
      definition_version: 1,
      definition_fingerprint: "",
      partition: "invalid partition"
    ]

    for {field, value} <- invalid_values do
      assert ContinuationIdentity.successor_run_id(Map.put(identity_attrs(), field, value)) ==
               {:error, {:invalid_continuation_identity, field}}

      assert ContinuationIdentity.successor_run_id(Map.delete(identity_attrs(), field)) ==
               {:error, {:invalid_continuation_identity, field}}
    end

    assert ContinuationIdentity.successor_run_id(:invalid) ==
             {:error, {:invalid_continuation_identity, :invalid}}
  end

  defp identity_attrs(overrides \\ []) do
    Map.merge(
      %{
        predecessor_run_id: @predecessor_run_id,
        continuation_key: "page-42",
        workflow: "Example.CursorWorkflow",
        trigger: "continue",
        definition_version: "v1",
        definition_fingerprint: String.duplicate("a", 64),
        partition: "tenant_acme"
      },
      Map.new(overrides)
    )
  end
end
