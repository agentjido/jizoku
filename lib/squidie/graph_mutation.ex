defmodule Squidie.GraphMutation do
  @moduledoc """
  Normalized, version-fenced request to change one run's active graph.

  Normalization establishes a deterministic transport contract. It does not
  decide whether identities, lifecycle transitions, or graph topology are
  valid for a particular run.
  """

  alias Squidie.GraphMutation.Operation

  @canonical_version 1
  @fields [:mutation_id, :expected_version, :origin, :additions, :removals]
  @field_names Map.new(@fields, &{Atom.to_string(&1), &1})

  @type t :: %__MODULE__{
          mutation_id: String.t(),
          expected_version: non_neg_integer(),
          origin: String.t(),
          additions: [Operation.t()],
          removals: [Operation.t()]
        }

  @enforce_keys [:mutation_id, :expected_version, :origin]
  defstruct [:mutation_id, :expected_version, :origin, additions: [], removals: []]

  @type normalize_error ::
          {:invalid_graph_mutation, {atom() | String.t(), term()}}

  @doc """
  Normalizes atom- or string-keyed mutation input without consulting run state.
  """
  @spec normalize(term()) :: {:ok, t()} | {:error, normalize_error()}
  def normalize(attrs) when is_map(attrs) do
    with :ok <- supported_fields(attrs),
         {:ok, mutation_id} <- required_binary(attrs, :mutation_id),
         {:ok, expected_version} <- expected_version(value(attrs, :expected_version)),
         {:ok, origin} <- required_binary(attrs, :origin),
         {:ok, additions} <- operations(value(attrs, :additions, []), :additions, :addition),
         {:ok, removals} <- operations(value(attrs, :removals, []), :removals, :removal) do
      {:ok,
       %__MODULE__{
         mutation_id: mutation_id,
         expected_version: expected_version,
         origin: origin,
         additions: sort_operations(additions),
         removals: sort_operations(removals)
       }}
    end
  end

  def normalize(_attrs) do
    invalid(:attrs, :invalid)
  end

  @doc """
  Converts a normalized mutation to its deterministic persistence map.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = mutation) do
    %{
      mutation_id: mutation.mutation_id,
      expected_version: mutation.expected_version,
      origin: mutation.origin,
      additions: Enum.map(mutation.additions, &Operation.to_map/1),
      removals: Enum.map(mutation.removals, &Operation.to_map/1)
    }
  end

  @doc """
  Returns versioned ordered content for duplicate comparison.
  """
  @spec canonical_content(t()) :: tuple()
  def canonical_content(%__MODULE__{} = mutation) do
    {
      :squidie_graph_mutation,
      @canonical_version,
      mutation.mutation_id,
      mutation.expected_version,
      mutation.origin,
      Enum.map(mutation.additions, &Operation.canonical/1),
      Enum.map(mutation.removals, &Operation.canonical/1)
    }
  end

  @doc """
  Returns a stable SHA-256 fingerprint of the canonical mutation content.
  """
  @spec fingerprint(t()) :: String.t()
  def fingerprint(%__MODULE__{} = mutation) do
    mutation
    |> canonical_content()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp operations(operations, field, mode) when is_list(operations) do
    operations
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {attrs, index}, {:ok, normalized} ->
      case Operation.normalize(attrs, mode) do
        {:ok, operation} -> {:cont, {:ok, [operation | normalized]}}
        {:error, reason} -> {:halt, invalid(field, {:operation, index, reason})}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, _reason} = error -> error
    end
  end

  defp operations(_operations, field, _mode) do
    invalid(field, :invalid)
  end

  defp sort_operations(operations) do
    Enum.sort_by(operations, &Operation.canonical/1)
  end

  defp supported_fields(attrs) do
    case Enum.find(Map.keys(attrs), &unsupported_field?/1) do
      nil -> :ok
      field -> invalid(field_name(field), :unsupported)
    end
  end

  defp unsupported_field?(field) do
    field_name(field) not in @fields
  end

  defp field_name(field) when is_binary(field) do
    Map.get(@field_names, field, field)
  end

  defp field_name(field) do
    field
  end

  defp expected_version(version) when is_integer(version) and version >= 0 do
    {:ok, version}
  end

  defp expected_version(_version) do
    invalid(:expected_version, :invalid)
  end

  defp required_binary(attrs, field) do
    case value(attrs, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> invalid(field, :invalid)
    end
  end

  defp value(map, field, default \\ nil) do
    Squidie.MapField.get(map, field, default)
  end

  defp invalid(field, reason) do
    {:error, {:invalid_graph_mutation, {field, reason}}}
  end
end
