defmodule Jizoku.GraphMutation.Limits do
  @moduledoc """
  Explicit host-owned limits for graph mutation batches and active run graphs.

  Jizoku defines the contract but does not choose defaults for host
  applications.
  """

  @fields [
    :max_nodes_per_mutation,
    :max_edges_per_mutation,
    :max_active_nodes_per_run,
    :max_active_edges_per_run
  ]
  @field_names Map.new(@fields, &{Atom.to_string(&1), &1})

  @type t :: %__MODULE__{
          max_nodes_per_mutation: pos_integer(),
          max_edges_per_mutation: pos_integer(),
          max_active_nodes_per_run: pos_integer(),
          max_active_edges_per_run: pos_integer()
        }

  @type normalize_error ::
          {:invalid_graph_mutation_limits, {atom() | String.t(), :invalid | :unsupported}}

  @enforce_keys @fields
  defstruct @fields

  @doc """
  Normalizes a complete atom- or string-keyed limit map.
  """
  @spec normalize(term()) :: {:ok, t()} | {:error, normalize_error()}
  def normalize(%__MODULE__{} = limits) do
    {:ok, limits}
  end

  def normalize(attrs) when is_map(attrs) and not is_struct(attrs) do
    with :ok <- supported_fields(attrs),
         {:ok, normalized} <- positive_fields(attrs) do
      {:ok, struct!(__MODULE__, normalized)}
    end
  end

  def normalize(_attrs) do
    invalid(:attrs, :invalid)
  end

  @doc """
  Converts graph mutation limits to a plain map.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = limits) do
    Map.from_struct(limits)
  end

  defp supported_fields(attrs) do
    unsupported =
      Enum.find_value(Map.keys(attrs), fn field ->
        normalized = field_name(field)

        if normalized in @fields do
          nil
        else
          normalized
        end
      end)

    case unsupported do
      nil -> :ok
      field -> invalid(field, :unsupported)
    end
  end

  defp field_name(field) when is_binary(field) do
    Map.get(@field_names, field, field)
  end

  defp field_name(field) do
    field
  end

  defp positive_fields(attrs) do
    Enum.reduce_while(@fields, {:ok, %{}}, fn field, {:ok, normalized} ->
      case Jizoku.MapField.get(attrs, field) do
        value when is_integer(value) and value > 0 ->
          {:cont, {:ok, Map.put(normalized, field, value)}}

        _invalid ->
          {:halt, invalid(field, :invalid)}
      end
    end)
  end

  defp invalid(field, reason) do
    {:error, {:invalid_graph_mutation_limits, {field, reason}}}
  end
end
