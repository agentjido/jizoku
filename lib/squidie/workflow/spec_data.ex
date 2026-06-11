defmodule Squidie.Workflow.SpecData do
  @moduledoc false

  alias Squidie.Workflow.Spec

  @doc """
  Converts a workflow spec struct to its persisted map representation.
  """
  @spec from_struct(Spec.t()) :: map()
  def from_struct(%Spec{} = spec), do: Map.from_struct(spec)

  @doc """
  Returns atom-keyed fields from a workflow spec struct or map.
  """
  @spec struct_fields(map()) :: map()
  def struct_fields(spec) when is_map(spec) do
    Spec
    |> struct()
    |> Map.from_struct()
    |> Map.keys()
    |> Map.new(fn field -> {field, Squidie.MapField.get(spec, field)} end)
  end
end
