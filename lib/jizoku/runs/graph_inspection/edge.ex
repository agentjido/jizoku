defmodule Jizoku.Runs.GraphInspection.Edge do
  @moduledoc """
  Compatibility edge struct for run graph inspection.

  `Jizoku.Inspection.GraphInspection.Edge` is the canonical inspection
  namespace. This module preserves the original `Jizoku.Runs.*` struct for
  existing callers and serializers.
  """

  alias Jizoku.Inspection

  @type edge_type :: :transition | :dependency | :dynamic
  @type edge_status :: :selected | :skipped | :pending | :blocked

  @type t :: %__MODULE__{
          id: String.t(),
          from: String.t(),
          to: String.t(),
          type: edge_type(),
          status: edge_status(),
          outcome: atom() | nil,
          condition: map() | nil,
          recovery: atom() | nil
        }

  @enforce_keys [:id, :from, :to, :type, :status]

  defstruct [
    :id,
    :from,
    :to,
    :type,
    :status,
    :outcome,
    :condition,
    :recovery
  ]

  @doc """
  Converts a compatibility edge into the stable host UI map shape.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = edge) do
    edge
    |> to_inspection_edge()
    |> Inspection.GraphInspection.Edge.to_map()
  end

  @doc """
  Converts a canonical inspection edge into the compatibility struct.
  """
  @spec from_inspection_edge(Inspection.GraphInspection.Edge.t()) :: t()
  def from_inspection_edge(%Inspection.GraphInspection.Edge{} = edge) do
    edge
    |> Map.from_struct()
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Converts a compatibility edge into the canonical inspection struct.
  """
  @spec to_inspection_edge(t()) :: Inspection.GraphInspection.Edge.t()
  def to_inspection_edge(%__MODULE__{} = edge) do
    edge
    |> Map.from_struct()
    |> then(&struct!(Inspection.GraphInspection.Edge, &1))
  end
end
