defmodule Jizoku.Runs.GraphInspection.Node do
  @moduledoc """
  Compatibility node struct for run graph inspection.

  `Jizoku.Inspection.GraphInspection.Node` is the canonical inspection
  namespace. This module preserves the original `Jizoku.Runs.*` struct for
  existing callers and serializers.
  """

  alias Jizoku.Inspection

  @type t :: %__MODULE__{
          id: String.t(),
          action: atom() | String.t() | nil,
          status: atom(),
          current?: boolean(),
          input: map() | nil,
          output: map() | nil,
          error: map() | nil,
          deadline: map() | nil,
          recovery: map() | nil,
          transition: map() | nil,
          manual_state: map() | nil,
          dynamic?: boolean(),
          origin: map() | nil,
          metadata: map(),
          attempts: [map()]
        }

  @enforce_keys [:id, :status, :current?]

  defstruct [
    :id,
    :action,
    :status,
    :current?,
    :input,
    :output,
    :error,
    :deadline,
    :recovery,
    :transition,
    :manual_state,
    dynamic?: false,
    origin: nil,
    metadata: %{},
    attempts: []
  ]

  @doc """
  Converts a compatibility node into the stable host UI map shape.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = node) do
    node
    |> to_inspection_node()
    |> Inspection.GraphInspection.Node.to_map()
  end

  @doc """
  Converts a canonical inspection node into the compatibility struct.
  """
  @spec from_inspection_node(Inspection.GraphInspection.Node.t()) :: t()
  def from_inspection_node(%Inspection.GraphInspection.Node{} = node) do
    node
    |> Map.from_struct()
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Converts a compatibility node into the canonical inspection struct.
  """
  @spec to_inspection_node(t()) :: Inspection.GraphInspection.Node.t()
  def to_inspection_node(%__MODULE__{} = node) do
    node
    |> Map.from_struct()
    |> then(&struct!(Inspection.GraphInspection.Node, &1))
  end
end
