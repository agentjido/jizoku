defmodule Squidie.Runs.GraphInspection.Edge do
  @moduledoc """
  Public graph edge for workflow run inspection.

  Edge statuses are derived from durable step and attempt state only. Conditional
  route selection can add richer skipped-edge evidence later without changing
  the node shape.
  """

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
  Converts a graph edge into the stable host UI map shape.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = edge) do
    %{
      id: edge.id,
      from: edge.from,
      to: edge.to,
      type: edge.type,
      status: edge.status,
      selected?: edge.status == :selected,
      skipped?: edge.status == :skipped,
      pending?: edge.status == :pending,
      blocked?: edge.status == :blocked,
      outcome: edge.outcome,
      condition: edge.condition,
      recovery: edge.recovery
    }
  end
end
