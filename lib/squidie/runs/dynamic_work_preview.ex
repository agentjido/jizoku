defmodule Squidie.Runs.DynamicWorkPreview do
  @moduledoc """
  Compatibility dynamic-work preview struct.

  `Squidie.Inspection.DynamicWorkPreview` is the canonical inspection
  namespace. This module preserves the original `Squidie.Runs.*` return shape
  for callers that pattern match or serialize the existing struct.
  """

  alias Squidie.Inspection
  alias Squidie.Runs.GraphInspection

  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          duplicate?: boolean(),
          recordable?: boolean(),
          origin_node_id: String.t() | nil,
          added_node_ids: [String.t()],
          added_edge_ids: [String.t()],
          warnings: [atom()],
          dynamic_work: map(),
          graph: GraphInspection.t()
        }

  defstruct [
    :run_id,
    :partition,
    :dynamic_work,
    :graph,
    :origin_node_id,
    added_node_ids: [],
    added_edge_ids: [],
    duplicate?: false,
    recordable?: false,
    warnings: []
  ]

  @doc """
  Builds a compatibility preview from dynamic-work validation output.
  """
  @spec new(String.t(), map(), boolean(), GraphInspection.t()) :: t()
  def new(run_id, dynamic_work, duplicate?, %GraphInspection{} = graph)
      when is_binary(run_id) and is_map(dynamic_work) and is_boolean(duplicate?) do
    run_id
    |> Inspection.DynamicWorkPreview.new(
      dynamic_work,
      duplicate?,
      GraphInspection.to_inspection_graph(graph)
    )
    |> from_inspection_preview()
  end

  @doc """
  Converts a compatibility preview to a plain map for JSON encoding.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = preview) do
    preview
    |> to_inspection_preview()
    |> Inspection.DynamicWorkPreview.to_map()
  end

  @doc """
  Converts a canonical inspection preview into the compatibility struct.
  """
  @spec from_inspection_preview(Inspection.DynamicWorkPreview.t()) :: t()
  def from_inspection_preview(%Inspection.DynamicWorkPreview{} = preview) do
    attrs =
      preview
      |> Map.from_struct()
      |> Map.update!(:graph, &GraphInspection.from_inspection_graph/1)

    struct!(__MODULE__, attrs)
  end

  @doc """
  Converts a compatibility preview into the canonical inspection struct.
  """
  @spec to_inspection_preview(t()) :: Inspection.DynamicWorkPreview.t()
  def to_inspection_preview(%__MODULE__{} = preview) do
    attrs =
      preview
      |> Map.from_struct()
      |> Map.update!(:graph, &GraphInspection.to_inspection_graph/1)

    struct!(Inspection.DynamicWorkPreview, attrs)
  end
end
