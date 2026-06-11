# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runs.DynamicWorkPreview do
  @moduledoc """
  Validated, read-only preview of one dynamic work record.

  Preview values are intended for dashboards, CLIs, and visual editors that need
  to inspect the graph impact of a dynamic work payload before appending a
  durable journal fact.
  """

  alias Squidie.Runs.GraphInspection

  @type t :: %__MODULE__{
          run_id: String.t(),
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
    :dynamic_work,
    :graph,
    :origin_node_id,
    added_node_ids: [],
    added_edge_ids: [],
    warnings: [],
    duplicate?: false,
    recordable?: true
  ]

  @doc false
  @spec new(String.t(), map(), boolean(), GraphInspection.t()) :: t()
  def new(run_id, dynamic_work, duplicate?, %GraphInspection{} = graph)
      when is_binary(run_id) and is_map(dynamic_work) and is_boolean(duplicate?) do
    overlay = overlay(dynamic_work, duplicate?)

    %__MODULE__{
      run_id: run_id,
      dynamic_work: dynamic_work,
      duplicate?: duplicate?,
      recordable?: overlay.recordable?,
      origin_node_id: overlay.origin_node_id,
      added_node_ids: overlay.added_node_ids,
      added_edge_ids: overlay.added_edge_ids,
      warnings: overlay.warnings,
      graph: graph
    }
  end

  @doc """
  Converts a dynamic work preview to a plain map for JSON encoding.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = preview) do
    %{
      run_id: preview.run_id,
      duplicate?: preview.duplicate?,
      recordable?: preview.recordable?,
      origin_node_id: preview.origin_node_id,
      added_node_ids: preview.added_node_ids,
      added_edge_ids: preview.added_edge_ids,
      warnings: preview.warnings,
      dynamic_work: preview.dynamic_work,
      graph: GraphInspection.to_map(preview.graph)
    }
  end

  defp overlay(dynamic_work, duplicate?) do
    %{
      recordable?: not duplicate?,
      origin_node_id: origin_node_id(dynamic_work),
      added_node_ids: added_ids(dynamic_work, :nodes, duplicate?),
      added_edge_ids: added_ids(dynamic_work, :edges, duplicate?),
      warnings: warnings(duplicate?)
    }
  end

  defp origin_node_id(dynamic_work) when is_map(dynamic_work) do
    dynamic_work
    |> value(:origin, %{})
    |> value(:step)
  end

  defp added_ids(_dynamic_work, _key, true), do: []

  defp added_ids(dynamic_work, key, false) when is_map(dynamic_work) do
    dynamic_work
    |> value(key, [])
    |> Enum.flat_map(&id_value/1)
  end

  defp id_value(item) when is_map(item) do
    case value(item, :id) do
      id when is_binary(id) -> [id]
      _invalid -> []
    end
  end

  defp id_value(_item), do: []

  defp warnings(true), do: [:duplicate_dynamic_work]
  defp warnings(false), do: []

  defp value(map, field, default \\ nil), do: Squidie.MapField.get(map, field, default)
end
