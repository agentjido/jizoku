defmodule SquidMesh.Runs.DynamicWorkPreview do
  @moduledoc """
  Validated, read-only preview of one dynamic work record.

  Preview values are intended for dashboards, CLIs, and visual editors that need
  to inspect the graph impact of a dynamic work payload before appending a
  durable journal fact.
  """

  alias SquidMesh.Runs.GraphInspection

  @type t :: %__MODULE__{
          run_id: String.t(),
          duplicate?: boolean(),
          dynamic_work: map(),
          graph: GraphInspection.t()
        }

  defstruct [:run_id, :dynamic_work, :graph, duplicate?: false]

  @doc false
  @spec new(String.t(), map(), boolean(), GraphInspection.t()) :: t()
  def new(run_id, dynamic_work, duplicate?, %GraphInspection{} = graph)
      when is_binary(run_id) and is_map(dynamic_work) and is_boolean(duplicate?) do
    %__MODULE__{
      run_id: run_id,
      dynamic_work: dynamic_work,
      duplicate?: duplicate?,
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
      dynamic_work: preview.dynamic_work,
      graph: GraphInspection.to_map(preview.graph)
    }
  end
end
