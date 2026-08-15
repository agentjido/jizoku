defmodule Jizoku.Runs.SpecPreview do
  @moduledoc """
  Compatibility preview struct for runtime-authored workflow specs.

  `Jizoku.Inspection.SpecPreview` is the canonical inspection namespace. This
  module preserves the original `Jizoku.Runs.*` struct for existing callers.
  """

  alias Jizoku.Inspection

  @type preview_node :: Inspection.SpecPreview.preview_node()

  @type t :: %__MODULE__{
          run_id: nil,
          workflow: module(),
          definition_version: String.t() | nil,
          trigger: atom() | nil,
          status: :completed | :failed | :blocked | :invalid,
          nodes: [preview_node()],
          errors: [map()]
        }

  defstruct [
    :workflow,
    :definition_version,
    :trigger,
    run_id: nil,
    status: :completed,
    nodes: [],
    errors: []
  ]

  @doc """
  Builds a compatibility spec preview struct.
  """
  @spec new(keyword()) :: t()
  def new(attrs) when is_list(attrs) do
    struct!(__MODULE__, attrs)
  end

  @doc """
  Converts a compatibility spec preview to a plain map for JSON encoding.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = preview) do
    preview
    |> Map.from_struct()
    |> then(&struct!(Inspection.SpecPreview, &1))
    |> Inspection.SpecPreview.to_map()
  end
end
