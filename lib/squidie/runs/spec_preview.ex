defmodule Squidie.Runs.SpecPreview do
  @moduledoc """
  Read-only execution-style preview for a runtime-authored workflow spec.

  Preview values are intended for visual editors and host tooling that need to
  inspect sample payload behavior before publishing or starting a durable run.
  """

  @type preview_node :: %{
          id: String.t(),
          step: atom(),
          action: atom() | String.t() | nil,
          status: :completed | :failed | :unsupported | :validation_error,
          input: map(),
          output: map() | nil,
          error: map() | nil,
          debug: map()
        }

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

  @doc false
  @spec new(keyword()) :: t()
  def new(attrs) when is_list(attrs) do
    struct!(__MODULE__, attrs)
  end

  @doc """
  Converts a spec execution preview to a plain map for JSON encoding.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = preview) do
    %{
      source: :workflow_spec,
      run_id: nil,
      workflow: preview.workflow,
      definition_version: preview.definition_version,
      trigger: preview.trigger,
      status: preview.status,
      nodes: preview.nodes,
      errors: preview.errors
    }
  end
end
