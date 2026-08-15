defmodule Jizoku.Workflow.TriggerSpec do
  @moduledoc """
  Spark entity for one Jizoku workflow trigger declaration.
  """

  defstruct [
    :name,
    :__identifier__,
    __spark_metadata__: nil,
    definitions: [],
    invalid_fields: [],
    payload: []
  ]

  @type t :: %__MODULE__{
          name: atom(),
          definitions: [Jizoku.Workflow.TriggerDefinitionSpec.t()],
          invalid_fields: [Jizoku.Workflow.PayloadFieldSpec.t()],
          payload: [Jizoku.Workflow.PayloadSpec.t()],
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
