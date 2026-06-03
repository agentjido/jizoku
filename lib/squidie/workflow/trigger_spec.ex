defmodule Squidie.Workflow.TriggerSpec do
  @moduledoc """
  Spark entity for one Squidie workflow trigger declaration.
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
          definitions: [Squidie.Workflow.TriggerDefinitionSpec.t()],
          invalid_fields: [Squidie.Workflow.PayloadFieldSpec.t()],
          payload: [Squidie.Workflow.PayloadSpec.t()],
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
