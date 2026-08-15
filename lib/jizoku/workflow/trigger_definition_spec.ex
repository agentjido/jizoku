defmodule Jizoku.Workflow.TriggerDefinitionSpec do
  @moduledoc """
  Spark entity for one concrete trigger kind inside a workflow trigger.
  """

  defstruct [
    :type,
    :config,
    :expression,
    :__identifier__,
    __spark_metadata__: nil,
    opts: []
  ]

  @type t :: %__MODULE__{
          type: :manual | :cron,
          config: map(),
          expression: String.t() | nil,
          opts: keyword(),
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
