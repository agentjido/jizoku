defmodule Squidie.Workflow.PayloadSpec do
  @moduledoc """
  Spark entity for a trigger payload block.
  """

  defstruct [:__identifier__, __spark_metadata__: nil, fields: []]

  @type t :: %__MODULE__{
          fields: [Squidie.Workflow.PayloadFieldSpec.t()],
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
