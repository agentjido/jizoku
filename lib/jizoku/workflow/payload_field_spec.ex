defmodule Jizoku.Workflow.PayloadFieldSpec do
  @moduledoc """
  Spark entity for one trigger payload field.
  """

  defstruct [:name, :type, :__identifier__, __spark_metadata__: nil, opts: []]

  @type t :: %__MODULE__{
          name: atom(),
          type: atom(),
          opts: keyword(),
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
