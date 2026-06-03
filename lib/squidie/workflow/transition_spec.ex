defmodule Squidie.Workflow.TransitionSpec do
  @moduledoc """
  Spark entity for one declared workflow transition.
  """

  defstruct [
    :from,
    :on,
    :to,
    :recovery,
    :condition,
    :__identifier__,
    __spark_metadata__: nil,
    opts: []
  ]

  @type t :: %__MODULE__{
          from: atom(),
          on: atom(),
          to: atom(),
          recovery: atom() | nil,
          condition: map() | nil,
          opts: keyword(),
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
