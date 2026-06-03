defmodule Squidie.Workflow.StepSpec do
  @moduledoc """
  Spark entity for one declared Squidie workflow step.

  A step spec captures the step name, implementation module or built-in kind,
  runtime options, and contract metadata discovered while compiling the workflow
  DSL. The runtime converts these entities into the durable workflow definition
  shape used by execution and inspection APIs.
  """

  defstruct [:name, :module, :__identifier__, __spark_metadata__: nil, opts: [], metadata: %{}]

  @type t :: %__MODULE__{
          name: atom(),
          module: module() | atom(),
          __identifier__: term(),
          opts: keyword(),
          metadata: map(),
          __spark_metadata__: term()
        }
end
