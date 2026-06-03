defmodule Squidie.Workflow.RunicPlanner.Runnable do
  @moduledoc false

  @type t :: %__MODULE__{
          id: integer(),
          step: atom(),
          input: term(),
          metadata: map(),
          runic_runnable: Runic.Workflow.Runnable.t()
        }

  defstruct [:id, :step, :input, :metadata, :runic_runnable]
end
