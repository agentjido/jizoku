defmodule Jizoku.Workflow.Compatibility.Result do
  @moduledoc "Deterministic compatibility classification and structured differences."

  alias Jizoku.Workflow.Compatibility.Difference

  @type t :: %__MODULE__{
          category: Difference.category(),
          differences: [Difference.t()]
        }

  @enforce_keys [:category, :differences]
  defstruct [:category, :differences]
end
