defmodule Jizoku.Workflow.Compatibility.Difference do
  @moduledoc "One deterministic structural difference between two workflow definitions."

  @type category :: :compatible | :migration_required | :incompatible

  @type t :: %__MODULE__{
          category: category(),
          kind: atom(),
          path: [String.t() | atom() | non_neg_integer()],
          old: term(),
          new: term()
        }

  @enforce_keys [:category, :kind, :path, :old, :new]
  defstruct [:category, :kind, :path, :old, :new]
end
