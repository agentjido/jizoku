defmodule Squidie.Test.Job do
  @moduledoc false

  defstruct [:id, :worker, :queue, :args, :inserted_at, :scheduled_at, :meta]

  @type t :: %__MODULE__{
          id: String.t(),
          worker: String.t(),
          queue: String.t(),
          args: map(),
          inserted_at: DateTime.t(),
          scheduled_at: DateTime.t() | nil,
          meta: map()
        }
end
