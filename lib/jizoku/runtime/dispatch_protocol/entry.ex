defmodule Jizoku.Runtime.DispatchProtocol.Entry do
  @moduledoc """
  One durable runtime journal entry.

  Entries are intentionally shaped as append-only facts. Checkpoints and live
  wakeups are derived from replaying entries; they are not the source of truth.
  """

  @type thread ::
          {:run, String.t()}
          | {:dispatch, String.t()}
          | {:jido_signal, String.t()}
          | {:run_index, String.t()}
          | {:run_catalog, String.t()}

  @type t :: %__MODULE__{
          type: atom(),
          thread: thread(),
          data: map(),
          occurred_at: DateTime.t()
        }

  @enforce_keys [:type, :thread, :data, :occurred_at]
  defstruct [:type, :thread, :data, :occurred_at]
end
