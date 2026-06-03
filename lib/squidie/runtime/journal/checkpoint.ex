defmodule Squidie.Runtime.Journal.Checkpoint do
  @moduledoc """
  A compact projection snapshot with the durable thread revision it covers.

  Checkpoints are rebuild accelerators. The journal remains the source of truth,
  and `thread_rev` records the last applied Jido thread revision so a projection
  can resume from a precise point instead of silently depending on current code.
  """

  alias Squidie.Runtime.DispatchProtocol.Entry

  @type t :: %__MODULE__{
          thread: Entry.thread(),
          thread_id: String.t(),
          thread_rev: non_neg_integer(),
          projection: term(),
          updated_at: DateTime.t()
        }

  @enforce_keys [:thread, :thread_id, :thread_rev, :projection, :updated_at]
  defstruct [:thread, :thread_id, :thread_rev, :projection, :updated_at]
end
