defmodule Squidie.Test.Runtime do
  @moduledoc false

  @enforce_keys [:id, :owner, :workflow, :storage, :storage_server, :queue, :now, :max_steps]
  defstruct [
    :id,
    :owner,
    :workflow,
    :storage,
    :storage_server,
    :queue,
    :partition,
    :now,
    :max_steps
  ]

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          owner: pid(),
          workflow: module(),
          storage: Squidie.Runtime.Journal.Storage.config(),
          storage_server: pid(),
          queue: String.t(),
          partition: String.t() | nil,
          now: DateTime.t(),
          max_steps: pos_integer()
        }
end
