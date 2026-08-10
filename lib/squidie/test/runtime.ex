defmodule Squidie.Test.Runtime do
  @moduledoc false

  @enforce_keys [:id, :owner, :workflow, :storage, :storage_server, :queue, :max_steps]
  defstruct [
    :id,
    :owner,
    :workflow,
    :storage,
    :storage_server,
    :queue,
    :partition,
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
          max_steps: pos_integer()
        }
end
