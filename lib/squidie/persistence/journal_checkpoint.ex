defmodule Squidie.Persistence.JournalCheckpoint do
  @moduledoc """
  Persisted checkpoint value for a Jido storage key.

  Checkpoint keys are arbitrary Elixir terms, so the table stores a stable hash
  for lookup and the encoded key for audit/debugging.
  """

  use Ecto.Schema

  @primary_key {:key_hash, :string, autogenerate: false}
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{}

  schema "squidie_journal_checkpoints" do
    field(:key, :binary)
    field(:checkpoint, :binary)

    timestamps()
  end
end
