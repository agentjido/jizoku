defmodule Jizoku.Persistence.JournalThread do
  @moduledoc """
  Persisted metadata for one Jido journal thread.

  Entries remain append-only in `jizoku_journal_entries`; this row tracks the
  thread revision and metadata needed to reconstruct the Jido thread envelope.
  """

  use Ecto.Schema

  @primary_key {:id, :string, autogenerate: false}
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{}

  schema "jizoku_journal_threads" do
    field(:rev, :integer, default: 0)
    field(:metadata, :map, default: %{})
    field(:created_at_ms, :integer)
    field(:updated_at_ms, :integer)

    timestamps()
  end
end
