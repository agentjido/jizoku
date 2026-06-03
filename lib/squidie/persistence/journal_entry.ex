defmodule Squidie.Persistence.JournalEntry do
  @moduledoc """
  Append-only persisted Jido journal entry.

  The adapter stores the canonical `Jido.Thread.Entry` term in `entry` and keeps
  `thread_id` plus `seq` as the ordered replay index.
  """

  use Ecto.Schema

  alias Squidie.Persistence.JournalThread

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :string
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{}

  schema "squidie_journal_entries" do
    field(:seq, :integer)
    field(:entry, :binary)

    belongs_to(:thread, JournalThread)

    timestamps()
  end
end
