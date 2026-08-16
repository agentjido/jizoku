defmodule Jizoku.Persistence.RunSearch do
  @moduledoc """
  Rebuildable Ecto projection row for indexed operational run queries.

  Journal facts remain authoritative. This table can be dropped and rebuilt
  without changing workflow history.
  """

  use Ecto.Schema

  @primary_key false
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{}

  schema "jizoku_run_search" do
    field(:partition_key, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:partition, :string)
    field(:workflow, :string)
    field(:status, :string)
    field(:terminal_status, :string)
    field(:definition_version, :string)
    field(:search_attributes, :map, default: %{})
    field(:started_at, :utc_datetime_usec)
    field(:terminal_at, :utc_datetime_usec)
    field(:archived_at, :utc_datetime_usec)
    field(:archive_reason, :string)
    field(:thread_revision, :integer)

    timestamps()
  end
end
