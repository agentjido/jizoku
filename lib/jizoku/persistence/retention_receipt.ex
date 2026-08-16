defmodule Jizoku.Persistence.RetentionReceipt do
  @moduledoc """
  Non-sensitive proof that one run was removed by an approved retention plan.

  Receipts remain outside deleted workflow history and intentionally exclude
  inputs, outputs, errors, archive reasons, search attributes, and metadata.
  """

  use Ecto.Schema

  @primary_key false
  @timestamps_opts [type: :utc_datetime_usec]

  @type t :: %__MODULE__{}

  schema "jizoku_retention_receipts" do
    field(:partition_key, :string, primary_key: true)
    field(:run_id, :string, primary_key: true)
    field(:plan_digest, :string)
    field(:workflow, :string)
    field(:queue, :string)
    field(:terminal_status, :string)
    field(:run_entries_deleted, :integer)
    field(:dispatch_entries_deleted, :integer)
    field(:deleted_at, :utc_datetime_usec)

    timestamps()
  end
end
