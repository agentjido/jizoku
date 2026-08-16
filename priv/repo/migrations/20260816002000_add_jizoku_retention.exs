defmodule Jizoku.Repo.Migrations.AddJizokuRetention do
  use Ecto.Migration

  def change do
    alter table(:jizoku_journal_entries) do
      add(:retention_run_id, :text)
    end

    create(
      index(:jizoku_journal_entries, [:thread_id, :retention_run_id],
        name: :jizoku_journal_entries_retention_owner_idx
      )
    )

    create table(:jizoku_retention_receipts, primary_key: false) do
      add(:partition_key, :text, primary_key: true)
      add(:run_id, :text, primary_key: true)
      add(:plan_digest, :string, null: false)
      add(:workflow, :text, null: false)
      add(:queue, :text, null: false)
      add(:terminal_status, :text, null: false)
      add(:run_entries_deleted, :bigint, null: false)
      add(:dispatch_entries_deleted, :bigint, null: false)
      add(:deleted_at, :utc_datetime_usec, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(
      index(:jizoku_retention_receipts, [:plan_digest], name: :jizoku_retention_receipts_plan_idx)
    )

    create(
      index(:jizoku_retention_receipts, [:partition_key, :deleted_at, :run_id],
        name: :jizoku_retention_receipts_deleted_idx
      )
    )
  end
end
