defmodule Jizoku.Repo.Migrations.AddJizokuRunArchives do
  use Ecto.Migration

  def change do
    alter table(:jizoku_run_search) do
      add(:archived_at, :utc_datetime_usec)
      add(:archive_reason, :text)
    end

    create(
      index(:jizoku_run_search, [:partition_key, :archived_at, :started_at, :run_id],
        name: :jizoku_run_search_archive_idx
      )
    )
  end
end
