defmodule Squidie.Repo.Migrations.CreateSquidieSchema do
  use Ecto.Migration

  def change do
    create table(:squidie_journal_threads, primary_key: false) do
      add :id, :text, primary_key: true
      add :rev, :bigint, null: false, default: 0
      add :metadata, :map, null: false, default: %{}
      add :created_at_ms, :bigint, null: false
      add :updated_at_ms, :bigint, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create table(:squidie_journal_entries, primary_key: false) do
      add :id, :binary_id, primary_key: true

      add :thread_id,
          references(:squidie_journal_threads,
            column: :id,
            type: :text,
            on_delete: :delete_all
          ),
          null: false

      add :seq, :bigint, null: false
      add :entry, :binary, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create unique_index(:squidie_journal_entries, [:thread_id, :seq])

    create table(:squidie_journal_checkpoints, primary_key: false) do
      add :key_hash, :string, primary_key: true
      add :key, :binary, null: false
      add :checkpoint, :binary, null: false

      timestamps(type: :utc_datetime_usec)
    end
  end
end
