defmodule Jizoku.Repo.Migrations.AddJizokuRunSearchProjection do
  use Ecto.Migration

  def change do
    create table(:jizoku_run_search, primary_key: false) do
      add(:partition_key, :text, primary_key: true)
      add(:run_id, :text, primary_key: true)
      add(:partition, :text)
      add(:workflow, :text, null: false)
      add(:status, :text, null: false)
      add(:terminal_status, :text)
      add(:definition_version, :text)
      add(:search_attributes, :map, null: false, default: %{})
      add(:started_at, :utc_datetime_usec, null: false)
      add(:terminal_at, :utc_datetime_usec)
      add(:thread_revision, :bigint, null: false)

      timestamps(type: :utc_datetime_usec)
    end

    create(
      index(:jizoku_run_search, [:partition_key, :started_at, :run_id],
        name: :jizoku_run_search_page_idx
      )
    )

    create(
      index(
        :jizoku_run_search,
        [:partition_key, :workflow, :started_at, :run_id],
        name: :jizoku_run_search_workflow_idx
      )
    )

    create(
      index(:jizoku_run_search, [:partition_key, :status, :started_at, :run_id],
        name: :jizoku_run_search_status_idx
      )
    )

    create(
      index(
        :jizoku_run_search,
        [:partition_key, :definition_version, :started_at, :run_id],
        name: :jizoku_run_search_definition_version_idx
      )
    )

    create(
      index(:jizoku_run_search, [:partition_key, :terminal_at, :run_id],
        name: :jizoku_run_search_terminal_idx
      )
    )

    create(
      index(:jizoku_run_search, [:search_attributes],
        using: :gin,
        name: :jizoku_run_search_attributes_gin_idx
      )
    )
  end
end
