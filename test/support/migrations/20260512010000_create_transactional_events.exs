defmodule Squidie.Test.Repo.Migrations.CreateTransactionalEvents do
  use Ecto.Migration

  @spec change() :: :ok
  def change do
    create table(:transactional_events) do
      add(:run_id, :uuid, null: false)
      add(:account_id, :string, null: false)
      add(:event, :string, null: false)

      timestamps()
    end

    create(index(:transactional_events, [:run_id]))
  end
end
