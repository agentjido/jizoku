defmodule Mix.Tasks.Squidie.InstallTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Squidie.Install

  @task "squidie.install"

  setup do
    tmp_dir =
      Path.join(System.tmp_dir!(), "squidie-install-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(tmp_dir, "priv/repo/migrations"))

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
      Mix.Task.reenable(@task)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "creates one current-schema migration", %{tmp_dir: tmp_dir} do
    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    installed_migrations = File.ls!(Path.join(tmp_dir, "priv/repo/migrations"))

    assert [migration] = installed_migrations
    assert String.ends_with?(migration, "create_squidie_schema.exs")
    assert output =~ "creating"

    migration_body = File.read!(Path.join([tmp_dir, "priv/repo/migrations", migration]))

    refute migration_body =~ "create table(:squidie_runs"
    refute migration_body =~ "squidie_runs_schedule_idempotency_index"
    refute migration_body =~ "create table(:squidie_step_runs"
    refute migration_body =~ "create table(:squidie_step_attempts"
    assert migration_body =~ "create table(:squidie_journal_threads"
    assert migration_body =~ "create table(:squidie_journal_entries"
    assert migration_body =~ "create table(:squidie_journal_checkpoints"

    assert output =~ "runtime: :journal"
    assert output =~ "read_model: :read_model"
    assert output =~ "Squidie.execute_next"
    refute output =~ "Squidie.Runtime.Runner.perform(payload)"
  end

  test "creates the current schema when an older copied migration exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_squidie_schema.exs"),
      """
      defmodule ExistingMigration do
        use Ecto.Migration

        def change do
          create table(:squidie_runs)
        end
      end
      """
    )

    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    installed_migrations = File.ls!(Path.join(tmp_dir, "priv/repo/migrations"))

    assert "20260101000000_create_squidie_schema.exs" in installed_migrations

    assert Enum.count(
             installed_migrations,
             &String.ends_with?(&1, "create_squidie_schema.exs")
           ) == 2

    assert output =~ "creating"
  end

  test "skips the current-schema migration when it already exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_squidie_schema.exs"),
      """
      defmodule ExistingMigration do
        use Ecto.Migration

        def change do
          create table(:squidie_journal_threads)
          create table(:squidie_journal_entries)
          create table(:squidie_journal_checkpoints)
        end
      end
      """
    )

    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    assert File.ls!(Path.join(tmp_dir, "priv/repo/migrations")) == [
             "20260101000000_create_squidie_schema.exs"
           ]

    assert output =~ "skipping create_squidie_schema.exs"
  end
end
