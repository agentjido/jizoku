defmodule Mix.Tasks.Jizoku.InstallTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Jizoku.Install

  @task "jizoku.install"

  setup do
    tmp_dir =
      Path.join(System.tmp_dir!(), "jizoku-install-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(tmp_dir, "priv/repo/migrations"))

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
      Mix.Task.reenable(@task)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "creates the current journal and run-search migrations", %{tmp_dir: tmp_dir} do
    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    installed_migrations = File.ls!(Path.join(tmp_dir, "priv/repo/migrations"))

    assert [_journal_migration, _search_migration] = installed_migrations
    assert Enum.any?(installed_migrations, &String.ends_with?(&1, "create_jizoku_schema.exs"))

    assert Enum.any?(
             installed_migrations,
             &String.ends_with?(&1, "add_jizoku_run_search_projection.exs")
           )

    assert output =~ "creating"

    migration_body =
      Enum.map_join(installed_migrations, "\n", fn migration ->
        File.read!(Path.join([tmp_dir, "priv/repo/migrations", migration]))
      end)

    refute migration_body =~ "create table(:jizoku_runs"
    refute migration_body =~ "jizoku_runs_schedule_idempotency_index"
    refute migration_body =~ "create table(:jizoku_step_runs"
    refute migration_body =~ "create table(:jizoku_step_attempts"
    assert migration_body =~ "create table(:jizoku_journal_threads"
    assert migration_body =~ "create table(:jizoku_journal_entries"
    assert migration_body =~ "create table(:jizoku_journal_checkpoints"
    assert migration_body =~ "create table(:jizoku_run_search"

    assert output =~ "runtime: :journal"
    assert output =~ "read_model: :read_model"
    assert output =~ "Jizoku.execute_next"
    refute output =~ "Jizoku.Runtime.Runner.perform(payload)"
  end

  test "creates current migrations when an unrelated older migration exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_jizoku_schema.exs"),
      """
      defmodule ExistingMigration do
        use Ecto.Migration

        def change do
          create table(:jizoku_runs)
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

    assert "20260101000000_create_jizoku_schema.exs" in installed_migrations

    assert [_existing_migration, _journal_migration, _search_migration] = installed_migrations

    assert Enum.count(installed_migrations, &String.ends_with?(&1, "create_jizoku_schema.exs")) ==
             2

    assert Enum.any?(
             installed_migrations,
             &String.ends_with?(&1, "add_jizoku_run_search_projection.exs")
           )

    assert output =~ "creating"
  end

  test "adds only run search when the journal baseline already exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_jizoku_schema.exs"),
      """
      defmodule ExistingMigration do
        use Ecto.Migration

        def change do
          create table(:jizoku_journal_threads)
          create table(:jizoku_journal_entries)
          create table(:jizoku_journal_checkpoints)
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

    assert "20260101000000_create_jizoku_schema.exs" in installed_migrations
    assert [_existing_migration, _search_migration] = installed_migrations

    assert Enum.any?(
             installed_migrations,
             &String.ends_with?(&1, "add_jizoku_run_search_projection.exs")
           )

    assert output =~ "creating"
  end

  test "skips migrations when journal and run search are already represented", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_jizoku_schema.exs"),
      """
      defmodule ExistingMigration do
        use Ecto.Migration

        def change do
          create table(:jizoku_journal_threads)
          create table(:jizoku_journal_entries)
          create table(:jizoku_journal_checkpoints)
          create table(:jizoku_run_search)
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
             "20260101000000_create_jizoku_schema.exs"
           ]

    assert output =~ "skipping Jizoku migrations"
  end
end
