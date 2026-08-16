defmodule Mix.Tasks.Jizoku.Install do
  @moduledoc """
  Installs Jizoku by creating its migration in the host application.

  ## Usage

      $ mix jizoku.install

  This task creates the current Jizoku migrations in
  `priv/repo/migrations` so the host application can run it through its normal
  Ecto migration flow.

  Backend-specific migrations are intentionally not copied. Jizoku assumes
  the host application owns the delivery backend and worker loop used to call
  `Jizoku.execute_next/1`.
  """

  @shortdoc "Installs Jizoku migrations into the host application"
  @journal_migration_name "create_jizoku_schema.exs"
  @search_migration_name "add_jizoku_run_search_projection.exs"
  @journal_tables [
    "jizoku_journal_threads",
    "jizoku_journal_entries",
    "jizoku_journal_checkpoints"
  ]
  @journal_markers Enum.map(@journal_tables, &"create table(:#{&1}")
  @search_markers ["create table(:jizoku_run_search"]
  @migrations [
    %{
      name: @journal_migration_name,
      source: "20260815000000_create_jizoku_schema.exs",
      markers: @journal_markers
    },
    %{
      name: @search_migration_name,
      source: "20260816000000_add_jizoku_run_search_projection.exs",
      markers: @search_markers
    }
  ]

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    dest_dir = Path.join(["priv", "repo", "migrations"])

    unless File.dir?(dest_dir) do
      Mix.raise("""
      Could not find migrations directory at #{dest_dir}.
      Please ensure your application has an Ecto repository set up.
      """)
    end

    validate_sources!()
    install_missing_migrations(dest_dir)

    Mix.shell().info("""

    Jizoku migrations have been installed!

    Next steps:
      1. Run `mix ecto.migrate` to apply the migrations
      2. Configure Jizoku in your config:

          config :jizoku,
            repo: YourApp.Repo,
            runtime: :journal,
            read_model: :read_model

      3. Start your chosen worker loop or backend delivery path and have it
         call `Jizoku.execute_next(owner_id: "your-worker-id")` when
         capacity is available. Bedrock is the recommended backend for
         distributed hosts that need durable lease ownership.

    See docs/host_app_integration.md for a copy-paste host setup.
    """)
  end

  defp install_missing_migrations(dest_dir) do
    installed_body = installed_migration_body(dest_dir)
    missing = Enum.reject(@migrations, &markers_present?(installed_body, &1.markers))

    case missing do
      [] ->
        Mix.shell().info("* skipping Jizoku migrations (current schema already installed)")

      migrations ->
        base_version = String.to_integer(timestamp())

        migrations
        |> Enum.with_index()
        |> Enum.each(fn {migration, offset} ->
          copy_migration!(dest_dir, migration, base_version + offset)
        end)
    end
  end

  defp installed_migration_body(dest_dir) do
    dest_dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".exs"))
    |> Enum.map_join("\n", &File.read!(Path.join(dest_dir, &1)))
  end

  defp markers_present?(body, markers) do
    Enum.all?(markers, &String.contains?(body, &1))
  end

  defp copy_migration!(dest_dir, migration, version) do
    filename = "#{version}_#{migration.name}"
    File.cp!(source_path(migration), Path.join(dest_dir, filename))
    Mix.shell().info("* creating #{filename}")
  end

  defp validate_sources! do
    Enum.each(@migrations, fn migration ->
      source = source_path(migration)

      unless File.regular?(source) do
        Mix.raise("Could not find Jizoku migration at #{source}")
      end
    end)
  end

  defp source_path(migration) do
    Application.app_dir(:jizoku, ["priv", "repo", "migrations", migration.source])
  end

  defp timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.universal_time()
    "#{year}#{pad(month)}#{pad(day)}#{pad(hour)}#{pad(minute)}#{pad(second)}"
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)
end
