defmodule Mix.Tasks.Squidie.Install do
  @moduledoc """
  Installs Squidie by creating its migration in the host application.

  ## Usage

      $ mix squidie.install

  This task creates one current-schema Squidie migration in
  `priv/repo/migrations` so the host application can run it through its normal
  Ecto migration flow.

  Backend-specific migrations are intentionally not copied. Squidie assumes
  the host application owns the delivery backend and worker loop used to call
  `Squidie.execute_next/1`.
  """

  @shortdoc "Installs Squidie migrations into the host application"
  @schema_migration_name "create_squidie_schema.exs"
  @required_schema_markers [
    "create table(:squidie_journal_threads",
    "create table(:squidie_journal_entries",
    "create table(:squidie_journal_checkpoints"
  ]

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    source_file =
      Application.app_dir(:squidie, ["priv", "repo", "migrations", source_filename()])

    dest_dir = Path.join(["priv", "repo", "migrations"])

    unless File.regular?(source_file) do
      Mix.raise("Could not find Squidie migration at #{source_file}")
    end

    unless File.dir?(dest_dir) do
      Mix.raise("""
      Could not find migrations directory at #{dest_dir}.
      Please ensure your application has an Ecto repository set up.
      """)
    end

    base_timestamp = timestamp()

    if current_schema_installed?(dest_dir) do
      Mix.shell().info("* skipping #{@schema_migration_name} (already installed)")
    else
      new_filename = "#{base_timestamp}_#{@schema_migration_name}"
      File.cp!(source_file, Path.join(dest_dir, new_filename))
      Mix.shell().info("* creating #{new_filename}")
    end

    Mix.shell().info("""

    Squidie migrations have been installed!

    Next steps:
      1. Run `mix ecto.migrate` to apply the migrations
      2. Configure Squidie in your config:

          config :squidie,
            repo: YourApp.Repo,
            runtime: :journal,
            read_model: :read_model

      3. Start your chosen worker loop or backend delivery path and have it
         call `Squidie.execute_next(owner_id: "your-worker-id")` when
         capacity is available. Bedrock is the recommended backend for
         distributed hosts that need durable lease ownership.

    See docs/host_app_integration.md for a copy-paste host setup.
    """)
  end

  defp current_schema_installed?(dest_dir) do
    dest_dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, @schema_migration_name))
    |> Enum.any?(&current_schema_migration?(dest_dir, &1))
  end

  defp current_schema_migration?(dest_dir, filename) do
    body = File.read!(Path.join(dest_dir, filename))
    Enum.all?(@required_schema_markers, &String.contains?(body, &1))
  end

  defp source_filename do
    "20260428000000_#{@schema_migration_name}"
  end

  defp timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.universal_time()
    "#{year}#{pad(month)}#{pad(day)}#{pad(hour)}#{pad(minute)}#{pad(second)}"
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)
end
