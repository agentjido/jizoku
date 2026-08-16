defmodule Mix.Tasks.Jizoku.Retention.Backfill do
  @moduledoc """
  Reports or migrates legacy journal ownership in bounded batches.

      mix jizoku.retention.backfill
      mix jizoku.retention.backfill --batch-size 500 --apply

  The default is read-only. `--apply` updates at most one batch in the selected
  partition; repeat it until `complete=true` before applying deletion plans.
  """

  @shortdoc "Previews or applies one retention ownership backfill batch"

  use Mix.Task

  alias Jizoku.Operations.CLI

  @switches [batch_size: :integer, partition: :string, apply: :boolean, json: :boolean]

  @impl Mix.Task
  def run(args) do
    opts = CLI.parse_options!("jizoku.retention.backfill", args, @switches)
    runtime_opts = runtime_options(opts)

    case CLI.run(fn -> operation(opts, runtime_opts) end) do
      {:ok, report} -> render(report, opts)
      {:error, reason} -> Mix.raise("Retention backfill failed: #{CLI.format_error(reason)}")
    end
  end

  defp operation(opts, runtime_opts) do
    if Keyword.get(opts, :apply, false) do
      Jizoku.backfill_retention_ownership(runtime_opts)
    else
      Jizoku.preview_retention_ownership_backfill(runtime_opts)
    end
  end

  defp runtime_options(opts) do
    []
    |> put_option(:batch_size, Keyword.get(opts, :batch_size))
    |> put_option(:partition, Keyword.get(opts, :partition))
  end

  defp render(report, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    else
      mode = if Keyword.get(opts, :apply, false), do: "applied", else: "preview"

      Mix.shell().info(
        "Retention ownership #{mode} scanned=#{report.scanned_entries} " <>
          "updated=#{report.updated_entries} pending=#{report.pending_entries} " <>
          "complete=#{report.complete?}"
      )

      render_partition(report.partition)
    end
  end

  defp render_partition(nil) do
    :ok
  end

  defp render_partition(partition) do
    Mix.shell().info("Partition: #{partition}")
  end

  defp put_option(opts, _key, nil) do
    opts
  end

  defp put_option(opts, key, value) do
    Keyword.put(opts, key, value)
  end
end
