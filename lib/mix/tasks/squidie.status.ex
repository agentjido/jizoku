defmodule Mix.Tasks.Squidie.Status do
  @moduledoc """
  Reports read-only Squidie run and queue health.

      mix squidie.status
      mix squidie.status --json

  The task reads durable journal projections and does not recover, schedule,
  apply, or delete workflow state.
  """

  @shortdoc "Reports read-only Squidie run and queue health"

  use Mix.Task

  alias Squidie.Operations.CLI
  alias Squidie.Operations.Status

  @switches [json: :boolean]

  @impl Mix.Task
  def run(args) do
    opts = CLI.parse_options!("squidie.status", args, @switches)

    case collect_report(opts) do
      {:ok, report} ->
        render(report, opts)

      {:error, reason} ->
        Mix.raise("Could not collect Squidie status: #{CLI.format_error(reason)}")
    end
  end

  defp collect_report(opts) do
    CLI.with_json_log_level(opts, fn -> CLI.run(&Status.report/0) end)
  end

  defp render(report, opts) do
    render_report(report, Keyword.get(opts, :json, false))
  end

  defp render_report(report, true), do: Mix.shell().info(Jason.encode!(report))

  defp render_report(report, false) do
    Mix.shell().info("Squidie status at #{report.generated_at}")
    render_partition(report.partition)
    Mix.shell().info("Runs: #{report.totals.runs}  Queues: #{report.totals.queues}")

    Enum.each(report.run_counts, fn row ->
      Mix.shell().info(
        "run #{row.workflow} queue=#{row.queue} status=#{row.status} count=#{row.count}"
      )
    end)

    Enum.each(report.queues, fn queue ->
      Mix.shell().info(
        "queue #{queue.queue} visible=#{queue.visible_attempt_depth} scheduled=#{queue.scheduled_attempt_depth} " <>
          "claimed=#{queue.claimed_attempt_depth} pending_dispatches=#{queue.pending_dispatch_count} " <>
          "pending_results=#{queue.pending_result_count} manual=#{queue.manual_intervention_count}"
      )
    end)
  end

  defp render_partition(nil), do: :ok
  defp render_partition(partition), do: Mix.shell().info("Partition: #{partition}")
end
