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
    opts = parse_options!(args)

    case collect_report(opts) do
      {:ok, report} ->
        render(report, opts)

      {:error, reason} ->
        Mix.raise("Could not collect Squidie status: #{CLI.format_error(reason)}")
    end
  end

  defp collect_report(opts) do
    with_json_log_level(opts, fn -> CLI.run(&Status.report/0) end)
  end

  defp parse_options!(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        opts

      {_opts, positional, invalid} ->
        values = positional ++ Enum.map(invalid, fn {option, _value} -> option end)
        Mix.raise("Invalid squidie.status options: #{Enum.join(values, ", ")}")
    end
  end

  defp render(report, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    else
      Mix.shell().info("Squidie status at #{report.generated_at}")
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
  end

  defp with_json_log_level(opts, fun) do
    if Keyword.get(opts, :json, false) do
      previous_level = :logger.get_primary_config()[:level]
      Logger.configure(level: :warning)

      try do
        fun.()
      after
        Logger.configure(level: previous_level)
      end
    else
      fun.()
    end
  end
end
