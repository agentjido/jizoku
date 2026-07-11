defmodule Mix.Tasks.Squidie.Doctor do
  @moduledoc """
  Reports read-only Squidie diagnostics and operator next actions.

      mix squidie.doctor
      mix squidie.doctor --json
      mix squidie.doctor --json --fail-on-drift

  The default path is informational. `--fail-on-drift` exits nonzero only when
  the configured database is behind or incompatible with Squidie's required
  schema baseline.
  """

  @shortdoc "Reports read-only Squidie diagnostics"

  use Mix.Task

  alias Squidie.Operations.CLI
  alias Squidie.Operations.Doctor

  @switches [json: :boolean, fail_on_drift: :boolean]

  @impl Mix.Task
  def run(args) do
    opts = parse_options!(args)

    case collect_report(opts) do
      {:ok, report} ->
        render(report, opts)
        maybe_fail_on_drift!(report, opts)

      {:error, reason} ->
        Mix.raise("Could not run Squidie doctor: #{CLI.format_error(reason)}")
    end
  end

  defp collect_report(opts) do
    with_json_log_level(opts, fn -> CLI.diagnose(&Doctor.report/0) end)
  end

  defp parse_options!(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        opts

      {_opts, positional, invalid} ->
        values = positional ++ Enum.map(invalid, fn {option, _value} -> option end)
        Mix.raise("Invalid squidie.doctor options: #{Enum.join(values, ", ")}")
    end
  end

  defp render(report, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    else
      Mix.shell().info("Squidie doctor at #{report.generated_at}")

      Mix.shell().info(
        "pass=#{report.summary.pass} warn=#{report.summary.warn} fail=#{report.summary.fail}"
      )

      Enum.each(report.checks, &render_check/1)
    end
  end

  defp render_check(check) do
    Mix.shell().info("[#{check.status}] #{check.id}: #{check.message}")
    Enum.each(check.next_actions, &Mix.shell().info("  next: #{&1}"))
  end

  defp maybe_fail_on_drift!(report, opts) do
    if Keyword.get(opts, :fail_on_drift, false) and Doctor.drift?(report) do
      Mix.raise("Squidie schema drift detected")
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
