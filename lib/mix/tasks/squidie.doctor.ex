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
    opts = CLI.parse_options!("squidie.doctor", args, @switches)

    case collect_report(opts) do
      {:ok, report} ->
        render(report, opts)
        maybe_fail_on_drift!(report, opts)

      {:error, reason} ->
        Mix.raise("Could not run Squidie doctor: #{CLI.format_error(reason)}")
    end
  end

  defp collect_report(opts) do
    CLI.with_json_log_level(opts, fn -> CLI.diagnose(&Doctor.report/0) end)
  end

  defp render(report, opts) do
    render_report(report, Keyword.get(opts, :json, false))
  end

  defp render_report(report, true), do: Mix.shell().info(Jason.encode!(report))

  defp render_report(report, false) do
    Mix.shell().info("Squidie doctor at #{report.generated_at}")
    render_partition(report.partition)

    Mix.shell().info(
      "pass=#{report.summary.pass} warn=#{report.summary.warn} fail=#{report.summary.fail}"
    )

    Enum.each(report.checks, &render_check/1)
  end

  defp render_partition(nil), do: :ok
  defp render_partition(partition), do: Mix.shell().info("Partition: #{partition}")

  defp render_check(check) do
    Mix.shell().info("[#{check.status}] #{check.id}: #{check.message}")
    Enum.each(check.next_actions, &Mix.shell().info("  next: #{&1}"))
  end

  defp maybe_fail_on_drift!(report, opts) do
    enforce_drift_gate(Keyword.get(opts, :fail_on_drift, false), Doctor.drift?(report))
  end

  defp enforce_drift_gate(true, true), do: Mix.raise("Squidie schema drift detected")
  defp enforce_drift_gate(_fail_on_drift, _drift), do: :ok
end
