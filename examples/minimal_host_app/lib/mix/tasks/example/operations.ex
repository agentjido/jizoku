defmodule Mix.Tasks.Example.Operations do
  @moduledoc """
  Verifies Jizoku's operational commands through the example host app.

  The verification runs both commands without starting the host application's
  supervision tree, decodes their JSON output, and enables the schema-drift
  failure gate for the doctor command.
  """

  use Mix.Task

  @shortdoc "Verifies Jizoku's operational commands"

  @impl Mix.Task
  def run([]) do
    ensure_host_not_started!()
    previous_shell = Mix.shell()
    Mix.shell(Mix.Shell.Process)

    try do
      status = run_json_task!("jizoku.status", ["--json"])
      validate_status!(status)
      ensure_host_not_started!()

      doctor =
        run_json_task!("jizoku.doctor", ["--json", "--fail-on-drift"])

      validate_doctor!(doctor)
      ensure_host_not_started!()
    after
      Mix.shell(previous_shell)
    end

    Mix.shell().info("Jizoku operational command verification passed.")
  end

  def run(_args) do
    Mix.raise("Usage: mix example.operations")
  end

  defp run_json_task!(task, args) do
    Mix.Task.reenable(task)
    Mix.Task.run(task, args)
    receive_json!(task)
  end

  defp receive_json!(task) do
    receive do
      {:mix_shell, :info, [output]} -> decode_json_output(output, task)
    after
      1_000 -> Mix.raise("#{task} did not emit a JSON report")
    end
  end

  defp decode_json_output(output, task) do
    case Jason.decode(output) do
      {:ok, report} -> report
      {:error, _reason} -> receive_json!(task)
    end
  end

  defp validate_status!(%{
         "schema_version" => 1,
         "totals" => %{"runs" => runs, "queues" => queues},
         "run_counts" => run_counts,
         "queues" => queue_reports
       })
       when is_integer(runs) and is_integer(queues) and is_list(run_counts) and
              is_list(queue_reports),
       do: :ok

  defp validate_status!(_report), do: Mix.raise("unexpected jizoku.status JSON contract")

  defp validate_doctor!(%{
         "schema_version" => 1,
         "checks" => checks,
         "summary" => %{"pass" => pass, "warn" => warn, "fail" => fail}
       })
       when is_list(checks) and is_integer(pass) and is_integer(warn) and is_integer(fail) do
    case Enum.find(checks, &(&1["id"] == "schema")) do
      %{"status" => "pass", "details" => %{"status" => "current"}} -> :ok
      _check -> Mix.raise("jizoku.doctor did not report the example schema as current")
    end
  end

  defp validate_doctor!(_report), do: Mix.raise("unexpected jizoku.doctor JSON contract")

  defp ensure_host_not_started! do
    if Process.whereis(MinimalHostApp.Supervisor) do
      Mix.raise("operational commands started the host supervision tree")
    end
  end
end
