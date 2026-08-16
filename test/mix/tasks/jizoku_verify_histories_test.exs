defmodule Mix.Tasks.Jizoku.VerifyHistoriesTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Jizoku.Workflow.Definition
  alias Mix.Tasks.Jizoku.VerifyHistories

  @task "jizoku.verify_histories"

  defmodule HistoricalV1 do
    use Jizoku.Workflow

    workflow do
      version "v1"

      trigger :manual do
        manual()
      end

      step :pause, :pause
      transition :pause, on: :ok, to: :complete
    end
  end

  defmodule CurrentWorkflow do
    use Jizoku.Workflow

    workflow do
      version "v2"

      trigger :manual do
        manual()
      end

      step :pause_v2, :pause
      transition :pause_v2, on: :ok, to: :complete
    end
  end

  setup do
    previous = Application.get_env(:jizoku, :workflow_versions)

    path =
      Path.join(System.tmp_dir!(), "jizoku-history-#{System.unique_integer([:positive])}.exs")

    Application.put_env(:jizoku, :workflow_versions, registry())

    on_exit(fn ->
      restore_env(previous)
      File.rm(path)
      Mix.Task.reenable(@task)
    end)

    {:ok, path: path}
  end

  test "verifies a trusted checked-in fixture and supports JSON output", %{path: path} do
    write_fixture!(path, fingerprint())

    output = capture_io(fn -> VerifyHistories.run(["--json", path]) end)
    report = Jason.decode!(output)

    assert report["total"] == 1
    assert report["verified"] == 1
    assert report["incompatible"] == 0
  end

  test "fails CI when a fixture no longer resolves exactly", %{path: path} do
    write_fixture!(path, "stale-fingerprint")

    assert_raise Mix.Error, ~r/1 of 1 workflow history fixtures are incompatible/, fn ->
      VerifyHistories.run([path])
    end
  end

  test "rejects untrusted task options", %{path: path} do
    assert_raise Mix.Error, ~r/Invalid jizoku.verify_histories options/, fn ->
      VerifyHistories.run([path, "second.exs"])
    end
  end

  defp write_fixture!(path, fingerprint) do
    File.write!(
      path,
      inspect([fixture(fingerprint)], limit: :infinity, printable_limit: :infinity)
    )
  end

  defp fixture(fingerprint) do
    %{
      workflow: CurrentWorkflow,
      definition_version: "v1",
      definition_fingerprint: fingerprint,
      golden_history: %{
        schema_version: 1,
        workflow: Atom.to_string(CurrentWorkflow),
        queue: "default",
        partition: nil,
        status: :paused,
        terminal_status: nil,
        events: [%{type: :run_started, offset_us: 0, run: "run-1", status: :running}]
      }
    }
  end

  defp fingerprint do
    Definition.fingerprint(HistoricalV1.workflow_definition())
  end

  defp registry do
    %{CurrentWorkflow => %{"v1" => HistoricalV1, "v2" => CurrentWorkflow}}
  end

  defp restore_env(nil) do
    Application.delete_env(:jizoku, :workflow_versions)
  end

  defp restore_env(value) do
    Application.put_env(:jizoku, :workflow_versions, value)
  end
end
