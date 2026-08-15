defmodule Mix.Tasks.Jizoku.DoctorTest do
  use Jizoku.DataCase, async: false

  import ExUnit.CaptureIO

  alias Jizoku.Runtime.Journal.Storage.Ecto
  alias Mix.Tasks.Jizoku.Doctor

  @task "jizoku.doctor"
  @table :jizoku_doctor_task_test

  setup do
    previous_storage = Application.get_env(:jizoku, :journal_storage)
    previous_queue = Application.get_env(:jizoku, :queue)
    previous_partition = Application.get_env(:jizoku, :partition)
    previous_heartbeat_interval = Application.get_env(:jizoku, :heartbeat_interval_ms)
    Application.delete_env(:jizoku, :partition)

    on_exit(fn ->
      restore_env(:journal_storage, previous_storage)
      restore_env(:queue, previous_queue)
      restore_env(:partition, previous_partition)
      restore_env(:heartbeat_interval_ms, previous_heartbeat_interval)
      drop_tables(@table)
      Mix.Task.reenable(@task)
    end)

    :ok
  end

  test "prints JSON diagnostics without prose" do
    Application.put_env(:jizoku, :journal_storage, {Jido.Storage.ETS, table: @table})
    Application.put_env(:jizoku, :queue, "default")
    Application.put_env(:jizoku, :partition, "tenant_acme")

    output = capture_io(fn -> Doctor.run(["--json"]) end)
    report = Jason.decode!(output)

    assert report["schema_version"] == 1
    assert report["partition"] == "tenant_acme"
    assert is_boolean(report["healthy"])
    assert [%{"id" => "configuration"} | _checks] = report["checks"]
    assert Enum.any?(report["checks"], &(&1["id"] == "schema"))
  end

  test "prints operator-readable checks and next actions" do
    Application.put_env(:jizoku, :journal_storage, {Jido.Storage.ETS, table: @table})
    Application.put_env(:jizoku, :queue, "default")
    Application.put_env(:jizoku, :partition, "tenant_acme")
    Application.put_env(:jizoku, :heartbeat_interval_ms, 1_000)

    output = capture_io(fn -> Doctor.run([]) end)

    assert output =~ "Jizoku doctor at"
    assert output =~ "Partition: tenant_acme"
    assert output =~ ~r/pass=\d+ warn=\d+ fail=\d+/
    assert output =~ "[warn] configuration:"
    assert output =~ "next: move_heartbeat_options_to_execute_next_worker_calls"
    assert output =~ "[pass] schema:"
  end

  test "emits complete JSON before failing an explicit schema drift gate" do
    Application.put_env(
      :jizoku,
      :journal_storage,
      {Ecto, repo: Jizoku.Test.Repo, prefix: "missing_jizoku_schema"}
    )

    output =
      capture_io(fn ->
        assert_raise Mix.Error, ~r/Jizoku schema drift detected/, fn ->
          Doctor.run(["--json", "--fail-on-drift"])
        end
      end)

    report = Jason.decode!(output)
    schema = Enum.find(report["checks"], &(&1["id"] == "schema"))
    assert schema["status"] == "fail"
    assert schema["details"]["status"] == "behind"
  end

  test "rejects unknown options without invoking recovery" do
    assert_raise Mix.Error, ~r/Invalid jizoku.doctor options/, fn ->
      Doctor.run(["--repair"])
    end
  end

  defp drop_tables(table) do
    ["#{table}_checkpoints", "#{table}_threads", "#{table}_thread_meta"]
    |> Enum.map(&String.to_existing_atom/1)
    |> Enum.each(fn name ->
      if :ets.whereis(name) != :undefined, do: :ets.delete(name)
    end)
  rescue
    ArgumentError -> :ok
  end

  defp restore_env(key, nil), do: Application.delete_env(:jizoku, key)
  defp restore_env(key, value), do: Application.put_env(:jizoku, key, value)
end
