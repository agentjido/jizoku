defmodule Mix.Tasks.Squidie.StatusTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Squidie.Status
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Journal

  @task "squidie.status"
  @table :squidie_status_task_test
  @storage {Jido.Storage.ETS, table: @table}
  @now ~U[2026-07-11 12:00:00Z]

  setup do
    previous_storage = Application.get_env(:squidie, :journal_storage)
    previous_queue = Application.get_env(:squidie, :queue)
    previous_partition = Application.get_env(:squidie, :partition)
    Application.put_env(:squidie, :journal_storage, @storage)
    Application.put_env(:squidie, :queue, "default")
    Application.delete_env(:squidie, :partition)
    :ok = Jido.Storage.ETS.Owner.ensure_tables(table: @table)

    on_exit(fn ->
      restore_env(:journal_storage, previous_storage)
      restore_env(:queue, previous_queue)
      restore_env(:partition, previous_partition)
      drop_tables(@table)
      Mix.Task.reenable(@task)
    end)

    :ok
  end

  test "prints parseable JSON and does not mutate durable journal entries" do
    append_run!()
    before = table_contents(@table)

    output = capture_io(fn -> Status.run(["--json"]) end)
    report = Jason.decode!(output)

    assert report["schema_version"] == 1
    assert report["totals"]["runs"] == 1

    assert [%{"workflow" => "BillingWorkflow", "status" => "started"}] =
             report["run_counts"]

    assert table_contents(@table) == before
  end

  test "prints operator-readable run and queue summaries" do
    append_run!()

    output = capture_io(fn -> Status.run([]) end)

    assert output =~ "Squidie status at"
    assert output =~ "Runs: 1  Queues: 1"
    assert output =~ "run BillingWorkflow queue=default status=started count=1"
    assert output =~ "queue default visible=0 scheduled=0 claimed=0"
    assert output =~ "pending_dispatches=0 pending_results=0 manual=0"
  end

  test "prints the configured partition in operator-readable output" do
    Application.put_env(:squidie, :partition, "tenant_acme")

    output = capture_io(fn -> Status.run([]) end)

    assert output =~ "Partition: tenant_acme"
  end

  test "rejects unknown options" do
    assert_raise Mix.Error, ~r/Invalid squidie.status options/, fn ->
      Status.run(["--recover"])
    end
  end

  defp append_run! do
    run_id = "run-status-task"

    {:ok, _thread} =
      Journal.append_entries(@storage, [
        %Entry{
          type: :run_cataloged,
          thread: {:run_catalog, "all"},
          occurred_at: @now,
          data: %{
            run_id: run_id,
            workflow: "BillingWorkflow",
            queue: "default",
            occurred_at: @now
          }
        }
      ])

    {:ok, _thread} =
      Journal.append_entries(@storage, [
        %Entry{
          type: :run_started,
          thread: {:run, run_id},
          occurred_at: @now,
          data: %{
            run_id: run_id,
            workflow: "BillingWorkflow",
            trigger: "manual",
            input: %{},
            context: %{},
            occurred_at: @now
          }
        }
      ])
  end

  defp table_contents(table) do
    ["#{table}_checkpoints", "#{table}_threads", "#{table}_thread_meta"]
    |> Enum.map(&String.to_existing_atom/1)
    |> Map.new(&{&1, :ets.tab2list(&1)})
  end

  defp drop_tables(table) do
    ["#{table}_checkpoints", "#{table}_threads", "#{table}_thread_meta"]
    |> Enum.map(&String.to_existing_atom/1)
    |> Enum.each(fn name ->
      if :ets.whereis(name) != :undefined, do: :ets.delete(name)
    end)
  end

  defp restore_env(key, nil), do: Application.delete_env(:squidie, key)
  defp restore_env(key, value), do: Application.put_env(:squidie, key, value)
end
