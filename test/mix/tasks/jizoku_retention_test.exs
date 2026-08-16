defmodule Mix.Tasks.Jizoku.RetentionTest do
  use Jizoku.DataCase, async: false

  import ExUnit.CaptureIO

  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Journal
  alias Mix.Tasks.Jizoku.Retention
  alias Mix.Tasks.Jizoku.Retention.Backfill

  @storage {Jizoku.Runtime.Journal.Storage.Ecto, repo: Repo}
  @retention_task "jizoku.retention"
  @backfill_task "jizoku.retention.backfill"

  defmodule Record do
    use Jizoku.Step, name: "retention_task_record"

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{recorded: true}}
    end
  end

  defmodule Workflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :record, Record
      transition :record, on: :ok, to: :complete
    end
  end

  setup do
    previous_runtime = Application.get_env(:jizoku, :runtime)
    previous_storage = Application.get_env(:jizoku, :journal_storage)
    Application.put_env(:jizoku, :runtime, :journal)
    Application.put_env(:jizoku, :journal_storage, @storage)

    on_exit(fn ->
      restore_env(:runtime, previous_runtime)
      restore_env(:journal_storage, previous_storage)
      Mix.Task.reenable(@retention_task)
      Mix.Task.reenable(@backfill_task)
    end)

    :ok
  end

  test "previews by default and applies only with the exact timestamp and confirmation" do
    run_id = Ecto.UUID.generate()
    queue = "retention-task-#{System.unique_integer([:positive])}"
    started_at = DateTime.add(DateTime.utc_now(), -120, :second)
    runtime_opts = [journal_storage: @storage, queue: queue, now: started_at, run_id: run_id]
    control_opts = Keyword.delete(runtime_opts, :run_id)

    assert {:ok, started} = Jizoku.start(Workflow, :manual, %{}, runtime_opts)

    assert {:ok, _cancelled} =
             Jizoku.cancel(
               started.run_id,
               Keyword.merge(control_opts, now: DateTime.add(started_at, 1, :second))
             )

    assert {:ok, _archived} =
             Jizoku.archive_run(
               started.run_id,
               Keyword.merge(control_opts,
                 now: DateTime.add(started_at, 2, :second),
                 reason: "task_test"
               )
             )

    terminal_before = DateTime.to_iso8601(DateTime.add(started_at, 60, :second))

    preview_output =
      capture_io(fn ->
        Retention.run(["--terminal-before", terminal_before, "--json"])
      end)

    preview = Jason.decode!(preview_output)
    assert preview["mode"] == "preview"
    assert [%{"run_id" => ^run_id}] = preview["plan"]["eligible"]
    assert {:ok, _snapshot} = Jizoku.inspect_run(run_id, journal_storage: @storage, queue: queue)

    Mix.Task.reenable(@retention_task)

    apply_output =
      capture_io(fn ->
        Retention.run([
          "--terminal-before",
          terminal_before,
          "--created-at",
          preview["plan"]["created_at"],
          "--apply",
          "--confirmation",
          preview["plan"]["confirmation_token"],
          "--json"
        ])
      end)

    applied = Jason.decode!(apply_output)
    assert applied["mode"] == "apply"
    assert applied["receipt"]["run_ids"] == [run_id]

    assert {:error, :not_found} =
             Jizoku.inspect_run(run_id, journal_storage: @storage, queue: queue)
  end

  test "ownership task is read-only by default and updates one explicit batch" do
    run_id = Ecto.UUID.generate()

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               %Entry{
                 type: :run_cataloged,
                 thread: {:run_catalog, "all"},
                 occurred_at: DateTime.utc_now(),
                 data: %{
                   run_id: run_id,
                   workflow: "RetentionTaskWorkflow",
                   queue: "retention-task",
                   occurred_at: DateTime.utc_now()
                 }
               }
             ])

    Repo.update_all(JournalEntry, set: [retention_run_id: nil])

    preview_output = capture_io(fn -> Backfill.run(["--json"]) end)
    assert %{"pending_entries" => 1, "updated_entries" => 0} = Jason.decode!(preview_output)

    assert Repo.aggregate(
             from(entry in JournalEntry, where: is_nil(entry.retention_run_id)),
             :count
           ) == 1

    Mix.Task.reenable(@backfill_task)

    apply_output = capture_io(fn -> Backfill.run(["--apply", "--json"]) end)

    assert %{"pending_entries" => 0, "updated_entries" => 1, "complete?" => true} =
             Jason.decode!(apply_output)

    assert Repo.get_by!(JournalEntry, retention_run_id: run_id)
  end

  test "apply mode requires both explicit confirmation inputs" do
    assert_raise Mix.Error, ~r/requires --confirmation/, fn ->
      Retention.run(["--terminal-before", "2025-01-01T00:00:00Z", "--apply"])
    end

    Mix.Task.reenable(@retention_task)

    assert_raise Mix.Error, ~r/requires the preview --created-at/, fn ->
      Retention.run([
        "--terminal-before",
        "2025-01-01T00:00:00Z",
        "--apply",
        "--confirmation",
        "token"
      ])
    end

    Mix.Task.reenable(@retention_task)

    assert_raise Mix.Error, ~r/--created-at requires --apply/, fn ->
      Retention.run([
        "--terminal-before",
        "2025-01-01T00:00:00Z",
        "--created-at",
        "2025-01-01T00:00:00Z"
      ])
    end
  end

  defp restore_env(key, nil) do
    Application.delete_env(:jizoku, key)
  end

  defp restore_env(key, value) do
    Application.put_env(:jizoku, key, value)
  end
end
