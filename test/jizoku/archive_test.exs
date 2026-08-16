defmodule Jizoku.ArchiveTest do
  use Jizoku.DataCase, async: false

  alias Jizoku.Persistence.RunSearch
  alias Jizoku.ReadModel.Listing.Page
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto

  @storage {Ecto, repo: Repo}
  @queue "archive-test"
  @now ~U[2026-08-16 22:00:00Z]

  defmodule Record do
    use Jizoku.Step, name: "archive_record"

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

  test "archives terminal runs, hides them by default, and restores them reversibly" do
    assert {:ok, started} = start_run()
    assert {:ok, cancelled} = Jizoku.cancel(started.run_id, runtime_options())
    assert cancelled.status == :cancelled

    assert {:ok, archived} =
             Jizoku.archive_run(
               started.run_id,
               runtime_options(reason: "retention_hold")
             )

    assert archived.archived?
    assert archived.archived_at == @now
    assert archived.archive_reason == "retention_hold"

    assert %RunSearch{archived_at: archived_at, archive_reason: "retention_hold"} =
             Repo.get_by!(RunSearch, partition_key: "", run_id: started.run_id)

    assert DateTime.compare(archived_at, @now) == :eq

    assert {:ok, []} = Jizoku.list_runs([workflow: Workflow], runtime_options())

    assert {:ok, [listed]} =
             Jizoku.list_runs(
               [workflow: Workflow, archived: :only],
               runtime_options(visibility_policy: :auditor)
             )

    assert listed.run_id == started.run_id
    assert listed.archived?
    assert listed.archive_reason == "retention_hold"

    assert {:ok, [redacted]} =
             Jizoku.list_runs([workflow: Workflow, archived: :include], runtime_options())

    assert redacted.archived?
    assert redacted.archive_reason == nil

    assert {:ok, inspected} =
             Jizoku.inspect_run(started.run_id, runtime_options(visibility_policy: :auditor))

    assert inspected.archived?
    assert inspected.archive_reason == "retention_hold"

    assert {:ok, unarchived} = Jizoku.unarchive_run(started.run_id, runtime_options())
    refute unarchived.archived?
    assert unarchived.archived_at == nil
    assert unarchived.archive_reason == nil

    assert {:ok, [visible]} = Jizoku.list_runs([workflow: Workflow], runtime_options())
    assert visible.run_id == started.run_id
  end

  test "archive and unarchive retries are idempotent and survive checkpoint loss" do
    assert {:ok, started} = start_run()
    assert {:ok, _cancelled} = Jizoku.cancel(started.run_id, runtime_options())

    assert {:ok, first} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: "support_case"))

    assert {:ok, duplicate} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: "support_case"))

    assert duplicate.thread_revisions.run == first.thread_revisions.run

    {:ok, thread} = Journal.load_thread(@storage, {:run, started.run_id})
    assert Enum.count(thread.entries, &(&1.type == :run_archived)) == 1

    assert :ok =
             Ecto.delete_checkpoint(
               {"jizoku", :checkpoint, Journal.thread_id({:run, started.run_id})},
               repo: Repo
             )

    assert {:ok, rebuilt} =
             Jizoku.inspect_run(started.run_id, runtime_options(visibility_policy: :auditor))

    assert rebuilt.archived?
    assert rebuilt.archive_reason == "support_case"

    assert {:ok, first_unarchive} = Jizoku.unarchive_run(started.run_id, runtime_options())
    assert {:ok, duplicate_unarchive} = Jizoku.unarchive_run(started.run_id, runtime_options())

    assert duplicate_unarchive.thread_revisions.run == first_unarchive.thread_revisions.run
  end

  test "rejects active runs, invalid reasons, and conflicting archive reasons" do
    assert {:ok, started} = start_run()

    assert {:error, {:invalid_transition, :running, :archiving}} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: "too_early"))

    assert {:error, {:invalid_transition, :running, :unarchiving}} =
             Jizoku.unarchive_run(started.run_id, runtime_options())

    assert {:ok, _cancelled} = Jizoku.cancel(started.run_id, runtime_options())

    assert {:error, {:invalid_option, {:reason, :invalid}}} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: " "))

    assert {:error, {:invalid_option, {:reason, :invalid}}} =
             Jizoku.archive_run(
               started.run_id,
               runtime_options(reason: String.duplicate("a", 257))
             )

    assert {:ok, _archived} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: "first_reason"))

    assert {:error, {:invalid_transition, :archived, :archiving}} =
             Jizoku.archive_run(started.run_id, runtime_options(reason: "second_reason"))
  end

  test "archive pages are cursor-stable and remain partition-scoped" do
    partition = "tenant_archive"

    run_ids =
      Enum.map(1..2, fn offset ->
        opts = runtime_options(partition: partition, now: DateTime.add(@now, offset, :second))
        assert {:ok, started} = start_run(opts)
        assert {:ok, _cancelled} = Jizoku.cancel(started.run_id, opts)

        assert {:ok, _archived} =
                 Jizoku.archive_run(started.run_id, Keyword.put(opts, :reason, "retention_hold"))

        started.run_id
      end)

    assert {:ok, []} =
             Jizoku.list_runs(
               [workflow: Workflow, archived: :include],
               runtime_options()
             )

    partition_opts = runtime_options(partition: partition, visibility_policy: :auditor)

    assert {:ok, %Page{items: [first], next_cursor: cursor}} =
             Jizoku.list_runs(
               [workflow: Workflow, archived: :only, first: 1],
               partition_opts
             )

    assert is_binary(cursor)

    assert {:ok, %Page{items: [second], next_cursor: nil}} =
             Jizoku.list_runs(
               [workflow: Workflow, archived: :only, first: 1, after: cursor],
               partition_opts
             )

    assert MapSet.new([first.run_id, second.run_id]) == MapSet.new(run_ids)
  end

  defp start_run(opts \\ runtime_options()) do
    Jizoku.start(Workflow, :manual, %{}, opts)
  end

  defp runtime_options(overrides \\ []) do
    Keyword.merge(
      [journal_storage: @storage, queue: @queue, now: @now],
      overrides
    )
  end
end
