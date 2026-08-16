defmodule Jizoku.RetentionOwnershipBackfillTest do
  use Jizoku.DataCase, async: false

  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage

  @storage {Jizoku.Runtime.Journal.Storage.Ecto, repo: Repo}
  @now ~U[2026-08-16 23:00:00Z]

  test "backfills bounded batches without crossing the selected partition" do
    first_id = Ecto.UUID.generate()
    second_id = Ecto.UUID.generate()
    partitioned_id = Ecto.UUID.generate()
    partition = "tenant_backfill"

    append_catalog!(@storage, first_id)
    append_catalog!(@storage, second_id)
    append_catalog!(scoped_storage!(partition), partitioned_id)

    Repo.update_all(JournalEntry, set: [retention_run_id: nil])

    assert {:ok, preview} =
             Jizoku.preview_retention_ownership_backfill(
               journal_storage: @storage,
               batch_size: 1
             )

    assert preview == %{
             partition: nil,
             batch_size: 1,
             pending_entries: 2,
             scanned_entries: 0,
             updated_entries: 0,
             complete?: false
           }

    assert {:ok, first_batch} =
             Jizoku.backfill_retention_ownership(
               journal_storage: @storage,
               batch_size: 1
             )

    assert first_batch.scanned_entries == 1
    assert first_batch.updated_entries == 1
    assert first_batch.pending_entries == 1
    refute first_batch.complete?

    assert {:ok, second_batch} =
             Jizoku.backfill_retention_ownership(
               journal_storage: @storage,
               batch_size: 1
             )

    assert second_batch.pending_entries == 0
    assert second_batch.complete?

    assert Enum.sort(owners(unpartitioned_thread_pattern())) ==
             Enum.sort([first_id, second_id])

    assert {:ok, idempotent_batch} =
             Jizoku.backfill_retention_ownership(
               journal_storage: @storage,
               batch_size: 1
             )

    assert idempotent_batch.scanned_entries == 0
    assert idempotent_batch.updated_entries == 0
    assert idempotent_batch.complete?

    assert [nil] == owners("jizoku:partition:#{partition}:%")

    assert {:ok, partition_batch} =
             Jizoku.backfill_retention_ownership(
               journal_storage: @storage,
               partition: partition,
               batch_size: 1
             )

    assert partition_batch.updated_entries == 1
    assert partition_batch.complete?
    assert [partitioned_id] == owners("jizoku:partition:#{partition}:%")
  end

  test "rolls back the complete batch when a legacy entry cannot be decoded" do
    valid_id = Ecto.UUID.generate()
    invalid_id = Ecto.UUID.generate()

    append_catalog!(@storage, valid_id)
    append_catalog!(@storage, invalid_id)
    Repo.update_all(JournalEntry, set: [retention_run_id: nil])

    invalid_entry_id =
      Repo.one!(
        from(entry in JournalEntry,
          where: is_nil(entry.retention_run_id),
          limit: 1,
          select: entry.id
        )
      )

    {1, _rows} =
      Repo.update_all(
        from(entry in JournalEntry, where: entry.id == ^invalid_entry_id),
        set: [entry: "invalid-etf"]
      )

    assert {:error, {:invalid_retention_ownership_entry, _entry_id, _reason}} =
             Jizoku.backfill_retention_ownership(
               journal_storage: @storage,
               batch_size: 10
             )

    assert Repo.aggregate(
             from(entry in JournalEntry, where: is_nil(entry.retention_run_id)),
             :count,
             :id
           ) == 2
  end

  test "declares adapter capabilities and rejects unsupported backfill adapters" do
    assert {:ok,
            %{
              archive?: true,
              preview?: true,
              transactional_apply?: true,
              ownership_backfill?: true
            }} =
             Jizoku.retention_capabilities(journal_storage: @storage)

    assert {:ok,
            %{
              archive?: true,
              preview?: true,
              transactional_apply?: false,
              ownership_backfill?: false
            }} =
             Jizoku.retention_capabilities(journal_storage: Jido.Storage.ETS)

    assert {:error, {:unsupported_retention_ownership_backfill, Jido.Storage.ETS}} =
             Jizoku.preview_retention_ownership_backfill(journal_storage: Jido.Storage.ETS)
  end

  defp append_catalog!(storage, run_id) do
    assert {:ok, _thread} =
             Journal.append_entries(storage, [
               %Entry{
                 type: :run_cataloged,
                 thread: {:run_catalog, "all"},
                 occurred_at: @now,
                 data: %{
                   run_id: run_id,
                   workflow: "RetentionBackfillWorkflow",
                   queue: "retention-backfill",
                   occurred_at: @now
                 }
               }
             ])
  end

  defp scoped_storage!(partition) do
    assert {:ok, storage} = Storage.scope(@storage, partition)
    storage
  end

  defp owners(pattern) do
    Repo.all(
      from(entry in JournalEntry,
        where: like(entry.thread_id, ^pattern),
        order_by: entry.retention_run_id,
        select: entry.retention_run_id
      )
    )
  end

  defp unpartitioned_thread_pattern do
    "jizoku:run_catalog:%"
  end
end
