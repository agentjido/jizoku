defmodule Jizoku.Retention.EctoApply do
  @moduledoc """
  Transactional Postgres deletion boundary for confirmed retention plans.

  This module is called through `Jizoku.apply_retention/3`. It owns locking,
  revision revalidation, physical cleanup, and payload-free receipt insertion.
  """

  import Ecto.Query

  alias Jizoku.Persistence
  alias Jizoku.Persistence.JournalCheckpoint
  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Persistence.JournalThread
  alias Jizoku.Persistence.RunSearch
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Retention
  alias Jizoku.Retention.Plan
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage

  @doc "Applies a confirmed plan or returns the existing receipt for an exact retry."
  @spec apply(Storage.t(), Plan.t(), DateTime.t()) ::
          {:ok, Retention.Receipt.t()} | {:error, term()}
  def apply(
        %Storage{adapter: Jizoku.Runtime.Journal.Storage.Ecto, opts: opts} = storage,
        %Plan{} = plan,
        %DateTime{} = now
      ) do
    repo = Keyword.fetch!(opts, :repo)
    now = DateTime.add(now, 0, :microsecond)

    case repo.transaction(fn -> apply_in_transaction(repo, storage, plan, now, opts) end) do
      {:ok, %Retention.Receipt{} = receipt} -> {:ok, receipt}
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in [DBConnection.ConnectionError, Ecto.QueryError, Postgrex.Error] ->
      {:error, {:retention_apply_failed, error.__struct__}}
  end

  def apply(%Storage{adapter: adapter}, %Plan{}, %DateTime{}) do
    {:error, {:unsupported_retention_apply, adapter}}
  end

  defp apply_in_transaction(repo, storage, plan, now, opts) do
    result =
      case existing_receipt(repo, plan, opts) do
        {:ok, %Retention.Receipt{} = receipt} ->
          {:ok, receipt}

        :not_found ->
          apply_new_plan(repo, storage, plan, now, opts)

        {:error, _reason} = error ->
          error
      end

    case result do
      {:ok, %Retention.Receipt{} = receipt} -> receipt
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp apply_new_plan(repo, storage, %Plan{} = plan, now, opts) do
    with :ok <- validate_new_plan(plan, now),
         :ok <- lock_run_identities(repo, plan, opts),
         {:ok, threads} <- lock_source_threads(repo, plan, opts),
         :ok <- validate_source_revisions(plan, threads),
         :ok <- require_backfilled_ownership(repo, plan, opts),
         :ok <- revalidate_candidates(storage, plan, now),
         ownership <- ownership_counts(repo, plan, opts),
         :ok <- validate_ownership_counts(plan, ownership) do
      delete_and_receipt(repo, plan, threads, ownership, now, opts)
    end
  end

  defp validate_new_plan(%Plan{eligible: []}, %DateTime{}) do
    {:error, :empty_retention_plan}
  end

  defp validate_new_plan(%Plan{} = plan, %DateTime{} = now) do
    if DateTime.compare(now, plan.expires_at) == :lt do
      :ok
    else
      {:error, :expired_retention_plan}
    end
  end

  defp existing_receipt(repo, %Plan{} = plan, opts) do
    rows =
      repo.all(
        from(receipt in Persistence.RetentionReceipt,
          where: receipt.plan_digest == ^plan.confirmation_token,
          order_by: receipt.run_id
        ),
        repo_opts(opts)
      )

    expected_ids = candidate_run_ids(plan)
    persisted_ids = Enum.map(rows, & &1.run_id)

    cond do
      rows == [] ->
        :not_found

      persisted_ids == expected_ids ->
        {:ok, aggregate_receipt(plan, rows, true)}

      true ->
        {:error, :incomplete_retention_receipt}
    end
  end

  defp lock_run_identities(repo, %Plan{} = plan, opts) do
    Enum.reduce_while(candidate_run_ids(plan), :ok, fn run_id, :ok ->
      case Jizoku.Runtime.Journal.Storage.Ecto.lock_retention_identity(
             repo,
             plan.partition,
             run_id,
             opts
           ) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp lock_source_threads(repo, %Plan{} = plan, opts) do
    ids = source_thread_ids(plan)

    threads =
      repo.all(
        from(thread in JournalThread,
          where: thread.id in ^ids,
          order_by: thread.id,
          lock: "FOR UPDATE"
        ),
        repo_opts(opts)
      )

    by_id = Map.new(threads, &{&1.id, &1})

    case Enum.reject(ids, &Map.has_key?(by_id, &1)) do
      [] -> {:ok, by_id}
      missing -> {:error, {:stale_retention_plan, {:missing_threads, missing}}}
    end
  end

  defp validate_source_revisions(%Plan{} = plan, threads) do
    catalog_id = catalog_thread_id(plan.partition)

    with :ok <- expected_revision(threads, catalog_id, plan.catalog_revision) do
      Enum.reduce_while(plan.eligible, :ok, fn candidate, :ok ->
        validate_candidate_revisions(plan, candidate, threads)
      end)
    end
  end

  defp validate_candidate_revisions(plan, candidate, threads) do
    result =
      with :ok <-
             expected_revision(
               threads,
               run_thread_id(plan.partition, candidate.run_id),
               candidate.run_revision
             ) do
        expected_revision(
          threads,
          dispatch_thread_id(plan.partition, candidate.queue),
          candidate.dispatch_revision
        )
      end

    case result do
      :ok -> {:cont, :ok}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp expected_revision(threads, thread_id, expected) do
    case Map.fetch!(threads, thread_id) do
      %JournalThread{rev: ^expected} -> :ok
      %JournalThread{} -> {:error, {:stale_retention_plan, thread_id}}
    end
  end

  defp require_backfilled_ownership(repo, %Plan{} = plan, opts) do
    shared_ids = shared_thread_ids(plan)

    if repo.exists?(
         from(entry in JournalEntry,
           where: entry.thread_id in ^shared_ids and is_nil(entry.retention_run_id)
         ),
         repo_opts(opts)
       ) do
      {:error, {:retention_ownership_backfill_required, shared_ids}}
    else
      :ok
    end
  end

  defp revalidate_candidates(storage, %Plan{} = plan, now) do
    Enum.reduce_while(plan.eligible, :ok, fn candidate, :ok ->
      result =
        with {:ok, snapshot} <-
               Inspection.snapshot(storage, candidate.run_id, queue: candidate.queue, now: now) do
          Retention.revalidate_candidate(snapshot, candidate, now)
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp ownership_counts(repo, %Plan{} = plan, opts) do
    run_ids = candidate_run_ids(plan)

    rows =
      repo.all(
        from(entry in JournalEntry,
          where:
            entry.thread_id in ^shared_thread_ids(plan) and
              entry.retention_run_id in ^run_ids,
          group_by: [entry.thread_id, entry.retention_run_id],
          select: {entry.thread_id, entry.retention_run_id, count(entry.id)}
        ),
        repo_opts(opts)
      )

    Map.new(rows, fn {thread_id, run_id, count} -> {{thread_id, run_id}, count} end)
  end

  defp validate_ownership_counts(%Plan{} = plan, counts) do
    Enum.reduce_while(plan.eligible, :ok, fn candidate, :ok ->
      catalog_count = count_for(counts, catalog_thread_id(plan.partition), candidate.run_id)

      index_count =
        count_for(
          counts,
          index_thread_id(plan.partition, candidate.workflow),
          candidate.run_id
        )

      dispatch_count =
        count_for(
          counts,
          dispatch_thread_id(plan.partition, candidate.queue),
          candidate.run_id
        )

      if catalog_count > 0 and index_count > 0 and
           dispatch_count == candidate.dispatch_entry_count do
        {:cont, :ok}
      else
        {:halt, {:error, {:stale_retention_ownership, candidate.run_id}}}
      end
    end)
  end

  defp delete_and_receipt(repo, plan, threads, ownership, now, opts) do
    run_ids = candidate_run_ids(plan)
    shared_ids = shared_thread_ids(plan)
    run_thread_ids = Enum.map(run_ids, &run_thread_id(plan.partition, &1))

    {shared_deleted, _rows} =
      repo.delete_all(
        from(entry in JournalEntry,
          where: entry.thread_id in ^shared_ids and entry.retention_run_id in ^run_ids
        ),
        repo_opts(opts)
      )

    expected_shared_deleted =
      Enum.reduce(ownership, 0, fn {_identity, count}, total -> total + count end)

    with true <- shared_deleted == expected_shared_deleted,
         :ok <- advance_shared_revisions(repo, shared_ids, threads, now, opts),
         :ok <- delete_checkpoints(repo, shared_ids ++ run_thread_ids, opts),
         :ok <- delete_run_threads(repo, run_thread_ids, opts),
         :ok <- delete_search_rows(repo, plan.partition, run_ids, opts),
         {:ok, rows} <- insert_receipts(repo, plan, ownership, now, opts) do
      {:ok, aggregate_receipt(plan, rows, false)}
    else
      false -> {:error, :retention_delete_count_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp advance_shared_revisions(repo, shared_ids, threads, now, opts) do
    now_ms = DateTime.to_unix(now, :millisecond)

    Enum.reduce_while(shared_ids, :ok, fn thread_id, :ok ->
      thread = Map.fetch!(threads, thread_id)

      metadata =
        Jizoku.Runtime.Journal.Storage.Ecto.retention_gap_metadata(thread.metadata || %{})

      {count, _rows} =
        repo.update_all(
          from(stored in JournalThread, where: stored.id == ^thread_id),
          [
            set: [
              rev: thread.rev + 1,
              metadata: metadata,
              updated_at_ms: now_ms,
              updated_at: now
            ]
          ],
          repo_opts(opts)
        )

      if count == 1 do
        {:cont, :ok}
      else
        {:halt, {:error, {:retention_thread_update_failed, thread_id}}}
      end
    end)
  end

  defp delete_checkpoints(repo, thread_ids, opts) do
    result =
      Enum.reduce_while(thread_ids, {:ok, []}, fn thread_id, {:ok, hashes} ->
        case Jizoku.Runtime.Journal.Storage.Ecto.checkpoint_key_hash(
               {"jizoku", :checkpoint, thread_id}
             ) do
          {:ok, hash} -> {:cont, {:ok, [hash | hashes]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, hashes} ->
        repo.delete_all(
          from(checkpoint in JournalCheckpoint, where: checkpoint.key_hash in ^hashes),
          repo_opts(opts)
        )

        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp delete_run_threads(repo, thread_ids, opts) do
    {count, _rows} =
      repo.delete_all(
        from(thread in JournalThread, where: thread.id in ^thread_ids),
        repo_opts(opts)
      )

    if count == length(thread_ids) do
      :ok
    else
      {:error, :retention_run_thread_delete_failed}
    end
  end

  defp delete_search_rows(repo, partition, run_ids, opts) do
    repo.delete_all(
      from(run in RunSearch,
        where: run.partition_key == ^partition_key(partition) and run.run_id in ^run_ids
      ),
      repo_opts(opts)
    )

    :ok
  end

  defp insert_receipts(repo, %Plan{} = plan, ownership, now, opts) do
    rows =
      Enum.map(plan.eligible, fn candidate ->
        %{
          partition_key: partition_key(plan.partition),
          run_id: candidate.run_id,
          plan_digest: plan.confirmation_token,
          workflow: candidate.workflow,
          queue: candidate.queue,
          terminal_status: Atom.to_string(candidate.terminal_status),
          run_entries_deleted: candidate.run_revision,
          dispatch_entries_deleted:
            count_for(
              ownership,
              dispatch_thread_id(plan.partition, candidate.queue),
              candidate.run_id
            ),
          deleted_at: now,
          inserted_at: now,
          updated_at: now
        }
      end)

    insert_opts =
      [on_conflict: :nothing, conflict_target: [:partition_key, :run_id]] ++ repo_opts(opts)

    expected_count = length(rows)

    case repo.insert_all(Persistence.RetentionReceipt, rows, insert_opts) do
      {^expected_count, _rows} ->
        {:ok, Enum.map(rows, &struct!(Persistence.RetentionReceipt, &1))}

      _conflict ->
        {:error, :retention_receipt_conflict}
    end
  end

  defp aggregate_receipt(%Plan{} = plan, rows, idempotent?) do
    run_ids = Enum.sort(Enum.map(rows, & &1.run_id))

    {run_entries_deleted, dispatch_entries_deleted} =
      Enum.reduce(rows, {0, 0}, fn row, {run_total, dispatch_total} ->
        {
          run_total + row.run_entries_deleted,
          dispatch_total + row.dispatch_entries_deleted
        }
      end)

    %Retention.Receipt{
      plan_digest: plan.confirmation_token,
      partition: plan.partition,
      run_ids: run_ids,
      run_count: length(rows),
      run_entries_deleted: run_entries_deleted,
      dispatch_entries_deleted: dispatch_entries_deleted,
      applied_at: hd(rows).deleted_at,
      idempotent?: idempotent?
    }
  end

  defp source_thread_ids(%Plan{} = plan) do
    run_ids = Enum.map(plan.eligible, &run_thread_id(plan.partition, &1.run_id))

    Enum.sort(Enum.uniq(shared_thread_ids(plan) ++ run_ids))
  end

  defp shared_thread_ids(%Plan{} = plan) do
    index_ids = Enum.map(plan.eligible, &index_thread_id(plan.partition, &1.workflow))
    dispatch_ids = Enum.map(plan.eligible, &dispatch_thread_id(plan.partition, &1.queue))

    Enum.sort(Enum.uniq([catalog_thread_id(plan.partition) | index_ids ++ dispatch_ids]))
  end

  defp candidate_run_ids(%Plan{} = plan) do
    plan.eligible
    |> Enum.map(& &1.run_id)
    |> Enum.sort()
  end

  defp count_for(counts, thread_id, run_id) do
    Map.get(counts, {thread_id, run_id}, 0)
  end

  defp run_thread_id(partition, run_id), do: Journal.thread_id({:run, run_id}, partition)

  defp dispatch_thread_id(partition, queue) do
    Journal.thread_id({:dispatch, queue}, partition)
  end

  defp index_thread_id(partition, workflow) do
    Journal.thread_id({:run_index, workflow}, partition)
  end

  defp catalog_thread_id(partition) do
    Journal.thread_id({:run_catalog, "all"}, partition)
  end

  defp partition_key(nil), do: ""
  defp partition_key(partition) when is_binary(partition), do: partition

  defp repo_opts(opts) do
    case Keyword.get(opts, :prefix) do
      prefix when is_binary(prefix) and prefix != "" -> [prefix: prefix]
      _default_prefix -> []
    end
  end
end
