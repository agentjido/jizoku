defmodule Jizoku.Retention.OwnershipBackfill do
  @moduledoc """
  Bounded ownership migration for journal rows created before retention support.

  Ownership is derived from the safely decoded journal envelope. Each apply
  batch locks only selected legacy rows, updates them atomically, and leaves
  failed rows unchanged so an operator can fix the source before retrying.
  """

  import Ecto.Query

  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Runtime.Journal.Storage

  @default_batch_size 500
  @max_batch_size 5_000

  @type result :: %{
          partition: String.t() | nil,
          batch_size: pos_integer(),
          pending_entries: non_neg_integer(),
          scanned_entries: non_neg_integer(),
          updated_entries: non_neg_integer(),
          complete?: boolean()
        }

  @doc "Counts legacy ownership rows in one explicit runtime partition without changing them."
  @spec preview(Storage.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def preview(%Storage{} = storage, opts \\ []) when is_list(opts) do
    with {:ok, batch_size} <- batch_size(opts),
         :ok <- supported(storage) do
      repo = Keyword.fetch!(storage.opts, :repo)
      pending_entries = repo.aggregate(scoped_query(storage), :count, :id, repo_opts(storage))

      {:ok,
       result(storage, batch_size,
         pending_entries: pending_entries,
         scanned_entries: 0,
         updated_entries: 0
       )}
    end
  rescue
    error in [DBConnection.ConnectionError, Ecto.QueryError, Postgrex.Error] ->
      {:error, {:retention_ownership_backfill_failed, error.__struct__}}
  end

  @doc "Backfills at most one locked batch and reports whether another batch remains."
  @spec apply(Storage.t(), keyword()) :: {:ok, result()} | {:error, term()}
  def apply(%Storage{} = storage, opts \\ []) when is_list(opts) do
    with {:ok, batch_size} <- batch_size(opts),
         :ok <- supported(storage) do
      repo = Keyword.fetch!(storage.opts, :repo)

      repo
      |> apply_transaction(storage, batch_size)
      |> normalize_transaction()
    end
  rescue
    error in [DBConnection.ConnectionError, Ecto.QueryError, Postgrex.Error] ->
      {:error, {:retention_ownership_backfill_failed, error.__struct__}}
  end

  defp apply_transaction(repo, storage, batch_size) do
    repo.transaction(fn ->
      rows =
        repo.all(
          from(entry in scoped_query(storage),
            order_by: entry.id,
            limit: ^batch_size,
            lock: "FOR UPDATE SKIP LOCKED",
            select: {entry.id, entry.entry}
          ),
          repo_opts(storage)
        )

      case owners(rows) do
        {:ok, owners} -> update_owners(repo, storage, batch_size, owners)
        {:error, reason} -> repo.rollback(reason)
      end
    end)
  end

  defp owners(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn {id, encoded}, {:ok, decoded} ->
      case Jizoku.Runtime.Journal.Storage.Ecto.retention_entry_owner(encoded) do
        {:ok, owner} -> {:cont, {:ok, [{id, owner} | decoded]}}
        {:error, reason} -> {:halt, {:error, {:invalid_retention_ownership_entry, id, reason}}}
      end
    end)
  end

  defp update_owners(repo, storage, batch_size, owners) do
    updated_entries =
      owners
      |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
      |> Enum.reduce(0, fn {owner, ids}, count ->
        {updated, _rows} =
          repo.update_all(
            from(entry in JournalEntry,
              where: entry.id in ^ids and is_nil(entry.retention_run_id)
            ),
            [set: [retention_run_id: owner]],
            repo_opts(storage)
          )

        count + updated
      end)

    pending_entries = repo.aggregate(scoped_query(storage), :count, :id, repo_opts(storage))

    result(storage, batch_size,
      pending_entries: pending_entries,
      scanned_entries: length(owners),
      updated_entries: updated_entries
    )
  end

  defp scoped_query(%Storage{partition: nil}) do
    from(entry in JournalEntry,
      where:
        is_nil(entry.retention_run_id) and like(entry.thread_id, "jizoku:%") and
          not like(entry.thread_id, "jizoku:partition:%")
    )
  end

  defp scoped_query(%Storage{partition: partition}) do
    prefix = "jizoku:partition:#{partition}:%"

    from(entry in JournalEntry,
      where: is_nil(entry.retention_run_id) and like(entry.thread_id, ^prefix)
    )
  end

  defp supported(%Storage{adapter: Jizoku.Runtime.Journal.Storage.Ecto}) do
    :ok
  end

  defp supported(%Storage{adapter: adapter}) do
    {:error, {:unsupported_retention_ownership_backfill, adapter}}
  end

  defp batch_size(opts) do
    case Keyword.get(opts, :batch_size, @default_batch_size) do
      value when is_integer(value) and value > 0 and value <= @max_batch_size -> {:ok, value}
      _invalid -> {:error, {:invalid_option, {:batch_size, :invalid}}}
    end
  end

  defp result(storage, batch_size, values) do
    pending_entries = Keyword.fetch!(values, :pending_entries)

    %{
      partition: storage.partition,
      batch_size: batch_size,
      pending_entries: pending_entries,
      scanned_entries: Keyword.fetch!(values, :scanned_entries),
      updated_entries: Keyword.fetch!(values, :updated_entries),
      complete?: pending_entries == 0
    }
  end

  defp repo_opts(%Storage{opts: opts}) do
    case Keyword.fetch(opts, :prefix) do
      {:ok, prefix} -> [prefix: prefix]
      :error -> []
    end
  end

  defp normalize_transaction({:ok, result}) do
    {:ok, result}
  end

  defp normalize_transaction({:error, reason}) do
    {:error, reason}
  end
end
