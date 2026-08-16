defmodule Jizoku.Runtime.Journal.Storage.Ecto do
  @moduledoc """
  Postgres-compatible Ecto storage adapter for Jizoku journal runtime state.

  Use this adapter when the host app wants the Jido journal runtime persisted in
  the same Postgres-compatible database boundary as the rest of the application:

      config :jizoku,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: {Jizoku.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}

  The adapter implements Jido's checkpoint and append-only thread callbacks.
  Thread appends are serialized with a row-level lock and honor Jido's
  `:expected_rev` optimistic concurrency option.
  """

  @behaviour Jido.Storage

  import Ecto.Query

  alias Ecto.Adapters.SQL
  alias Jido.Thread
  alias Jido.Thread.EntryNormalizer
  alias Jizoku.Persistence.JournalCheckpoint
  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Persistence.JournalThread
  alias Jizoku.Persistence.RetentionReceipt
  alias Jizoku.ReadModel.RunSearch.EctoProjector
  alias Jizoku.Runtime.Journal.Storage.Metadata

  @encoded_term_tag :jizoku_ecto_term_v1
  @retention_gap_metadata_key "jizoku_retention_gaps"
  @retention_unowned_marker "__jizoku_unowned__"

  @type opts :: keyword()

  @doc "Declares Ecto retention operations that are implemented transactionally."
  @spec retention_capabilities() :: map()
  def retention_capabilities do
    %{transactional_apply?: true, ownership_backfill?: true}
  end

  @impl Jido.Storage
  @spec get_checkpoint(term(), opts()) :: {:ok, term()} | :not_found | {:error, term()}
  def get_checkpoint(key, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key) do
      case repo.get(JournalCheckpoint, key_hash(key_binary), repo_opts(opts)) do
        nil -> :not_found
        checkpoint -> decode_term(checkpoint.checkpoint)
      end
    end
  end

  @impl Jido.Storage
  @spec put_checkpoint(term(), term(), opts()) :: :ok | {:error, term()}
  def put_checkpoint(key, data, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key),
         {:ok, checkpoint_binary} <- encode_term(data) do
      now = DateTime.utc_now(:microsecond)

      row = %{
        key_hash: key_hash(key_binary),
        key: key_binary,
        checkpoint: checkpoint_binary,
        inserted_at: now,
        updated_at: now
      }

      {_count, _rows} =
        repo.insert_all(
          JournalCheckpoint,
          [row],
          [on_conflict: {:replace, [:key, :checkpoint, :updated_at]}, conflict_target: :key_hash] ++
            repo_opts(opts)
        )

      :ok
    end
  end

  @impl Jido.Storage
  @spec delete_checkpoint(term(), opts()) :: :ok | {:error, term()}
  def delete_checkpoint(key, opts) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, key_binary} <- encode_term(key) do
      repo.delete_all(
        from(checkpoint in JournalCheckpoint,
          where: checkpoint.key_hash == ^key_hash(key_binary)
        ),
        repo_opts(opts)
      )

      :ok
    end
  end

  @impl Jido.Storage
  @spec load_thread(String.t(), opts()) :: {:ok, Thread.t()} | :not_found | {:error, term()}
  def load_thread(thread_id, opts) when is_binary(thread_id) do
    with {:ok, repo} <- fetch_repo(opts) do
      result = repo.transaction(fn -> load_thread_in_transaction(repo, thread_id, opts) end)
      normalize_load_thread_result(result)
    end
  end

  @impl Jido.Storage
  @spec append_thread(String.t(), [Jido.Thread.Entry.t()], opts()) ::
          {:ok, Thread.t()} | {:error, term()}
  def append_thread(thread_id, entries, opts) when is_binary(thread_id) and is_list(entries) do
    with {:ok, repo} <- fetch_repo(opts),
         {:ok, metadata} <- Metadata.normalize(Keyword.get(opts, :metadata, %{})) do
      opts = Keyword.put(opts, :metadata, metadata)
      expected_rev = Keyword.get(opts, :expected_rev)
      now_ms = System.system_time(:millisecond)

      result =
        repo.transaction(fn ->
          append_thread_in_transaction(repo, thread_id, entries, expected_rev, now_ms, opts)
        end)

      normalize_thread_result(result)
    end
  end

  @impl Jido.Storage
  @spec delete_thread(String.t(), opts()) :: :ok | {:error, term()}
  def delete_thread(thread_id, opts) when is_binary(thread_id) do
    with {:ok, repo} <- fetch_repo(opts) do
      repo.delete_all(
        from(thread in JournalThread, where: thread.id == ^thread_id),
        repo_opts(opts)
      )

      :ok
    end
  end

  @doc "Acquires the transaction-scoped lock that serializes one retained run identity."
  @spec lock_retention_identity(module(), String.t() | nil, String.t(), opts()) ::
          :ok | {:error, term()}
  def lock_retention_identity(repo, partition, run_id, opts)
      when is_atom(repo) and is_binary(run_id) and is_list(opts) do
    key = Enum.join([Keyword.get(opts, :prefix, ""), partition || "", run_id], ":")

    case SQL.query(repo, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [key]) do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:retention_lock_failed, reason}}
    end
  end

  @doc "Marks shared thread metadata as intentionally containing retention sequence gaps."
  @spec retention_gap_metadata(map()) :: map()
  def retention_gap_metadata(metadata) when is_map(metadata) do
    Map.put(metadata, @retention_gap_metadata_key, true)
  end

  @doc "Returns the persisted hash for one canonical Jizoku checkpoint key."
  @spec checkpoint_key_hash(term()) :: {:ok, String.t()} | {:error, term()}
  def checkpoint_key_hash(key) do
    with {:ok, key_binary} <- encode_term(key) do
      {:ok, key_hash(key_binary)}
    end
  end

  @doc false
  @spec retention_entry_owner(binary()) :: {:ok, String.t()} | {:error, term()}
  def retention_entry_owner(binary) when is_binary(binary) do
    with {:ok, entry} <- decode_entry(binary) do
      {:ok, retention_run_id(entry)}
    end
  end

  defp fetch_repo(opts) do
    case Keyword.get(opts, :repo) do
      repo when is_atom(repo) and not is_nil(repo) ->
        if Code.ensure_loaded?(repo) and function_exported?(repo, :transaction, 1) do
          {:ok, repo}
        else
          {:error, {:invalid_option, :repo}}
        end

      _invalid ->
        {:error, {:missing_option, :repo}}
    end
  end

  defp load_thread_in_transaction(repo, thread_id, opts) do
    case locked_thread_for_read(repo, thread_id, opts) do
      nil -> repo.rollback(:not_found)
      %JournalThread{} = thread -> load_locked_thread(repo, thread, opts)
    end
  end

  defp load_locked_thread(repo, %JournalThread{} = thread, opts) do
    case load_entries(repo, thread.id, opts) do
      {:ok, []} -> load_empty_thread_or_rollback(repo, thread)
      {:ok, entries} -> reconstruct_or_rollback(repo, thread, entries)
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp load_empty_thread_or_rollback(repo, %JournalThread{} = thread) do
    if retention_gaps?(thread) and thread.rev > 0 do
      reconstruct_thread(thread, [])
    else
      repo.rollback(:not_found)
    end
  end

  defp reconstruct_or_rollback(repo, %JournalThread{} = thread, entries) do
    case validate_and_reconstruct_thread(thread, entries) do
      %Thread{} = reconstructed_thread -> reconstructed_thread
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp normalize_load_thread_result({:ok, %Thread{} = thread}), do: {:ok, thread}
  defp normalize_load_thread_result({:error, :not_found}), do: :not_found
  defp normalize_load_thread_result({:error, reason}), do: {:error, reason}

  defp append_thread_in_transaction(repo, thread_id, entries, expected_rev, now_ms, opts) do
    case lock_and_reject_retained_run(repo, opts) do
      :ok ->
        thread = ensure_locked_thread(repo, thread_id, now_ms, opts)

        case validate_expected_rev(expected_rev, thread.rev) do
          :ok -> append_locked_thread(repo, thread, entries, now_ms, opts)
          {:error, reason} -> repo.rollback(reason)
        end

      {:error, reason} ->
        repo.rollback(reason)
    end
  end

  defp lock_and_reject_retained_run(repo, opts) do
    case retention_identity(opts) do
      {:ok, partition, run_id} ->
        with :ok <- lock_retention_identity(repo, partition, run_id, opts),
             {:ok, false} <- retained_run?(repo, partition, run_id, opts) do
          :ok
        else
          {:ok, true} -> {:error, {:retained_run, run_id}}
          {:error, _reason} = error -> error
        end

      :not_run_thread ->
        :ok
    end
  end

  defp retention_identity(opts) do
    case Keyword.get(opts, :jizoku_projection_context) do
      %{thread: {:run, run_id}, partition: partition} when is_binary(run_id) ->
        {:ok, partition, run_id}

      _other_thread ->
        :not_run_thread
    end
  end

  defp retained_run?(repo, partition, run_id, opts) do
    with {:ok, true} <- retention_receipts_available?(repo, opts) do
      {:ok,
       repo.exists?(
         from(receipt in RetentionReceipt,
           where: receipt.partition_key == ^partition_key(partition) and receipt.run_id == ^run_id
         ),
         repo_opts(opts)
       )}
    end
  end

  defp retention_receipts_available?(repo, opts) do
    relation =
      case Keyword.get(opts, :prefix) do
        prefix when is_binary(prefix) and prefix != "" ->
          "#{prefix}.jizoku_retention_receipts"

        _default_prefix ->
          "jizoku_retention_receipts"
      end

    case SQL.query(repo, "SELECT to_regclass($1)::text", [relation]) do
      {:ok, %{rows: [[nil]]}} -> {:ok, false}
      {:ok, %{rows: [[_relation]]}} -> {:ok, true}
      {:error, reason} -> {:error, {:retention_receipt_lookup_failed, reason}}
    end
  end

  defp append_locked_thread(repo, %JournalThread{} = thread, entries, now_ms, opts) do
    prepared_entries = EntryNormalizer.normalize_many(entries, thread.rev, now_ms)

    with :ok <- insert_entries(repo, thread.id, prepared_entries, opts),
         %JournalThread{} = updated_thread <-
           update_thread_revision(
             repo,
             thread,
             thread.rev + length(prepared_entries),
             now_ms,
             opts
           ),
         {:ok, all_entries} <- load_entries(repo, thread.id, opts),
         :ok <- project_run_search(repo, all_entries, updated_thread, opts) do
      reconstruct_thread(updated_thread, all_entries)
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp normalize_thread_result({:ok, %Thread{} = thread}), do: {:ok, thread}
  defp normalize_thread_result({:error, reason}), do: {:error, reason}

  defp project_run_search(repo, entries, thread, opts) do
    case Keyword.get(opts, :jizoku_projection_context) do
      %{thread: {:run, run_id}, partition: partition} ->
        case EctoProjector.project_thread(repo, entries, run_id, partition, thread.rev, opts) do
          result when result in [:ok, :unavailable] -> :ok
          {:error, _reason} = error -> error
        end

      _other_thread ->
        :ok
    end
  end

  defp validate_expected_rev(nil, _current_rev), do: :ok
  defp validate_expected_rev(current_rev, current_rev), do: :ok
  defp validate_expected_rev(_expected_rev, _current_rev), do: {:error, :conflict}

  defp ensure_locked_thread(repo, thread_id, now_ms, opts) do
    db_now = DateTime.utc_now(:microsecond)

    row = %{
      id: thread_id,
      rev: 0,
      metadata: Keyword.get(opts, :metadata, %{}),
      created_at_ms: now_ms,
      updated_at_ms: now_ms,
      inserted_at: db_now,
      updated_at: db_now
    }

    repo.insert_all(
      JournalThread,
      [row],
      [on_conflict: :nothing, conflict_target: :id] ++ repo_opts(opts)
    )

    locked_thread_for_update(repo, thread_id, opts)
  end

  defp locked_thread_for_read(repo, thread_id, opts) do
    repo.one(
      from(thread in JournalThread, where: thread.id == ^thread_id, lock: "FOR SHARE"),
      repo_opts(opts)
    )
  end

  defp locked_thread_for_update(repo, thread_id, opts) do
    repo.one(
      from(thread in JournalThread, where: thread.id == ^thread_id, lock: "FOR UPDATE"),
      repo_opts(opts)
    )
  end

  defp insert_entries(_repo, _thread_id, [], _opts), do: :ok

  defp insert_entries(repo, thread_id, entries, opts) do
    now = DateTime.utc_now(:microsecond)

    with {:ok, rows} <- journal_entry_rows(entries, thread_id, now) do
      rows_count = length(rows)

      case repo.insert_all(JournalEntry, rows, repo_opts(opts)) do
        {count, _rows} when count == rows_count -> :ok
        {count, _rows} -> {:error, {:entries_not_inserted, count, rows_count}}
      end
    end
  end

  defp journal_entry_rows(entries, thread_id, now) do
    result =
      Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, rows} ->
        case encode_term(entry) do
          {:ok, entry_binary} ->
            row = %{
              id: Ecto.UUID.generate(),
              thread_id: thread_id,
              seq: entry.seq,
              entry: entry_binary,
              retention_run_id: retention_run_id(entry),
              inserted_at: now,
              updated_at: now
            }

            {:cont, {:ok, [row | rows]}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, _reason} = error -> error
    end
  end

  defp retention_run_id(%Jido.Thread.Entry{payload: %{data: data}}) when is_map(data) do
    case Map.get(data, :run_id) do
      run_id when is_binary(run_id) and run_id != "" -> run_id
      _missing_or_invalid -> @retention_unowned_marker
    end
  end

  defp retention_run_id(%Jido.Thread.Entry{}), do: @retention_unowned_marker

  defp update_thread_revision(repo, %JournalThread{} = thread, rev, now_ms, opts) do
    db_now = DateTime.utc_now(:microsecond)

    {1, _rows} =
      repo.update_all(
        from(stored_thread in JournalThread, where: stored_thread.id == ^thread.id),
        [set: [rev: rev, updated_at_ms: now_ms, updated_at: db_now]],
        repo_opts(opts)
      )

    %JournalThread{thread | rev: rev, updated_at_ms: now_ms, updated_at: db_now}
  end

  defp load_entries(repo, thread_id, opts) do
    entries =
      repo.all(
        from(entry in JournalEntry, where: entry.thread_id == ^thread_id, order_by: entry.seq),
        repo_opts(opts)
      )

    result =
      Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, decoded_entries} ->
        case decode_entry(entry.entry) do
          {:ok, decoded_entry} -> {:cont, {:ok, [decoded_entry | decoded_entries]}}
          {:error, reason} -> {:halt, {:error, {:invalid_journal_entry, entry.seq, reason}}}
        end
      end)

    case result do
      {:ok, decoded_entries} -> {:ok, Enum.reverse(decoded_entries)}
      {:error, _reason} = error -> error
    end
  end

  defp reconstruct_thread(%JournalThread{} = thread, entries) do
    %Thread{
      id: thread.id,
      rev: thread.rev,
      entries: entries,
      created_at: thread.created_at_ms || (List.first(entries) && List.first(entries).at),
      updated_at:
        thread.updated_at_ms || if(entries == [], do: nil, else: Enum.at(entries, -1).at),
      metadata: thread.metadata || %{},
      stats: %{entry_count: length(entries)}
    }
  end

  defp validate_and_reconstruct_thread(%JournalThread{} = thread, entries) do
    with :ok <- validate_thread_revision(thread, entries),
         :ok <- validate_entry_sequences(thread, entries) do
      reconstruct_thread(thread, entries)
    end
  end

  defp validate_thread_revision(%JournalThread{} = thread, entries) do
    valid? =
      if retention_gaps?(thread) do
        case Enum.at(entries, -1) do
          nil -> thread.rev > 0
          entry -> thread.rev > entry.seq
        end
      else
        thread.rev == length(entries)
      end

    if valid? do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:rev_mismatch, thread.rev, length(entries)}}}
    end
  end

  defp validate_entry_sequences(%JournalThread{} = thread, entries) do
    sequences = Enum.map(entries, & &1.seq)

    valid? =
      if retention_gaps?(thread) do
        sequences == Enum.uniq(sequences) and sequences == Enum.sort(sequences) and
          Enum.all?(sequences, &(&1 >= 0 and &1 < thread.rev))
      else
        sequences == Enum.to_list(0..(length(entries) - 1)//1)
      end

    if valid? do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:seq_gap, sequences}}}
    end
  end

  defp retention_gaps?(%JournalThread{metadata: metadata}) when is_map(metadata) do
    Map.get(metadata, @retention_gap_metadata_key) == true
  end

  defp retention_gaps?(%JournalThread{}), do: false

  defp partition_key(nil), do: ""
  defp partition_key(partition) when is_binary(partition), do: partition

  defp repo_opts(opts) do
    case Keyword.fetch(opts, :prefix) do
      {:ok, prefix} -> [prefix: prefix]
      :error -> []
    end
  end

  defp encode_term(term) do
    with {:ok, encoded} <- encode_value(term) do
      {:ok, :erlang.term_to_binary({@encoded_term_tag, encoded})}
    end
  end

  defp decode_entry(binary) when is_binary(binary) do
    case decode_term(binary) do
      {:ok, %Jido.Thread.Entry{} = entry} -> {:ok, entry}
      {:ok, _invalid} -> {:error, :invalid_entry}
      {:error, _reason} = error -> error
    end
  end

  defp decode_term(binary) when is_binary(binary) do
    # Journal rows are trusted storage, but not trusted code. The versioned
    # envelope lets us safe-decode ETF without interning atoms from tampered
    # rows. Atoms inside the envelope must already exist in the VM; unknown
    # atoms fail closed instead of creating permanent atom table entries.
    case :erlang.binary_to_term(binary, [:safe]) do
      {@encoded_term_tag, encoded} -> decode_value(encoded)
      _other -> {:error, :invalid_encoded_term}
    end
  rescue
    error in [ArgumentError] -> {:error, {error.__struct__, Exception.message(error)}}
  end

  defp encode_value(term) when is_atom(term), do: {:ok, {:atom, Atom.to_string(term)}}
  defp encode_value(term) when is_binary(term), do: {:ok, {:binary, term}}
  defp encode_value(term) when is_integer(term), do: {:ok, {:integer, term}}
  defp encode_value(term) when is_float(term), do: {:ok, {:float, term}}

  defp encode_value(term) when is_list(term) do
    with {:ok, values} <- encode_many(term) do
      {:ok, {:list, values}}
    end
  end

  defp encode_value(term) when is_tuple(term) do
    term
    |> Tuple.to_list()
    |> encode_many()
    |> case do
      {:ok, values} -> {:ok, {:tuple, values}}
      {:error, _reason} = error -> error
    end
  end

  defp encode_value(term) when is_map(term) do
    term
    |> Map.to_list()
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, pairs} ->
      with {:ok, encoded_key} <- encode_value(key),
           {:ok, encoded_value} <- encode_value(value) do
        {:cont, {:ok, [{encoded_key, encoded_value} | pairs]}}
      else
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, pairs} -> {:ok, {:map, Enum.reverse(pairs)}}
      {:error, _reason} = error -> error
    end
  end

  defp encode_value(term), do: {:error, {:unsupported_term, term}}

  defp encode_many(values) when is_list(values) do
    result =
      Enum.reduce_while(values, {:ok, []}, fn value, {:ok, encoded_values} ->
        case encode_value(value) do
          {:ok, encoded_value} -> {:cont, {:ok, [encoded_value | encoded_values]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, encoded_values} -> {:ok, Enum.reverse(encoded_values)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_value({:atom, value}) when is_binary(value), do: existing_atom(value)

  defp decode_value({:binary, value}) when is_binary(value), do: {:ok, value}
  defp decode_value({:integer, value}) when is_integer(value), do: {:ok, value}
  defp decode_value({:float, value}) when is_float(value), do: {:ok, value}

  defp decode_value({:list, values}) when is_list(values) do
    decode_many(values)
  end

  defp decode_value({:tuple, values}) when is_list(values) do
    with {:ok, values} <- decode_many(values) do
      {:ok, List.to_tuple(values)}
    end
  end

  defp decode_value({:map, pairs}) when is_list(pairs) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn
      {encoded_key, encoded_value}, {:ok, decoded_map} ->
        with {:ok, key} <- decode_value(encoded_key),
             {:ok, value} <- decode_value(encoded_value) do
          {:cont, {:ok, Map.put(decoded_map, key, value)}}
        else
          {:error, reason} -> {:halt, {:error, reason}}
        end

      _invalid, _acc ->
        {:halt, {:error, :invalid_encoded_map}}
    end)
  end

  defp decode_value(_encoded), do: {:error, :invalid_encoded_term}

  defp existing_atom(value) do
    {:ok, String.to_existing_atom(value)}
  rescue
    ArgumentError -> {:error, {:unknown_atom, value}}
  end

  defp decode_many(values) when is_list(values) do
    result =
      Enum.reduce_while(values, {:ok, []}, fn value, {:ok, decoded_values} ->
        case decode_value(value) do
          {:ok, decoded_value} -> {:cont, {:ok, [decoded_value | decoded_values]}}
          {:error, reason} -> {:halt, {:error, reason}}
        end
      end)

    case result do
      {:ok, decoded_values} -> {:ok, Enum.reverse(decoded_values)}
      {:error, _reason} = error -> error
    end
  end

  defp key_hash(key_binary) do
    Base.encode16(:crypto.hash(:sha256, key_binary), case: :lower)
  end
end
