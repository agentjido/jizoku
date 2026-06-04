defmodule Squidie.Runtime.Journal.Storage.Ecto do
  @moduledoc """
  Postgres-compatible Ecto storage adapter for Squidie journal runtime state.

  Use this adapter when the host app wants the Jido journal runtime persisted in
  the same Postgres-compatible database boundary as the rest of the application:

      config :squidie,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: {Squidie.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}

  The adapter implements Jido's checkpoint and append-only thread callbacks.
  Thread appends are serialized with a row-level lock and honor Jido's
  `:expected_rev` optimistic concurrency option.
  """

  @behaviour Jido.Storage

  import Ecto.Query

  alias Jido.Thread
  alias Jido.Thread.EntryNormalizer
  alias Squidie.Persistence.JournalCheckpoint
  alias Squidie.Persistence.JournalEntry
  alias Squidie.Persistence.JournalThread

  @encoded_term_tag :squidie_ecto_term_v1

  @type opts :: keyword()

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
    with {:ok, repo} <- fetch_repo(opts) do
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
        from(entry in JournalEntry, where: entry.thread_id == ^thread_id),
        repo_opts(opts)
      )

      repo.delete_all(
        from(thread in JournalThread, where: thread.id == ^thread_id),
        repo_opts(opts)
      )

      :ok
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
      {:ok, []} -> repo.rollback(:not_found)
      {:ok, entries} -> reconstruct_or_rollback(repo, thread, entries)
      {:error, reason} -> repo.rollback(reason)
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
    thread = ensure_locked_thread(repo, thread_id, now_ms, opts)

    case validate_expected_rev(expected_rev, thread.rev) do
      :ok -> append_locked_thread(repo, thread, entries, now_ms, opts)
      {:error, reason} -> repo.rollback(reason)
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
         {:ok, all_entries} <- load_entries(repo, thread.id, opts) do
      reconstruct_thread(updated_thread, all_entries)
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp normalize_thread_result({:ok, %Thread{} = thread}), do: {:ok, thread}
  defp normalize_thread_result({:error, reason}), do: {:error, reason}

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

    rows =
      Enum.map(entries, fn entry ->
        {:ok, entry_binary} = encode_term(entry)

        %{
          id: Ecto.UUID.generate(),
          thread_id: thread_id,
          seq: entry.seq,
          entry: entry_binary,
          inserted_at: now,
          updated_at: now
        }
      end)

    rows_count = length(rows)

    case repo.insert_all(JournalEntry, rows, repo_opts(opts)) do
      {count, _rows} when count == rows_count -> :ok
      {count, _rows} -> {:error, {:entries_not_inserted, count, rows_count}}
    end
  end

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
    entry_count = length(entries)

    if thread.rev == entry_count do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:rev_mismatch, thread.rev, entry_count}}}
    end
  end

  defp validate_entry_sequences(%JournalThread{} = thread, entries) do
    sequences = Enum.map(entries, & &1.seq)
    expected_sequences = Enum.to_list(0..(length(entries) - 1)//1)

    if sequences == expected_sequences do
      :ok
    else
      {:error, {:invalid_journal_thread, thread.id, {:seq_gap, sequences}}}
    end
  end

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
    error -> {:error, {error.__struct__, Exception.message(error)}}
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
