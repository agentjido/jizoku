defmodule Jizoku.ReadModel.RunSearch.EctoProjector do
  @moduledoc """
  Projects one authoritative run journal into the optional Ecto search row.
  """

  alias Ecto.Adapters.SQL
  alias Jido.Thread.Entry
  alias Jizoku.Persistence.RunSearch
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @replace_fields [
    :partition,
    :workflow,
    :status,
    :terminal_status,
    :definition_version,
    :search_attributes,
    :started_at,
    :terminal_at,
    :thread_revision,
    :updated_at
  ]

  @doc "Projects a complete persisted Jido run thread inside its append transaction."
  @spec project_thread(
          module(),
          [Entry.t()],
          String.t(),
          String.t() | nil,
          non_neg_integer(),
          keyword()
        ) ::
          :ok | :unavailable | {:error, term()}
  def project_thread(repo, entries, run_id, partition, thread_revision, opts)
      when is_atom(repo) and is_list(entries) and is_binary(run_id) and
             (is_binary(partition) or is_nil(partition)) and is_integer(thread_revision) and
             thread_revision >= 0 and is_list(opts) do
    with {:ok, facts} <- decode_entries(entries, run_id),
         {:ok, row} <- row(Projection.rebuild(facts), partition, thread_revision) do
      upsert(repo, row, opts)
    end
  rescue
    error in Postgrex.Error ->
      if undefined_table?(error), do: :unavailable, else: {:error, error}
  end

  @doc "Builds one normalized search row from a rebuilt workflow projection."
  @spec row(Projection.t(), String.t() | nil, non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def row(%Projection{} = projection, partition, thread_revision)
      when (is_binary(partition) or is_nil(partition)) and is_integer(thread_revision) and
             thread_revision >= 0 do
    with run_id when is_binary(run_id) and run_id != "" <- projection.run_id,
         workflow when is_binary(workflow) and workflow != "" <- projection.workflow,
         %DateTime{} = started_at <- projection.started_at,
         status when is_atom(status) <- Projection.status(projection),
         {:ok, terminal_status} <- optional_atom(Projection.terminal_status(projection)) do
      now = DateTime.utc_now(:microsecond)

      {:ok,
       %{
         partition_key: partition_key(partition),
         partition: partition,
         run_id: run_id,
         workflow: workflow,
         status: Atom.to_string(status),
         terminal_status: terminal_status,
         definition_version: projection.definition_version,
         search_attributes: Projection.search_attributes(projection),
         started_at: ecto_datetime(started_at),
         terminal_at: optional_ecto_datetime(projection.terminal_at),
         thread_revision: thread_revision,
         inserted_at: now,
         updated_at: now
       }}
    else
      _invalid -> {:error, :invalid_run_search_projection}
    end
  end

  @doc "Returns the non-null storage key for an optional runtime partition."
  @spec partition_key(String.t() | nil) :: String.t()
  def partition_key(nil), do: ""
  def partition_key(partition) when is_binary(partition), do: partition

  @doc "Checks whether the optional projection table is available without raising."
  @spec available?(module(), keyword()) :: boolean() | {:error, term()}
  def available?(repo, opts) when is_atom(repo) and is_list(opts) do
    relation =
      case Keyword.get(opts, :prefix) do
        prefix when is_binary(prefix) and prefix != "" -> "#{prefix}.jizoku_run_search"
        _default_prefix -> "jizoku_run_search"
      end

    case SQL.query(repo, "SELECT to_regclass($1)::text", [relation]) do
      {:ok, %{rows: [[nil]]}} -> false
      {:ok, %{rows: [[_relation]]}} -> true
      {:ok, _unexpected} -> {:error, :invalid_run_search_capability_result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_entries(entries, run_id) do
    result =
      Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, facts} ->
        case decode_entry(entry, run_id) do
          {:ok, fact} -> {:cont, {:ok, [fact | facts]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, facts} -> {:ok, Enum.reverse(facts)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_entry(%Entry{kind: kind, payload: payload, at: at}, run_id)
       when is_atom(kind) and is_map(payload) do
    with {:ok, data} <- fetch_data(payload),
         {:ok, occurred_at} <- occurred_at(payload, at) do
      {:ok,
       %DispatchProtocol.Entry{
         type: kind,
         thread: {:run, run_id},
         data: data,
         occurred_at: occurred_at
       }}
    end
  end

  defp decode_entry(_entry, _run_id) do
    {:error, :invalid_run_search_entry}
  end

  defp fetch_data(payload) do
    case Map.fetch(payload, :data) do
      {:ok, data} when is_map(data) -> {:ok, data}
      _missing_or_invalid -> {:error, :invalid_run_search_entry}
    end
  end

  defp occurred_at(payload, at) do
    case Map.get(payload, :occurred_at, at) do
      %DateTime{} = occurred_at -> {:ok, occurred_at}
      milliseconds when is_integer(milliseconds) -> DateTime.from_unix(milliseconds, :millisecond)
      _invalid -> {:error, :invalid_run_search_entry}
    end
  end

  defp optional_atom(nil), do: {:ok, nil}
  defp optional_atom(value) when is_atom(value), do: {:ok, Atom.to_string(value)}

  defp optional_ecto_datetime(nil), do: nil
  defp optional_ecto_datetime(%DateTime{} = value), do: ecto_datetime(value)

  defp ecto_datetime(%DateTime{} = value) do
    DateTime.add(value, 0, :microsecond)
  end

  defp upsert(repo, row, opts) do
    case available?(repo, opts) do
      true -> do_upsert(repo, row, opts)
      false -> :unavailable
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_upsert(repo, row, opts) do
    case repo.insert_all(
           RunSearch,
           [row],
           [
             on_conflict: {:replace, @replace_fields},
             conflict_target: [:partition_key, :run_id]
           ] ++ repo_opts(opts)
         ) do
      {1, _rows} -> :ok
      {count, _rows} -> {:error, {:run_search_not_projected, count}}
    end
  end

  defp repo_opts(opts) do
    case Keyword.fetch(opts, :prefix) do
      {:ok, prefix} -> [prefix: prefix]
      :error -> []
    end
  end

  defp undefined_table?(%Postgrex.Error{postgres: %{code: :undefined_table}}), do: true
  defp undefined_table?(_error), do: false
end
