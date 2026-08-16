defmodule Jizoku.ReadModel.RunSearch.EctoQuery do
  @moduledoc """
  Executes bounded candidate queries against the optional Ecto run-search projection.
  """

  import Ecto.Query

  alias Jizoku.Persistence.RunSearch
  alias Jizoku.ReadModel.RunSearch.EctoProjector
  alias Jizoku.Runtime.Journal.Storage

  @type query_result ::
          {:ok, [String.t()]}
          | {:fallback, :unsupported | :unavailable | :incomplete}
          | {:error, term()}

  @doc "Returns ordered candidate run IDs or requests the journal fallback."
  @spec candidates(Storage.t() | Storage.config(), map(), non_neg_integer()) :: query_result()
  def candidates(storage, query, expected_count)
      when is_map(query) and is_integer(expected_count) and expected_count >= 0 do
    with {:ok, %Storage{} = storage} <- Storage.normalize(storage) do
      do_candidates(storage, query, expected_count)
    end
  end

  @doc "Builds the Ecto query used by indexed run listing."
  @spec candidate_query(map(), String.t()) :: Ecto.Query.t()
  def candidate_query(query, partition_key) when is_map(query) and is_binary(partition_key) do
    RunSearch
    |> where([run], run.partition_key == ^partition_key)
    |> maybe_workflow(query.workflow)
    |> maybe_status(query.status)
    |> maybe_definition_version(query.definition_version)
    |> maybe_attributes(query.attributes)
    |> maybe_after_time(:started_at, query.started_after)
    |> maybe_before_time(:started_at, query.started_before)
    |> maybe_after_time(:terminal_at, query.terminal_after)
    |> maybe_before_time(:terminal_at, query.terminal_before)
    |> maybe_after_cursor(query.cursor_position)
    |> order_by([run], desc: run.started_at, desc: run.run_id)
    |> maybe_limit(query.collection_limit)
    |> select([run], run.run_id)
  end

  defp do_candidates(
         %Storage{adapter: Jizoku.Runtime.Journal.Storage.Ecto, opts: opts},
         query,
         expected_count
       ) do
    repo = Keyword.fetch!(opts, :repo)
    storage_partition = Map.get(query, :storage_partition)
    partition_key = EctoProjector.partition_key(storage_partition)

    case {query.partition == storage_partition, EctoProjector.available?(repo, opts)} do
      {false, _available} -> {:ok, []}
      {true, false} -> {:fallback, :unavailable}
      {true, true} -> query_candidates(repo, query, partition_key, expected_count, opts)
      {true, {:error, reason}} -> {:error, {:run_search_query_failed, reason}}
    end
  rescue
    error in Postgrex.Error ->
      if undefined_table?(error),
        do: {:fallback, :unavailable},
        else: {:error, {:run_search_query_failed, error}}
  end

  defp do_candidates(%Storage{}, _query, _expected_count) do
    {:fallback, :unsupported}
  end

  defp query_candidates(repo, query, partition_key, expected_count, opts) do
    count =
      repo.one(
        from(run in RunSearch,
          where: run.partition_key == ^partition_key,
          select: count(run.run_id)
        ),
        repo_opts(opts)
      )

    if count == expected_count do
      {:ok, repo.all(candidate_query(query, partition_key), repo_opts(opts))}
    else
      {:fallback, :incomplete}
    end
  end

  defp maybe_workflow(query, nil), do: query
  defp maybe_workflow(query, workflow), do: where(query, [run], run.workflow == ^workflow)

  defp maybe_status(query, nil), do: query

  defp maybe_status(query, status) when is_atom(status) do
    where(query, [run], run.status == ^Atom.to_string(status))
  end

  defp maybe_definition_version(query, nil), do: query

  defp maybe_definition_version(query, definition_version) do
    where(query, [run], run.definition_version == ^definition_version)
  end

  defp maybe_attributes(query, attributes) when map_size(attributes) == 0, do: query

  defp maybe_attributes(query, attributes) do
    where(query, [run], fragment("? @> ?", run.search_attributes, type(^attributes, :map)))
  end

  defp maybe_after_time(query, _field, nil), do: query

  defp maybe_after_time(query, field, value) do
    where(query, [run], field(run, ^field) > ^value)
  end

  defp maybe_before_time(query, _field, nil), do: query

  defp maybe_before_time(query, field, value) do
    where(query, [run], field(run, ^field) < ^value)
  end

  defp maybe_after_cursor(query, nil), do: query

  defp maybe_after_cursor(query, {started_at_us, run_id}) do
    {:ok, started_at} = DateTime.from_unix(started_at_us, :microsecond)

    where(
      query,
      [run],
      run.started_at < ^started_at or (run.started_at == ^started_at and run.run_id < ^run_id)
    )
  end

  defp maybe_limit(query, nil), do: query
  defp maybe_limit(query, limit), do: limit(query, ^limit)

  defp repo_opts(opts) do
    Keyword.take(opts, [:prefix])
  end

  defp undefined_table?(%Postgrex.Error{postgres: %{code: :undefined_table}}), do: true
  defp undefined_table?(_error), do: false
end
