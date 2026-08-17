defmodule Jizoku.ReadModel.RunSearch.Rebuilder do
  @moduledoc """
  Rebuilds the optional Ecto run-search projection from authoritative journals.

  Rebuilds are explicitly scoped to the selected runtime partition. The
  operation never discovers or rewrites another partition.
  """

  import Ecto.Query

  alias Jido.Agent
  alias Jizoku.Persistence.JournalThread
  alias Jizoku.Persistence.RunSearch
  alias Jizoku.ReadModel.RunSearch.EctoProjector
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage
  alias Jizoku.Runtime.RunCatalogProjection
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @retries 3
  @batch_size 500

  @type result :: %{
          partition: String.t() | nil,
          rebuilt: non_neg_integer(),
          catalog_revision: non_neg_integer()
        }

  @doc """
  Refreshes one partition projection and verifies that source revisions stayed
  stable across the rebuild.
  """
  @spec rebuild(Storage.t() | Storage.config()) :: {:ok, result()} | {:error, term()}
  def rebuild(storage) do
    rebuild(storage, [])
  end

  @doc false
  @spec rebuild(Storage.t() | Storage.config(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def rebuild(storage, opts) when is_list(opts) do
    with {:ok, %Storage{} = storage} <- Storage.normalize(storage) do
      do_rebuild(storage, @retries, opts)
    end
  end

  defp do_rebuild(
         %Storage{adapter: Jizoku.Runtime.Journal.Storage.Ecto} = storage,
         retries_left,
         opts
       ) do
    with {:ok, catalog} <- RunCatalogProjection.load(storage),
         {:ok, rows} <- build_rows(storage, catalog.projection),
         :ok <- upsert_rows(storage, rows),
         :ok <- run_test_before_source_revision_check(opts),
         :ok <- verify_source_revisions(storage, catalog.rev, rows) do
      {:ok,
       %{
         partition: Storage.partition(storage),
         rebuilt: length(rows),
         catalog_revision: catalog.rev
       }}
    else
      {:error, :projection_changed} when retries_left > 0 ->
        do_rebuild(storage, retries_left - 1, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp do_rebuild(%Storage{adapter: adapter}, _retries_left, _opts) do
    {:error, {:unsupported_run_search_projection, adapter}}
  end

  defp build_rows(storage, %RunCatalogProjection{} = catalog) do
    catalog
    |> RunCatalogProjection.runs()
    |> Enum.reduce_while({:ok, []}, fn run, {:ok, rows} ->
      case build_row(storage, run.run_id) do
        {:ok, row} -> {:cont, {:ok, [row | rows]}}
        {:error, reason} -> {:halt, {:error, {:run_search_rebuild_failed, run.run_id, reason}}}
      end
    end)
    |> case do
      {:ok, rows} -> {:ok, Enum.reverse(rows)}
      {:error, _reason} = error -> error
    end
  end

  defp build_row(storage, run_id) do
    with {:ok,
          %Agent{state: %{projection: %Projection{} = projection, thread_rev: thread_revision}}} <-
           WorkflowAgent.rebuild(storage, run_id) do
      EctoProjector.row(projection, Storage.partition(storage), thread_revision)
    end
  end

  defp upsert_rows(%Storage{opts: opts}, rows) do
    repo = Keyword.fetch!(opts, :repo)

    case repo.transaction(fn ->
           rows
           |> Enum.chunk_every(@batch_size)
           |> Enum.each(&insert_rows(repo, &1, opts))
         end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> {:error, {:run_search_rebuild_failed, reason}}
    end
  rescue
    error in Postgrex.Error -> {:error, {:run_search_rebuild_failed, error}}
  end

  defp insert_rows(_repo, [], _opts), do: :ok

  defp insert_rows(repo, rows, opts) do
    expected_count = length(rows)

    insert_opts =
      [
        on_conflict: conflict_query(),
        conflict_target: [:partition_key, :run_id]
      ] ++ repo_opts(opts)

    case repo.insert_all(RunSearch, rows, insert_opts) do
      {count, _rows} when count <= expected_count -> :ok
      {count, _rows} -> repo.rollback({:rows_not_inserted, count, expected_count})
    end
  end

  defp conflict_query do
    from(run in RunSearch,
      update: [
        set: [
          partition: fragment("EXCLUDED.partition"),
          workflow: fragment("EXCLUDED.workflow"),
          status: fragment("EXCLUDED.status"),
          terminal_status: fragment("EXCLUDED.terminal_status"),
          definition_version: fragment("EXCLUDED.definition_version"),
          search_attributes: fragment("EXCLUDED.search_attributes"),
          started_at: fragment("EXCLUDED.started_at"),
          terminal_at: fragment("EXCLUDED.terminal_at"),
          archived_at: fragment("EXCLUDED.archived_at"),
          archive_reason: fragment("EXCLUDED.archive_reason"),
          thread_revision: fragment("EXCLUDED.thread_revision"),
          updated_at: fragment("EXCLUDED.updated_at")
        ]
      ],
      where: fragment("EXCLUDED.thread_revision >= ?", run.thread_revision)
    )
  end

  defp verify_source_revisions(%Storage{opts: opts} = storage, catalog_rev, rows) do
    with {:ok, %{rev: ^catalog_rev}} <- RunCatalogProjection.load(storage),
         true <- current_run_revisions?(storage, rows, opts) do
      :ok
    else
      _changed -> {:error, :projection_changed}
    end
  end

  defp current_run_revisions?(storage, rows, opts) do
    repo = Keyword.fetch!(opts, :repo)

    expected =
      Map.new(rows, fn row ->
        {Journal.thread_id({:run, row.run_id}, Storage.partition(storage)), row.thread_revision}
      end)

    current =
      Map.new(
        repo.all(
          from(thread in JournalThread,
            where: thread.id in ^Map.keys(expected),
            select: {thread.id, thread.rev}
          ),
          repo_opts(opts)
        )
      )

    current == expected
  end

  defp repo_opts(opts) do
    case Keyword.get(opts, :prefix) do
      prefix when is_binary(prefix) and prefix != "" -> [prefix: prefix]
      _default_prefix -> []
    end
  end

  defp run_test_before_source_revision_check(opts) do
    case Keyword.get(opts, :test_before_source_revision_check) do
      nil -> :ok
      hook when is_function(hook, 0) -> hook.()
      _invalid -> {:error, :invalid_test_before_source_revision_check}
    end
  end
end
