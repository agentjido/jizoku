defmodule Jizoku.ReadModel.Listing do
  @moduledoc """
  Projection-backed run listing for the journal-backed runtime.

  The journal catalog is a global lookup projection, so this module can list all
  known journal-backed runs without adapter-specific storage scans.
  """

  alias Jido.Agent
  alias Jizoku.ReadModel.HistoryPolicy
  alias Jizoku.ReadModel.Listing.Cursor
  alias Jizoku.ReadModel.Listing.Page
  alias Jizoku.ReadModel.Listing.Summary
  alias Jizoku.ReadModel.Visibility
  alias Jizoku.Runtime.Deadline
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Journal.Storage
  alias Jizoku.Runtime.RunCatalogProjection
  alias Jizoku.Runtime.SearchAttributes
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  @supported_filters [
    :workflow,
    :status,
    :limit,
    :first,
    :after,
    :attributes,
    :partition,
    :started_after,
    :started_before,
    :terminal_after,
    :terminal_before,
    :definition_version
  ]
  @supported_options [:queue, :now, :search_attribute_schema, :actor, :visibility_policy]
  @default_page_size 50
  @max_page_size 100

  @type list_filter ::
          {:workflow, module() | String.t()}
          | {:status, atom()}
          | {:limit, pos_integer()}
          | {:first, 1..100}
          | {:after, String.t()}
          | {:attributes, map()}
          | {:partition, String.t() | nil}
          | {:started_after, DateTime.t()}
          | {:started_before, DateTime.t()}
          | {:terminal_after, DateTime.t()}
          | {:terminal_before, DateTime.t()}
          | {:definition_version, String.t()}
  @type list_option ::
          {:queue, atom() | String.t()}
          | {:now, DateTime.t()}
          | {:search_attribute_schema, SearchAttributes.schema() | nil}
          | {:actor, term()}
          | {:visibility_policy, Visibility.policy()}
  @type list_error ::
          {:invalid_option,
           {:filters, :invalid}
           | {:filter, atom()}
           | {:workflow, :invalid | :required}
           | {:status, :invalid}
           | {:limit, :invalid}
           | {:first, :invalid}
           | {:after, :invalid}
           | {:pagination, :conflicting}
           | {:attributes, :invalid}
           | {:partition, :invalid}
           | {:started_after, :invalid}
           | {:started_before, :invalid}
           | {:terminal_after, :invalid}
           | {:terminal_before, :invalid}
           | {:definition_version, :invalid}
           | {:opts, :invalid}
           | {:option, atom()}
           | {:queue, :invalid}
           | {:now, :invalid}}
          | {:run_catalog_anomalies, [RunCatalogProjection.anomaly()]}
          | {:run_catalog_summary_failed, String.t(), term()}
          | Cursor.cursor_error()
          | Visibility.visibility_error()
          | term()

  @doc """
  Lists redacted summaries from the global journal run catalog.

  Results are ordered newest first by `(started_at, run_id)`. Legacy calls
  return a list. Passing `:first` or `:after` returns a `Page` with an opaque,
  versioned `next_cursor`. Cursor queries are bound to their normalized filters
  and selected storage partition.

  Status, terminal-time, definition-version, and attribute filters rebuild the
  authoritative run projection. Filters never inspect workflow payloads,
  results, or arbitrary journal metadata. Search attributes are redacted by
  default and are only returned when the selected visibility policy resolves to
  `:auditor`.
  """
  @spec list(Journal.storage_config(), [list_filter()], [list_option()]) ::
          {:ok, [Summary.t()] | Page.t()} | {:error, list_error()}
  def list(storage, filters, opts \\ [])

  def list(storage, filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, filters} <- validate_filters(filters),
         {:ok, opts} <- validate_options(opts),
         {:ok, workflow} <- workflow_filter(filters),
         {:ok, attributes} <- attribute_filter(filters, opts),
         {:ok, partition} <- partition_filter(filters, storage),
         :ok <- validate_queue_option(opts),
         {:ok, now} <- now_option(opts),
         query <- build_query(filters, workflow, attributes, partition, storage),
         query_fingerprint <- Cursor.query_fingerprint(cursor_query(query)),
         {:ok, cursor_position} <- cursor_position(query, query_fingerprint, now),
         {:ok, projection} <- Journal.rebuild_run_catalog_projection(storage),
         :ok <- reject_catalog_anomalies(projection) do
      list_query(
        storage,
        projection,
        %{query | cursor_position: cursor_position},
        query_fingerprint,
        now,
        opts
      )
    end
  end

  def list(_storage, _filters, opts) when is_list(opts) do
    {:error, {:invalid_option, {:filters, :invalid}}}
  end

  def list(_storage, _filters, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp validate_filters(filters) do
    with :ok <- validate_filter_keyword(filters),
         :ok <- validate_supported_filters(filters),
         :ok <- validate_filter_value(Keyword.get(filters, :status), &valid_status?/1, :status),
         :ok <- validate_filter_value(Keyword.get(filters, :limit), &valid_limit?/1, :limit),
         :ok <- validate_optional_filter(filters, :first, &valid_first?/1),
         :ok <- validate_optional_filter(filters, :after, &valid_after?/1),
         :ok <- validate_pagination_combination(filters),
         :ok <- validate_optional_filter(filters, :attributes, &is_map/1),
         :ok <-
           validate_optional_filter(filters, :definition_version, &non_empty_binary?/1),
         :ok <- validate_optional_filter(filters, :started_after, &datetime?/1),
         :ok <- validate_optional_filter(filters, :started_before, &datetime?/1),
         :ok <- validate_optional_filter(filters, :terminal_after, &datetime?/1),
         :ok <- validate_optional_filter(filters, :terminal_before, &datetime?/1) do
      {:ok, filters}
    end
  end

  defp validate_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in @supported_options)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        {:ok, opts}
    end
  end

  defp workflow_filter(filters) do
    case Keyword.fetch(filters, :workflow) do
      {:ok, workflow} -> normalize_workflow(workflow)
      :error -> {:ok, nil}
    end
  end

  defp normalize_workflow(workflow) when is_atom(workflow) and not is_nil(workflow) do
    workflow
    |> Definition.serialize_workflow()
    |> Options.thread_part(:workflow)
  end

  defp normalize_workflow(workflow) when is_binary(workflow) do
    Options.thread_part(workflow, :workflow)
  end

  defp normalize_workflow(_workflow), do: {:error, {:invalid_option, {:workflow, :invalid}}}

  defp attribute_filter(filters, opts) do
    filters
    |> Keyword.get(:attributes, %{})
    |> SearchAttributes.normalize(Keyword.get(opts, :search_attribute_schema))
    |> case do
      {:ok, attributes} -> {:ok, attributes}
      {:error, _reason} -> {:error, {:invalid_option, {:attributes, :invalid}}}
    end
  end

  defp partition_filter(filters, storage) do
    case Keyword.fetch(filters, :partition) do
      {:ok, partition} ->
        case Jizoku.Runtime.Partition.normalize(partition) do
          {:ok, partition} -> {:ok, partition}
          {:error, _reason} -> {:error, {:invalid_option, {:partition, :invalid}}}
        end

      :error ->
        {:ok, Storage.partition(storage)}
    end
  end

  defp validate_queue_option(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
    |> case do
      {:ok, _queue} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp now_option(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp reject_catalog_anomalies(%RunCatalogProjection{} = projection) do
    case RunCatalogProjection.anomalies(projection) do
      [] ->
        :ok

      anomalies ->
        {:error, {:run_catalog_anomalies, anomalies}}
    end
  end

  defp list_query(storage, projection, query, query_fingerprint, now, opts) do
    with {:ok, summaries} <- summaries(storage, projection, query, now) do
      if query.page? do
        page(summaries, query, query_fingerprint, now, opts)
      else
        redact(summaries, opts)
      end
    end
  end

  defp summaries(storage, %RunCatalogProjection{} = projection, query, %DateTime{} = now) do
    projection
    |> RunCatalogProjection.runs()
    |> Enum.reverse()
    |> Enum.filter(&catalog_match?(&1, query))
    |> Enum.reduce_while({:ok, []}, fn run_index_summary, {:ok, summaries} ->
      case summary(storage, run_index_summary, now) do
        {:ok, %Summary{} = summary} ->
          maybe_collect_summary(summary, summaries, query)

        {:error, reason} ->
          {:halt, {:error, {:run_catalog_summary_failed, run_index_summary.run_id, reason}}}
      end
    end)
    |> case do
      {:ok, summaries} -> {:ok, Enum.reverse(summaries)}
      {:error, _reason} = error -> error
    end
  end

  defp summary(
         storage,
         %{run_id: run_id, workflow: workflow, queue: queue, indexed_at: indexed_at},
         %DateTime{} = now
       ) do
    with {:ok, %Agent{state: %{projection: %Projection{} = projection, thread_rev: thread_rev}}} <-
           WorkflowAgent.rebuild(storage, run_id),
         :ok <- validate_catalog_summary(projection, run_id, workflow, queue) do
      {:ok,
       %Summary{
         run_id: run_id,
         partition: Storage.partition(storage),
         workflow: workflow,
         definition_version: projection.definition_version,
         continuation: Projection.continuation(projection),
         history: HistoryPolicy.summary(thread_rev),
         queue: queue,
         status: Projection.status(projection),
         terminal?: Projection.terminal?(projection),
         terminal_status: Projection.terminal_status(projection),
         deadline: summary_deadline(projection, now),
         started_at: projection.started_at,
         terminal_at: projection.terminal_at,
         indexed_at: indexed_at,
         thread_revision: thread_rev,
         search_attributes: Projection.search_attributes(projection),
         anomalies: Projection.anomalies(projection)
       }}
    end
  end

  defp summary_deadline(%Projection{} = projection, %DateTime{} = now) do
    if Projection.terminal?(projection) do
      nil
    else
      manual_deadline =
        case Projection.manual_state(projection) do
          %{deadline: deadline, step: step} ->
            deadline
            |> Deadline.evaluate(now, step: step)
            |> Deadline.public_summary()

          _manual_state ->
            nil
        end

      runnable_deadlines =
        projection
        |> active_planned_runnables()
        |> Enum.map(fn runnable ->
          Deadline.public_summary(
            Deadline.evaluate(map_value(runnable, :deadline), now,
              step: runnable_step(runnable),
              runnable_key: runnable_key(runnable)
            )
          )
        end)

      Deadline.most_urgent([manual_deadline | runnable_deadlines])
    end
  end

  defp active_planned_runnables(%Projection{} = projection) do
    applied_keys = Projection.applied_runnable_keys(projection)

    projection
    |> Projection.planned_runnables()
    |> latest_attempt_per_step()
    |> Enum.reject(fn runnable -> MapSet.member?(applied_keys, runnable_key(runnable)) end)
  end

  defp latest_attempt_per_step(runnables) do
    runnables
    |> Enum.group_by(&runnable_step/1)
    |> Enum.flat_map(fn {_step, step_runnables} ->
      step_runnables
      |> Enum.sort_by(&attempt_number/1, :desc)
      |> Enum.take(1)
    end)
  end

  defp runnable_key(runnable) when is_map(runnable) do
    map_value(runnable, :runnable_key) || map_value(runnable, :key) || ""
  end

  defp runnable_step(runnable) when is_map(runnable) do
    map_value(runnable, :step) || runnable_key(runnable)
  end

  defp attempt_number(runnable) when is_map(runnable) do
    case map_value(runnable, :attempt_number) do
      attempt_number when is_integer(attempt_number) -> attempt_number
      _missing_or_invalid -> 0
    end
  end

  defp map_value(map, key, default \\ nil), do: Jizoku.MapField.get(map, key, default)

  defp validate_catalog_summary(%Projection{} = projection, run_id, workflow, queue) do
    cond do
      projection.run_id != run_id ->
        {:error, {:catalog_run_mismatch, catalog_mismatch(run_id, projection.run_id, run_id)}}

      projection.workflow != workflow ->
        {:error,
         {:catalog_workflow_mismatch, catalog_mismatch(workflow, projection.workflow, run_id)}}

      not catalog_queue_matches?(projection, queue) ->
        {:error,
         {:catalog_queue_mismatch, catalog_mismatch(queue, projection_queues(projection), run_id)}}

      true ->
        :ok
    end
  end

  defp catalog_mismatch(expected, actual, run_id) do
    Map.new(expected: expected, actual: actual, run_id: run_id)
  end

  defp catalog_queue_matches?(%Projection{} = projection, queue) do
    case projection_queues(projection) do
      [] -> true
      queues -> queues == [queue]
    end
  end

  defp projection_queues(%Projection{} = projection) do
    projection
    |> Projection.planned_runnables()
    |> Enum.map(&Map.get(&1, :queue))
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp maybe_collect_summary(
         %Summary{} = summary,
         summaries,
         query
       ) do
    if summary_match?(summary, query) do
      summaries = [summary | summaries]

      if limit_reached?(summaries, query.collection_limit) do
        {:halt, {:ok, summaries}}
      else
        {:cont, {:ok, summaries}}
      end
    else
      {:cont, {:ok, summaries}}
    end
  end

  defp build_query(filters, workflow, attributes, partition, storage) do
    page? = Keyword.has_key?(filters, :first) or Keyword.has_key?(filters, :after)
    page_size = if page?, do: Keyword.get(filters, :first, @default_page_size)
    collection_limit = if page?, do: page_size + 1, else: Keyword.get(filters, :limit)

    %{
      page?: page?,
      page_size: page_size,
      collection_limit: collection_limit,
      after: Keyword.get(filters, :after),
      cursor_position: nil,
      workflow: workflow,
      status: Keyword.get(filters, :status),
      attributes: attributes,
      partition: partition,
      storage_partition: Storage.partition(storage),
      started_after: Keyword.get(filters, :started_after),
      started_before: Keyword.get(filters, :started_before),
      terminal_after: Keyword.get(filters, :terminal_after),
      terminal_before: Keyword.get(filters, :terminal_before),
      definition_version: Keyword.get(filters, :definition_version)
    }
  end

  defp cursor_query(query) do
    Map.take(query, [
      :workflow,
      :status,
      :attributes,
      :partition,
      :storage_partition,
      :started_after,
      :started_before,
      :terminal_after,
      :terminal_before,
      :definition_version
    ])
  end

  defp cursor_position(%{after: nil}, _query_fingerprint, _now) do
    {:ok, nil}
  end

  defp cursor_position(%{after: cursor}, query_fingerprint, now) do
    Cursor.decode(cursor, query_fingerprint, now)
  end

  defp page(summaries, query, query_fingerprint, now, opts) do
    items = Enum.take(summaries, query.page_size)
    has_more? = length(summaries) > query.page_size

    next_cursor =
      if has_more? do
        items
        |> List.last()
        |> summary_position()
        |> Cursor.encode(query_fingerprint, now)
      end

    with {:ok, items} <- redact(items, opts) do
      {:ok, %Page{items: items, next_cursor: next_cursor}}
    end
  end

  defp redact(summaries, opts) do
    Visibility.redact(
      summaries,
      Keyword.get(opts, :actor),
      Keyword.get(opts, :visibility_policy, :external)
    )
  end

  defp catalog_match?(run, query) do
    workflow_match?(run, query.workflow) and
      partition_match?(query.partition, query.storage_partition) and
      after_cursor?(run, query.cursor_position) and
      after_time?(run.indexed_at, query.started_after) and
      before_time?(run.indexed_at, query.started_before)
  end

  defp summary_match?(%Summary{} = summary, query) do
    status_match?(summary, query.status) and
      definition_version_match?(summary, query.definition_version) and
      attributes_match?(summary, query.attributes) and
      after_time?(summary.terminal_at, query.terminal_after) and
      before_time?(summary.terminal_at, query.terminal_before)
  end

  defp workflow_match?(_run, nil), do: true
  defp workflow_match?(%{workflow: current}, expected), do: current == expected

  defp status_match?(%Summary{}, nil), do: true
  defp status_match?(%Summary{status: current}, expected), do: current == expected

  defp definition_version_match?(%Summary{}, nil), do: true

  defp definition_version_match?(%Summary{definition_version: current}, expected) do
    current == expected
  end

  defp attributes_match?(%Summary{}, attributes) when map_size(attributes) == 0 do
    true
  end

  defp attributes_match?(%Summary{search_attributes: current}, attributes) do
    Enum.all?(attributes, fn {key, expected} -> Map.get(current, key) == expected end)
  end

  defp partition_match?(partition, partition), do: true
  defp partition_match?(_requested, _storage), do: false

  defp after_cursor?(_run, nil), do: true

  defp after_cursor?(run, cursor_position) do
    position_before?(run_position(run), cursor_position)
  end

  defp position_before?({started_at_us, run_id}, {cursor_started_at_us, cursor_run_id}) do
    started_at_us < cursor_started_at_us or
      (started_at_us == cursor_started_at_us and run_id < cursor_run_id)
  end

  defp run_position(%{run_id: run_id, indexed_at: %DateTime{} = indexed_at}) do
    {DateTime.to_unix(indexed_at, :microsecond), run_id}
  end

  defp summary_position(%Summary{run_id: run_id, indexed_at: %DateTime{} = indexed_at}) do
    {DateTime.to_unix(indexed_at, :microsecond), run_id}
  end

  defp after_time?(_actual, nil), do: true

  defp after_time?(%DateTime{} = actual, %DateTime{} = expected) do
    DateTime.compare(actual, expected) == :gt
  end

  defp after_time?(_actual, %DateTime{}), do: false

  defp before_time?(_actual, nil), do: true

  defp before_time?(%DateTime{} = actual, %DateTime{} = expected) do
    DateTime.compare(actual, expected) == :lt
  end

  defp before_time?(_actual, %DateTime{}), do: false

  defp limit_reached?(_summaries, nil), do: false
  defp limit_reached?(summaries, limit), do: length(summaries) >= limit

  defp valid_status?(nil), do: true
  defp valid_status?(status), do: is_atom(status)

  defp valid_limit?(nil), do: true
  defp valid_limit?(limit), do: is_integer(limit) and limit > 0

  defp valid_first?(first), do: is_integer(first) and first > 0 and first <= @max_page_size

  defp valid_after?(cursor), do: non_empty_binary?(cursor)

  defp pagination_conflict?(filters) do
    Keyword.has_key?(filters, :limit) and
      (Keyword.has_key?(filters, :first) or Keyword.has_key?(filters, :after))
  end

  defp validate_filter_keyword(filters) do
    if Keyword.keyword?(filters) do
      :ok
    else
      {:error, {:invalid_option, {:filters, :invalid}}}
    end
  end

  defp validate_supported_filters(filters) do
    case Enum.find(Keyword.keys(filters), &(&1 not in @supported_filters)) do
      nil -> :ok
      unsupported -> {:error, {:invalid_option, {:filter, unsupported}}}
    end
  end

  defp validate_filter_value(value, validator, key) do
    if validator.(value) do
      :ok
    else
      {:error, {:invalid_option, {key, :invalid}}}
    end
  end

  defp validate_optional_filter(filters, key, validator) do
    case Keyword.fetch(filters, key) do
      {:ok, value} -> validate_filter_value(value, validator, key)
      :error -> :ok
    end
  end

  defp validate_pagination_combination(filters) do
    if pagination_conflict?(filters) do
      {:error, {:invalid_option, {:pagination, :conflicting}}}
    else
      :ok
    end
  end

  defp datetime?(value) do
    match?(%DateTime{}, value)
  end

  defp non_empty_binary?(value) do
    is_binary(value) and value != ""
  end
end
