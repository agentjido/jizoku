defmodule Jizoku.Retention do
  @moduledoc "Read-only, partition-scoped retention planning over authoritative journals."

  alias Jizoku.ReadModel.Inspection
  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.Retention.Plan
  alias Jizoku.Retention.Plan.Blocked
  alias Jizoku.Retention.Plan.Candidate
  alias Jizoku.Retention.Policy
  alias Jizoku.Retention.Receipt
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage
  alias Jizoku.Runtime.Routing
  alias Jizoku.Runtime.RunCatalogProjection

  @statuses [:cancelled, :completed, :continued, :failed]
  @default_limit 100
  @max_limit 500
  @expires_in_seconds 900
  @active_attempt_statuses [:claimed]

  @type preview_filter ::
          {:terminal_before, DateTime.t()} | {:statuses, [atom()]} | {:limit, pos_integer()}
  @type preview_option ::
          {:runtime, :journal}
          | {:repo, module()}
          | {:journal_storage, term()}
          | {:partition, String.t() | nil}
          | {:now, DateTime.t()}

  @type apply_option :: preview_option()

  @doc """
  Builds deterministic, expiring retention evidence without deleting data.

  Preview scans only the explicitly selected storage partition. It returns
  archived terminal candidates pinned to current catalog, run, and dispatch
  revisions plus blocked candidates and non-sensitive safety reasons.
  """
  @spec preview([preview_filter()], [preview_option()]) :: {:ok, Plan.t()} | {:error, term()}
  def preview(filters, overrides \\ []) do
    with {:ok, policy} <- normalize_filters(filters),
         :ok <- Routing.public_retention_preview_options(overrides),
         {:ok, :journal} <- Routing.runtime(overrides),
         {:ok, storage} <- Routing.journal_storage(overrides),
         {:ok, now} <- now(overrides),
         {:ok, catalog} <- RunCatalogProjection.load(storage),
         :ok <- safe_catalog(catalog.projection),
         {:ok, scoped} <- scoped_runs(storage, catalog.projection, policy, now),
         selected <- Enum.take(scoped, policy.limit),
         {:ok, dispatch_entries} <- load_dispatch_entries(storage, selected) do
      {:ok, build_plan(storage, catalog.rev, selected, dispatch_entries, policy, now)}
    end
  end

  @doc false
  @spec valid_confirmation?(Plan.t(), String.t(), DateTime.t()) :: boolean()
  defdelegate valid_confirmation?(plan, token, now), to: Plan

  @doc """
  Applies one confirmed retention plan through the transactional Ecto boundary.

  Exact retries return the original receipt. New deletion attempts reject
  expired, modified, stale, cross-partition, unsupported, or empty plans.
  """
  @spec apply(Plan.t(), String.t(), [apply_option()]) ::
          {:ok, Receipt.t()} | {:error, term()}
  def apply(plan, confirmation, overrides \\ [])

  def apply(%Plan{} = plan, confirmation, overrides)
      when is_binary(confirmation) and is_list(overrides) do
    with :ok <- Routing.public_retention_apply_options(overrides),
         {:ok, :journal} <- Routing.runtime(overrides),
         {:ok, storage} <- Routing.journal_storage(overrides),
         {:ok, now} <- now(overrides),
         :ok <- matching_partition(plan, storage),
         true <- Plan.confirmation_matches?(plan, confirmation) do
      Jizoku.Retention.EctoApply.apply(storage, plan, now)
    else
      false -> {:error, :invalid_retention_confirmation}
      {:error, _reason} = error -> error
    end
  end

  def apply(%Plan{}, _confirmation, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  def apply(_plan, _confirmation, _overrides) do
    {:error, {:invalid_option, {:plan, :invalid}}}
  end

  @doc "Re-evaluates one candidate against current lifecycle and host policy state."
  @spec revalidate_candidate(Snapshot.t(), Candidate.t(), DateTime.t()) ::
          :ok | {:error, term()}
  def revalidate_candidate(%Snapshot{} = snapshot, %Candidate{} = candidate, %DateTime{} = now) do
    current = {
      snapshot.run_id,
      snapshot.workflow,
      snapshot.queue,
      snapshot.terminal_status,
      snapshot.terminal_at,
      snapshot.archived_at,
      snapshot.thread_revisions.run,
      snapshot.thread_revisions.dispatch
    }

    expected = {
      candidate.run_id,
      candidate.workflow,
      candidate.queue,
      candidate.terminal_status,
      candidate.terminal_at,
      candidate.archived_at,
      candidate.run_revision,
      candidate.dispatch_revision
    }

    with true <- current == expected,
         {:ok, []} <- eligibility_reasons(snapshot, now) do
      :ok
    else
      false -> {:error, {:stale_retention_plan, candidate.run_id}}
      {:ok, reasons} -> {:error, {:retention_candidate_blocked, candidate.run_id, reasons}}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_filters(filters) when is_list(filters) do
    with :ok <- keyword_filters(filters),
         :ok <- supported_filters(filters),
         {:ok, terminal_before} <- terminal_before(filters),
         {:ok, statuses} <- statuses(filters),
         {:ok, limit} <- limit(filters) do
      {:ok, %{terminal_before: terminal_before, statuses: statuses, limit: limit}}
    end
  end

  defp normalize_filters(_filters) do
    {:error, {:invalid_option, {:filters, :invalid}}}
  end

  defp keyword_filters(filters) do
    if Keyword.keyword?(filters),
      do: :ok,
      else: {:error, {:invalid_option, {:filters, :invalid}}}
  end

  defp supported_filters(filters) do
    case Enum.find(Keyword.keys(filters), &(&1 not in [:terminal_before, :statuses, :limit])) do
      nil -> :ok
      unsupported -> {:error, {:invalid_option, {:filter, unsupported}}}
    end
  end

  defp terminal_before(filters) do
    case Keyword.get(filters, :terminal_before) do
      %DateTime{} = value -> {:ok, value}
      _missing_or_invalid -> {:error, {:invalid_option, {:terminal_before, :invalid}}}
    end
  end

  defp statuses(filters) do
    statuses = Keyword.get(filters, :statuses, @statuses)

    if is_list(statuses) and statuses != [] and
         Enum.all?(statuses, &(&1 in @statuses)) and
         length(Enum.uniq(statuses)) == length(statuses) do
      {:ok, Enum.sort(statuses)}
    else
      {:error, {:invalid_option, {:statuses, :invalid}}}
    end
  end

  defp limit(filters) do
    case Keyword.get(filters, :limit, @default_limit) do
      value when is_integer(value) and value > 0 and value <= @max_limit -> {:ok, value}
      _invalid -> {:error, {:invalid_option, {:limit, :invalid}}}
    end
  end

  defp now(overrides) do
    case Keyword.get(overrides, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp matching_partition(%Plan{partition: partition}, %Storage{} = storage) do
    if Storage.partition(storage) == partition do
      :ok
    else
      {:error, :retention_partition_mismatch}
    end
  end

  defp safe_catalog(%RunCatalogProjection{} = catalog) do
    case RunCatalogProjection.anomalies(catalog) do
      [] -> :ok
      _anomalies -> {:error, {:retention_preview_failed, :catalog_anomalies}}
    end
  end

  defp scoped_runs(storage, catalog, policy, now) do
    result =
      catalog
      |> RunCatalogProjection.runs()
      |> Enum.reduce_while({:ok, []}, fn run, {:ok, scoped} ->
        case inspect_catalog_run(storage, run, policy, now) do
          {:ok, nil} -> {:cont, {:ok, scoped}}
          {:ok, record} -> {:cont, {:ok, [record | scoped]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, scoped} -> {:ok, Enum.sort_by(scoped, &scope_position/1, :asc)}
      {:error, _reason} = error -> error
    end
  end

  defp inspect_catalog_run(storage, run, policy, now) do
    case Inspection.snapshot(storage, run.run_id, queue: run.queue, now: now) do
      {:ok, %Snapshot{} = snapshot} -> scope_snapshot(snapshot, policy, now)
      {:error, reason} -> {:error, {:retention_preview_failed, run.run_id, reason}}
    end
  end

  defp scope_snapshot(%Snapshot{} = snapshot, policy, now) do
    if snapshot.terminal? and snapshot.terminal_status in policy.statuses and
         before?(snapshot.terminal_at, policy.terminal_before) do
      with {:ok, reasons} <- eligibility_reasons(snapshot, now) do
        {:ok, %{snapshot: snapshot, reasons: reasons}}
      end
    else
      {:ok, nil}
    end
  end

  defp eligibility_reasons(%Snapshot{} = snapshot, now) do
    intrinsic =
      []
      |> block_unless(snapshot.archived?, :not_archived)
      |> block_unless(snapshot.anomalies == [], :anomalies_present)
      |> block_unless(
        snapshot.reconciliation_status in [:not_required, :completed],
        :reconciliation_incomplete
      )
      |> block_unless(snapshot.pending_dispatches == [], :pending_dispatches)
      |> block_unless(snapshot.pending_results == [], :pending_results)
      |> block_unless(not active_attempts?(snapshot), :active_attempts)
      |> block_unless(snapshot.jido_signals.pending_count == 0, :pending_jido_signals)
      |> block_unless(not lineage_dependent?(snapshot), :lineage_dependent)

    case Policy.evaluate(snapshot, %{partition: snapshot.partition, now: now}) do
      :allow -> {:ok, normalize_reasons(intrinsic)}
      {:block, reason} -> {:ok, normalize_reasons([reason | intrinsic])}
      {:error, reason} -> {:error, {:retention_policy_failed, reason}}
    end
  end

  defp normalize_reasons(reasons) do
    reasons
    |> Enum.reverse()
    |> Enum.uniq()
  end

  defp block_unless(reasons, true, _reason), do: reasons
  defp block_unless(reasons, false, reason), do: [reason | reasons]

  defp active_attempts?(snapshot) do
    Enum.any?(snapshot.attempts, &(&1.status in @active_attempt_statuses))
  end

  defp lineage_dependent?(snapshot) do
    snapshot.parent_run != nil or snapshot.child_runs != [] or
      snapshot.continuation.continued_from != nil or snapshot.continuation.continued_to != nil
  end

  defp load_dispatch_entries(storage, selected) do
    selected
    |> Enum.map(& &1.snapshot.queue)
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn queue, {:ok, entries_by_queue} ->
      case Journal.load_entries(storage, {:dispatch, queue}) do
        {:ok, entries} -> {:cont, {:ok, Map.put(entries_by_queue, queue, entries)}}
        {:error, :not_found} -> {:cont, {:ok, Map.put(entries_by_queue, queue, [])}}
        {:error, reason} -> {:halt, {:error, {:retention_preview_failed, queue, reason}}}
      end
    end)
  end

  defp build_plan(storage, catalog_rev, selected, dispatch_entries, policy, now) do
    {eligible, blocked} =
      Enum.reduce(selected, {[], []}, fn record, {eligible, blocked} ->
        if record.reasons == [] do
          candidate = candidate(storage, record.snapshot, dispatch_entries)
          {[candidate | eligible], blocked}
        else
          {eligible, [blocked(record.snapshot, record.reasons) | blocked]}
        end
      end)

    Plan.new(%{
      partition: Storage.partition(storage),
      created_at: now,
      expires_at: DateTime.add(now, @expires_in_seconds, :second),
      terminal_before: policy.terminal_before,
      statuses: policy.statuses,
      limit: policy.limit,
      catalog_revision: catalog_rev,
      eligible: Enum.reverse(eligible),
      blocked: Enum.reverse(blocked)
    })
  end

  defp candidate(storage, snapshot, dispatch_entries) do
    run_id = snapshot.run_id
    partition = Storage.partition(storage)

    dispatch_entry_count =
      dispatch_entries
      |> Map.fetch!(snapshot.queue)
      |> Enum.count(&(Map.get(&1.data, :run_id) == run_id))

    affected = %{
      run_thread: Journal.thread_id({:run, run_id}, partition),
      run_checkpoint: Journal.thread_id({:run, run_id}, partition),
      dispatch_thread: Journal.thread_id({:dispatch, snapshot.queue}, partition),
      catalog_thread: Journal.thread_id({:run_catalog, "all"}, partition),
      workflow_index_thread: Journal.thread_id({:run_index, snapshot.workflow}, partition),
      search_row: %{partition: partition, run_id: run_id}
    }

    %Candidate{
      run_id: run_id,
      workflow: snapshot.workflow,
      queue: snapshot.queue,
      terminal_status: snapshot.terminal_status,
      terminal_at: snapshot.terminal_at,
      archived_at: snapshot.archived_at,
      run_revision: snapshot.thread_revisions.run,
      dispatch_revision: snapshot.thread_revisions.dispatch,
      dispatch_entry_count: dispatch_entry_count,
      affected: affected,
      estimated_entries: snapshot.thread_revisions.run + dispatch_entry_count
    }
  end

  defp blocked(snapshot, reasons) do
    %Blocked{
      run_id: snapshot.run_id,
      terminal_status: snapshot.terminal_status,
      terminal_at: snapshot.terminal_at,
      reasons: reasons
    }
  end

  defp scope_position(%{snapshot: snapshot}) do
    {DateTime.to_unix(snapshot.terminal_at, :microsecond), snapshot.run_id}
  end

  defp before?(%DateTime{} = actual, %DateTime{} = expected) do
    DateTime.compare(actual, expected) == :lt
  end

  defp before?(_actual, %DateTime{}), do: false
end
