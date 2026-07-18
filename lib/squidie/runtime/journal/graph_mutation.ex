# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.GraphMutation do
  @moduledoc false

  alias Squidie.GraphMutation.Report
  alias Squidie.Runs.GraphMutationPreview
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.EntryBuilder
  alias Squidie.Runtime.Journal.GraphMutation.Materializer.Result
  alias Squidie.Runtime.Journal.GraphMutation.Reconciler

  @conflict_retries 25

  @doc false
  @spec apply(Journal.storage_config(), String.t(), term(), keyword()) ::
          {:ok, Report.t()} | {:error, term()}
  def apply(storage, run_id, attrs, opts) when is_binary(run_id) and is_list(opts) do
    apply_with_retries(storage, run_id, attrs, opts, @conflict_retries)
  end

  defp apply_with_retries(_storage, _run_id, _attrs, _opts, 0) do
    {:error, :conflict}
  end

  defp apply_with_retries(storage, run_id, attrs, opts, retries) do
    with {:ok, evaluation} <- GraphMutationPreview.evaluate(storage, run_id, attrs, opts) do
      persist_evaluation(storage, run_id, attrs, opts, retries, evaluation)
    end
  end

  defp persist_evaluation(storage, run_id, _attrs, opts, _retries, %{result: :duplicate} = value) do
    report_after_reconciliation(storage, run_id, value, opts, :duplicate)
  end

  defp persist_evaluation(
         storage,
         run_id,
         attrs,
         opts,
         retries,
         %{result: %Result{} = result} = evaluation
       ) do
    with {:ok, entries} <- mutation_entries(run_id, result, evaluation.now),
         {:ok, _thread} <-
           Journal.append_entries(storage, entries,
             expected_rev: evaluation.workflow_agent.state.thread_rev,
             telemetry_projection: evaluation.workflow_agent.state.projection
           ) do
      report_after_reconciliation(storage, run_id, evaluation, opts, :committed)
    else
      {:error, :conflict} ->
        apply_with_retries(storage, run_id, attrs, opts, retries - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp mutation_entries(run_id, result, now) do
    attrs = Map.put(result.mutation_attrs, :occurred_at, now)

    with {:ok, mutation_entry} <- DispatchProtocol.new_entry(:dynamic_graph_mutated, attrs),
         {:ok, runnable_entries} <- runnable_entries(run_id, result.runnables, now) do
      {:ok, [mutation_entry | runnable_entries]}
    end
  end

  defp runnable_entries(_run_id, [], _now) do
    {:ok, []}
  end

  defp runnable_entries(run_id, runnables, now) do
    case EntryBuilder.runnables_planned(run_id, runnables, now) do
      {:ok, entry} -> {:ok, [entry]}
      {:error, _reason} = error -> error
    end
  end

  defp report_after_reconciliation(storage, run_id, evaluation, opts, status) do
    case Reconciler.reconcile(storage, run_id, opts) do
      {:ok, _reconciliation} ->
        {:ok, report(evaluation, status, :completed, [])}

      {:error, _reason} ->
        unresolved_report(evaluation, status)
    end
  end

  defp unresolved_report(evaluation, :duplicate) do
    {:ok, report(evaluation, :duplicate, :required, [:reconciliation_required])}
  end

  defp unresolved_report(evaluation, :committed) do
    {:ok,
     report(
       evaluation,
       :committed_needs_reconciliation,
       :required,
       [:reconciliation_required]
     )}
  end

  defp report(
         %{mutation: mutation, context: context, result: result},
         status,
         reconciliation,
         warnings
       ) do
    Report.new(mutation,
      base_version: context.graph.version,
      result_version: result_version(context, result),
      duplicate?: result == :duplicate,
      status: status,
      applied_operations: operations(mutation),
      reconciliation: reconciliation,
      warnings: warnings
    )
  end

  defp result_version(context, :duplicate) do
    context.graph.version
  end

  defp result_version(_context, %Result{} = result) do
    result.mutation_attrs.result_version
  end

  defp operations(mutation) do
    Enum.map(mutation.additions, &{:add, &1}) ++ Enum.map(mutation.removals, &{:remove, &1})
  end
end
