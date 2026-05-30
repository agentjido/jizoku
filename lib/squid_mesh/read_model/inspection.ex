defmodule SquidMesh.ReadModel.Inspection do
  @moduledoc """
  Projection-backed inspection for the journal-backed runtime.

  The current public `SquidMesh.inspect_run/2` API reads the durable Jido
  journal through the configured `Jido.Storage`. This module is the read-model
  boundary for the journal-backed runtime: it rebuilds workflow and dispatch
  agents from journal entries, combines their projections, and returns a
  factual snapshot of one run.

  The snapshot is intentionally read-only. It does not recover missing dispatch
  entries, apply completed results, or mutate checkpoints. Recovery remains
  owned by `SquidMesh.Runtime.AgentRecovery`; inspection reports what the
  durable journals currently prove.
  """

  alias Jido.Agent
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.DispatchProtocol.ActionAttempt
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Workflow.Definition

  @type storage_config :: Journal.storage_config()
  @type snapshot_option :: {:queue, atom() | String.t()} | {:now, DateTime.t()}
  @type snapshot_error ::
          :not_found
          | {:invalid_option,
             {:now, :invalid}
             | {:queue, :invalid}
             | {:run_id, :invalid}
             | {:opts, :invalid}
             | {:option, atom()}}
          | term()

  @doc """
  Builds a projection-backed inspection snapshot for one workflow run.

  Options:

  - `:queue` selects the dispatch queue projection to join with the run
    projection. It defaults to `"default"`.
  - `:now` controls visibility and lease-expiry calculations. It defaults to
    `DateTime.utc_now/0`.

  Missing run threads return `{:error, :not_found}`. A missing dispatch thread is
  treated as an empty queue projection because a run can be planned before its
  dispatch intents have been recovered.
  """
  @spec snapshot(storage_config(), WorkflowAgent.run_id(), [snapshot_option()]) ::
          {:ok, Snapshot.t()} | {:error, snapshot_error()}
  def snapshot(storage, run_id, opts \\ [])

  def snapshot(storage, run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, run_id} <- Options.thread_part(run_id, :run_id),
         {:ok, opts} <- snapshot_options(opts),
         {:ok, queue} <- snapshot_queue(opts),
         {:ok, now} <- snapshot_time(opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
      {:ok, build_snapshot(workflow_agent, dispatch_agent, queue, now)}
    end
  end

  def snapshot(_storage, run_id, _opts) when is_binary(run_id) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp build_snapshot(
         %Agent{
           agent_module: WorkflowAgent,
           state: %{
             run_id: run_id,
             workflow: workflow,
             projection: %WorkflowAgent.Projection{} = workflow_projection,
             thread_rev: run_thread_rev
           }
         } = workflow_agent,
         %Agent{
           agent_module: DispatchAgent,
           state: %{
             projection: %DispatchProtocol.Projection{} = dispatch_projection,
             thread_rev: dispatch_thread_rev
           }
         } = dispatch_agent,
         queue,
         %DateTime{} = now
       ) do
    pending_dispatches = WorkflowAgent.pending_dispatches(workflow_agent, dispatch_agent)
    pending_results = WorkflowAgent.pending_results(workflow_agent, dispatch_agent)

    terminal? = WorkflowAgent.Projection.terminal?(workflow_projection)

    manual_state =
      if terminal?, do: nil, else: WorkflowAgent.Projection.manual_state(workflow_projection)

    attempts = run_attempts(dispatch_projection, run_id)

    {visible_attempts, scheduled_attempts, expired_claims} =
      if terminal? do
        {[], [], []}
      else
        {
          attempts_for(DispatchAgent.visible_attempts(dispatch_agent, now), run_id),
          scheduled_attempts(attempts, now),
          attempts_for(DispatchAgent.expired_claims(dispatch_agent, now), run_id)
        }
      end

    recovery_by_runnable_key =
      workflow_agent
      |> WorkflowAgent.planned_runnables()
      |> recovery_by_runnable_key()

    %Snapshot{
      run_id: run_id,
      workflow: workflow,
      trigger: workflow_projection.trigger,
      input: workflow_projection.input,
      definition_version: workflow_projection.definition_version,
      context: snapshot_context(workflow_projection),
      parent_run: parent_run(workflow_projection),
      child_runs: WorkflowAgent.Projection.child_runs(workflow_projection),
      replayed_from_run_id: workflow_projection.replayed_from_run_id,
      queue: queue,
      status: WorkflowAgent.status(workflow_agent),
      reason:
        snapshot_reason(
          workflow_projection,
          pending_dispatches,
          pending_results,
          manual_state,
          visible_attempts,
          scheduled_attempts,
          expired_claims,
          attempts
        ),
      terminal?: terminal?,
      terminal_status: WorkflowAgent.Projection.terminal_status(workflow_projection),
      manual_state: manual_state,
      command_history: WorkflowAgent.Projection.command_history(workflow_projection),
      thread_revisions: %{run: run_thread_rev, dispatch: dispatch_thread_rev},
      planned_runnables: normalize_runnables(WorkflowAgent.planned_runnables(workflow_agent)),
      planned_runnable_keys: WorkflowAgent.planned_runnable_keys(workflow_agent),
      applied_runnable_keys:
        workflow_agent
        |> WorkflowAgent.applied_runnable_keys()
        |> MapSet.to_list()
        |> Enum.sort(),
      pending_dispatches: normalize_runnables(pending_dispatches),
      pending_results: Enum.map(pending_results, &attempt_snapshot(&1, recovery_by_runnable_key)),
      visible_attempts:
        Enum.map(visible_attempts, &attempt_snapshot(&1, recovery_by_runnable_key)),
      scheduled_attempts:
        Enum.map(scheduled_attempts, &attempt_snapshot(&1, recovery_by_runnable_key)),
      next_visible_at: next_visible_at(scheduled_attempts),
      expired_claims: Enum.map(expired_claims, &attempt_snapshot(&1, recovery_by_runnable_key)),
      attempts: Enum.map(attempts, &attempt_snapshot(&1, recovery_by_runnable_key)),
      anomalies: projection_anomalies(workflow_projection, dispatch_projection)
    }
  end

  defp snapshot_reason(
         %WorkflowAgent.Projection{} = workflow_projection,
         pending_dispatches,
         pending_results,
         manual_state,
         visible_attempts,
         scheduled_attempts,
         expired_claims,
         attempts
       ) do
    cond do
      WorkflowAgent.Projection.terminal?(workflow_projection) ->
        :terminal

      not is_nil(manual_state) ->
        :manual_intervention_required

      pending_results != [] ->
        :completed_result_pending_apply

      pending_dispatches != [] ->
        :planned_dispatch_pending_schedule

      expired_claims != [] ->
        :expired_claim

      visible_attempts != [] ->
        :attempt_visible

      true ->
        idle_snapshot_reason(workflow_projection, scheduled_attempts, attempts)
    end
  end

  defp snapshot_context(%WorkflowAgent.Projection{} = projection) do
    projection
    |> applied_result_context()
    |> Map.merge(projection.context)
  end

  defp parent_run(%WorkflowAgent.Projection{context: context}) when is_map(context) do
    case Map.fetch(context, :parent) do
      {:ok, parent} -> parent
      :error -> Map.get(context, "parent")
    end
  end

  defp applied_result_context(%WorkflowAgent.Projection{} = projection) do
    projection
    |> WorkflowAgent.Projection.applied_results()
    |> Enum.sort_by(fn {runnable_key, _result} ->
      {applied_result_sort_value(projection, runnable_key), runnable_key}
    end)
    |> Enum.map(fn {_runnable_key, result} -> result end)
    |> Enum.filter(&is_map/1)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp applied_result_sort_value(%WorkflowAgent.Projection{} = projection, runnable_key) do
    case WorkflowAgent.Projection.applied_at(projection, runnable_key) do
      %DateTime{} = applied_at -> DateTime.to_unix(applied_at, :microsecond)
      _missing -> -1
    end
  end

  defp idle_snapshot_reason(workflow_projection, scheduled_attempts, attempts) do
    cond do
      Enum.any?(attempts, &(&1.status == :claimed)) ->
        :attempt_claimed

      scheduled_attempts != [] ->
        :attempt_scheduled_for_later

      WorkflowAgent.Projection.status(workflow_projection) == :idle ->
        :idle

      attempts == [] ->
        :run_started

      true ->
        :waiting_for_dispatch
    end
  end

  defp attempts_for(attempts, run_id) do
    attempts
    |> Enum.filter(&(&1.run_id == run_id))
    |> sort_attempts()
  end

  defp run_attempts(%DispatchProtocol.Projection{attempts: attempts}, run_id) do
    attempts
    |> Map.values()
    |> attempts_for(run_id)
  end

  defp sort_attempts(attempts) do
    Enum.sort_by(attempts, fn attempt ->
      {DateTime.to_unix(attempt.visible_at, :microsecond), attempt.runnable_key,
       attempt.attempt_number}
    end)
  end

  defp scheduled_attempts(attempts, %DateTime{} = now) do
    attempts
    |> Enum.filter(fn %ActionAttempt{} = attempt ->
      attempt.status in [:available, :retry_scheduled] and after?(attempt.visible_at, now)
    end)
    |> sort_attempts()
  end

  defp next_visible_at([%ActionAttempt{visible_at: %DateTime{} = visible_at} | _attempts]) do
    visible_at
  end

  defp next_visible_at([]), do: nil

  defp normalize_runnables(runnables) do
    runnables
    |> Enum.map(&Map.new/1)
    |> Enum.sort_by(&runnable_key/1)
  end

  defp runnable_key(runnable) when is_map(runnable) do
    Map.get(runnable, :runnable_key) || Map.get(runnable, "runnable_key") ||
      Map.get(runnable, :key) || Map.get(runnable, "key") || ""
  end

  defp recovery_by_runnable_key(runnables) when is_list(runnables) do
    Map.new(runnables, fn runnable ->
      {runnable_key(runnable), normalize_recovery(Map.get(runnable, :recovery))}
    end)
  end

  defp attempt_snapshot(%ActionAttempt{} = attempt, recovery_by_runnable_key) do
    snapshot = %{
      runnable_key: attempt.runnable_key,
      status: attempt.status,
      attempt_number: attempt.attempt_number,
      step: normalize_step(attempt.step),
      input: attempt.input,
      visible_at: attempt.visible_at,
      idempotency_key: attempt.idempotency_key,
      claim_id: attempt.claim_id,
      owner_id: attempt.owner_id,
      lease_until: attempt.lease_until,
      result: attempt.result,
      transition: attempt.transition,
      error: attempt.error,
      recovery: Map.get(recovery_by_runnable_key, attempt.runnable_key),
      wakeup_emitted?: attempt.wakeup_emitted?,
      applied?: attempt.applied?
    }

    compact(snapshot)
  end

  defp normalize_recovery(recovery) when is_map(recovery) do
    Definition.normalize_recovery_policy(recovery)
  end

  defp normalize_recovery(_recovery), do: nil

  defp projection_anomalies(
         %WorkflowAgent.Projection{} = workflow_projection,
         %DispatchProtocol.Projection{} = dispatch_projection
       ) do
    workflow_projection
    |> WorkflowAgent.Projection.anomalies()
    |> Enum.map(&Map.put(&1, :source, :workflow))
    |> Kernel.++(
      dispatch_projection
      |> DispatchProtocol.Projection.anomalies()
      |> Enum.map(&Map.put(&1, :source, :dispatch))
    )
  end

  defp snapshot_time(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp snapshot_options(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in [:queue, :now])) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        {:ok, opts}
    end
  end

  defp snapshot_queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp normalize_step(step) when is_atom(step), do: Atom.to_string(step)
  defp normalize_step(step), do: step

  defp after?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :gt
  end
end
