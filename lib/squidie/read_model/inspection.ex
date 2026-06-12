defmodule Squidie.ReadModel.Inspection do
  @moduledoc """
  Projection-backed inspection for the journal-backed runtime.

  The current public `Squidie.inspect_run/2` API reads the durable Jido
  journal through the configured `Jido.Storage`. This module is the read-model
  boundary for the journal-backed runtime: it rebuilds workflow and dispatch
  agents from journal entries, combines their projections, and returns a
  factual snapshot of one run.

  The snapshot is intentionally read-only. It does not recover missing dispatch
  entries, apply completed results, or mutate checkpoints. Recovery remains
  owned by `Squidie.Runtime.AgentRecovery`; inspection reports what the
  durable journals currently prove.
  """

  alias Jido.Agent
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.ReadModel.Timeline
  alias Squidie.Runtime.Deadline
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Workflow.Definition

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

    planned_runnables = WorkflowAgent.planned_runnables(workflow_agent)

    recovery_by_runnable_key = recovery_by_runnable_key(planned_runnables)

    deferred_by_runnable_key = deferred_by_runnable_key(planned_runnables)
    deadline_by_runnable_key = deadline_by_runnable_key(planned_runnables, now)

    normalized_manual_state = normalize_manual_state(manual_state, now)
    normalized_planned_runnables = normalize_runnables(planned_runnables, now)

    attempt_snapshot_fun =
      &attempt_snapshot(
        &1,
        recovery_by_runnable_key,
        deferred_by_runnable_key,
        deadline_by_runnable_key,
        now
      )

    pending_result_snapshots = Enum.map(pending_results, attempt_snapshot_fun)
    visible_attempt_snapshots = Enum.map(visible_attempts, attempt_snapshot_fun)
    scheduled_attempt_snapshots = Enum.map(scheduled_attempts, attempt_snapshot_fun)
    expired_claim_snapshots = Enum.map(expired_claims, attempt_snapshot_fun)
    attempt_snapshots = Enum.map(attempts, attempt_snapshot_fun)

    active_attempt_snapshots =
      attempts
      |> active_attempts()
      |> Enum.map(attempt_snapshot_fun)

    normalized_pending_dispatches = normalize_runnables(pending_dispatches, now)

    %Snapshot{
      run_id: run_id,
      workflow: workflow,
      trigger: workflow_projection.trigger,
      input: workflow_projection.input,
      started_at: workflow_projection.started_at,
      definition_version: workflow_projection.definition_version,
      context: snapshot_context(workflow_projection),
      parent_run: parent_run(workflow_projection),
      child_runs: WorkflowAgent.Projection.child_runs(workflow_projection),
      dynamic_work: WorkflowAgent.Projection.dynamic_work(workflow_projection),
      guardrails: guardrail_decisions(normalized_planned_runnables, attempt_snapshots),
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
      terminal_at: workflow_projection.terminal_at,
      terminal_error:
        public_terminal_error(WorkflowAgent.Projection.terminal_error(workflow_projection)),
      deadline:
        if(terminal?,
          do: nil,
          else:
            active_deadline([
              normalized_manual_state,
              normalized_pending_dispatches,
              active_attempt_snapshots
            ])
        ),
      manual_state: normalized_manual_state,
      command_history: WorkflowAgent.Projection.command_history(workflow_projection),
      thread_revisions: %{run: run_thread_rev, dispatch: dispatch_thread_rev},
      planned_runnables: normalized_planned_runnables,
      planned_runnable_keys: WorkflowAgent.planned_runnable_keys(workflow_agent),
      applied_runnable_keys:
        workflow_agent
        |> WorkflowAgent.applied_runnable_keys()
        |> MapSet.to_list()
        |> Enum.sort(),
      applied_at: workflow_projection.applied_at,
      pending_dispatches: normalized_pending_dispatches,
      pending_results: pending_result_snapshots,
      visible_attempts: visible_attempt_snapshots,
      scheduled_attempts: scheduled_attempt_snapshots,
      next_visible_at: next_visible_at(scheduled_attempts),
      expired_claims: expired_claim_snapshots,
      attempts: attempt_snapshots,
      anomalies: projection_anomalies(workflow_projection, dispatch_projection)
    }
  end

  @doc """
  Builds a chronological, redaction-safe operator timeline from an inspection snapshot.
  """
  @spec timeline(Snapshot.t()) :: {:ok, Timeline.t()}
  def timeline(%Snapshot{} = snapshot), do: {:ok, Timeline.from_snapshot(snapshot)}

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

      deferred_attempts?(scheduled_attempts, workflow_projection) ->
        :deferred_continuation

      true ->
        idle_snapshot_reason(workflow_projection, scheduled_attempts, attempts)
    end
  end

  defp deferred_attempts?(scheduled_attempts, %WorkflowAgent.Projection{} = workflow_projection) do
    deferred_keys =
      workflow_projection
      |> WorkflowAgent.Projection.planned_runnables()
      |> deferred_by_runnable_key()
      |> Map.keys()
      |> MapSet.new()

    Enum.any?(scheduled_attempts, &MapSet.member?(deferred_keys, &1.runnable_key))
  end

  defp snapshot_context(%WorkflowAgent.Projection{} = projection) do
    projection
    |> applied_result_context()
    |> Map.merge(projection.context)
  end

  defp public_terminal_error(nil), do: nil

  defp public_terminal_error(error) when is_map(error) do
    keys = [
      :code,
      :message,
      :retryable?,
      :retry_after,
      :type,
      :persisted_definition_version,
      :persisted_definition_fingerprint,
      :current_definition_version,
      :current_definition_fingerprint,
      :path,
      :target,
      :missing_at
    ]

    sanitized =
      Enum.reduce(keys, %{}, fn key, acc ->
        value =
          case Map.fetch(error, key) do
            {:ok, atom_value} ->
              {:ok, atom_value}

            :error ->
              Map.fetch(error, Atom.to_string(key))
          end

        case value do
          {:ok, nil} -> acc
          {:ok, field_value} -> Map.put(acc, key, field_value)
          :error -> acc
        end
      end)

    if map_size(sanitized) == 0, do: nil, else: sanitized
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

  defp active_attempts(attempts) do
    attempts
    |> Enum.filter(fn %ActionAttempt{} = attempt ->
      attempt.status in [:available, :retry_scheduled, :claimed]
    end)
    |> sort_attempts()
  end

  defp next_visible_at([%ActionAttempt{visible_at: %DateTime{} = visible_at} | _attempts]) do
    visible_at
  end

  defp next_visible_at([]), do: nil

  defp normalize_runnables(runnables, %DateTime{} = now) do
    runnables
    |> Enum.map(&normalize_runnable(&1, now))
    |> Enum.sort_by(&runnable_key/1)
  end

  defp normalize_runnable(runnable, %DateTime{} = now) when is_map(runnable) do
    normalized_runnable =
      case map_value(runnable, :recovery) do
        recovery when is_map(recovery) ->
          Map.put(runnable, :recovery, normalize_recovery(recovery))

        _missing ->
          runnable
      end

    case Deadline.evaluate(
           map_value(normalized_runnable, :deadline),
           now,
           step: map_value(normalized_runnable, :step),
           runnable_key: runnable_key(normalized_runnable)
         ) do
      nil -> normalized_runnable
      deadline -> Map.put(normalized_runnable, :deadline, deadline)
    end
  end

  defp guardrail_decisions(planned_runnables, attempts) do
    planned =
      Enum.flat_map(planned_runnables, fn runnable ->
        runnable_guardrails = map_value(runnable, :guardrails, [])

        Enum.map(runnable_guardrails, &Map.put(&1, :step, map_value(runnable, :step)))
      end)

    completed =
      Enum.flat_map(attempts, fn attempt ->
        attempt
        |> map_value(:guardrails, [])
        |> Enum.map(&Map.put_new(&1, :step, map_value(attempt, :step)))
      end)

    failures =
      Enum.flat_map(attempts, fn attempt ->
        attempt_error = map_value(attempt, :error, %{})

        case map_value(attempt_error, :guardrail) do
          guardrail when is_map(guardrail) ->
            [
              guardrail
              |> Map.put_new(:step, map_value(attempt, :step))
              |> Map.put_new(:status, :failed)
            ]

          _missing ->
            []
        end
      end)

    durable = completed ++ failures
    durable_keys = MapSet.new(durable, &guardrail_decision_key/1)

    pending =
      Enum.reject(planned, fn guardrail ->
        MapSet.member?(durable_keys, guardrail_decision_key(guardrail))
      end)

    Enum.uniq(pending ++ durable)
  end

  defp guardrail_decision_key(guardrail) when is_map(guardrail) do
    {
      map_value(guardrail, :step),
      map_value(guardrail, :key),
      map_value(guardrail, :placement),
      map_value(guardrail, :policy)
    }
  end

  defp normalize_manual_state(nil, %DateTime{}), do: nil

  defp normalize_manual_state(manual_state, %DateTime{} = now) when is_map(manual_state) do
    manual_state = Map.new(manual_state)

    case Deadline.evaluate(
           map_value(manual_state, :deadline),
           now,
           step: map_value(manual_state, :step)
         ) do
      nil -> manual_state
      deadline -> Map.put(manual_state, :deadline, deadline)
    end
  end

  defp deadline_by_runnable_key(runnables, %DateTime{} = now) when is_list(runnables) do
    runnables
    |> Enum.flat_map(fn runnable ->
      deadline =
        Deadline.evaluate(map_value(runnable, :deadline), now,
          step: map_value(runnable, :step),
          runnable_key: runnable_key(runnable)
        )

      case deadline do
        nil -> []
        deadline -> [{runnable_key(runnable), deadline}]
      end
    end)
    |> Map.new()
  end

  defp active_deadline(sources) do
    sources
    |> List.flatten()
    |> Enum.map(&Map.get(&1 || %{}, :deadline))
    |> Deadline.most_urgent()
  end

  defp runnable_key(runnable) when is_map(runnable) do
    map_value(runnable, :runnable_key) || map_value(runnable, :key) || ""
  end

  defp recovery_by_runnable_key(runnables) when is_list(runnables) do
    Map.new(runnables, fn runnable ->
      {runnable_key(runnable), normalize_recovery(Map.get(runnable, :recovery))}
    end)
  end

  defp deferred_by_runnable_key(runnables) when is_list(runnables) do
    runnables
    |> Enum.flat_map(fn runnable ->
      case map_value(runnable, :deferred) do
        deferred when is_map(deferred) -> [{runnable_key(runnable), normalize_deferred(deferred)}]
        _missing -> []
      end
    end)
    |> Map.new()
  end

  defp attempt_snapshot(
         %ActionAttempt{} = attempt,
         recovery_by_runnable_key,
         deferred_by_runnable_key,
         deadline_by_runnable_key,
         %DateTime{} = now
       ) do
    snapshot = %{
      runnable_key: attempt.runnable_key,
      status: attempt.status,
      attempt_number: attempt.attempt_number,
      step: normalize_step(attempt.step),
      input: attempt.input,
      scheduled_at: attempt.scheduled_at,
      visible_at: attempt.visible_at,
      idempotency_key: attempt.idempotency_key,
      claim_id: attempt.claim_id,
      owner_id: attempt.owner_id,
      lease_until: attempt.lease_until,
      claimed_at: attempt.claimed_at,
      result: attempt.result,
      completed_at: attempt.completed_at,
      transition: attempt.transition,
      error: attempt.error,
      guardrails: guardrails(attempt.guardrails),
      recovery: Map.get(recovery_by_runnable_key, attempt.runnable_key),
      deferred: Map.get(deferred_by_runnable_key, attempt.runnable_key),
      deadline:
        Map.get(deadline_by_runnable_key, attempt.runnable_key) ||
          Deadline.evaluate(attempt.deadline, now,
            step: normalize_step(attempt.step),
            runnable_key: attempt.runnable_key
          ),
      wakeup_emitted?: attempt.wakeup_emitted?,
      applied?: attempt.applied?
    }

    compact(snapshot)
  end

  defp normalize_recovery(recovery) when is_map(recovery) do
    Definition.normalize_recovery_policy(recovery)
  end

  defp normalize_recovery(_recovery), do: nil

  defp normalize_deferred(deferred) when is_map(deferred) do
    deferred
    |> Map.new(fn {key, value} -> {normalize_deferred_key(key), value} end)
    |> update_in([:reason], &normalize_deferred_reason/1)
  end

  defp normalize_deferred_key(key) when is_binary(key) do
    case key do
      "reason" -> :reason
      "from_runnable_key" -> :from_runnable_key
      "deferred_at" -> :deferred_at
      other -> other
    end
  end

  defp normalize_deferred_key(key), do: key

  defp normalize_deferred_reason(reason) when is_map(reason) do
    reason
  end

  defp normalize_deferred_reason(reason), do: reason

  defp guardrails(guardrails) when is_list(guardrails) and guardrails != [], do: guardrails
  defp guardrails(_guardrails), do: nil

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

  defp map_value(map, key, default \\ nil), do: Squidie.MapField.get(map, key, default)

  defp normalize_step(step) when is_atom(step), do: Atom.to_string(step)
  defp normalize_step(step), do: step

  defp after?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :gt
  end
end
