defmodule Squidie.ReadModel.Explanation do
  @moduledoc """
  Projection-backed explanation for the journal-backed runtime.

  `Squidie.ReadModel.Inspection` answers what durable journal
  projections currently show. This module answers why that state matters to an
  operator by deriving a deterministic reason, high-signal details, and the
  runtime boundary that would make progress.

  Explanations are read-only. They do not schedule missing dispatches, apply
  completed results, recover expired claims, or mutate checkpoints.
  """

  alias Squidie.ReadModel.Explanation.Diagnostic
  alias Squidie.ReadModel.Inspection
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.Workflow.Definition

  @type storage_config :: Inspection.storage_config()
  @type explanation_option :: Inspection.snapshot_option()
  @type explanation_error ::
          Inspection.snapshot_error() | {:invalid_option, {:run_id, :invalid}}

  @doc """
  Builds a projection-backed explanation for one workflow run.

  Options are the same as `Squidie.ReadModel.Inspection.snapshot/3`.
  Missing runs and invalid options return the same structured errors as the
  underlying snapshot call.
  """
  @spec explain(storage_config(), String.t(), [explanation_option()]) ::
          {:ok, Diagnostic.t()} | {:error, explanation_error()}
  def explain(storage, run_id, opts \\ [])

  def explain(storage, run_id, opts) when is_binary(run_id) do
    with {:ok, snapshot} <- Inspection.snapshot(storage, run_id, opts) do
      {:ok, from_snapshot(snapshot)}
    end
  end

  def explain(_storage, _run_id, _opts) do
    {:error, {:invalid_option, {:run_id, :invalid}}}
  end

  @doc """
  Derives an explanation from an existing projection-backed snapshot.

  This is useful when a caller already has a snapshot and wants a stable
  diagnostic view without re-reading storage.
  """
  @spec from_snapshot(Snapshot.t()) :: Diagnostic.t()
  def from_snapshot(%Snapshot{} = snapshot) do
    {summary, details, next_actions, step} = explanation_parts(snapshot)
    command_details = command_details(snapshot.command_history)
    dynamic_work_details = dynamic_work_details(snapshot.dynamic_work)
    deadline_details = deadline_details(snapshot.deadline)

    %Diagnostic{
      run_id: snapshot.run_id,
      workflow: snapshot.workflow,
      definition_version: snapshot.definition_version,
      queue: snapshot.queue,
      status: snapshot.status,
      reason: snapshot.reason,
      step: step,
      summary: summary,
      details:
        details
        |> Map.merge(command_details)
        |> Map.merge(dynamic_work_details)
        |> Map.merge(deadline_details),
      next_actions: Enum.uniq(next_actions ++ deadline_next_actions(snapshot.deadline)),
      evidence: evidence(snapshot)
    }
  end

  defp explanation_parts(%Snapshot{reason: :planned_dispatch_pending_schedule} = snapshot) do
    runnables = snapshot.pending_dispatches

    {
      "Planned runnable has not been recorded in the dispatch journal.",
      %{
        pending_dispatch_count: length(runnables),
        runnable_keys: runnable_keys(runnables)
      },
      [:schedule_pending_dispatch],
      first_step(runnables)
    }
  end

  defp explanation_parts(%Snapshot{reason: :completed_result_pending_apply} = snapshot) do
    attempts = snapshot.pending_results

    {
      "Dispatch result is complete but has not been applied to the run journal.",
      %{
        pending_result_count: length(attempts),
        runnable_keys: runnable_keys(attempts)
      },
      [:apply_pending_result],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :expired_claim} = snapshot) do
    attempts = snapshot.expired_claims

    {
      "A claimed dispatch attempt has expired and is recoverable.",
      %{
        expired_claim_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        oldest_lease_until: oldest_lease_until(attempts)
      },
      [:recover_expired_claim],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_visible} = snapshot) do
    attempts = snapshot.visible_attempts

    {
      "A dispatch attempt is visible and waiting for a worker claim.",
      %{
        visible_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts)
      },
      [:wait_for_worker_claim],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_scheduled_for_later} = snapshot) do
    attempts = snapshot.scheduled_attempts

    {
      "A dispatch attempt is scheduled for future visibility.",
      %{
        scheduled_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        next_visible_at: snapshot.next_visible_at
      },
      [:wait_until_attempt_visible],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :deferred_continuation} = snapshot) do
    attempts = snapshot.scheduled_attempts

    {
      "A workflow step intentionally deferred its continuation until a future visibility time.",
      %{
        deferred_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        next_visible_at: snapshot.next_visible_at,
        deferred: deferred_metadata(attempts)
      },
      [:wait_until_attempt_visible],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_claimed} = snapshot) do
    attempts = claimed_attempts(snapshot.attempts)

    {
      "A dispatch attempt is claimed and waiting for completion.",
      %{
        claimed_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        earliest_lease_until: oldest_lease_until(attempts)
      },
      [:wait_for_attempt_completion],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :manual_intervention_required} = snapshot) do
    manual_state = snapshot.manual_state || %{}

    {
      "The run is paused for manual intervention.",
      manual_state,
      [:resolve_manual_step],
      item_value(manual_state, :step)
    }
  end

  defp explanation_parts(%Snapshot{reason: :terminal} = snapshot) do
    details =
      maybe_put(
        %{
          terminal?: snapshot.terminal?,
          terminal_status: snapshot.terminal_status
        },
        :terminal_error,
        snapshot.terminal_error
      )

    {
      "The run is terminal according to the run journal.",
      details,
      [:inspect_terminal_run],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :idle} = snapshot) do
    {
      "All currently planned runnables have been applied.",
      %{
        planned_count: length(snapshot.planned_runnable_keys),
        applied_count: length(snapshot.applied_runnable_keys)
      },
      [:wait_for_new_runnables],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :run_started} = snapshot) do
    {
      "The run has started but no dispatch attempts have been recorded yet.",
      %{planned_count: length(snapshot.planned_runnable_keys)},
      [:inspect_dispatch_state],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :waiting_for_dispatch} = snapshot) do
    {
      "The run has dispatch state but no visible recovery action is currently implied.",
      %{attempt_count: length(snapshot.attempts)},
      [:inspect_dispatch_state],
      first_step(snapshot.attempts)
    }
  end

  defp evidence(%Snapshot{} = snapshot) do
    %{
      snapshot_reason: snapshot.reason,
      definition_version: snapshot.definition_version,
      thread_revisions: snapshot.thread_revisions,
      terminal_status: snapshot.terminal_status,
      terminal_error: snapshot.terminal_error,
      manual_state: snapshot.manual_state,
      parent_run: snapshot.parent_run,
      child_runs: snapshot.child_runs,
      dynamic_work: snapshot.dynamic_work,
      deadline: snapshot.deadline,
      command_history: snapshot.command_history,
      command_counts: command_counts(snapshot.command_history),
      duplicate_commands: duplicate_commands(snapshot.command_history),
      planned_runnable_keys: snapshot.planned_runnable_keys,
      applied_runnable_keys: snapshot.applied_runnable_keys,
      recovery_policies: recovery_policies(snapshot),
      next_visible_at: snapshot.next_visible_at,
      attempt_counts: attempt_counts(snapshot.attempts),
      anomaly_count: length(snapshot.anomalies),
      anomalies: snapshot.anomalies
    }
  end

  defp command_details([]), do: %{}

  defp command_details(commands) when is_list(commands) do
    details = %{
      command_count: length(commands),
      latest_command: List.last(commands)
    }

    maybe_put_non_empty(details, :duplicate_commands, duplicate_commands(commands))
  end

  defp command_details(_commands), do: %{}

  defp dynamic_work_details([]), do: %{}

  defp dynamic_work_details(dynamic_work) when is_list(dynamic_work) do
    %{
      dynamic_work_count: length(dynamic_work),
      dynamic_work_keys: Enum.map(dynamic_work, &dynamic_work_key/1)
    }
  end

  defp dynamic_work_details(_dynamic_work), do: %{}

  defp deadline_details(nil), do: %{}

  defp deadline_details(deadline) when is_map(deadline) do
    %{
      deadline_status: item_value(deadline, :status),
      deadline_due_at: item_value(deadline, :due_at),
      deadline_escalation: item_value(deadline, :escalation)
    }
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
  end

  defp deadline_details(_deadline), do: %{}

  defp deadline_next_actions(%{status: status}) when status in [:overdue, :escalated] do
    [:apply_host_escalation_policy]
  end

  defp deadline_next_actions(%{"status" => status}) when status in [:overdue, :escalated] do
    [:apply_host_escalation_policy]
  end

  defp deadline_next_actions(%{status: :due_soon}), do: [:inspect_deadline_policy]
  defp deadline_next_actions(%{"status" => :due_soon}), do: [:inspect_deadline_policy]
  defp deadline_next_actions(_deadline), do: []

  defp dynamic_work_key(dynamic_work) when is_map(dynamic_work) do
    Map.get(dynamic_work, :dynamic_key) || Map.get(dynamic_work, "dynamic_key")
  end

  defp dynamic_work_key(_dynamic_work), do: nil

  defp command_counts(commands) when is_list(commands) do
    Enum.reduce(commands, %{}, fn command, counts ->
      case command_signal_type(command) do
        nil -> counts
        signal_type -> Map.update(counts, signal_type, 1, &(&1 + 1))
      end
    end)
  end

  defp command_counts(_commands), do: %{}

  defp duplicate_commands(commands) when is_list(commands) do
    commands
    |> Enum.group_by(&command_duplicate_key/1)
    |> Enum.flat_map(fn
      {nil, _commands} ->
        []

      {key, commands} ->
        count = length(commands)

        if count > 1 do
          [duplicate_command_summary(key, count)]
        else
          []
        end
    end)
    |> Enum.sort_by(&{Map.get(&1, :signal_type), Map.get(&1, :idempotency_key, "")})
  end

  defp duplicate_commands(_commands), do: []

  defp command_signal_type(command) when is_map(command) do
    case item_value(command, :signal_type) do
      signal_type when is_atom(signal_type) -> Atom.to_string(signal_type)
      signal_type when is_binary(signal_type) -> signal_type
      _other -> nil
    end
  end

  defp command_duplicate_key(command) when is_map(command) do
    with signal_type when is_binary(signal_type) <- command_signal_type(command) do
      case item_value(command, :idempotency_key) do
        idempotency_key when is_binary(idempotency_key) ->
          {signal_type, :idempotency_key, idempotency_key}

        _missing ->
          command_payload_key(signal_type, item_value(command, :payload))
      end
    end
  end

  defp command_payload_key(_signal_type, nil), do: nil
  defp command_payload_key(signal_type, payload), do: {signal_type, :payload, payload}

  defp duplicate_command_summary({signal_type, :idempotency_key, idempotency_key}, count) do
    %{
      signal_type: signal_type,
      idempotency_key: idempotency_key,
      count: count
    }
  end

  defp duplicate_command_summary({signal_type, :payload, _payload}, count) do
    %{
      signal_type: signal_type,
      count: count
    }
  end

  defp recovery_policies(%Snapshot{} = snapshot) do
    snapshot.attempts
    |> Kernel.++(snapshot.visible_attempts)
    |> Kernel.++(snapshot.scheduled_attempts)
    |> Kernel.++(snapshot.pending_dispatches)
    |> Kernel.++(snapshot.pending_results)
    |> Kernel.++(snapshot.expired_claims)
    |> Enum.reduce(%{}, fn attempt, policies ->
      case {item_value(attempt, :step), item_value(attempt, :recovery)} do
        {step, recovery} when is_binary(step) and is_map(recovery) ->
          Map.put_new(policies, step, Definition.normalize_recovery_policy(recovery))

        _missing ->
          policies
      end
    end)
  end

  defp attempt_counts(attempts) do
    Enum.reduce(attempts, %{}, fn attempt, counts ->
      case item_value(attempt, :status) do
        nil -> counts
        status -> Map.update(counts, status, 1, &(&1 + 1))
      end
    end)
  end

  defp claimed_attempts(attempts) do
    Enum.filter(attempts, &(item_value(&1, :status) == :claimed))
  end

  defp runnable_keys(items) do
    items
    |> Enum.map(&item_value(&1, :runnable_key))
    |> Enum.reject(&is_nil/1)
    |> Enum.sort()
  end

  defp deferred_metadata(attempts) do
    attempts
    |> Enum.map(&Map.get(&1, :deferred))
    |> Enum.reject(&is_nil/1)
  end

  defp first_step([first | _items]), do: item_value(first, :step)
  defp first_step([]), do: nil

  defp oldest_lease_until(attempts) do
    attempts
    |> Enum.map(&item_value(&1, :lease_until))
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(&DateTime.to_unix(&1, :microsecond))
    |> List.first()
  end

  defp item_value(item, key) when is_map(item) and is_atom(key) do
    Map.get(item, key) || Map.get(item, Atom.to_string(key))
  end

  defp maybe_put_non_empty(map, _key, []), do: map
  defp maybe_put_non_empty(map, key, value), do: Map.put(map, key, value)

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
