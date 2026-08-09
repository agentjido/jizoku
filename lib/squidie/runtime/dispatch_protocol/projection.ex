# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.DispatchProtocol.Projection do
  @moduledoc """
  Rebuildable projection over durable dispatch journal entries.

  The projection is deliberately pure. Storage adapters can rebuild it from
  Jido thread journals, backend lease lifecycle signals, or from a single
  append-only Squidie journal table without changing the runtime invariants.
  """

  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Trace

  @continuation_blocked_entry_types [
    :attempt_scheduled,
    :attempt_claimed,
    :attempt_heartbeat,
    :attempt_completed,
    :attempt_failed,
    :live_wakeup_emitted
  ]

  @continuation_identity_fields [
    :run_id,
    :successor_run_id,
    :continuation_key,
    :workflow,
    :trigger,
    :input,
    :definition,
    :definition_version,
    :definition_fingerprint,
    :queue
  ]

  @continuation_integrity_anomaly_reasons [
    :conflicting_runnable_intent,
    :malformed_entry
  ]

  @continuation_unknown_intent_entry_types [
    :attempt_claimed,
    :attempt_completed,
    :attempt_failed,
    :runnable_applied
  ]

  @type anomaly :: %{
          required(:reason) => atom(),
          required(:entry_type) => atom(),
          optional(:runnable_key) => String.t(),
          optional(:run_id) => String.t(),
          optional(:idempotency_key) => String.t(),
          optional(:claim_id) => String.t(),
          optional(:claim_token_hash) => String.t()
        }

  @type string_set :: MapSet.t(String.t()) | %MapSet{}

  @type t :: %__MODULE__{
          attempts: %{optional(String.t()) => ActionAttempt.t()},
          anomalies: [anomaly()],
          continuation_fences: %{optional(String.t()) => map()},
          queued_run_ids: string_set(),
          terminal_runs: string_set()
        }

  defstruct attempts: %{},
            anomalies: [],
            continuation_fences: %{},
            queued_run_ids: MapSet.new(),
            terminal_runs: MapSet.new()

  @doc false
  @spec new() :: t()
  def new do
    %__MODULE__{
      continuation_fences: %{},
      queued_run_ids: MapSet.new(),
      terminal_runs: MapSet.new()
    }
  end

  @doc false
  @spec rebuild([Entry.t()]) :: t()
  def rebuild(entries) when is_list(entries) do
    replay(new(), entries)
  end

  @doc false
  @spec replay(t(), [Entry.t()]) :: t()
  def replay(%__MODULE__{} = projection, entries) when is_list(entries) do
    Enum.reduce(entries, normalize(projection), &apply_entry/2)
  end

  @doc false
  @spec normalize(t()) :: t()
  def normalize(%__MODULE__{} = projection) do
    %__MODULE__{
      attempts: normalize_attempts(Map.get(projection, :attempts, %{})),
      anomalies: Map.get(projection, :anomalies, []),
      continuation_fences: Map.get(projection, :continuation_fences, %{}),
      queued_run_ids: Map.get(projection, :queued_run_ids, MapSet.new()),
      terminal_runs: Map.get(projection, :terminal_runs, MapSet.new())
    }
  end

  @doc false
  @spec checkpoint_compatible?(term()) :: boolean()
  def checkpoint_compatible?(%__MODULE__{} = projection) do
    Map.has_key?(projection, :continuation_fences)
  end

  def checkpoint_compatible?(_projection) do
    false
  end

  @doc false
  @spec visible_attempts(t(), DateTime.t()) :: [ActionAttempt.t()]
  def visible_attempts(%__MODULE__{} = projection, %DateTime{} = at) do
    projection
    |> ordered_attempts()
    |> Enum.filter(fn attempt ->
      attempt.status in [:available, :retry_scheduled] and not after?(attempt.visible_at, at) and
        not terminal_run?(projection, attempt.run_id) and
        not continuation_fenced?(projection, attempt.run_id)
    end)
  end

  @doc false
  @spec expired_claims(t(), DateTime.t()) :: [ActionAttempt.t()]
  def expired_claims(%__MODULE__{} = projection, %DateTime{} = at) do
    projection
    |> ordered_attempts()
    |> Enum.filter(fn attempt ->
      attempt.status == :claimed and not is_nil(attempt.lease_until) and
        not after?(attempt.lease_until, at) and not terminal_run?(projection, attempt.run_id) and
        not continuation_fenced?(projection, attempt.run_id)
    end)
  end

  @doc false
  @spec completed_results(t()) :: [ActionAttempt.t()]
  def completed_results(%__MODULE__{} = projection) do
    projection
    |> ordered_attempts()
    |> Enum.filter(&(&1.status == :completed))
  end

  @doc false
  @spec attempt_runnable_keys(t()) :: MapSet.t(String.t())
  def attempt_runnable_keys(%__MODULE__{attempts: attempts}) do
    attempts
    |> Map.keys()
    |> MapSet.new()
  end

  @doc false
  @spec run_ids(t()) :: MapSet.t(String.t())
  def run_ids(%__MODULE__{
        attempts: attempts,
        continuation_fences: continuation_fences,
        queued_run_ids: queued_run_ids
      }) do
    attempt_run_ids =
      attempts
      |> Enum.map(fn {_key, attempt} -> attempt.run_id end)
      |> MapSet.new()

    fence_run_ids = MapSet.new(Map.keys(continuation_fences))

    attempt_run_ids
    |> MapSet.union(fence_run_ids)
    |> MapSet.union(queued_run_ids)
  end

  @doc false
  @spec results_ready_to_apply(t()) :: [ActionAttempt.t()]
  def results_ready_to_apply(%__MODULE__{} = projection) do
    projection
    |> completed_results()
    |> Enum.reject(
      &(&1.applied? or terminal_run?(projection, &1.run_id) or
          continuation_fenced?(projection, &1.run_id))
    )
  end

  @doc false
  @spec continuation_fence(t(), String.t()) :: map() | nil
  def continuation_fence(%__MODULE__{continuation_fences: fences}, run_id)
      when is_binary(run_id) do
    Map.get(fences, run_id)
  end

  @doc false
  @spec valid_continuation_fence?(term()) :: boolean()
  def valid_continuation_fence?(data) do
    continuation_fence_data?(data)
  end

  @doc false
  @spec same_continuation_fence?(map(), map()) :: boolean()
  def same_continuation_fence?(left, right) when is_map(left) and is_map(right) do
    Map.take(left, @continuation_identity_fields) ==
      Map.take(right, @continuation_identity_fields)
  end

  @doc false
  @spec continuation_blockers(t(), String.t()) :: [map()]
  def continuation_blockers(%__MODULE__{} = projection, run_id) when is_binary(run_id) do
    terminal_blockers(projection, run_id) ++
      anomaly_blockers(projection, run_id) ++ attempt_blockers(projection, run_id)
  end

  @doc false
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: Enum.reverse(anomalies)

  defp apply_entry(
         %Entry{type: type, data: %{run_id: run_id}} = entry,
         %__MODULE__{} = projection
       )
       when type in @continuation_blocked_entry_types do
    if continuation_fenced?(projection, run_id) do
      add_anomaly(projection, entry, :continuation_fenced)
    else
      apply_dispatch_mutation(entry, projection)
    end
  end

  defp apply_entry(
         %Entry{type: :run_continuation_fenced} = entry,
         %__MODULE__{} = projection
       ) do
    if continuation_fence_data?(entry.data) do
      put_continuation_fence(projection, entry)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(%Entry{type: :run_queued, data: data}, %__MODULE__{} = projection) do
    %__MODULE__{projection | queued_run_ids: MapSet.put(projection.queued_run_ids, data.run_id)}
  end

  defp apply_entry(%Entry{type: :runnable_applied} = entry, projection) do
    case Map.fetch(projection.attempts, entry.data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        apply_completed_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_entry(%Entry{type: :run_terminal, data: data}, %__MODULE__{} = projection) do
    %__MODULE__{projection | terminal_runs: MapSet.put(projection.terminal_runs, data.run_id)}
  end

  defp apply_entry(%Entry{}, projection), do: projection

  defp apply_dispatch_mutation(
         %Entry{type: :attempt_scheduled, data: data} = entry,
         projection
       ) do
    if terminal_run?(projection, data.run_id) do
      add_anomaly(projection, entry, :terminal_run)
    else
      put_new_attempt(projection, build_attempt(data), data)
    end
  end

  defp apply_dispatch_mutation(
         %Entry{type: :attempt_claimed, data: data} = entry,
         projection
       ) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        claim_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_dispatch_mutation(
         %Entry{type: :attempt_heartbeat, data: data} = entry,
         projection
       ) do
    update_matching_claim(projection, entry, fn %ActionAttempt{} = attempt ->
      %ActionAttempt{attempt | lease_until: data.lease_until}
    end)
  end

  defp apply_dispatch_mutation(
         %Entry{type: :attempt_completed, data: data} = entry,
         projection
       ) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        complete_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_dispatch_mutation(
         %Entry{type: :attempt_failed, data: data} = entry,
         projection
       ) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        fail_matching_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_dispatch_mutation(
         %Entry{type: :live_wakeup_emitted, data: data} = entry,
         projection
       ) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        if terminal_attempt?(projection, attempt) do
          add_anomaly(projection, entry, :terminal_run)
        else
          put_attempt(projection, %ActionAttempt{attempt | wakeup_emitted?: true})
        end

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp put_continuation_fence(
         %__MODULE__{} = projection,
         %Entry{data: %{run_id: run_id} = data} = entry
       ) do
    case Map.fetch(projection.continuation_fences, run_id) do
      {:ok, existing} ->
        if same_continuation_fence?(existing, data) do
          projection
        else
          add_anomaly(projection, entry, :conflicting_continuation_fence)
        end

      :error ->
        %__MODULE__{
          projection
          | continuation_fences: Map.put(projection.continuation_fences, run_id, data)
        }
    end
  end

  defp continuation_fence_data?(data) when is_map(data) do
    required_present?(data, [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint,
      :queue,
      :trace,
      :occurred_at
    ]) and
      Map.has_key?(data, :definition_version) and
      valid_continuation_fence_identifiers?(data) and
      is_map(Map.fetch!(data, :input)) and
      Map.fetch!(data, :definition) == :current and
      optional_non_empty_binary?(Map.fetch!(data, :definition_version)) and
      non_empty_binary?(Map.fetch!(data, :definition_fingerprint)) and
      Trace.valid?(Map.fetch!(data, :trace)) and
      match?(%DateTime{}, Map.fetch!(data, :occurred_at))
  end

  defp continuation_fence_data?(_data) do
    false
  end

  defp valid_continuation_fence_identifiers?(data) do
    Enum.all?(
      [:run_id, :successor_run_id, :continuation_key, :workflow, :trigger, :queue],
      &non_empty_binary?(Map.fetch!(data, &1))
    )
  end

  defp terminal_blockers(%__MODULE__{} = projection, run_id) do
    if terminal_run?(projection, run_id) do
      [%{reason: :terminal_run}]
    else
      []
    end
  end

  defp anomaly_blockers(%__MODULE__{} = projection, run_id) do
    projection.anomalies
    |> Enum.filter(
      &(continuation_integrity_anomaly?(&1) and anomaly_for_run?(&1, projection, run_id))
    )
    |> Enum.map(&%{reason: :dispatch_anomaly, anomaly: &1})
  end

  defp anomaly_for_run?(%{run_id: run_id}, _projection, run_id) do
    true
  end

  defp anomaly_for_run?(%{runnable_key: runnable_key}, %__MODULE__{} = projection, run_id) do
    match?(%ActionAttempt{run_id: ^run_id}, Map.get(projection.attempts, runnable_key))
  end

  defp anomaly_for_run?(_anomaly, _projection, _run_id) do
    false
  end

  defp continuation_integrity_anomaly?(%{reason: reason})
       when reason in @continuation_integrity_anomaly_reasons do
    true
  end

  defp continuation_integrity_anomaly?(%{
         reason: :unknown_runnable_intent,
         entry_type: entry_type
       })
       when entry_type in @continuation_unknown_intent_entry_types do
    true
  end

  defp continuation_integrity_anomaly?(_anomaly) do
    false
  end

  defp attempt_blockers(%__MODULE__{} = projection, run_id) do
    projection.attempts
    |> Map.values()
    |> Enum.filter(&(&1.run_id == run_id))
    |> Enum.flat_map(&attempt_blocker/1)
    |> Enum.sort_by(&Map.get(&1, :runnable_key, ""))
  end

  defp attempt_blocker(%ActionAttempt{status: status, applied?: true})
       when status in [:completed, :failed] do
    []
  end

  defp attempt_blocker(%ActionAttempt{status: :available, runnable_key: runnable_key}) do
    [%{reason: :available_attempt, runnable_key: runnable_key}]
  end

  defp attempt_blocker(%ActionAttempt{status: :retry_scheduled, runnable_key: runnable_key}) do
    [%{reason: :retry_attempt, runnable_key: runnable_key}]
  end

  defp attempt_blocker(%ActionAttempt{status: :claimed, runnable_key: runnable_key}) do
    [%{reason: :claimed_attempt, runnable_key: runnable_key}]
  end

  defp attempt_blocker(%ActionAttempt{
         status: status,
         applied?: false,
         runnable_key: runnable_key
       })
       when status in [:completed, :failed] do
    [%{reason: :pending_result, runnable_key: runnable_key}]
  end

  defp attempt_blocker(%ActionAttempt{status: status, runnable_key: runnable_key}) do
    [%{reason: :unknown_attempt_status, runnable_key: runnable_key, status: status}]
  end

  defp required_present?(data, fields) do
    Enum.all?(fields, &(Map.has_key?(data, &1) and not is_nil(Map.fetch!(data, &1))))
  end

  defp optional_non_empty_binary?(nil) do
    true
  end

  defp optional_non_empty_binary?(value) do
    non_empty_binary?(value)
  end

  defp non_empty_binary?(value) when is_binary(value) do
    value != ""
  end

  defp non_empty_binary?(_value) do
    false
  end

  defp put_new_attempt(%__MODULE__{} = projection, %ActionAttempt{} = attempt, data \\ nil) do
    case Map.fetch(projection.attempts, attempt.runnable_key) do
      {:ok, %ActionAttempt{} = existing_attempt} ->
        if same_intent?(existing_attempt, attempt) do
          projection
        else
          add_conflicting_intent_anomaly(projection, attempt, data)
        end

      :error ->
        put_attempt(projection, attempt)
    end
  end

  defp claim_attempt(projection, entry, %ActionAttempt{} = attempt) do
    cond do
      terminal_attempt?(projection, attempt) ->
        add_anomaly(projection, entry, :terminal_run)

      attempt.status in [:completed, :failed] ->
        add_anomaly(projection, entry, :terminal_attempt)

      attempt.status == :claimed and expired_claim?(attempt, entry.data.occurred_at) ->
        if claim_visible?(attempt, entry.data.occurred_at) do
          put_claimed_attempt(projection, attempt, entry.data)
        else
          add_anomaly(projection, entry, :attempt_not_visible)
        end

      attempt.status == :claimed ->
        add_anomaly(projection, entry, :active_claim)

      claim_visible?(attempt, entry.data.occurred_at) ->
        put_claimed_attempt(projection, attempt, entry.data)

      true ->
        add_anomaly(projection, entry, :attempt_not_visible)
    end
  end

  defp update_matching_claim(projection, entry, fun) when is_function(fun, 1) do
    case Map.fetch(projection.attempts, entry.data.runnable_key) do
      {:ok, %ActionAttempt{status: :claimed} = attempt} ->
        if terminal_attempt?(projection, attempt) do
          add_anomaly(projection, entry, :terminal_run)
        else
          update_current_claim(projection, entry, attempt, fun)
        end

      {:ok, %ActionAttempt{}} ->
        add_anomaly(projection, entry, :stale_claim)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp update_current_claim(projection, entry, attempt, fun) do
    case claim_fence(attempt, entry.data) do
      :current -> put_attempt(projection, fun.(attempt))
      {:error, reason} -> add_anomaly(projection, entry, reason)
    end
  end

  defp complete_attempt(projection, entry, %ActionAttempt{} = attempt) do
    cond do
      terminal_attempt?(projection, attempt) ->
        add_anomaly(projection, entry, :terminal_run)

      attempt.status == :completed and attempt.result == entry.data.result ->
        if matching_claim?(attempt, entry.data) do
          projection
        else
          add_anomaly(projection, entry, :stale_claim)
        end

      attempt.status == :completed ->
        if matching_claim?(attempt, entry.data) do
          add_anomaly(projection, entry, :conflicting_completion)
        else
          add_anomaly(projection, entry, :stale_claim)
        end

      true ->
        complete_matching_claim(projection, entry, attempt)
    end
  end

  defp complete_matching_claim(projection, entry, %ActionAttempt{} = attempt) do
    update_matching_claim(projection, entry, fn %ActionAttempt{} ->
      %ActionAttempt{
        attempt
        | status: :completed,
          result: entry.data.result,
          guardrails: guardrails(entry.data),
          execution_opts: Map.get(entry.data, :execution_opts, []),
          completed_at: entry.occurred_at,
          error: nil
      }
    end)
  end

  defp fail_matching_attempt(projection, entry, %ActionAttempt{} = attempt) do
    cond do
      terminal_attempt?(projection, attempt) ->
        add_anomaly(projection, entry, :terminal_run)

      attempt.status != :claimed ->
        add_anomaly(projection, entry, :stale_claim)

      true ->
        case claim_fence(attempt, entry.data) do
          :current ->
            projection
            |> fail_attempt(entry, attempt)
            |> maybe_schedule_retry(attempt, entry.data)

          {:error, reason} ->
            add_anomaly(projection, entry, reason)
        end
    end
  end

  defp apply_completed_attempt(projection, entry, %ActionAttempt{} = attempt) do
    cond do
      terminal_attempt?(projection, attempt) ->
        add_anomaly(projection, entry, :terminal_run)

      attempt.status == :completed ->
        put_attempt(projection, %ActionAttempt{
          attempt
          | applied?: true,
            guardrails: guardrails(entry.data, attempt.guardrails),
            transition: Map.get(entry.data, :transition)
        })

      attempt.status == :failed and is_map(Map.get(entry.data, :transition)) ->
        put_attempt(projection, %ActionAttempt{
          attempt
          | applied?: true,
            guardrails: guardrails(entry.data, attempt.guardrails),
            transition: Map.get(entry.data, :transition)
        })

      true ->
        add_anomaly(projection, entry, :result_not_completed)
    end
  end

  defp fail_attempt(projection, entry, %ActionAttempt{} = attempt) do
    put_attempt(projection, %ActionAttempt{
      attempt
      | status: :failed,
        completed_at: entry.occurred_at,
        error: entry.data.error
    })
  end

  defp maybe_schedule_retry(projection, %ActionAttempt{} = attempt, data) do
    case {Map.get(data, :retry_runnable_key), Map.get(data, :retry_visible_at)} do
      {retry_key, %DateTime{} = retry_visible_at} when is_binary(retry_key) ->
        retry_attempt = %ActionAttempt{
          attempt
          | runnable_key: retry_key,
            attempt_number: attempt.attempt_number + 1,
            status: :retry_scheduled,
            scheduled_at: data.occurred_at,
            visible_at: retry_visible_at,
            trace: Map.get(data, :retry_trace),
            deadline: Map.get(data, :retry_deadline),
            claim_id: nil,
            claim_token_hash: nil,
            owner_id: nil,
            lease_until: nil,
            claimed_at: nil,
            result: nil,
            guardrails: [],
            completed_at: nil,
            transition: nil,
            error: nil,
            wakeup_emitted?: false,
            applied?: false
        }

        put_new_attempt(projection, retry_attempt)

      _no_retry ->
        projection
    end
  end

  defp build_attempt(data) do
    %ActionAttempt{
      run_id: data.run_id,
      workflow: Map.get(data, :workflow),
      runnable_key: data.runnable_key,
      idempotency_key: data.idempotency_key,
      attempt_number: data.attempt_number,
      step: data.step,
      input: data.input,
      trace: Map.get(data, :trace),
      scheduled_at: data.occurred_at,
      visible_at: data.visible_at,
      deadline: Map.get(data, :deadline),
      status: :available
    }
  end

  defp put_attempt(%__MODULE__{} = projection, %ActionAttempt{} = attempt) do
    %__MODULE__{
      projection
      | attempts: Map.put(projection.attempts, attempt.runnable_key, attempt)
    }
  end

  defp normalize_attempts(attempts) when is_map(attempts) do
    Map.new(attempts, fn {runnable_key, %ActionAttempt{} = attempt} ->
      {runnable_key, ActionAttempt.upgrade(attempt)}
    end)
  end

  defp put_claimed_attempt(projection, %ActionAttempt{} = attempt, data) do
    put_attempt(projection, %ActionAttempt{
      attempt
      | status: :claimed,
        claim_id: data.claim_id,
        claim_token_hash: data.claim_token_hash,
        owner_id: data.owner_id,
        lease_until: data.lease_until,
        claimed_at: data.occurred_at
    })
  end

  defp guardrails(data, default \\ []) when is_map(data) do
    case Map.get(data, :guardrails, default) do
      guardrails when is_list(guardrails) -> guardrails
      _invalid -> default
    end
  end

  defp matching_claim?(%ActionAttempt{} = attempt, data) do
    attempt.claim_id == data.claim_id and attempt.claim_token_hash == data.claim_token_hash
  end

  defp claim_fence(%ActionAttempt{} = attempt, data) do
    cond do
      not matching_claim?(attempt, data) ->
        {:error, :stale_claim}

      expired_claim?(attempt, data.occurred_at) ->
        {:error, :expired_claim}

      true ->
        :current
    end
  end

  defp same_intent?(%ActionAttempt{} = left, %ActionAttempt{} = right) do
    left.run_id == right.run_id and left.idempotency_key == right.idempotency_key and
      left.attempt_number == right.attempt_number and left.step == right.step and
      left.input == right.input and left.visible_at == right.visible_at and
      left.deadline == right.deadline
  end

  defp expired_claim?(%ActionAttempt{lease_until: %DateTime{} = lease_until}, %DateTime{} = at) do
    not after?(lease_until, at)
  end

  defp expired_claim?(%ActionAttempt{}, _at), do: false

  defp claim_visible?(
         %ActionAttempt{visible_at: %DateTime{} = visible_at},
         %DateTime{} = occurred_at
       ) do
    not after?(visible_at, occurred_at)
  end

  defp terminal_run?(%__MODULE__{terminal_runs: terminal_runs}, run_id) do
    MapSet.member?(terminal_runs, run_id)
  end

  defp continuation_fenced?(%__MODULE__{continuation_fences: fences}, run_id) do
    Map.has_key?(fences, run_id)
  end

  defp terminal_attempt?(projection, %ActionAttempt{run_id: run_id}) do
    terminal_run?(projection, run_id)
  end

  defp add_anomaly(%__MODULE__{} = projection, %Entry{} = entry, reason) do
    anomaly =
      %{
        reason: reason,
        entry_type: entry.type
      }
      |> maybe_put_run_id(Map.get(entry.data, :run_id))
      |> maybe_put_runnable_key(Map.get(entry.data, :runnable_key))
      |> maybe_put_claim_id(Map.get(entry.data, :claim_id))
      |> maybe_put_claim_token_hash(Map.get(entry.data, :claim_token_hash))

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp add_conflicting_intent_anomaly(
         %__MODULE__{} = projection,
         %ActionAttempt{} = attempt,
         data
       ) do
    anomaly =
      maybe_put_idempotency_key(
        %{
          reason: :conflicting_runnable_intent,
          run_id: attempt.run_id,
          runnable_key: attempt.runnable_key,
          entry_type: :attempt_scheduled
        },
        Map.get(data || %{}, :idempotency_key)
      )

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp maybe_put_claim_id(anomaly, nil), do: anomaly
  defp maybe_put_claim_id(anomaly, claim_id), do: Map.put(anomaly, :claim_id, claim_id)

  defp maybe_put_run_id(anomaly, nil), do: anomaly
  defp maybe_put_run_id(anomaly, run_id), do: Map.put(anomaly, :run_id, run_id)

  defp maybe_put_runnable_key(anomaly, nil), do: anomaly

  defp maybe_put_runnable_key(anomaly, runnable_key) do
    Map.put(anomaly, :runnable_key, runnable_key)
  end

  defp maybe_put_claim_token_hash(anomaly, nil), do: anomaly

  defp maybe_put_claim_token_hash(anomaly, claim_token_hash) do
    Map.put(anomaly, :claim_token_hash, claim_token_hash)
  end

  defp maybe_put_idempotency_key(anomaly, nil), do: anomaly

  defp maybe_put_idempotency_key(anomaly, idempotency_key) do
    Map.put(anomaly, :idempotency_key, idempotency_key)
  end

  defp ordered_attempts(%__MODULE__{attempts: attempts}) do
    attempts
    |> Map.values()
    |> Enum.sort_by(&{&1.run_id, &1.attempt_number, &1.runnable_key})
  end

  defp after?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :gt
  end
end
