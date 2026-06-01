defmodule SquidMesh.Runtime.DispatchProtocol.Projection do
  @moduledoc """
  Rebuildable projection over durable dispatch journal entries.

  The projection is deliberately pure. Storage adapters can rebuild it from
  Jido thread journals, backend lease lifecycle signals, or from a single
  append-only Squid Mesh journal table without changing the runtime invariants.
  """

  alias SquidMesh.Runtime.DispatchProtocol.ActionAttempt
  alias SquidMesh.Runtime.DispatchProtocol.Entry

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
          queued_run_ids: string_set(),
          terminal_runs: string_set()
        }

  defstruct attempts: %{},
            anomalies: [],
            queued_run_ids: MapSet.new(),
            terminal_runs: MapSet.new()

  @doc false
  @spec new() :: t()
  def new do
    %__MODULE__{queued_run_ids: MapSet.new(), terminal_runs: MapSet.new()}
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
      attempts: Map.get(projection, :attempts, %{}),
      anomalies: Map.get(projection, :anomalies, []),
      queued_run_ids: Map.get(projection, :queued_run_ids, MapSet.new()),
      terminal_runs: Map.get(projection, :terminal_runs, MapSet.new())
    }
  end

  @doc false
  @spec visible_attempts(t(), DateTime.t()) :: [ActionAttempt.t()]
  def visible_attempts(%__MODULE__{} = projection, %DateTime{} = at) do
    projection
    |> ordered_attempts()
    |> Enum.filter(fn attempt ->
      attempt.status in [:available, :retry_scheduled] and not after?(attempt.visible_at, at) and
        not terminal_run?(projection, attempt.run_id)
    end)
  end

  @doc false
  @spec expired_claims(t(), DateTime.t()) :: [ActionAttempt.t()]
  def expired_claims(%__MODULE__{} = projection, %DateTime{} = at) do
    projection
    |> ordered_attempts()
    |> Enum.filter(fn attempt ->
      attempt.status == :claimed and not is_nil(attempt.lease_until) and
        not after?(attempt.lease_until, at) and not terminal_run?(projection, attempt.run_id)
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
  def run_ids(%__MODULE__{attempts: attempts, queued_run_ids: queued_run_ids}) do
    attempts
    |> Map.values()
    |> Enum.map(& &1.run_id)
    |> MapSet.new()
    |> MapSet.union(queued_run_ids)
  end

  @doc false
  @spec results_ready_to_apply(t()) :: [ActionAttempt.t()]
  def results_ready_to_apply(%__MODULE__{} = projection) do
    projection
    |> completed_results()
    |> Enum.reject(&(&1.applied? or terminal_run?(projection, &1.run_id)))
  end

  @doc false
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: Enum.reverse(anomalies)

  defp apply_entry(%Entry{type: :run_queued, data: data}, %__MODULE__{} = projection) do
    %__MODULE__{projection | queued_run_ids: MapSet.put(projection.queued_run_ids, data.run_id)}
  end

  defp apply_entry(%Entry{type: :attempt_scheduled, data: data} = entry, projection) do
    if terminal_run?(projection, data.run_id) do
      add_anomaly(projection, entry, :terminal_run)
    else
      put_new_attempt(projection, build_attempt(data), data)
    end
  end

  defp apply_entry(%Entry{type: :attempt_claimed, data: data} = entry, projection) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        claim_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_entry(%Entry{type: :attempt_heartbeat, data: data} = entry, projection) do
    update_matching_claim(projection, entry, fn %ActionAttempt{} = attempt ->
      %ActionAttempt{attempt | lease_until: data.lease_until}
    end)
  end

  defp apply_entry(%Entry{type: :attempt_completed, data: data} = entry, projection) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        complete_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_entry(%Entry{type: :attempt_failed, data: data} = entry, projection) do
    case Map.fetch(projection.attempts, data.runnable_key) do
      {:ok, %ActionAttempt{} = attempt} ->
        fail_matching_attempt(projection, entry, attempt)

      :error ->
        add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp apply_entry(%Entry{type: :live_wakeup_emitted, data: data} = entry, projection) do
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
            transition: Map.get(entry.data, :transition)
        })

      attempt.status == :failed and is_map(Map.get(entry.data, :transition)) ->
        put_attempt(projection, %ActionAttempt{
          attempt
          | applied?: true,
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
            visible_at: retry_visible_at,
            deadline: Map.get(data, :retry_deadline),
            claim_id: nil,
            claim_token_hash: nil,
            owner_id: nil,
            lease_until: nil,
            result: nil,
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
      runnable_key: data.runnable_key,
      idempotency_key: data.idempotency_key,
      attempt_number: data.attempt_number,
      step: data.step,
      input: data.input,
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

  defp put_claimed_attempt(projection, %ActionAttempt{} = attempt, data) do
    put_attempt(projection, %ActionAttempt{
      attempt
      | status: :claimed,
        claim_id: data.claim_id,
        claim_token_hash: data.claim_token_hash,
        owner_id: data.owner_id,
        lease_until: data.lease_until
    })
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
    anomaly = %{
      reason: :conflicting_runnable_intent,
      runnable_key: attempt.runnable_key,
      entry_type: :attempt_scheduled,
      idempotency_key: Map.get(data || %{}, :idempotency_key)
    }

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

  defp ordered_attempts(%__MODULE__{attempts: attempts}) do
    attempts
    |> Map.values()
    |> Enum.sort_by(&{&1.run_id, &1.attempt_number, &1.runnable_key})
  end

  defp after?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :gt
  end
end
