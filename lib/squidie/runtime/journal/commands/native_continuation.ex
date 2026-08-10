defmodule Squidie.Runtime.Journal.Commands.NativeContinuation do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.Runtime.ContinuationActivation
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal.Commands.Continuation
  alias Squidie.Runtime.Journal.Commands.ContinuationRecovery
  alias Squidie.Runtime.Journal.ContinuationIntent
  alias Squidie.Runtime.WorkflowAgent

  @dispatch_retries 25

  @doc false
  @spec complete_claim(Squidie.Runtime.Journal.storage_config(), map()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, {:native_continuation_rejected, term()} | term()}
  def complete_claim(
        storage,
        %{
          dispatch_agent: %Agent{agent_module: DispatchAgent} = dispatch_agent,
          workflow_agent: %Agent{agent_module: WorkflowAgent} = workflow_agent,
          attempt: %ActionAttempt{} = attempt,
          claim_id: claim_id,
          claim_token: claim_token,
          request:
            %{
              input: request_input,
              continuation_key: continuation_key,
              definition: :current
            } = request,
          queue: queue,
          now: %DateTime{} = now,
          guardrails: guardrails
        }
      )
      when is_binary(claim_id) and is_binary(claim_token) and is_map(request_input) and
             is_binary(continuation_key) and is_binary(queue) and is_list(guardrails) do
    context = %{
      dispatch_agent: dispatch_agent,
      workflow_agent: workflow_agent,
      attempt: attempt,
      claim_id: claim_id,
      claim_token: claim_token,
      request: request,
      guardrails: guardrails,
      queue: queue,
      now: now
    }

    do_complete_claim(storage, context)
  end

  def complete_claim(_storage, _attrs) do
    {:error, {:native_continuation_rejected, :invalid}}
  end

  defp do_complete_claim(storage, %{dispatch_agent: dispatch_agent, attempt: attempt} = context) do
    case DispatchAgent.continuation_fence(dispatch_agent, attempt.run_id) do
      nil -> complete_new_claim(storage, context)
      fence -> complete_existing_claim(storage, context, fence)
    end
  end

  defp complete_new_claim(
         storage,
         %{
           workflow_agent: workflow_agent,
           attempt: attempt,
           request: request,
           queue: queue,
           now: now
         } = context
       ) do
    with :ok <- reject_unless_enabled(),
         {:ok, intent} <-
           ContinuationIntent.prepare_current(
             storage,
             workflow_agent,
             request.input,
             request.continuation_key,
             queue,
             now,
             parent_trace: attempt.trace
           ) do
      persist_and_repair(storage, Map.put(context, :intent, intent), @dispatch_retries)
    else
      {:error, {:native_continuation_rejected, _reason}} = error -> error
      {:error, reason} -> {:error, {:native_continuation_rejected, reason}}
    end
  end

  defp complete_existing_claim(storage, context, fence) do
    with {:ok, intent} <- ContinuationIntent.from_fence(fence),
         :ok <- validate_existing_request(context, fence) do
      context = Map.put(context, :intent, intent)
      candidate = Map.drop(fence, [:queue, :occurred_at])
      complete_and_repair(storage, context, candidate, @dispatch_retries)
    end
  end

  defp validate_existing_request(
         %{attempt: attempt, request: request},
         %{source_runnable_key: source_runnable_key} = fence
       ) do
    request_input = Map.get(fence, :request_input, fence.input)

    if source_runnable_key == attempt.runnable_key and request.input == request_input and
         request.continuation_key == fence.continuation_key and request.definition == :current do
      :ok
    else
      {:error, :conflicting_continuation_fence}
    end
  end

  defp validate_existing_request(_context, _fence) do
    {:error, :conflicting_continuation_fence}
  end

  defp persist_and_repair(_storage, _context, 0) do
    {:error, :conflict}
  end

  defp persist_and_repair(
         storage,
         %{
           dispatch_agent: dispatch_agent,
           attempt: attempt,
           request: request,
           intent: intent
         } = context,
         retries_left
       ) do
    fence =
      ContinuationIntent.fence_attrs(intent, request.input, %{
        source_runnable_key: attempt.runnable_key
      })

    case DispatchAgent.continuation_fence(dispatch_agent, attempt.run_id) do
      nil ->
        persist_new_fence(storage, context, fence, retries_left)

      _existing ->
        complete_and_repair(storage, context, fence, retries_left)
    end
  end

  defp persist_new_fence(
         storage,
         %{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: attempt,
           intent: intent
         } = context,
         fence,
         retries_left
       ) do
    with :ok <- ContinuationActivation.ensure_enabled(),
         :ok <-
           Continuation.validate_native_intent(
             storage,
             dispatch_agent,
             workflow_agent,
             intent,
             attempt.runnable_key
           ) do
      complete_and_repair(storage, context, fence, retries_left)
    else
      {:error, {:native_continuation_storage_failed, reason}} -> {:error, reason}
      {:error, reason} -> {:error, {:native_continuation_rejected, reason}}
    end
  end

  defp complete_and_repair(
         storage,
         %{
           dispatch_agent: dispatch_agent,
           attempt: attempt,
           claim_id: claim_id,
           claim_token: claim_token,
           request: request,
           intent: intent,
           guardrails: guardrails
         } = context,
         fence,
         retries_left
       ) do
    completion = %{
      runnable_key: attempt.runnable_key,
      claim_id: claim_id,
      claim_token: claim_token,
      result: %{},
      fence: fence
    }

    case DispatchAgent.complete_with_continuation_fence(
           storage,
           dispatch_agent,
           completion,
           now: intent.occurred_at,
           execution_opts: [continue_as_new: request],
           guardrails: guardrails
         ) do
      {:ok, _update} ->
        resolve(storage, intent)

      {:error, :conflict} ->
        retry_after_conflict(storage, context, retries_left - 1)

      {:error, {:continuation_already_aborted, _run_id}} ->
        resolve(storage, intent)

      {:error, {:continuation_persistence_failed, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:native_continuation_rejected, reason}}
    end
  end

  defp retry_after_conflict(
         storage,
         %{intent: intent} = context,
         retries_left
       ) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, intent.queue),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, intent.run_id) do
      persist_and_repair(
        storage,
        %{context | dispatch_agent: dispatch_agent, workflow_agent: workflow_agent},
        retries_left
      )
    end
  end

  defp resolve(storage, intent) do
    case ContinuationRecovery.resolve_fenced_run(
           storage,
           intent.run_id,
           intent.queue,
           now: intent.occurred_at
         ) do
      {:ok, {:repaired, %{successor: successor}}} -> {:ok, successor}
      {:ok, {:aborted, update}} -> {:error, {:continuation_aborted, update.abort.abort_reason}}
      {:error, _reason} = error -> error
    end
  end

  defp reject_unless_enabled do
    case ContinuationActivation.ensure_enabled() do
      :ok -> :ok
      {:error, reason} -> {:error, {:native_continuation_rejected, reason}}
    end
  end
end
