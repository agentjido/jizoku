defmodule Squidie.Runtime.Journal.Commands.ContinuationRecovery do
  @moduledoc false

  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Continuation
  alias Squidie.Runtime.WorkflowAgent

  @abort_retries 25

  @type resolution ::
          {:repaired, Continuation.repair_result()}
          | {:aborted, DispatchAgent.continuation_abort_update()}

  @doc false
  @spec resolve_fenced_run(Journal.storage_config(), String.t(), String.t(), keyword()) ::
          {:ok, resolution()} | {:error, term()}
  def resolve_fenced_run(storage, run_id, queue, opts \\ [])

  def resolve_fenced_run(storage, run_id, queue, opts)
      when is_binary(run_id) and run_id != "" and is_binary(queue) and queue != "" and
             is_list(opts) do
    if Keyword.keyword?(opts) do
      resolve_fenced_run(storage, run_id, queue, opts, @abort_retries)
    else
      {:error, {:invalid_continuation, :invalid}}
    end
  end

  def resolve_fenced_run(_storage, _run_id, _queue, _opts) do
    {:error, {:invalid_continuation, :invalid}}
  end

  defp resolve_fenced_run(_storage, _run_id, _queue, _opts, 0) do
    {:error, :conflict}
  end

  defp resolve_fenced_run(storage, run_id, queue, opts, retries_left) do
    case Continuation.repair_fenced_run(storage, run_id, queue) do
      {:ok, repair} ->
        {:ok, {:repaired, repair}}

      {:error, reason} ->
        resolve_repair_error(storage, run_id, queue, opts, retries_left, reason)
    end
  end

  defp resolve_repair_error(storage, run_id, queue, opts, retries_left, repair_error) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id) do
      abort_reason = Continuation.abort_reason(storage, dispatch_agent, workflow_agent)

      cond do
        DispatchAgent.continuation_repair(dispatch_agent, run_id) ->
          resolve_fenced_run(storage, run_id, queue, opts, retries_left - 1)

        abort = DispatchAgent.continuation_abort(dispatch_agent, run_id) ->
          {:ok, {:aborted, DispatchAgent.continuation_abort_update(dispatch_agent, abort, false)}}

        match?({:ok, _reason}, abort_reason) ->
          {:ok, reason} = abort_reason

          abort_fence(
            storage,
            dispatch_agent,
            run_id,
            queue,
            reason,
            opts,
            retries_left,
            repair_error
          )

        true ->
          {:error, repair_error}
      end
    end
  end

  defp abort_fence(
         storage,
         dispatch_agent,
         run_id,
         queue,
         reason,
         opts,
         retries_left,
         repair_error
       ) do
    case DispatchAgent.abort_continuation_fence(
           storage,
           dispatch_agent,
           run_id,
           reason,
           Keyword.take(opts, [:now])
         ) do
      {:ok, abort} ->
        {:ok, {:aborted, abort}}

      {:error, :conflict} ->
        resolve_fenced_run(storage, run_id, queue, opts, retries_left - 1)

      {:error, {:continuation_already_repaired, ^run_id}} ->
        resolve_fenced_run(storage, run_id, queue, opts, retries_left - 1)

      {:error, {:continuation_fence_not_found, ^run_id}} ->
        {:error, repair_error}

      {:error, _reason} = error ->
        error
    end
  end
end
