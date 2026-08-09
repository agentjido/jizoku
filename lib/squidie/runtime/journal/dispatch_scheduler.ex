defmodule Squidie.Runtime.Journal.DispatchScheduler do
  @moduledoc false

  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.WorkflowAgent

  @doc """
  Schedules pending workflow dispatches with optimistic-conflict retry.
  """
  @spec schedule_pending_dispatches(term(), term(), term(), DateTime.t(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def schedule_pending_dispatches(
        storage,
        workflow_agent,
        dispatch_agent,
        %DateTime{} = now,
        retries
      ) do
    do_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now, retries)
  end

  defp do_schedule_pending_dispatches(
         _storage,
         workflow_agent,
         dispatch_agent,
         _now,
         _retries_left
       )
       when workflow_agent.state.projection.terminal_status != nil do
    {:ok, %{agent: dispatch_agent, runnables: []}}
  end

  defp do_schedule_pending_dispatches(_storage, _workflow_agent, _dispatch_agent, _now, 0),
    do: {:error, :conflict}

  defp do_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now, retries_left) do
    case WorkflowAgent.schedule_pending_dispatches(storage, workflow_agent, dispatch_agent,
           now: now
         ) do
      {:ok, _schedule_update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id),
             {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          do_schedule_pending_dispatches(
            storage,
            workflow_agent,
            dispatch_agent,
            now,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end
end
