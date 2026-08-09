defmodule Squidie.Runtime.AgentRecovery do
  @moduledoc """
  Restart recovery coordinator for Jido-native workflow and dispatch agents.

  The coordinator rebuilds both agents from durable journals, then drains the
  two restart-safe recovery windows in order: missing dispatch intents first,
  completed dispatch results second.
  """

  alias Jido.Agent
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.WorkflowAgent

  @type queue :: DispatchAgent.queue() | atom()
  @type recovery_update :: %{
          required(:workflow_agent) => Agent.t(),
          required(:dispatch_agent) => Agent.t(),
          required(:scheduled_runnables) => [map()],
          required(:applied_attempts) => [ActionAttempt.t()]
        }
  @type storage_config :: Journal.storage_config()

  @doc """
  Rebuilds a workflow agent and dispatch agent, then drains restart recovery.

  Planned runnable dispatch recovery runs before completed result recovery so a
  restart observes all durable workflow intent before applying durable dispatch
  outcomes back to the run thread.
  """
  @spec recover(storage_config(), WorkflowAgent.run_id(), queue(), keyword()) ::
          {:ok, recovery_update()} | {:error, term()}
  def recover(storage, run_id, queue \\ "default", opts \\ [])

  def recover(storage, run_id, queue, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, storage} <- recovery_storage(storage, opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         :ok <- ensure_continuation_recovery_ready(dispatch_agent, run_id),
         {:ok, %{agent: dispatch_agent, runnables: scheduled_runnables}} <-
           WorkflowAgent.schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             opts
           ),
         {:ok, %{agent: workflow_agent, attempts: applied_attempts}} <-
           WorkflowAgent.apply_pending_results(
             storage,
             workflow_agent,
             dispatch_agent,
             opts
           ) do
      {:ok,
       %{
         workflow_agent: workflow_agent,
         dispatch_agent: dispatch_agent,
         scheduled_runnables: scheduled_runnables,
         applied_attempts: applied_attempts
       }}
    end
  end

  defp ensure_continuation_recovery_ready(dispatch_agent, run_id) do
    if DispatchAgent.continuation_fence(dispatch_agent, run_id) do
      {:error, {:continuation_repair_required, run_id}}
    else
      :ok
    end
  end

  defp recovery_storage(storage, opts) do
    opts
    |> Keyword.put(:journal_storage, storage)
    |> Options.storage_from_opts()
  end
end
