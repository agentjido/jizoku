defmodule Squidie.Runtime.AgentRecovery do
  @moduledoc """
  Restart recovery coordinator for Jido-native workflow and dispatch agents.

  The coordinator rebuilds both agents from durable journals, resolves a pending
  continuation for the target run, then drains the remaining restart-safe
  windows in order: missing dispatch intents first, completed dispatch results
  second. Resolution repairs a valid continuation or aborts its fence after a
  durable competing predecessor transition wins.
  """

  alias Jido.Agent
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.ContinuationRecovery
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

  Pending continuation resolution runs before ordinary recovery so a durable
  fence cannot expose predecessor work again. Planned runnable dispatch recovery
  then runs before completed result recovery so a restart observes all durable
  workflow intent before applying durable dispatch outcomes back to the run
  thread.
  """
  @spec recover(storage_config(), WorkflowAgent.run_id(), queue(), keyword()) ::
          {:ok, recovery_update()} | {:error, term()}
  def recover(storage, run_id, queue \\ "default", opts \\ [])

  def recover(storage, run_id, queue, opts)
      when is_binary(run_id) and is_list(opts) do
    with {:ok, storage} <- recovery_storage(storage, opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, %{workflow_agent: workflow_agent, dispatch_agent: dispatch_agent}} <-
           repair_pending_continuation(
             storage,
             workflow_agent,
             dispatch_agent,
             run_id,
             opts
           ),
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

  defp repair_pending_continuation(
         storage,
         workflow_agent,
         dispatch_agent,
         run_id,
         opts
       ) do
    pending? =
      dispatch_agent
      |> DispatchAgent.pending_continuation_fences()
      |> Enum.any?(&(&1.run_id == run_id))

    if pending? do
      queue = dispatch_agent.state.queue

      with {:ok, _resolution} <-
             ContinuationRecovery.resolve_fenced_run(storage, run_id, queue, opts),
           {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
           {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
        {:ok, %{workflow_agent: workflow_agent, dispatch_agent: dispatch_agent}}
      end
    else
      {:ok, %{workflow_agent: workflow_agent, dispatch_agent: dispatch_agent}}
    end
  end

  defp recovery_storage(storage, opts) do
    opts
    |> Keyword.put(:journal_storage, storage)
    |> Options.storage_from_opts()
  end
end
