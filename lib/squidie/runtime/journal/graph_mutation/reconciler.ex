defmodule Squidie.Runtime.Journal.GraphMutation.Reconciler do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.MapField
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.WorkflowAgent

  @conflict_retries 25

  @type queue_result :: %{
          required(:queue) => String.t(),
          required(:run_queued?) => boolean(),
          required(:scheduled_runnables) => [map()]
        }
  @type result :: %{
          required(:workflow_agent) => Agent.t(),
          required(:queues) => [queue_result()]
        }

  @doc false
  @spec reconcile(Journal.storage_config(), WorkflowAgent.run_id(), keyword()) ::
          {:ok, result()} | {:error, term()}
  def reconcile(storage, run_id, opts \\ [])

  def reconcile(storage, run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, now} <- Options.now_from_opts(opts),
         {:ok, default_queue} <- Options.queue_from_opts(opts) do
      reconcile_run(storage, run_id, default_queue, now, opts, @conflict_retries)
    end
  end

  defp reconcile_run(_storage, _run_id, _default_queue, _now, _opts, 0) do
    {:error, :conflict}
  end

  defp reconcile_run(storage, run_id, default_queue, now, opts, retries) do
    context = %{
      storage: storage,
      run_id: run_id,
      default_queue: default_queue,
      now: now,
      opts: opts,
      retries: retries
    }

    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, queues} <- durable_queues(workflow_agent, default_queue),
         {:ok, queue_results} <- reconcile_queues(storage, workflow_agent, queues, now, opts),
         {:ok, current_agent} <- WorkflowAgent.rebuild(storage, run_id) do
      stable_reconciliation(context, workflow_agent, current_agent, queue_results)
    end
  end

  defp stable_reconciliation(
         _context,
         workflow_agent,
         current_agent,
         queue_results
       )
       when workflow_agent.state.thread_rev == current_agent.state.thread_rev do
    {:ok, %{workflow_agent: current_agent, queues: queue_results}}
  end

  defp stable_reconciliation(
         context,
         _workflow_agent,
         _current_agent,
         _queue_results
       ) do
    reconcile_run(
      context.storage,
      context.run_id,
      context.default_queue,
      context.now,
      context.opts,
      context.retries - 1
    )
  end

  defp durable_queues(workflow_agent, default_queue) do
    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.reduce_while({:ok, MapSet.new()}, fn runnable, {:ok, queues} ->
      queue = MapField.get(runnable, :queue) || default_queue

      case Options.queue(queue) do
        {:ok, queue} -> {:cont, {:ok, MapSet.put(queues, queue)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, queues} -> {:ok, Enum.sort(MapSet.to_list(queues))}
      {:error, _reason} = error -> error
    end
  end

  defp reconcile_queues(storage, workflow_agent, queues, now, opts) do
    reconciliation =
      Enum.reduce_while(queues, {:ok, []}, fn queue, {:ok, results} ->
        case reconcile_queue(
               storage,
               workflow_agent,
               queue,
               now,
               opts,
               @conflict_retries
             ) do
          {:ok, result} -> {:cont, {:ok, [result | results]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case reconciliation do
      {:ok, results} -> {:ok, Enum.reverse(results)}
      {:error, _reason} = error -> error
    end
  end

  defp reconcile_queue(_storage, _workflow_agent, _queue, _now, _opts, 0) do
    {:error, :conflict}
  end

  defp reconcile_queue(storage, workflow_agent, queue, now, opts, retries) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, queue_update} <-
           DispatchAgent.ensure_run_queued(storage, dispatch_agent, workflow_agent.state.run_id,
             now: now
           ),
         {:ok, schedule_update} <-
           WorkflowAgent.schedule_pending_dispatches(
             storage,
             workflow_agent,
             queue_update.agent,
             Keyword.put(opts, :now, now)
           ) do
      {:ok,
       %{
         queue: queue,
         run_queued?: queue_update.queued?,
         scheduled_runnables: schedule_update.runnables
       }}
    else
      {:error, :conflict} ->
        reconcile_queue(storage, workflow_agent, queue, now, opts, retries - 1)

      {:error, _reason} = error ->
        error
    end
  end
end
