defmodule Squidie.Runtime.WorkflowAgent do
  @moduledoc """
  Jido-native workflow coordination state for one durable workflow run.

  The agent rebuilds from run-thread journal entries and checkpoints. It does
  not execute workflow steps; it provides the restartable coordination state
  needed by the journal-backed runtime.
  """

  use Jido.Agent,
    name: "squidie_workflow_agent",
    description: "Rebuildable workflow coordination state for one Squidie run.",
    default_plugins: false

  alias Jido.Agent
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Jido.ResultEnvelope
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Checkpoint
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.Partition
  alias Squidie.Runtime.WorkflowAgent.Projection

  @type run_id :: String.t()
  @type apply_update :: %{
          required(:agent) => Agent.t(),
          required(:attempt) => ActionAttempt.t()
        }
  @type apply_many_update :: %{
          required(:agent) => Agent.t(),
          required(:attempts) => [ActionAttempt.t()]
        }
  @type dispatch_schedule_update :: %{
          required(:agent) => Agent.t(),
          required(:runnables) => [map()]
        }
  @type storage_config :: Journal.storage_config()

  @doc """
  Rebuilds a workflow agent for one run from its durable run thread.
  """
  @spec rebuild(storage_config(), run_id()) :: {:ok, Agent.t()} | {:error, term()}
  def rebuild(storage, run_id) when is_binary(run_id) do
    partition = Storage.partition(storage)

    with {:ok, loaded_thread} <- Journal.load_thread(storage, {:run, run_id}),
         {:ok, projection} <- current_projection(storage, loaded_thread) do
      {:ok,
       new(
         id: agent_id(run_id, partition),
         state: %{
           run_id: run_id,
           partition: partition,
           workflow: projection.workflow,
           projection: projection,
           thread_rev: loaded_thread.rev
         }
       )}
    end
  end

  @doc """
  Returns the stable Jido agent id for a workflow run.
  """
  @spec agent_id(run_id()) :: String.t()
  def agent_id(run_id), do: "squidie.workflow.#{run_id}"

  @doc false
  @spec agent_id(run_id(), String.t() | nil) :: String.t()
  def agent_id(run_id, nil), do: agent_id(run_id)

  def agent_id(run_id, partition) when is_binary(run_id) and is_binary(partition) do
    "squidie.workflow.partition.#{Partition.identity([partition, run_id])}"
  end

  @doc """
  Stores the current workflow projection as a checkpoint for faster rebuilds.
  """
  @spec put_checkpoint(storage_config(), Agent.t(), keyword()) :: :ok | {:error, term()}
  def put_checkpoint(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %{run_id: run_id, projection: projection, thread_rev: thread_rev}
        } = agent,
        opts \\ []
      )
      when is_binary(run_id) and is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    with :ok <- validate_agent_partition(storage, agent) do
      Journal.put_checkpoint(storage, {:run, run_id}, projection, thread_rev, opts)
    end
  end

  @doc """
  Returns the workflow projection status.
  """
  @spec status(Agent.t()) :: atom()
  def status(%Agent{agent_module: __MODULE__, state: %{projection: projection}}) do
    Projection.status(projection)
  end

  @doc """
  Returns runnable keys planned by the workflow thread.
  """
  @spec planned_runnable_keys(Agent.t()) :: [String.t()]
  def planned_runnable_keys(%Agent{agent_module: __MODULE__, state: %{projection: projection}}) do
    Projection.planned_runnable_keys(projection)
  end

  @doc """
  Returns planned runnable payloads in deterministic order.
  """
  @spec planned_runnables(Agent.t()) :: [map()]
  def planned_runnables(%Agent{agent_module: __MODULE__, state: %{projection: projection}}) do
    Projection.planned_runnables(projection)
  end

  @doc """
  Returns runnable keys whose dispatch results have been applied to the run.
  """
  @spec applied_runnable_keys(Agent.t()) :: MapSet.t(String.t())
  def applied_runnable_keys(%Agent{agent_module: __MODULE__, state: %{projection: projection}}) do
    Projection.applied_runnable_keys(projection)
  end

  @doc """
  Lists completed dispatch results that still need workflow application.
  """
  @spec pending_results(Agent.t(), Agent.t()) :: [
          Squidie.Runtime.DispatchProtocol.ActionAttempt.t()
        ]
  def pending_results(
        %Agent{
          agent_module: __MODULE__,
          state: %{run_id: run_id, partition: partition, projection: projection}
        },
        %Agent{agent_module: DispatchAgent, state: %{partition: partition}} = dispatch_agent
      ) do
    applied_keys = Projection.applied_runnable_keys(projection)

    dispatch_agent
    |> DispatchAgent.results_ready_to_apply()
    |> Enum.filter(fn result ->
      result.run_id == run_id and
        Projection.planned_runnable_key?(projection, result.runnable_key) and
        not MapSet.member?(applied_keys, result.runnable_key)
    end)
    |> reject_when_terminal(projection)
  end

  def pending_results(
        %Agent{agent_module: __MODULE__},
        %Agent{agent_module: DispatchAgent}
      ),
      do: []

  @doc """
  Lists planned runnables that still need dispatch scheduling.
  """
  @spec pending_dispatches(Agent.t(), Agent.t()) :: [map()]
  def pending_dispatches(
        %Agent{
          agent_module: __MODULE__,
          state: %{run_id: run_id, partition: partition, projection: projection}
        },
        %Agent{
          agent_module: DispatchAgent,
          state: %{partition: partition, queue: queue}
        } = dispatch_agent
      ) do
    if DispatchAgent.active_continuation_fence(dispatch_agent, run_id) do
      []
    else
      pending_dispatches_for_active_run(projection, dispatch_agent, queue)
    end
  end

  def pending_dispatches(
        %Agent{agent_module: __MODULE__},
        %Agent{agent_module: DispatchAgent}
      ),
      do: []

  @doc """
  Schedules every planned runnable that is missing from the dispatch journal.

  This is the restart recovery boundary for the crash window between durable
  workflow planning and durable dispatch scheduling. The workflow agent derives
  missing planned runnables from the run-thread projection, and the dispatch
  agent appends their `:attempt_scheduled` entries with its current dispatch
  thread revision as the append fence.
  """
  @spec schedule_pending_dispatches(storage_config(), Agent.t(), Agent.t(), keyword()) ::
          {:ok, dispatch_schedule_update()} | {:error, term()}
  def schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, opts \\ [])

  def schedule_pending_dispatches(
        storage,
        %Agent{agent_module: __MODULE__, state: %{run_id: run_id}} = workflow_agent,
        %Agent{agent_module: DispatchAgent} = dispatch_agent,
        opts
      )
      when is_binary(run_id) and is_list(opts) do
    DispatchAgent.schedule_attempts(
      storage,
      dispatch_agent,
      run_id,
      workflow_agent
      |> pending_dispatches(dispatch_agent)
      |> Enum.map(&Map.put(&1, :workflow, workflow_agent.state.projection.workflow)),
      opts
    )
  end

  @doc """
  Applies every completed dispatch result that is still pending for the workflow run.

  This is the restart recovery boundary for lost live wakeups: both agents can be
  rebuilt from durable journals, pending completed attempts can be derived again,
  and each missing workflow application is appended to the run thread with the
  current workflow-agent revision as the append fence.
  """
  @spec apply_pending_results(storage_config(), Agent.t(), Agent.t(), keyword()) ::
          {:ok, apply_many_update()} | {:error, term()}
  def apply_pending_results(storage, workflow_agent, dispatch_agent, opts \\ [])

  def apply_pending_results(
        storage,
        %Agent{agent_module: __MODULE__} = workflow_agent,
        %Agent{agent_module: DispatchAgent} = dispatch_agent,
        opts
      )
      when is_list(opts) do
    workflow_agent
    |> pending_results(dispatch_agent)
    |> Enum.reduce_while(
      {:ok, workflow_agent, []},
      &apply_pending_result(storage, opts, &1, &2)
    )
    |> case do
      {:ok, updated_agent, applied_attempts} ->
        {:ok, %{agent: updated_agent, attempts: Enum.reverse(applied_attempts)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_pending_result(
         storage,
         opts,
         %ActionAttempt{} = attempt,
         {:ok, current_agent, applied_attempts}
       ) do
    if deferred_completion?(attempt) or
         ResultEnvelope.native_effect?(ActionAttempt.completion_encoding(attempt)) do
      {:cont, {:ok, current_agent, applied_attempts}}
    else
      apply_pending_result_attempt(storage, opts, attempt, current_agent, applied_attempts)
    end
  end

  defp apply_pending_result_attempt(storage, opts, attempt, current_agent, applied_attempts) do
    case apply_result(storage, current_agent, attempt, opts) do
      {:ok, %{agent: next_agent, attempt: applied_attempt}} ->
        {:cont, {:ok, next_agent, [applied_attempt | applied_attempts]}}

      {:error, _reason} = error ->
        {:halt, error}
    end
  end

  @doc """
  Records that a durable dispatch completion has been applied to the workflow run.

  The dispatch attempt must be completed, belong to the workflow agent's run,
  and reference a planned runnable that has not already been applied. Stale
  workflow-agent callers race at the run-thread append boundary and receive
  `{:error, :conflict}` from the journal.
  """
  @spec apply_result(storage_config(), Agent.t(), ActionAttempt.t(), keyword()) ::
          {:ok, apply_update()} | {:error, term()}
  def apply_result(storage, agent, attempt, opts \\ [])

  def apply_result(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %{run_id: run_id, projection: %Projection{} = projection, thread_rev: thread_rev}
        } = agent,
        %ActionAttempt{} = attempt,
        opts
      )
      when is_binary(run_id) and is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    with :ok <- reject_deferred_completion(attempt),
         {:ok, now} <- apply_now(opts),
         {:ok, target} <- apply_target(projection, run_id, attempt),
         {:pending, %ActionAttempt{} = pending_attempt} <- target,
         {:ok, applied_entry} <-
           DispatchProtocol.new_entry(:runnable_applied, %{
             run_id: pending_attempt.run_id,
             runnable_key: pending_attempt.runnable_key,
             result: pending_attempt.result,
             guardrails: pending_attempt.guardrails,
             execution_opts: pending_attempt.execution_opts,
             occurred_at: now
           }),
         {:ok, applied_agent} <-
           persist_workflow_entry(storage, agent, projection, thread_rev, applied_entry) do
      {:ok, %{agent: applied_agent, attempt: pending_attempt}}
    else
      {:applied, %ActionAttempt{} = applied_attempt} ->
        {:ok, %{agent: agent, attempt: applied_attempt}}

      {:error, _reason} = error ->
        error
    end
  end

  defp pending_dispatches_for_active_run(projection, dispatch_agent, queue) do
    dispatched_keys = DispatchAgent.runnable_keys(dispatch_agent)
    applied_keys = Projection.applied_runnable_keys(projection)
    dispatchable_keys = Projection.dispatchable_runnable_keys(projection)

    projection
    |> Projection.planned_runnables()
    |> Enum.reject(fn runnable ->
      key = runnable_key(runnable)

      normalize_queue(runnable_queue(runnable) || queue) != queue or
        not MapSet.member?(dispatchable_keys, key) or
        MapSet.member?(dispatched_keys, key) or
        MapSet.member?(applied_keys, key)
    end)
    |> reject_when_terminal(projection)
  end

  defp reject_deferred_completion(%ActionAttempt{} = attempt) do
    if deferred_completion?(attempt) do
      {:error, :deferred_completion_requires_executor}
    else
      :ok
    end
  end

  defp deferred_completion?(%ActionAttempt{execution_opts: execution_opts})
       when is_list(execution_opts) do
    Keyword.has_key?(execution_opts, :defer)
  end

  defp deferred_completion?(_attempt), do: false

  defp reject_when_terminal(results, %Projection{} = projection) do
    if Projection.terminal?(projection), do: [], else: results
  end

  defp runnable_key(runnable) when is_map(runnable) do
    map_value(runnable, :runnable_key)
  end

  defp runnable_key(_runnable), do: nil

  defp runnable_queue(runnable) when is_map(runnable) do
    map_value(runnable, :queue)
  end

  defp runnable_queue(_runnable), do: nil

  defp normalize_queue(queue) when is_binary(queue), do: queue
  defp normalize_queue(queue), do: to_string(queue)

  defp map_value(map, key, default \\ nil), do: Squidie.MapField.get(map, key, default)

  defp persist_workflow_entry(
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entry
       ) do
    with :ok <- validate_agent_partition(storage, agent),
         {:ok, thread} <-
           Journal.append_entries(storage, [entry],
             expected_rev: thread_rev,
             telemetry_projection: projection
           ) do
      {:ok, apply_workflow_entry(agent, projection, entry, thread.rev)}
    end
  end

  defp validate_agent_partition(storage, %Agent{state: state}) do
    case {Storage.partition(storage), Map.get(state, :partition)} do
      {partition, partition} -> :ok
      {_storage_partition, _agent_partition} -> {:error, {:partition_mismatch, :workflow_agent}}
    end
  end

  defp apply_workflow_entry(%Agent{} = agent, %Projection{} = projection, entry, thread_rev) do
    %Agent{
      agent
      | state: %{
          agent.state
          | projection: Projection.replay(projection, [entry]),
            thread_rev: thread_rev
        }
    }
  end

  defp apply_now(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())

    if match?(%DateTime{}, now) do
      {:ok, now}
    else
      {:error, {:invalid_option, :now}}
    end
  end

  defp apply_target(%Projection{} = projection, run_id, %ActionAttempt{} = attempt) do
    cond do
      attempt.status != :completed ->
        {:error, :result_not_completed}

      not is_map(attempt.result) ->
        {:error, :missing_result}

      ResultEnvelope.native_effect?(ActionAttempt.completion_encoding(attempt)) ->
        {:error, :native_jido_effect_requires_executor_recovery}

      attempt.run_id != run_id ->
        {:error, :wrong_run}

      Projection.terminal?(projection) ->
        {:error, :terminal_run}

      not Projection.planned_runnable_key?(projection, attempt.runnable_key) ->
        {:error, :unknown_runnable_intent}

      MapSet.member?(Projection.applied_runnable_keys(projection), attempt.runnable_key) ->
        apply_duplicate_target(projection, attempt)

      true ->
        {:ok, {:pending, attempt}}
    end
  end

  defp apply_duplicate_target(%Projection{} = projection, %ActionAttempt{} = attempt) do
    case Projection.applied_result(projection, attempt.runnable_key) do
      {:ok, result} when result == attempt.result ->
        {:ok, {:applied, attempt}}

      {:ok, _other_result} ->
        {:error, {:conflicting_result, attempt.runnable_key}}

      :error ->
        {:error, {:conflicting_result, attempt.runnable_key}}
    end
  end

  defp current_projection(storage, %{thread: thread, rev: rev, entries: entries}) do
    case Journal.fetch_checkpoint(storage, thread) do
      {:ok, %Checkpoint{thread_rev: checkpoint_rev, projection: %Projection{} = projection}}
      when is_integer(checkpoint_rev) and checkpoint_rev >= 0 and checkpoint_rev <= rev ->
        if Projection.checkpoint_compatible?(projection) do
          {:ok,
           projection
           |> Projection.upgrade()
           |> Projection.replay(Enum.drop(entries, checkpoint_rev))}
        else
          {:ok, Projection.rebuild(entries)}
        end

      {:error, :not_found} ->
        {:ok, Projection.rebuild(entries)}

      {:error, _reason} = error ->
        error

      _future_or_invalid_checkpoint ->
        {:ok, Projection.rebuild(entries)}
    end
  end
end
