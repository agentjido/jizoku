defmodule Squidie.Runtime.Journal.Replay do
  @moduledoc """
  Journal-backed workflow replay.

  Replay rebuilds the source run from its durable run thread, checks completed
  steps against the recovery policy persisted with each runnable, then starts a
  fresh journal run through the normal journal starter. The replayed run stores
  the source run id as lineage metadata on its `:run_started` fact.
  """

  alias Jido.Agent
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.Starter
  alias Squidie.Runtime.Journal.WorkflowDefinitionLoader
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition

  @type replay_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:incompatible_workflow_definition, :replay, map()}
          | {:invalid_replay_source,
             :workflow
             | :trigger
             | :runtime_spec
             | :missing_input
             | {:missing_recovery, term()}}
          | {:invalid_replay_source, :workflow, term()}
          | {:unsafe_replay, map()}
          | Starter.start_error()

  @doc """
  Starts a new journal run from a prior journal run.
  """
  @spec replay(String.t(), keyword(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, replay_error()}
  def replay(run_id, replay_opts \\ [], config_opts \\ [])

  def replay(run_id, replay_opts, config_opts)
      when is_binary(run_id) and is_list(replay_opts) and is_list(config_opts) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, storage} <- journal_storage(config_opts),
         {:ok, queue} <- queue(config_opts),
         {:ok, source_agent} <- source_agent(storage, run_id),
         {:ok, completed_dispatch_keys} <- completed_dispatch_keys(storage, source_agent, queue),
         {:ok, workflow, definition} <- source_workflow(storage, source_agent),
         :ok <- validate_definition_fingerprint(source_agent, definition),
         :ok <-
           ensure_replay_allowed(source_agent, definition, replay_opts, completed_dispatch_keys),
         {:ok, trigger} <- source_trigger(source_agent, definition),
         {:ok, input} <- source_input(source_agent),
         {:ok, context} <- source_context(source_agent) do
      Starter.start_run(
        workflow,
        trigger,
        input,
        config_opts
        |> Keyword.put(:initial_context, context)
        |> Keyword.put(:replayed_from_run_id, run_id)
        |> Keyword.put(:allow_irreversible, Keyword.get(replay_opts, :allow_irreversible, false))
      )
    end
  end

  def replay(_run_id, _replay_opts, _config_opts), do: {:error, :invalid_run_id}

  defp source_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp source_workflow(
         storage,
         %Agent{state: %{run_id: run_id, projection: %Projection{workflow: workflow}}}
       )
       when is_binary(workflow) do
    case WorkflowDefinitionLoader.runtime_spec_run?(storage, run_id) do
      {:ok, true} -> {:error, {:invalid_replay_source, :runtime_spec}}
      {:ok, false} -> load_replay_workflow(workflow)
      {:error, _reason} = error -> error
    end
  end

  defp source_workflow(_storage, %Agent{}) do
    {:error, {:invalid_replay_source, :workflow}}
  end

  defp load_replay_workflow(workflow) do
    case Definition.load_serialized(workflow) do
      {:ok, _workflow, _definition} = ok -> ok
      {:error, {:invalid_workflow, _workflow}} -> {:error, {:invalid_replay_source, :workflow}}
    end
  rescue
    error in CaseClauseError ->
      case error.term do
        {:error, reason} -> {:error, {:invalid_replay_source, :workflow, reason}}
        reason -> {:error, {:invalid_replay_source, :workflow, reason}}
      end
  end

  defp ensure_replay_allowed(%Agent{} = source_agent, definition, opts, completed_dispatch_keys) do
    with {:ok, completed_steps} <- completed_steps(source_agent, completed_dispatch_keys) do
      if Keyword.get(opts, :allow_irreversible) == true do
        :ok
      else
        validate_safe_replay(definition, completed_steps)
      end
    end
  end

  defp validate_safe_replay(definition, completed_steps) do
    unsafe_steps = Definition.unsafe_replay_steps(definition, completed_steps)

    case unsafe_steps do
      [] ->
        :ok

      steps ->
        {:error,
         {:unsafe_replay,
          %{
            message:
              "replay requires explicit approval after irreversible or non-compensatable steps",
            steps: steps
          }}}
    end
  end

  defp validate_definition_fingerprint(
         %Agent{state: %{projection: %Projection{} = projection}},
         definition
       ) do
    persisted = %{
      definition_version: projection.definition_version,
      definition_fingerprint: projection.definition_fingerprint
    }

    if projection.definition_fingerprint == Definition.fingerprint(definition) do
      :ok
    else
      {:error,
       {:incompatible_workflow_definition, :replay,
        Definition.compatibility_metadata(definition, persisted)}}
    end
  end

  defp completed_steps(
         %Agent{state: %{projection: %Projection{} = projection}},
         completed_dispatch_keys
       ) do
    completed_keys = MapSet.union(projection.applied_runnable_keys, completed_dispatch_keys)

    result =
      projection.planned_runnables
      |> Map.values()
      |> Enum.reduce_while({:ok, []}, &put_completed_step(&1, &2, completed_keys))

    case result do
      {:ok, completed_steps} -> {:ok, Enum.reverse(completed_steps)}
      {:error, _reason} = error -> error
    end
  end

  defp put_completed_step(runnable, {:ok, completed_steps}, completed_keys) do
    if MapSet.member?(completed_keys, runnable_value(runnable, :runnable_key)) do
      put_completed_runnable(runnable, completed_steps)
    else
      {:cont, {:ok, completed_steps}}
    end
  end

  defp put_completed_runnable(runnable, completed_steps) do
    case runnable_value(runnable, :recovery) do
      recovery when is_map(recovery) ->
        step = runnable_value(runnable, :step)
        {:cont, {:ok, [{step, recovery} | completed_steps]}}

      _missing_recovery ->
        {:halt,
         {:error, {:invalid_replay_source, {:missing_recovery, runnable_value(runnable, :step)}}}}
    end
  end

  defp source_trigger(
         %Agent{state: %{projection: %Projection{trigger: trigger}}},
         definition
       ) do
    trigger = Definition.deserialize_trigger(definition, trigger)

    case trigger do
      trigger when is_atom(trigger) -> {:ok, trigger}
      _missing_or_invalid -> {:error, {:invalid_replay_source, :trigger}}
    end
  end

  defp source_input(%Agent{state: %{projection: %Projection{input: input}}}) when is_map(input) do
    {:ok, input}
  end

  defp source_input(%Agent{}) do
    {:error, {:invalid_replay_source, :missing_input}}
  end

  defp source_context(%Agent{state: %{projection: %Projection{context: context}}})
       when is_map(context) do
    {:ok, replay_context(context)}
  end

  defp source_context(%Agent{}), do: {:ok, %{}}

  defp replay_context(context) do
    context
    |> Map.take([:schedule, "schedule"])
    |> normalize_replay_context()
  end

  defp normalize_replay_context(%{:schedule => schedule}) do
    case replay_schedule_context(schedule) do
      nil -> %{}
      schedule -> %{schedule: schedule}
    end
  end

  defp normalize_replay_context(%{"schedule" => schedule}) do
    case replay_schedule_context(schedule) do
      nil -> %{}
      schedule -> %{schedule: schedule}
    end
  end

  defp normalize_replay_context(_context), do: %{}

  defp replay_schedule_context(schedule) when is_map(schedule) do
    Map.drop(schedule, [:idempotency, "idempotency", :idempotency_key, "idempotency_key"])
  end

  defp replay_schedule_context(_schedule), do: nil

  defp run_id(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, :invalid_run_id}
    end
  end

  defp completed_dispatch_keys(storage, %Agent{state: %{run_id: run_id}} = source_agent, queue) do
    queues =
      source_agent
      |> planned_queues()
      |> MapSet.put(queue)

    Enum.reduce_while(queues, {:ok, MapSet.new()}, fn queue, {:ok, keys} ->
      case DispatchAgent.rebuild(storage, queue) do
        {:ok, dispatch_agent} ->
          dispatch_keys =
            dispatch_agent
            |> DispatchAgent.completed_results()
            |> Enum.filter(&(&1.run_id == run_id))
            |> Enum.map(& &1.runnable_key)
            |> MapSet.new()

          {:cont, {:ok, MapSet.union(keys, dispatch_keys)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp planned_queues(%Agent{state: %{projection: %Projection{} = projection}}) do
    projection.planned_runnables
    |> Enum.map(fn {_key, runnable} -> runnable_value(runnable, :queue) end)
    |> Enum.reduce(MapSet.new(), fn queue, queues ->
      case Options.queue(queue) do
        {:ok, queue} -> MapSet.put(queues, queue)
        {:error, _reason} -> queues
      end
    end)
  end

  defp journal_storage(opts) do
    opts
    |> Keyword.get(:journal_storage)
    |> Options.storage()
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp runnable_value(runnable, key) when is_map(runnable) and is_atom(key) do
    Map.get(runnable, key) || Map.get(runnable, Atom.to_string(key))
  end
end
