# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.Compensation do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition

  @doc false
  @spec next_runnable(
          Agent.t(),
          Definition.t(),
          String.t(),
          String.t(),
          DateTime.t(),
          map(),
          String.t() | nil,
          MapSet.t()
        ) :: {:ok, map() | nil}
  def next_runnable(
        %Agent{} = workflow_agent,
        definition,
        run_id,
        queue,
        %DateTime{} = now,
        failure,
        failure_runnable_key,
        applied_compensation_keys
      )
      when is_binary(run_id) and is_map(failure) do
    compensated_origin_steps =
      compensated_origin_steps(workflow_agent, applied_compensation_keys)

    workflow_agent
    |> completed_candidates(definition)
    |> Enum.reject(&MapSet.member?(compensated_origin_steps, &1.step_label))
    |> case do
      [] ->
        {:ok, nil}

      [candidate | _rest] ->
        {:ok, runnable(candidate, run_id, queue, now, failure, failure_runnable_key)}
    end
  end

  @doc false
  @spec applied_runnable_keys(Agent.t()) :: MapSet.t()
  def applied_runnable_keys(%Agent{} = workflow_agent) do
    applied_keys = WorkflowAgent.applied_runnable_keys(workflow_agent)

    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.reduce(MapSet.new(), fn runnable, acc ->
      runnable_key = runnable_value(runnable, :runnable_key)

      if runnable?(runnable) and MapSet.member?(applied_keys, runnable_key) do
        MapSet.put(acc, runnable_key)
      else
        acc
      end
    end)
  end

  @doc false
  @spec planned_for_failure?(Agent.t(), String.t() | nil) :: boolean()
  def planned_for_failure?(%Agent{} = workflow_agent, failure_runnable_key)
      when is_binary(failure_runnable_key) do
    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.any?(fn runnable ->
      dynamic_work = dynamic_work(runnable)

      runnable?(runnable) and
        dynamic_value(dynamic_work, :failure_runnable_key) == failure_runnable_key
    end)
  end

  def planned_for_failure?(%Agent{}, _failure_runnable_key), do: false

  @doc false
  @spec runnable?(map() | term()) :: boolean()
  def runnable?(runnable) when is_map(runnable) do
    runnable
    |> dynamic_work()
    |> dynamic_value(:kind)
    |> then(&(&1 in [:compensation, "compensation"]))
  end

  def runnable?(_runnable), do: false

  @doc false
  @spec failure(map()) :: map()
  def failure(runnable) when is_map(runnable) do
    runnable
    |> dynamic_work()
    |> map_value(:failure, %{})
  end

  @doc false
  @spec failure_runnable_key(map()) :: String.t() | nil
  def failure_runnable_key(runnable) when is_map(runnable) do
    runnable
    |> dynamic_work()
    |> dynamic_value(:failure_runnable_key)
  end

  defp completed_candidates(%Agent{} = workflow_agent, definition) do
    projection = workflow_agent.state.projection
    applied_keys = WorkflowAgent.applied_runnable_keys(workflow_agent)

    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.flat_map(fn runnable ->
      runnable_key = runnable_value(runnable, :runnable_key)
      step_label = runnable_value(runnable, :step)

      with true <- is_binary(runnable_key),
           true <- MapSet.member?(applied_keys, runnable_key),
           step_name when is_atom(step_name) <-
             Definition.deserialize_step(definition, step_label),
           {:ok, callback} when is_atom(callback) and not is_nil(callback) <-
             Definition.step_compensation_callback(definition, step_name),
           {:ok, output} <- Projection.applied_result(projection, runnable_key) do
        [
          %{
            runnable_key: runnable_key,
            step: step_name,
            step_label: step_label,
            input: runnable_value(runnable, :input) || %{},
            output: output || %{},
            applied_at: Projection.applied_at(projection, runnable_key),
            callback: callback
          }
        ]
      else
        _not_compensatable -> []
      end
    end)
    |> Enum.sort_by(
      fn candidate ->
        {sort_time(candidate.applied_at), candidate.runnable_key}
      end,
      :desc
    )
  end

  defp runnable(candidate, run_id, queue, %DateTime{} = now, failure, failure_key) do
    compensation_step = "compensate:#{candidate.step_label}"
    compensation_key = "#{run_id}:#{compensation_step}:1"

    %{
      run_id: run_id,
      runnable_key: compensation_key,
      idempotency_key: compensation_key,
      attempt_number: 1,
      queue: queue,
      step: compensation_step,
      input: input(candidate, run_id, failure),
      recovery: recovery_policy(),
      visible_at: now,
      dynamic?: true,
      dynamic_work: %{
        kind: :compensation,
        module: candidate.callback,
        origin_step: candidate.step_label,
        origin_runnable_key: candidate.runnable_key,
        failure_runnable_key: failure_key,
        failure: failure
      }
    }
  end

  defp input(candidate, run_id, failure) do
    %{
      source: %{run_id: run_id, failure: failure},
      step: %{
        name: candidate.step,
        runnable_key: candidate.runnable_key,
        input: candidate.input,
        output: candidate.output,
        applied_at: candidate.applied_at
      }
    }
  end

  defp recovery_policy do
    %{
      "irreversible?" => false,
      "compensatable?" => false,
      "replay" => "manual_review_required",
      "recovery" => "manual_intervention"
    }
  end

  defp compensated_origin_steps(workflow_agent, applied_compensation_keys) do
    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.reduce(MapSet.new(), fn runnable, acc ->
      runnable_key = runnable_value(runnable, :runnable_key)
      dynamic_work = dynamic_work(runnable)

      if runnable?(runnable) and MapSet.member?(applied_compensation_keys, runnable_key) do
        MapSet.put(acc, dynamic_value(dynamic_work, :origin_step))
      else
        acc
      end
    end)
    |> MapSet.delete(nil)
  end

  defp sort_time(%DateTime{} = applied_at), do: DateTime.to_unix(applied_at, :microsecond)
  defp sort_time(_missing), do: -1

  defp dynamic_work(runnable) when is_map(runnable) do
    map_value(runnable, :dynamic_work, %{})
  end

  defp dynamic_value(dynamic_work, key) when is_map(dynamic_work) and is_atom(key) do
    map_value(dynamic_work, key)
  end

  defp runnable_value(runnable, key) when is_map(runnable) and is_atom(key) do
    map_value(runnable, key)
  end

  defp map_value(map, key, default \\ nil)

  defp map_value(map, key, default) when is_map(map) and is_atom(key) do
    value = Map.get(map, key)

    if is_nil(value) do
      Map.get(map, Atom.to_string(key), default)
    else
      value
    end
  end

  defp map_value(_map, _key, default), do: default
end
