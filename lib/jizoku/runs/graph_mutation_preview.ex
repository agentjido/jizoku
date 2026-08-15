# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runs.GraphMutationPreview do
  @moduledoc false

  alias Jizoku.GraphMutation
  alias Jizoku.GraphMutation.Limits
  alias Jizoku.GraphMutation.Preview
  alias Jizoku.MapField
  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.Journal.Compensation
  alias Jizoku.Runtime.Journal.GraphMutation.Materializer
  alias Jizoku.Runtime.Journal.GraphMutation.Materializer.Result
  alias Jizoku.Runtime.Journal.GraphMutation.ValidationContext
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  @doc false
  @spec preview(term(), String.t(), term(), keyword()) ::
          {:ok, Preview.t()} | {:error, term()}
  def preview(storage, run_id, attrs, opts) when is_list(opts) do
    with {:ok, evaluation} <- evaluate(storage, run_id, attrs, opts) do
      {:ok, outcome(evaluation.mutation, evaluation.context, evaluation.result)}
    end
  end

  @doc false
  @spec evaluate(term(), String.t(), term(), keyword()) :: {:ok, map()} | {:error, term()}
  def evaluate(storage, run_id, attrs, opts) when is_list(opts) do
    with {:ok, mutation} <- GraphMutation.normalize(attrs),
         {:ok, limits} <- limits(opts),
         {:ok, registry} <- action_registry(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- time(opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, definition} <- definition(storage, workflow_agent),
         {:ok, context} <- context(storage, workflow_agent, definition, limits, queue),
         {:ok, result} <-
           Materializer.evaluate(run_id, mutation, context, registry, queue, now) do
      {:ok,
       %{
         mutation: mutation,
         context: context,
         result: result,
         workflow_agent: workflow_agent,
         queue: queue,
         now: now
       }}
    end
  end

  defp limits(opts) do
    case Keyword.fetch(opts, :limits) do
      {:ok, attrs} -> Limits.normalize(attrs)
      :error -> {:error, {:invalid_option, {:limits, :required}}}
    end
  end

  defp action_registry(opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} -> {:ok, registry}
      :error -> {:error, {:invalid_option, {:action_registry, :required}}}
    end
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp time(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp definition(storage, workflow_agent) do
    run_id = workflow_agent.state.run_id
    workflow = workflow_agent.state.workflow

    case WorkflowDefinitionLoader.load(storage, run_id, workflow) do
      {:ok, _workflow, definition} -> {:ok, definition}
      {:error, reason} -> {:error, {:invalid_graph_mutation, {:definition, reason}}}
    end
  end

  defp context(storage, workflow_agent, definition, limits, default_queue) do
    projection = workflow_agent.state.projection
    runnables = Projection.planned_runnables(projection)

    with {:ok, attempts} <- dispatch_attempts(storage, runnables, default_queue) do
      node_runnable_keys = node_runnable_keys(runnables)
      applied_node_ids = applied_node_ids(node_runnable_keys, projection.applied_runnable_keys)
      {ready_node_ids, blocked_node_ids} = readiness(projection.graph, applied_node_ids)

      {:ok,
       ValidationContext.new(projection.graph, limits,
         declared_node_ids: declared_node_ids(definition),
         declared_edge_ids: declared_edge_ids(definition),
         node_runnable_keys: node_runnable_keys,
         applied_runnable_keys: projection.applied_runnable_keys,
         ready_node_ids: ready_node_ids,
         blocked_node_ids: blocked_node_ids,
         dispatch_attempts: attempts,
         retry_node_ids: retry_node_ids(runnables),
         compensation_node_ids: compensation_node_ids(runnables),
         terminal?: Projection.terminal?(projection)
       )}
    end
  end

  defp dispatch_attempts(storage, runnables, default_queue) do
    runnables
    |> Enum.map(&(MapField.get(&1, :queue) || default_queue))
    |> then(&[default_queue | &1])
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce_while({:ok, []}, fn queue, {:ok, attempts} ->
      case DispatchAgent.rebuild(storage, queue) do
        {:ok, agent} ->
          current = Map.values(agent.state.projection.attempts)

          updated_attempts =
            current
            |> Enum.reverse()
            |> Enum.reduce(attempts, fn attempt, accumulated ->
              [attempt | accumulated]
            end)

          {:cont, {:ok, updated_attempts}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp node_runnable_keys(runnables) do
    Enum.reduce(runnables, %{}, fn runnable, grouped ->
      node_id = MapField.get(runnable, :step)
      runnable_key = MapField.get(runnable, :runnable_key)

      if is_binary(node_id) and is_binary(runnable_key) do
        Map.update(grouped, node_id, [runnable_key], &[runnable_key | &1])
      else
        grouped
      end
    end)
  end

  defp applied_node_ids(node_runnable_keys, applied_runnable_keys) do
    Enum.reduce(node_runnable_keys, MapSet.new(), fn {node_id, keys}, applied_node_ids ->
      if Enum.any?(keys, &MapSet.member?(applied_runnable_keys, &1)) do
        MapSet.put(applied_node_ids, node_id)
      else
        applied_node_ids
      end
    end)
  end

  defp readiness(graph, applied_node_ids) do
    initial_predecessors =
      Map.new(graph.topology.nodes, fn {node_id, _node} -> {node_id, MapSet.new()} end)

    predecessors =
      Enum.reduce(graph.topology.edges, initial_predecessors, fn {_edge_id, edge}, grouped ->
        target = MapField.get(edge, :to)
        source = MapField.get(edge, :from)

        if Map.has_key?(grouped, target) do
          Map.update!(grouped, target, &MapSet.put(&1, source))
        else
          grouped
        end
      end)

    Enum.reduce(predecessors, {MapSet.new(), MapSet.new()}, fn
      {node_id, dependencies}, {ready, blocked} ->
        cond do
          MapSet.member?(applied_node_ids, node_id) -> {ready, blocked}
          MapSet.subset?(dependencies, applied_node_ids) -> {MapSet.put(ready, node_id), blocked}
          true -> {ready, MapSet.put(blocked, node_id)}
        end
    end)
  end

  defp declared_node_ids(definition) do
    Enum.map(definition.steps, &to_string(&1.name))
  end

  defp declared_edge_ids(definition) do
    if Definition.dependency_mode?(definition) do
      dependency_edge_ids(definition)
    else
      transition_edge_ids(definition)
    end
  end

  defp dependency_edge_ids(definition) do
    definition
    |> Definition.inspect_steps()
    |> Enum.flat_map(fn step ->
      Enum.map(step.depends_on, &Enum.join([&1, "dependency", step.step], ":"))
    end)
  end

  defp transition_edge_ids(definition) do
    definition.transitions
    |> Enum.with_index()
    |> Enum.map(fn {transition, index} ->
      condition = if Map.has_key?(transition, :condition), do: "condition:#{index}"

      [transition.from, transition.on, transition.to, condition]
      |> Enum.reject(&is_nil/1)
      |> Enum.join(":")
    end)
  end

  defp retry_node_ids(runnables) do
    runnables
    |> Enum.filter(&(MapField.get(&1, :attempt_number, 1) > 1))
    |> Enum.map(&MapField.get(&1, :step))
  end

  defp compensation_node_ids(runnables) do
    runnables
    |> Enum.filter(&Compensation.runnable?/1)
    |> Enum.map(&MapField.get(MapField.get(&1, :dynamic_work, %{}), :origin_step))
  end

  defp outcome(mutation, context, :duplicate) do
    {ready_node_ids, blocked_node_ids} = readiness(context.graph, applied_node_ids(context))

    Preview.new(mutation,
      base_version: context.graph.version,
      result_version: context.graph.version,
      status: :duplicate,
      duplicate?: true,
      active_node_ids: sorted(context.graph.active_node_ids),
      ready_node_ids: sorted(ready_node_ids),
      blocked_node_ids: sorted(blocked_node_ids),
      tombstoned_node_ids: sorted(context.graph.tombstoned_node_ids)
    )
  end

  defp outcome(mutation, context, %Result{} = result) do
    topology = result.topology
    removed_node_ids = operation_ids(mutation.removals, :node)
    tombstoned_node_ids = MapSet.union(context.graph.tombstoned_node_ids, removed_node_ids)

    Preview.new(mutation,
      base_version: context.graph.version,
      result_version: mutation.expected_version + 1,
      status: :applicable,
      applied_operations: operations(mutation),
      active_node_ids: sorted(topology.active_node_ids),
      ready_node_ids: sorted(topology.ready_node_ids),
      blocked_node_ids: sorted(topology.blocked_node_ids),
      tombstoned_node_ids: sorted(tombstoned_node_ids)
    )
  end

  defp applied_node_ids(context) do
    applied_node_ids(context.node_runnable_keys, context.applied_runnable_keys)
  end

  defp operations(mutation) do
    Enum.map(mutation.additions, &{:add, &1}) ++ Enum.map(mutation.removals, &{:remove, &1})
  end

  defp operation_ids(operations, kind) do
    operations
    |> Enum.filter(&(&1.kind == kind))
    |> Enum.map(& &1.id)
    |> MapSet.new()
  end

  defp sorted(values) do
    values
    |> MapSet.to_list()
    |> Enum.sort()
  end
end
