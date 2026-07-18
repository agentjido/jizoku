defmodule Squidie.ReadModel.Inspection.GraphState do
  @moduledoc false

  alias Squidie.MapField
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  @identity_limit 500
  @history_limit 100
  @provenance_values [:legacy_eager, :dependency_ordered]
  @reconciliation_statuses [:not_required, :required, :completed, :unknown]

  @doc """
  Builds a bounded public graph-state summary from a rebuilt workflow agent.
  """
  @spec from_agent(term(), Jido.Agent.t(), String.t()) :: map()
  def from_agent(storage, workflow_agent, default_queue) when is_binary(default_queue) do
    projection = workflow_agent.state.projection
    graph = projection.graph
    {ready_node_ids, blocked_node_ids} = readiness(projection)

    %{
      graph_version: valid_version(graph.version),
      graph_provenance: provenance(graph.provenance),
      active_node_ids: identities(graph.active_node_ids),
      active_edge_ids: identities(graph.active_edge_ids),
      ready_node_ids: identities(ready_node_ids),
      blocked_node_ids: identities(blocked_node_ids),
      tombstoned_node_ids: identities(graph.tombstoned_node_ids),
      tombstoned_edge_ids: identities(graph.tombstoned_edge_ids),
      mutation_history: mutation_history(graph.mutation_history),
      reconciliation_status: reconciliation_status(storage, projection, graph, default_queue)
    }
  end

  @doc """
  Normalizes graph-state fields from current or legacy inspection data.
  """
  @spec sanitize(map()) :: map()
  def sanitize(state) when is_map(state) do
    %{
      graph_version: valid_version(MapField.get(state, :graph_version)),
      graph_provenance: provenance(MapField.get(state, :graph_provenance, %{})),
      active_node_ids: identities(MapField.get(state, :active_node_ids, [])),
      active_edge_ids: identities(MapField.get(state, :active_edge_ids, [])),
      ready_node_ids: identities(MapField.get(state, :ready_node_ids, [])),
      blocked_node_ids: identities(MapField.get(state, :blocked_node_ids, [])),
      tombstoned_node_ids: identities(MapField.get(state, :tombstoned_node_ids, [])),
      tombstoned_edge_ids: identities(MapField.get(state, :tombstoned_edge_ids, [])),
      mutation_history: mutation_history(MapField.get(state, :mutation_history, [])),
      reconciliation_status:
        normalize_reconciliation_status(MapField.get(state, :reconciliation_status))
    }
  end

  defp readiness(%Projection{} = projection) do
    graph = projection.graph
    applied_node_ids = applied_node_ids(projection)
    predecessors = active_predecessors(graph)

    Enum.reduce(graph.active_node_ids, {MapSet.new(), MapSet.new()}, fn node_id,
                                                                        {ready, blocked} ->
      dependency_ordered? = Map.get(graph.provenance.nodes, node_id) == :dependency_ordered

      cond do
        not dependency_ordered? or MapSet.member?(applied_node_ids, node_id) ->
          {ready, blocked}

        MapSet.subset?(Map.get(predecessors, node_id, MapSet.new()), applied_node_ids) ->
          {MapSet.put(ready, node_id), blocked}

        true ->
          {ready, MapSet.put(blocked, node_id)}
      end
    end)
  end

  defp applied_node_ids(%Projection{} = projection) do
    projection
    |> Projection.planned_runnables()
    |> Enum.reduce(MapSet.new(), fn runnable, node_ids ->
      runnable_key = MapField.get(runnable, :runnable_key)
      node_id = MapField.get(runnable, :step)

      if is_binary(node_id) and
           MapSet.member?(projection.applied_runnable_keys, runnable_key) do
        MapSet.put(node_ids, node_id)
      else
        node_ids
      end
    end)
  end

  defp active_predecessors(graph) do
    Enum.reduce(graph.active_edge_ids, %{}, fn edge_id, predecessors ->
      edge = Map.get(graph.topology.edges, edge_id, %{})
      source = MapField.get(edge, :from)
      target = MapField.get(edge, :to)

      if is_binary(source) and is_binary(target) do
        Map.update(predecessors, target, MapSet.new([source]), &MapSet.put(&1, source))
      else
        predecessors
      end
    end)
  end

  defp reconciliation_status(_storage, _projection, graph, _default_queue)
       when map_size(graph.mutation_history) == 0 do
    :not_required
  end

  defp reconciliation_status(storage, projection, graph, default_queue) do
    runnables = dependency_runnables(projection, graph)
    dispatchable_keys = Projection.dispatchable_runnable_keys(projection)

    runnables
    |> Enum.filter(&MapSet.member?(dispatchable_keys, MapField.get(&1, :runnable_key)))
    |> Enum.group_by(&(MapField.get(&1, :queue) || default_queue))
    |> missing_dispatch_status(storage)
  end

  defp dependency_runnables(projection, graph) do
    projection
    |> Projection.planned_runnables()
    |> Enum.filter(fn runnable ->
      Map.get(graph.provenance.nodes, MapField.get(runnable, :step)) == :dependency_ordered
    end)
  end

  defp missing_dispatch_status(queues, storage) do
    Enum.reduce_while(queues, :completed, fn {queue, runnables}, _status ->
      case DispatchAgent.rebuild(storage, queue) do
        {:ok, dispatch_agent} ->
          queue_dispatch_status(dispatch_agent, runnables)

        {:error, _reason} ->
          {:halt, :unknown}
      end
    end)
  end

  defp queue_dispatch_status(dispatch_agent, runnables) do
    attempts = dispatch_agent.state.projection.attempts

    if Enum.any?(runnables, &missing_attempt?(&1, attempts)) do
      {:halt, :required}
    else
      {:cont, :completed}
    end
  end

  defp missing_attempt?(runnable, attempts) do
    runnable_key = MapField.get(runnable, :runnable_key)
    not is_binary(runnable_key) or not Map.has_key?(attempts, runnable_key)
  end

  defp provenance(provenance) when is_map(provenance) do
    %{
      nodes: provenance_entries(MapField.get(provenance, :nodes, %{})),
      edges: provenance_entries(MapField.get(provenance, :edges, %{}))
    }
  end

  defp provenance(_provenance) do
    %{nodes: [], edges: []}
  end

  defp provenance_entries(entries) when is_map(entries) do
    entries
    |> Enum.flat_map(fn {id, value} ->
      case {id, normalize_provenance(value)} do
        {id, provenance} when is_binary(id) and not is_nil(provenance) ->
          [%{id: id, provenance: provenance}]

        _invalid ->
          []
      end
    end)
    |> Enum.sort_by(& &1.id)
    |> Enum.take(@identity_limit)
  end

  defp provenance_entries(entries) when is_list(entries) do
    entries
    |> Enum.flat_map(fn entry ->
      id = MapField.get(entry, :id)
      value = normalize_provenance(MapField.get(entry, :provenance))

      if is_binary(id) and not is_nil(value) do
        [%{id: id, provenance: value}]
      else
        []
      end
    end)
    |> Enum.uniq_by(& &1.id)
    |> Enum.sort_by(& &1.id)
    |> Enum.take(@identity_limit)
  end

  defp provenance_entries(_entries) do
    []
  end

  defp normalize_provenance(value) when value in @provenance_values do
    value
  end

  defp normalize_provenance("legacy_eager") do
    :legacy_eager
  end

  defp normalize_provenance("dependency_ordered") do
    :dependency_ordered
  end

  defp normalize_provenance(_value) do
    nil
  end

  defp identities(%MapSet{} = values) do
    values
    |> MapSet.to_list()
    |> identities()
  end

  defp identities(values) when is_list(values) do
    values
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.sort_by(& &1)
    |> Enum.take(@identity_limit)
  end

  defp identities(_values) do
    []
  end

  defp mutation_history(history) when is_map(history) do
    history
    |> Enum.map(fn {mutation_id, entry} ->
      if is_map(entry), do: Map.put_new(entry, :mutation_id, mutation_id), else: entry
    end)
    |> mutation_history()
  end

  defp mutation_history(history) when is_list(history) do
    history
    |> Enum.flat_map(&mutation_summary/1)
    |> Enum.sort_by(&{Map.get(&1, :result_version, -1), Map.get(&1, :mutation_id, "")})
    |> Enum.take(-@history_limit)
  end

  defp mutation_history(_history) do
    []
  end

  defp mutation_summary(entry) when is_map(entry) do
    mutation_id = MapField.get(entry, :mutation_id)

    if is_binary(mutation_id) do
      [
        compact(%{
          mutation_id: mutation_id,
          origin: binary_value(entry, :origin),
          expected_version: version_value(entry, :expected_version),
          result_version: version_value(entry, :result_version),
          occurred_at: datetime_value(entry, :occurred_at),
          added_node_ids: mutation_ids(entry, :added_node_ids, :additions, :node),
          added_edge_ids: mutation_ids(entry, :added_edge_ids, :additions, :edge),
          removed_node_ids: mutation_ids(entry, :removed_node_ids, :removals, :node),
          removed_edge_ids: mutation_ids(entry, :removed_edge_ids, :removals, :edge)
        })
      ]
    else
      []
    end
  end

  defp mutation_summary(_entry) do
    []
  end

  defp mutation_ids(entry, summary_field, operations_field, kind) do
    case MapField.get(entry, summary_field) do
      values when is_list(values) -> identities(values)
      _missing -> operation_ids(entry, operations_field, kind)
    end
  end

  defp operation_ids(entry, field, kind) do
    case MapField.get(entry, field) do
      operations when is_list(operations) ->
        operations
        |> Enum.filter(&(normalize_kind(MapField.get(&1, :kind)) == kind))
        |> Enum.map(&MapField.get(&1, :id))
        |> identities()

      _invalid ->
        nil
    end
  end

  defp normalize_kind(kind) when kind in [:node, :edge] do
    kind
  end

  defp normalize_kind("node") do
    :node
  end

  defp normalize_kind("edge") do
    :edge
  end

  defp normalize_kind(_kind) do
    nil
  end

  defp normalize_reconciliation_status(status) when status in @reconciliation_statuses do
    status
  end

  defp normalize_reconciliation_status("not_required") do
    :not_required
  end

  defp normalize_reconciliation_status("required") do
    :required
  end

  defp normalize_reconciliation_status("completed") do
    :completed
  end

  defp normalize_reconciliation_status("unknown") do
    :unknown
  end

  defp normalize_reconciliation_status(_status) do
    :unknown
  end

  defp valid_version(value) when is_integer(value) and value >= 0 do
    value
  end

  defp valid_version(_value) do
    0
  end

  defp version_value(entry, field) do
    case MapField.get(entry, field) do
      value when is_integer(value) and value >= 0 -> value
      _invalid -> nil
    end
  end

  defp binary_value(entry, field) do
    case MapField.get(entry, field) do
      value when is_binary(value) -> value
      _invalid -> nil
    end
  end

  defp datetime_value(entry, field) do
    case MapField.get(entry, field) do
      %DateTime{} = value -> value
      _invalid -> nil
    end
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
