defmodule Squidie.Runtime.Journal.GraphMutation.Validator do
  @moduledoc false

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Operation
  alias Squidie.Runtime.Journal.GraphMutation.ValidationContext

  @type validation_result :: {:ok, :valid | :duplicate} | {:error, term()}

  @doc false
  @spec validate(GraphMutation.t(), ValidationContext.t()) :: validation_result()
  def validate(%GraphMutation{} = mutation, %ValidationContext{} = context) do
    fingerprint = GraphMutation.fingerprint(mutation)

    case Map.get(context.graph.mutation_history, mutation.mutation_id) do
      %{fingerprint: ^fingerprint} ->
        {:ok, :duplicate}

      nil ->
        validate_new(mutation, context)

      _conflicting_history ->
        invalid(:mutation_id, {:conflict, mutation.mutation_id})
    end
  end

  defp validate_new(mutation, context) do
    with :ok <- expected_version(mutation, context),
         :ok <- active_run(context),
         :ok <- unique_operation_ids(mutation),
         :ok <- immutable_removals(mutation, context),
         :ok <- available_addition_ids(mutation, context),
         :ok <- existing_removals(mutation, context),
         :ok <- within_limits(mutation, context),
         :ok <- removable_nodes(mutation, context),
         :ok <- stable_existing_targets(mutation, context) do
      {:ok, :valid}
    end
  end

  defp expected_version(mutation, context) do
    if mutation.expected_version == context.graph.version do
      :ok
    else
      invalid(:expected_version, {:stale, context.graph.version})
    end
  end

  defp active_run(%ValidationContext{terminal?: false}) do
    :ok
  end

  defp active_run(%ValidationContext{}) do
    invalid(:run, :terminal)
  end

  defp unique_operation_ids(mutation) do
    mutation.additions
    |> Kernel.++(mutation.removals)
    |> Enum.reduce_while(MapSet.new(), fn operation, seen ->
      identity = {operation.kind, operation.id}

      if MapSet.member?(seen, identity) do
        {:halt, invalid(:operations, {:duplicate_identity, operation.kind, operation.id})}
      else
        {:cont, MapSet.put(seen, identity)}
      end
    end)
    |> case do
      %MapSet{} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp immutable_removals(mutation, context) do
    Enum.reduce_while(mutation.removals, :ok, fn operation, :ok ->
      case immutable_identity(operation, context) do
        nil -> {:cont, :ok}
        reason -> {:halt, invalid(:removals, reason)}
      end
    end)
  end

  defp immutable_identity(%Operation{kind: :node, id: id}, context) do
    immutable_identity(id, :node, context.declared_node_ids, context.legacy_node_ids)
  end

  defp immutable_identity(%Operation{kind: :edge, id: id}, context) do
    immutable_identity(id, :edge, context.declared_edge_ids, context.legacy_edge_ids)
  end

  defp immutable_identity(id, kind, declared_ids, legacy_ids) do
    cond do
      MapSet.member?(declared_ids, id) -> {:declared_identity, kind, id}
      MapSet.member?(legacy_ids, id) -> {:legacy_identity, kind, id}
      true -> nil
    end
  end

  defp available_addition_ids(mutation, context) do
    Enum.reduce_while(mutation.additions, :ok, fn operation, :ok ->
      case unavailable_addition_identity(operation, context) do
        nil -> {:cont, :ok}
        reason -> {:halt, invalid(:additions, reason)}
      end
    end)
  end

  defp unavailable_addition_identity(%Operation{kind: :node, id: id}, context) do
    unavailable_identity(
      id,
      :node,
      context.declared_node_ids,
      context.legacy_node_ids,
      context.graph.reserved_node_ids
    )
  end

  defp unavailable_addition_identity(%Operation{kind: :edge, id: id}, context) do
    unavailable_identity(
      id,
      :edge,
      context.declared_edge_ids,
      context.legacy_edge_ids,
      context.graph.reserved_edge_ids
    )
  end

  defp unavailable_identity(id, kind, declared_ids, legacy_ids, reserved_ids) do
    cond do
      MapSet.member?(declared_ids, id) -> {:declared_identity, kind, id}
      MapSet.member?(legacy_ids, id) -> {:legacy_identity, kind, id}
      MapSet.member?(reserved_ids, id) -> {:reserved_identity, kind, id}
      true -> nil
    end
  end

  defp existing_removals(mutation, context) do
    Enum.reduce_while(mutation.removals, :ok, fn operation, :ok ->
      if active_identity?(operation, context) do
        {:cont, :ok}
      else
        {:halt, invalid(:removals, {:unknown_identity, operation.kind, operation.id})}
      end
    end)
  end

  defp active_identity?(%Operation{kind: :node, id: id}, context) do
    MapSet.member?(context.graph.active_node_ids, id)
  end

  defp active_identity?(%Operation{kind: :edge, id: id}, context) do
    MapSet.member?(context.graph.active_edge_ids, id)
  end

  defp within_limits(mutation, context) do
    counts = operation_counts(mutation)
    limits = context.limits

    with :ok <- limit(:max_nodes_per_mutation, counts.nodes, limits.max_nodes_per_mutation),
         :ok <- limit(:max_edges_per_mutation, counts.edges, limits.max_edges_per_mutation),
         :ok <-
           limit(
             :max_active_nodes_per_run,
             result_count(context.graph.active_node_ids, mutation, :node),
             limits.max_active_nodes_per_run
           ) do
      limit(
        :max_active_edges_per_run,
        result_count(context.graph.active_edge_ids, mutation, :edge),
        limits.max_active_edges_per_run
      )
    end
  end

  defp operation_counts(mutation) do
    Enum.reduce(mutation.additions ++ mutation.removals, %{nodes: 0, edges: 0}, fn
      %Operation{kind: :node}, counts ->
        Map.update!(counts, :nodes, &(&1 + 1))

      %Operation{kind: :edge}, counts ->
        Map.update!(counts, :edges, &(&1 + 1))
    end)
  end

  defp result_count(active_ids, mutation, kind) do
    added_ids = operation_ids(mutation.additions, kind)
    removed_ids = operation_ids(mutation.removals, kind)

    active_ids
    |> MapSet.difference(removed_ids)
    |> MapSet.union(added_ids)
    |> MapSet.size()
  end

  defp operation_ids(operations, kind) do
    operations
    |> Enum.filter(&(&1.kind == kind))
    |> Enum.map(& &1.id)
    |> MapSet.new()
  end

  defp limit(_name, actual, maximum) when actual <= maximum do
    :ok
  end

  defp limit(name, actual, maximum) do
    invalid(:limits, {name, actual, maximum})
  end

  defp removable_nodes(mutation, context) do
    removed_edge_ids = operation_ids(mutation.removals, :edge)

    mutation.removals
    |> Enum.filter(&(&1.kind == :node))
    |> Enum.reduce_while(:ok, fn operation, :ok ->
      case node_removal_reason(operation.id, removed_edge_ids, context) do
        nil -> {:cont, :ok}
        reason -> {:halt, invalid(:removals, {:node_not_removable, operation.id, reason})}
      end
    end)
  end

  defp node_removal_reason(node_id, removed_edge_ids, context) do
    runnable_keys = Map.get(context.node_runnable_keys, node_id, MapSet.new())

    cond do
      not MapSet.member?(context.blocked_node_ids, node_id) ->
        :not_blocked

      intersects?(runnable_keys, context.applied_runnable_keys) ->
        :applied

      reason = dispatch_reason(node_id, context.dispatch_attempts) ->
        reason

      MapSet.member?(context.retry_node_ids, node_id) ->
        :retry_pending

      MapSet.member?(context.compensation_node_ids, node_id) ->
        :compensation_pending

      incident_edges = active_incident_edges(node_id, removed_edge_ids, context) ->
        {:incident_edges_active, incident_edges}

      true ->
        nil
    end
  end

  defp dispatch_reason(node_id, attempts) do
    statuses =
      attempts
      |> Enum.filter(&(&1.step == node_id))
      |> Enum.map(& &1.status)
      |> MapSet.new()

    cond do
      MapSet.member?(statuses, :failed) -> :failed
      MapSet.member?(statuses, :completed) -> :completed
      MapSet.member?(statuses, :claimed) -> :claimed
      MapSet.size(statuses) > 0 -> :dispatched
      true -> nil
    end
  end

  defp active_incident_edges(node_id, removed_edge_ids, context) do
    incident_edges =
      context.graph.topology.edges
      |> Enum.filter(fn {edge_id, edge} ->
        not MapSet.member?(removed_edge_ids, edge_id) and
          (Map.get(edge, :from) == node_id or Map.get(edge, :to) == node_id)
      end)
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()

    case incident_edges do
      [] -> nil
      edges -> edges
    end
  end

  defp stable_existing_targets(mutation, context) do
    mutation.additions
    |> Enum.filter(&(&1.kind == :edge))
    |> Enum.reduce_while(:ok, &stable_existing_target(&1, &2, context))
  end

  defp stable_existing_target(edge, :ok, context) do
    if existing_node?(edge.to, context) do
      validate_existing_target(edge, context)
    else
      {:cont, :ok}
    end
  end

  defp existing_node?(node_id, context) do
    MapSet.member?(context.graph.active_node_ids, node_id) or
      MapSet.member?(context.declared_node_ids, node_id)
  end

  defp validate_existing_target(edge, context) do
    case started_node_reason(edge.to, context) do
      nil -> {:cont, :ok}
      reason -> {:halt, predecessor_error(edge, reason)}
    end
  end

  defp started_node_reason(node_id, context) do
    runnable_keys = Map.get(context.node_runnable_keys, node_id, MapSet.new())

    cond do
      MapSet.member?(context.ready_node_ids, node_id) -> :ready
      intersects?(runnable_keys, context.applied_runnable_keys) -> :completed
      true -> dispatch_reason(node_id, context.dispatch_attempts)
    end
  end

  defp predecessor_error(edge, reason) do
    invalid(
      :additions,
      {:predecessor_for_started_node, edge.id, edge.to, reason}
    )
  end

  defp intersects?(left, right) do
    not MapSet.disjoint?(left, right)
  end

  defp invalid(field, reason) do
    {:error, {:invalid_graph_mutation, {field, reason}}}
  end
end
