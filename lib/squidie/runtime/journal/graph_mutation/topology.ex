defmodule Squidie.Runtime.Journal.GraphMutation.Topology.Result do
  @moduledoc false

  @type string_set :: MapSet.t(String.t())

  @type t :: %__MODULE__{
          topology: %{
            required(:nodes) => %{optional(String.t()) => map()},
            required(:edges) => %{optional(String.t()) => map()}
          },
          active_node_ids: string_set(),
          active_edge_ids: string_set(),
          predecessors: %{optional(String.t()) => string_set()},
          ready_node_ids: string_set(),
          blocked_node_ids: string_set()
        }

  @enforce_keys [
    :topology,
    :active_node_ids,
    :active_edge_ids,
    :predecessors,
    :ready_node_ids,
    :blocked_node_ids
  ]
  defstruct @enforce_keys
end

defmodule Squidie.Runtime.Journal.GraphMutation.Topology do
  @moduledoc false

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Operation
  alias Squidie.Runtime.Journal.GraphMutation.Topology.Result
  alias Squidie.Runtime.Journal.GraphMutation.ValidationContext
  alias Squidie.Runtime.Journal.GraphMutation.Validator
  alias Squidie.Workflow.DependencyGraph

  @type string_set :: MapSet.t(String.t())
  @type adjacency :: %{optional(String.t()) => string_set()}

  @type evaluation_result ::
          {:ok, Result.t() | :duplicate}
          | {:error, {:invalid_graph_mutation, {atom(), term()}}}

  @doc false
  @spec evaluate(GraphMutation.t(), ValidationContext.t()) :: evaluation_result()
  def evaluate(%GraphMutation{} = mutation, %ValidationContext{} = context) do
    case Validator.validate(mutation, context) do
      {:ok, :duplicate} ->
        {:ok, :duplicate}

      {:ok, :valid} ->
        mutation
        |> apply_candidate(context)
        |> validate_candidate(mutation, context)

      {:error, _reason} = error ->
        error
    end
  end

  defp apply_candidate(mutation, context) do
    initial_candidate = %{
      topology: context.graph.topology,
      active_node_ids: context.graph.active_node_ids,
      active_edge_ids: context.graph.active_edge_ids
    }

    candidate = Enum.reduce(mutation.additions, initial_candidate, &add_operation/2)
    Enum.reduce(mutation.removals, candidate, &remove_operation/2)
  end

  defp add_operation(%Operation{kind: :node} = operation, candidate) do
    %{
      candidate
      | topology:
          Map.update!(candidate.topology, :nodes, fn nodes ->
            Map.put(nodes, operation.id, Operation.to_map(operation))
          end),
        active_node_ids: MapSet.put(candidate.active_node_ids, operation.id)
    }
  end

  defp add_operation(%Operation{kind: :edge} = operation, candidate) do
    %{
      candidate
      | topology:
          Map.update!(candidate.topology, :edges, fn edges ->
            Map.put(edges, operation.id, Operation.to_map(operation))
          end),
        active_edge_ids: MapSet.put(candidate.active_edge_ids, operation.id)
    }
  end

  defp remove_operation(%Operation{kind: :node, id: id}, candidate) do
    %{
      candidate
      | topology: Map.update!(candidate.topology, :nodes, &Map.delete(&1, id)),
        active_node_ids: MapSet.delete(candidate.active_node_ids, id)
    }
  end

  defp remove_operation(%Operation{kind: :edge, id: id}, candidate) do
    %{
      candidate
      | topology: Map.update!(candidate.topology, :edges, &Map.delete(&1, id)),
        active_edge_ids: MapSet.delete(candidate.active_edge_ids, id)
    }
  end

  defp validate_candidate(candidate, mutation, context) do
    applied_node_ids = applied_node_ids(context)

    with :ok <- applied_declared_origin(mutation.origin, context, applied_node_ids),
         :ok <- active_endpoints(candidate.topology, context),
         {:ok, adjacency} <- acyclic_adjacency(candidate.topology),
         :ok <- reachable_nodes(candidate.topology, adjacency, context, applied_node_ids) do
      {:ok, build_result(candidate, applied_node_ids)}
    end
  end

  defp applied_declared_origin(origin, context, applied_node_ids) do
    cond do
      not MapSet.member?(context.declared_node_ids, origin) ->
        invalid(:origin, {:unknown_declared, origin})

      not MapSet.member?(applied_node_ids, origin) ->
        invalid(:origin, {:unapplied_declared, origin})

      true ->
        :ok
    end
  end

  defp active_endpoints(topology, context) do
    dynamic_node_ids =
      topology.nodes
      |> Map.keys()
      |> MapSet.new()

    endpoint_ids = MapSet.union(dynamic_node_ids, context.declared_node_ids)

    topology.edges
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while(:ok, fn {edge_id, edge}, :ok ->
      validate_edge_endpoints(edge_id, edge, endpoint_ids, dynamic_node_ids)
    end)
  end

  defp validate_edge_endpoints(edge_id, edge, endpoint_ids, dynamic_node_ids) do
    case Enum.find([Map.get(edge, :from), Map.get(edge, :to)], fn endpoint ->
           not MapSet.member?(endpoint_ids, endpoint)
         end) do
      nil ->
        validate_edge_target(edge_id, edge, dynamic_node_ids)

      endpoint ->
        {:halt, invalid(:topology, {:unknown_endpoint, edge_id, endpoint})}
    end
  end

  defp validate_edge_target(edge_id, edge, dynamic_node_ids) do
    if MapSet.member?(dynamic_node_ids, Map.get(edge, :to)) do
      {:cont, :ok}
    else
      {:halt, invalid(:topology, {:invalid_target, edge_id, Map.get(edge, :to)})}
    end
  end

  defp acyclic_adjacency(topology) do
    vertices = topology_vertices(topology)
    adjacency = adjacency(vertices, topology.edges)

    if DependencyGraph.acyclic?(adjacency) do
      {:ok, adjacency}
    else
      invalid(:topology, :cycle)
    end
  end

  defp topology_vertices(topology) do
    Enum.reduce(topology.edges, MapSet.new(Map.keys(topology.nodes)), fn {_id, edge}, vertices ->
      vertices
      |> MapSet.put(Map.get(edge, :from))
      |> MapSet.put(Map.get(edge, :to))
    end)
  end

  @spec adjacency(string_set(), map()) :: adjacency()
  defp adjacency(vertices, edges) do
    adjacency = Map.new(vertices, &{&1, MapSet.new()})

    Enum.reduce(edges, adjacency, fn {_id, edge}, graph ->
      Map.update!(graph, Map.get(edge, :from), &MapSet.put(&1, Map.get(edge, :to)))
    end)
  end

  defp reachable_nodes(topology, adjacency, context, applied_node_ids) do
    roots = MapSet.intersection(context.declared_node_ids, applied_node_ids)

    reachable =
      roots
      |> MapSet.to_list()
      |> reachable_from(adjacency, %{})
      |> Map.keys()
      |> MapSet.new()

    node_ids = MapSet.new(Map.keys(topology.nodes))

    unreachable =
      node_ids
      |> MapSet.difference(reachable)
      |> MapSet.to_list()
      |> Enum.sort()

    case unreachable do
      [] ->
        :ok

      unreachable ->
        invalid(:topology, {:unreachable_nodes, unreachable})
    end
  end

  @spec reachable_from([String.t()], adjacency(), %{optional(String.t()) => :visited}) :: %{
          optional(String.t()) => :visited
        }
  defp reachable_from([], _adjacency, visited) do
    visited
  end

  defp reachable_from([node_id | pending], adjacency, visited) do
    if Map.has_key?(visited, node_id) do
      reachable_from(pending, adjacency, visited)
    else
      successors =
        adjacency
        |> Map.get(node_id, MapSet.new())
        |> MapSet.to_list()

      reachable_from(successors ++ pending, adjacency, Map.put(visited, node_id, :visited))
    end
  end

  defp build_result(candidate, applied_node_ids) do
    predecessors = predecessors(candidate.topology)

    {ready_node_ids, blocked_node_ids} =
      Enum.reduce(predecessors, {MapSet.new(), MapSet.new()}, fn
        {node_id, node_predecessors}, {ready, blocked} ->
          cond do
            MapSet.member?(applied_node_ids, node_id) ->
              {ready, blocked}

            MapSet.subset?(node_predecessors, applied_node_ids) ->
              {MapSet.put(ready, node_id), blocked}

            true ->
              {ready, MapSet.put(blocked, node_id)}
          end
      end)

    %Result{
      topology: candidate.topology,
      active_node_ids: candidate.active_node_ids,
      active_edge_ids: candidate.active_edge_ids,
      predecessors: predecessors,
      ready_node_ids: ready_node_ids,
      blocked_node_ids: blocked_node_ids
    }
  end

  defp predecessors(topology) do
    predecessors = Map.new(Map.keys(topology.nodes), &{&1, MapSet.new()})

    Enum.reduce(topology.edges, predecessors, fn {_id, edge}, node_predecessors ->
      target = Map.get(edge, :to)

      if Map.has_key?(node_predecessors, target) do
        Map.update!(node_predecessors, target, &MapSet.put(&1, Map.get(edge, :from)))
      else
        node_predecessors
      end
    end)
  end

  defp applied_node_ids(context) do
    Enum.reduce(context.node_runnable_keys, MapSet.new(), fn {node_id, runnable_keys}, applied ->
      if MapSet.disjoint?(runnable_keys, context.applied_runnable_keys) do
        applied
      else
        MapSet.put(applied, node_id)
      end
    end)
  end

  defp invalid(field, reason) do
    {:error, {:invalid_graph_mutation, {field, reason}}}
  end
end
