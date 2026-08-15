defmodule Jizoku.Runtime.Journal.GraphMutation.ValidationContext do
  @moduledoc false

  alias Jizoku.GraphMutation.Limits
  alias Jizoku.Runtime.DispatchProtocol.ActionAttempt
  alias Jizoku.Runtime.WorkflowAgent.Projection.GraphState

  @type string_set :: MapSet.t(String.t())

  @type t :: %__MODULE__{
          graph: GraphState.t(),
          limits: Limits.t(),
          declared_node_ids: string_set(),
          declared_edge_ids: string_set(),
          legacy_node_ids: string_set(),
          legacy_edge_ids: string_set(),
          node_runnable_keys: %{optional(String.t()) => string_set()},
          applied_runnable_keys: string_set(),
          ready_node_ids: string_set(),
          blocked_node_ids: string_set(),
          dispatch_attempts: [ActionAttempt.t()],
          retry_node_ids: string_set(),
          compensation_node_ids: string_set(),
          terminal?: boolean()
        }

  @enforce_keys [:graph, :limits]
  defstruct [
    :graph,
    :limits,
    declared_node_ids: MapSet.new(),
    declared_edge_ids: MapSet.new(),
    legacy_node_ids: MapSet.new(),
    legacy_edge_ids: MapSet.new(),
    node_runnable_keys: %{},
    applied_runnable_keys: MapSet.new(),
    ready_node_ids: MapSet.new(),
    blocked_node_ids: MapSet.new(),
    dispatch_attempts: [],
    retry_node_ids: MapSet.new(),
    compensation_node_ids: MapSet.new(),
    terminal?: false
  ]

  @doc false
  @spec new(GraphState.t(), Limits.t(), keyword()) :: t()
  def new(%GraphState{} = graph, %Limits{} = limits, attrs \\ []) when is_list(attrs) do
    %__MODULE__{
      graph: graph,
      limits: limits,
      declared_node_ids: string_set(attrs[:declared_node_ids]),
      declared_edge_ids: string_set(attrs[:declared_edge_ids]),
      legacy_node_ids: legacy_ids(graph.provenance.nodes),
      legacy_edge_ids: legacy_ids(graph.provenance.edges),
      node_runnable_keys: node_runnable_keys(attrs[:node_runnable_keys]),
      applied_runnable_keys: string_set(attrs[:applied_runnable_keys]),
      ready_node_ids: string_set(attrs[:ready_node_ids]),
      blocked_node_ids: string_set(attrs[:blocked_node_ids]),
      dispatch_attempts: attempts(attrs[:dispatch_attempts]),
      retry_node_ids: string_set(attrs[:retry_node_ids]),
      compensation_node_ids: string_set(attrs[:compensation_node_ids]),
      terminal?: attrs[:terminal?] == true
    }
  end

  defp legacy_ids(provenance) when is_map(provenance) do
    Enum.reduce(provenance, MapSet.new(), fn
      {id, :legacy_eager}, ids when is_binary(id) ->
        MapSet.put(ids, id)

      _entry, ids ->
        ids
    end)
  end

  defp node_runnable_keys(keys) when is_map(keys) do
    Map.new(keys, fn {node_id, runnable_keys} ->
      {node_id, string_set(runnable_keys)}
    end)
  end

  defp node_runnable_keys(_keys) do
    %{}
  end

  defp attempts(attempts) when is_list(attempts) do
    Enum.filter(attempts, &match?(%ActionAttempt{}, &1))
  end

  defp attempts(_attempts) do
    []
  end

  defp string_set(%MapSet{} = values) do
    values
  end

  defp string_set(values) when is_list(values) do
    values
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
    |> MapSet.new()
  end

  defp string_set(_values) do
    MapSet.new()
  end
end
