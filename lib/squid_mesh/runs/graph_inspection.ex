defmodule SquidMesh.Runs.GraphInspection do
  @moduledoc """
  Graph-oriented inspection output for one workflow run.

  This projection is read-only and backend-neutral. It is built from the
  journal inspection data returned by `SquidMesh.inspect_run/2`, then overlays
  the declared workflow graph when the workflow module can still be loaded.
  """

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runs.GraphInspection.Edge
  alias SquidMesh.Runs.GraphInspection.Node
  alias SquidMesh.Workflow.Definition

  @type source :: :read_model

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: module() | String.t() | nil,
          definition_version: String.t() | nil,
          source: source(),
          status: atom(),
          current_node_id: String.t() | nil,
          current_node_ids: [String.t()],
          terminal?: boolean(),
          nodes: [Node.t()],
          edges: [Edge.t()],
          child_runs: [map()],
          child_links: [map()],
          dynamic_work: [map()],
          anomalies: [map()]
        }

  @enforce_keys [:run_id, :workflow, :source, :status, :terminal?]

  defstruct [
    :run_id,
    :workflow,
    :definition_version,
    :source,
    :status,
    :current_node_id,
    :terminal?,
    child_runs: [],
    child_links: [],
    current_node_ids: [],
    nodes: [],
    edges: [],
    anomalies: [],
    dynamic_work: []
  ]

  @doc false
  @spec from_snapshot(Snapshot.t(), keyword()) :: t()
  def from_snapshot(%Snapshot{} = snapshot, opts) when is_list(opts) do
    source = Keyword.get(opts, :source, :read_model)
    include_details? = Keyword.get(opts, :include_details, false)
    definition = Keyword.get(opts, :definition) || load_definition(snapshot.workflow)
    current_node_ids = snapshot_current_node_ids(snapshot)
    current_node_id = List.first(current_node_ids)
    initial_nodes = snapshot_nodes(snapshot, definition, include_details?)
    nodes = mark_current_nodes(initial_nodes, current_node_ids)

    %__MODULE__{
      run_id: snapshot.run_id,
      workflow: snapshot.workflow,
      definition_version: snapshot.definition_version,
      source: source,
      status: snapshot.status,
      current_node_id: current_node_id,
      current_node_ids: current_node_ids,
      terminal?: snapshot.terminal?,
      nodes: nodes,
      edges: graph_edges(definition, nodes) ++ dynamic_edges(snapshot.dynamic_work),
      child_runs: snapshot.child_runs,
      child_links: child_links(snapshot.child_runs),
      dynamic_work: snapshot.dynamic_work,
      anomalies: sanitize_anomalies(snapshot.anomalies)
    }
  end

  @doc """
  Converts graph inspection data into a stable map for host UI serializers.

  `inspect_run_graph/2` continues to return the graph structs for existing
  callers. Host apps that need a JSON-ready dashboard payload can call this
  function at their HTTP or LiveView boundary after applying their own
  authorization and redaction policy.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = graph) do
    %{
      run_id: graph.run_id,
      workflow: workflow_name(graph.workflow),
      definition_version: graph.definition_version,
      source: graph.source,
      status: graph.status,
      current_node_id: graph.current_node_id,
      current_node_ids: graph.current_node_ids,
      terminal?: graph.terminal?,
      nodes: Enum.map(graph.nodes, &Node.to_map/1),
      edges: Enum.map(graph.edges, &Edge.to_map/1),
      child_runs: graph.child_runs,
      child_links: graph.child_links,
      dynamic_work: graph.dynamic_work,
      anomalies: graph.anomalies
    }
  end

  defp child_links(child_runs) when is_list(child_runs) do
    Enum.flat_map(child_runs, &child_link/1)
  end

  defp child_links(_child_runs), do: []

  defp child_link(child_run) when is_map(child_run) do
    origin = field(child_run, :origin)
    child_run_id = field(child_run, :child_run_id)
    from = origin_step(origin)

    if is_binary(child_run_id) and is_binary(from) and from != "" do
      [
        compact(%{
          id: Enum.join([from, "child_run", child_run_id], ":"),
          from: from,
          to: child_run_id,
          type: :child_run,
          status: :linked,
          child_run_id: child_run_id,
          child_workflow: field(child_run, :child_workflow),
          child_trigger: field(child_run, :child_trigger),
          child_key: field(child_run, :child_key),
          origin: origin,
          metadata: field(child_run, :metadata, %{}),
          started_at: field(child_run, :started_at)
        })
      ]
    else
      []
    end
  end

  defp child_link(_child_run), do: []

  defp origin_step(origin) when is_map(origin) do
    origin
    |> field(:step)
    |> normalize_id()
  end

  defp origin_step(_origin), do: nil

  defp snapshot_nodes(%Snapshot{} = snapshot, definition, include_details?) do
    attempts_by_step = Enum.group_by(snapshot.attempts, &Map.fetch!(&1, :step))

    step_sources =
      snapshot.attempts ++ snapshot.planned_runnables ++ snapshot.pending_dispatches

    step_sources_by_id = Enum.group_by(step_sources, &snapshot_step_id/1)
    node_ids = ordered_node_ids(definition, step_sources, &snapshot_step_id/1)

    declared_nodes =
      Enum.map(node_ids, fn node_id ->
        attempts = Map.get(attempts_by_step, node_id, [])

        %Node{
          id: node_id,
          action: snapshot_node_action(Map.get(step_sources_by_id, node_id, [])),
          status: snapshot_node_status(snapshot, node_id, attempts),
          current?: false,
          input: detail(include_details?, latest_attempt_value(attempts, :input)),
          output: detail(include_details?, latest_attempt_value(attempts, :result)),
          error: detail(include_details?, latest_attempt_value(attempts, :error)),
          recovery: latest_attempt_value(attempts, :recovery),
          transition:
            Definition.deserialize_transition_decision(
              definition,
              latest_attempt_value(attempts, :transition)
            ),
          manual_state: detail(include_details?, snapshot_manual_state(snapshot, node_id)),
          attempts: detail(include_details?, Enum.map(attempts, &snapshot_attempt/1), [])
        }
      end)

    declared_nodes ++ dynamic_nodes(snapshot.dynamic_work)
  end

  defp dynamic_nodes(dynamic_work) when is_list(dynamic_work) do
    Enum.flat_map(dynamic_work, fn dynamic ->
      origin = Map.get(dynamic, :origin)

      dynamic
      |> Map.get(:nodes, [])
      |> Enum.map(fn node ->
        %Node{
          id: Map.fetch!(node, :id),
          action: Map.get(node, :action),
          status: Map.get(node, :status, :recorded),
          current?: false,
          dynamic?: true,
          origin: origin,
          metadata: Map.get(node, :metadata, %{})
        }
      end)
    end)
  end

  defp dynamic_edges(dynamic_work) when is_list(dynamic_work) do
    dynamic_work
    |> Enum.flat_map(&Map.get(&1, :edges, []))
    |> Enum.map(fn edge ->
      %Edge{
        id: Map.fetch!(edge, :id),
        from: Map.fetch!(edge, :from),
        to: Map.fetch!(edge, :to),
        type: Map.get(edge, :type, :dynamic),
        status: Map.get(edge, :status, :pending)
      }
    end)
  end

  defp snapshot_node_status(%Snapshot{} = snapshot, node_id, attempts) do
    cond do
      manual_node?(snapshot, node_id) ->
        :paused

      Enum.any?(attempts, &(Map.get(&1, :status) == :completed and Map.get(&1, :applied?))) ->
        :completed

      Enum.any?(attempts, &(Map.get(&1, :status) == :claimed)) ->
        :running

      Enum.any?(attempts, &(Map.get(&1, :status) == :retry_scheduled)) ->
        :retrying

      Enum.any?(attempts, &(Map.get(&1, :status) in [:available, :retry_scheduled])) ->
        :pending

      Enum.any?(attempts, &(Map.get(&1, :status) == :failed)) ->
        :failed

      true ->
        :waiting
    end
  end

  defp snapshot_attempt(attempt) do
    compact(%{
      attempt_number: Map.fetch!(attempt, :attempt_number),
      status: Map.fetch!(attempt, :status),
      error: Map.get(attempt, :error)
    })
  end

  defp snapshot_node_action(step_sources) do
    Enum.find_value(step_sources, fn
      %{action: action} when is_atom(action) or is_binary(action) ->
        action

      %{metadata: metadata} when is_map(metadata) ->
        metadata_action(metadata)

      _source ->
        nil
    end)
  end

  defp metadata_action(metadata) do
    case Map.get(metadata, :action) || Map.get(metadata, "action") do
      action when is_atom(action) or is_binary(action) -> action
      _other -> nil
    end
  end

  defp latest_attempt_value(attempts, key) do
    attempts
    |> Enum.reverse()
    |> Enum.find_value(&Map.get(&1, key))
  end

  defp snapshot_manual_state(%Snapshot{} = snapshot, node_id) do
    if manual_node?(snapshot, node_id), do: snapshot.manual_state, else: nil
  end

  defp manual_node?(%Snapshot{manual_state: %{step: step}}, node_id), do: step == node_id
  defp manual_node?(%Snapshot{}, _node_id), do: false

  defp snapshot_current_node_ids(%Snapshot{terminal?: true}), do: []

  defp snapshot_current_node_ids(%Snapshot{manual_state: %{step: step}}), do: [normalize_id(step)]

  defp snapshot_current_node_ids(%Snapshot{} = snapshot) do
    snapshot.visible_attempts
    |> Kernel.++(snapshot.expired_claims)
    |> Kernel.++(claimed_attempts(snapshot.attempts))
    |> Kernel.++(snapshot.scheduled_attempts)
    |> Enum.map(&normalize_id(Map.get(&1, :step)))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp graph_edges(nil, _nodes), do: []

  defp graph_edges(definition, nodes) do
    if Definition.dependency_mode?(definition) do
      dependency_edges(definition, nodes)
    else
      transition_edges(definition, nodes)
    end
  end

  defp transition_edges(definition, nodes) do
    nodes_by_id = Map.new(nodes, &{&1.id, &1})

    definition.transitions
    |> Enum.with_index()
    |> Enum.map(fn {transition, index} ->
      from = normalize_id(transition.from)
      to = normalize_id(transition.to)
      outcome = transition.on
      from_node = Map.get(nodes_by_id, from)
      conditional_group? = conditional_transition_group?(definition.transitions, transition)

      %Edge{
        id: transition_edge_id(from, outcome, to, transition, index),
        from: from,
        to: to,
        type: :transition,
        outcome: outcome,
        condition: Map.get(transition, :condition),
        recovery: Map.get(transition, :recovery),
        status: transition_edge_status(from_node, transition, conditional_group?)
      }
    end)
  end

  defp transition_edge_id(from, outcome, to, transition, index) do
    [from, outcome, to, transition_condition_id(Map.get(transition, :condition), index)]
    |> Enum.reject(&is_nil/1)
    |> Enum.join(":")
  end

  defp transition_condition_id(nil, _index), do: nil

  defp transition_condition_id(_condition, index) do
    "condition:#{index}"
  end

  defp conditional_transition_group?(transitions, transition) do
    Enum.any?(
      transitions,
      &(&1.from == transition.from and &1.on == transition.on and Map.has_key?(&1, :condition))
    )
  end

  defp transition_edge_status(nil, _transition, _conditional_group?), do: :pending

  defp transition_edge_status(%Node{transition: %{} = selected}, transition, _conditional_group?) do
    if selected_transition?(selected, transition), do: :selected, else: :skipped
  end

  defp transition_edge_status(%Node{status: :completed}, %{on: :ok}, true), do: :pending
  defp transition_edge_status(%Node{status: :failed}, %{on: :error}, true), do: :pending

  defp transition_edge_status(%Node{status: :completed}, %{on: :ok}, false), do: :selected
  defp transition_edge_status(%Node{status: :failed}, %{on: :error}, false), do: :selected

  defp transition_edge_status(%Node{status: status}, _transition, _conditional_group?)
       when status in [:completed, :failed], do: :skipped

  defp transition_edge_status(%Node{}, _transition, _conditional_group?), do: :pending

  defp selected_transition?(selected, transition) do
    normalize_id(Map.get(selected, :from)) == normalize_id(transition.from) and
      normalize_outcome(Map.get(selected, :on)) == transition.on and
      normalize_id(Map.get(selected, :to)) == normalize_id(transition.to) and
      normalize_condition(Map.get(selected, :condition)) ==
        normalize_condition(Map.get(transition, :condition))
  end

  defp normalize_outcome(outcome) when is_atom(outcome), do: outcome
  defp normalize_outcome("ok"), do: :ok
  defp normalize_outcome("error"), do: :error
  defp normalize_outcome(outcome), do: outcome

  defp normalize_condition(nil), do: nil

  defp normalize_condition(condition),
    do: SquidMesh.Workflow.TransitionCondition.serialize(condition)

  defp dependency_edges(definition, nodes) do
    nodes_by_id = Map.new(nodes, &{&1.id, &1})

    definition
    |> Definition.inspect_steps()
    |> Enum.flat_map(fn %{step: step, depends_on: dependencies} ->
      to = normalize_id(step)

      Enum.map(dependencies, fn dependency ->
        from = normalize_id(dependency)

        %Edge{
          id: Enum.join([from, "dependency", to], ":"),
          from: from,
          to: to,
          type: :dependency,
          status: dependency_edge_status(Map.get(nodes_by_id, from))
        }
      end)
    end)
  end

  defp dependency_edge_status(nil), do: :pending
  defp dependency_edge_status(%Node{status: :completed}), do: :selected
  defp dependency_edge_status(%Node{status: :failed}), do: :blocked
  defp dependency_edge_status(%Node{}), do: :pending

  defp claimed_attempts(attempts) do
    Enum.filter(attempts, &(Map.get(&1, :status) == :claimed))
  end

  defp mark_current_nodes(nodes, current_node_ids) do
    current_node_ids = MapSet.new(current_node_ids)

    Enum.map(nodes, fn %Node{} = node ->
      %Node{node | current?: MapSet.member?(current_node_ids, node.id)}
    end)
  end

  defp ordered_node_ids(nil, step_sources, step_id_fun) do
    step_sources
    |> Enum.map(step_id_fun)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp ordered_node_ids(definition, step_sources, step_id_fun) do
    declared_ids =
      definition
      |> Definition.inspect_steps()
      |> Enum.map(&normalize_id(&1.step))

    extra_ids =
      step_sources
      |> Enum.map(step_id_fun)
      |> Enum.reject(&(is_nil(&1) or &1 in declared_ids))
      |> Enum.uniq()

    declared_ids ++ extra_ids
  end

  defp snapshot_step_id(step_source) when is_map(step_source) do
    step_source
    |> Map.get(:step)
    |> normalize_id()
  end

  defp snapshot_step_id(_step_source), do: nil

  defp load_definition(workflow) when is_atom(workflow) do
    case Definition.load(workflow) do
      {:ok, definition} -> definition
      {:error, _reason} -> nil
    end
  end

  defp load_definition(workflow) when is_binary(workflow) do
    case Definition.load_serialized(workflow) do
      {:ok, _workflow, definition} -> definition
      {:error, _reason} -> nil
    end
  end

  defp load_definition(_workflow), do: nil

  defp workflow_name(workflow) when is_atom(workflow), do: Atom.to_string(workflow)
  defp workflow_name(workflow), do: workflow

  defp field(map, key, default \\ nil) when is_map(map) and is_atom(key) do
    Map.get(map, key, Map.get(map, Atom.to_string(key), default))
  end

  defp normalize_id(nil), do: nil
  defp normalize_id(step) when is_atom(step), do: Atom.to_string(step)
  defp normalize_id(step) when is_binary(step), do: step

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp detail(true, value, _default), do: value
  defp detail(false, _value, default), do: default

  defp detail(include_details?, value) do
    detail(include_details?, value, nil)
  end

  defp sanitize_anomalies(anomalies) when is_list(anomalies) do
    Enum.map(
      anomalies,
      &Map.take(&1, [:source, :reason, :entry_type, :run_id, :step, :runnable_key])
    )
  end
end
