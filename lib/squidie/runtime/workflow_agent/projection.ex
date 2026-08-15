# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.WorkflowAgent.Projection.GraphState do
  @moduledoc false

  @type provenance_value :: :legacy_eager | :dependency_ordered
  @type provenance :: %{
          required(:nodes) => %{optional(String.t()) => provenance_value()},
          required(:edges) => %{optional(String.t()) => provenance_value()}
        }
  @type string_set :: MapSet.t(String.t()) | %MapSet{}

  @type t :: %__MODULE__{
          version: non_neg_integer(),
          topology: %{
            required(:nodes) => %{optional(String.t()) => map()},
            required(:edges) => %{optional(String.t()) => map()}
          },
          provenance: provenance(),
          active_node_ids: string_set(),
          active_edge_ids: string_set(),
          reserved_node_ids: string_set(),
          reserved_edge_ids: string_set(),
          tombstoned_node_ids: string_set(),
          tombstoned_edge_ids: string_set(),
          mutation_history: %{optional(String.t()) => map()}
        }

  defstruct version: 0,
            topology: %{nodes: %{}, edges: %{}},
            provenance: %{nodes: %{}, edges: %{}},
            active_node_ids: MapSet.new(),
            active_edge_ids: MapSet.new(),
            reserved_node_ids: MapSet.new(),
            reserved_edge_ids: MapSet.new(),
            tombstoned_node_ids: MapSet.new(),
            tombstoned_edge_ids: MapSet.new(),
            mutation_history: %{}
end

defmodule Squidie.Runtime.WorkflowAgent.Projection do
  @moduledoc """
  Rebuildable workflow-agent projection over one run-thread journal.

  Dispatch completion is not treated as workflow progress here. A runnable is
  applied only after the run thread records `:runnable_applied`, preserving the
  durable ordering between dispatch results and workflow state transitions.
  """

  alias Squidie.GraphMutation
  alias Squidie.GraphMutation.Operation
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.DynamicEdge
  alias Squidie.Runtime.Jido.Outbox
  alias Squidie.Runtime.Trace
  alias Squidie.Runtime.WorkflowAgent.Projection.GraphState

  @checkpoint_version 2
  @checkpoint_version_key "squidie.workflow_projection.checkpoint_version"
  @command_history_count_key "squidie.workflow_projection.command_history_count"
  @jido_outbox_key "squidie.workflow_projection.jido_outbox"

  @type anomaly :: %{
          required(:reason) => atom(),
          required(:entry_type) => atom(),
          optional(:child_run_id) => String.t(),
          optional(:component) => :jido_outbox,
          optional(:mutation_id) => String.t(),
          optional(:node_id) => String.t(),
          optional(:runnable_key) => String.t(),
          optional(:run_id) => String.t(),
          optional(:step) => String.t()
        }

  @type manual_state :: %{
          required(:step) => String.t(),
          required(:kind) => String.t(),
          required(:paused_at) => DateTime.t(),
          required(:metadata) => map(),
          optional(:deadline) => map()
        }

  @type string_set :: MapSet.t(String.t()) | %MapSet{}

  @type t :: %__MODULE__{
          run_id: String.t() | nil,
          workflow: String.t() | nil,
          trigger: String.t() | nil,
          input: map() | nil,
          context: map(),
          trace: Trace.t() | nil,
          started_at: DateTime.t() | nil,
          replayed_from_run_id: String.t() | nil,
          definition_version: String.t() | nil,
          definition_fingerprint: String.t() | nil,
          status: atom(),
          planned_runnables: %{optional(String.t()) => map()},
          applied_runnable_keys: string_set(),
          applied_results: %{optional(String.t()) => map() | nil},
          applied_execution_opts: %{optional(String.t()) => keyword()},
          applied_at: %{optional(String.t()) => DateTime.t()},
          command_history: [map()],
          child_runs: [map()],
          continued_from_run_id: String.t() | nil,
          continued_from_key: String.t() | nil,
          continued_to_run_id: String.t() | nil,
          continued_to_key: String.t() | nil,
          continuation_request: map() | nil,
          continuation_origin: map() | nil,
          dynamic_work: [map()],
          graph: GraphState.t(),
          manual_state: manual_state() | nil,
          terminal_status: atom() | nil,
          terminal_at: DateTime.t() | nil,
          terminal_error: map() | nil,
          anomalies: [anomaly()]
        }

  defstruct run_id: nil,
            workflow: nil,
            trigger: nil,
            input: nil,
            context: %{},
            trace: nil,
            started_at: nil,
            replayed_from_run_id: nil,
            definition_version: nil,
            definition_fingerprint: nil,
            status: :new,
            planned_runnables: %{},
            applied_runnable_keys: MapSet.new(),
            applied_results: %{},
            applied_execution_opts: %{},
            applied_at: %{},
            command_history: [],
            child_runs: [],
            continued_from_run_id: nil,
            continued_from_key: nil,
            continued_to_run_id: nil,
            continued_to_key: nil,
            continuation_request: nil,
            continuation_origin: nil,
            dynamic_work: [],
            graph: %GraphState{},
            manual_state: nil,
            terminal_status: nil,
            terminal_at: nil,
            terminal_error: nil,
            anomalies: []

  @doc false
  @spec new() :: t()
  def new do
    %__MODULE__{applied_runnable_keys: MapSet.new()}
    |> Map.put(@checkpoint_version_key, @checkpoint_version)
    |> Map.put(@command_history_count_key, 0)
    |> Map.put(@jido_outbox_key, Outbox.new_projection())
  end

  @doc false
  @spec rebuild([Entry.t()]) :: t()
  def rebuild(entries) when is_list(entries) do
    replay(new(), entries)
  end

  @doc false
  @spec replay(t(), [Entry.t()]) :: t()
  def replay(%__MODULE__{} = projection, entries) when is_list(entries) do
    Enum.reduce(entries, projection, &apply_entry/2)
  end

  @doc false
  @spec trace(t()) :: Trace.t() | nil
  def trace(%__MODULE__{} = projection), do: Map.get(projection, :trace)

  @doc false
  @spec status(t()) :: atom()
  def status(%__MODULE__{status: status}), do: status

  @doc false
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{terminal_status: nil}), do: false
  def terminal?(%__MODULE__{}), do: true

  @doc false
  @spec terminal_status(t()) :: atom() | nil
  def terminal_status(%__MODULE__{terminal_status: terminal_status}), do: terminal_status

  @doc false
  @spec terminal_error(t()) :: map() | nil
  def terminal_error(%__MODULE__{terminal_error: terminal_error}), do: terminal_error

  @doc false
  @spec manual_state(t()) :: manual_state() | nil
  def manual_state(%__MODULE__{manual_state: manual_state}), do: manual_state

  @doc false
  @spec planned_runnable_keys(t()) :: [String.t()]
  def planned_runnable_keys(%__MODULE__{planned_runnables: planned_runnables}) do
    planned_runnables
    |> Map.keys()
    |> Enum.sort()
  end

  @doc false
  @spec planned_runnables(t()) :: [map()]
  def planned_runnables(%__MODULE__{planned_runnables: planned_runnables}) do
    planned_runnables
    |> Map.values()
    |> Enum.sort_by(&runnable_key/1)
  end

  @doc false
  @spec planned_runnable(t(), String.t()) :: {:ok, map()} | :error
  def planned_runnable(%__MODULE__{planned_runnables: planned_runnables}, runnable_key)
      when is_binary(runnable_key) do
    Map.fetch(planned_runnables, runnable_key)
  end

  @doc false
  @spec planned_runnable_key?(t(), String.t()) :: boolean()
  def planned_runnable_key?(%__MODULE__{planned_runnables: planned_runnables}, runnable_key)
      when is_binary(runnable_key) do
    Map.has_key?(planned_runnables, runnable_key)
  end

  @doc false
  @spec applied_runnable_keys(t()) :: MapSet.t(String.t())
  def applied_runnable_keys(%__MODULE__{applied_runnable_keys: applied_runnable_keys}) do
    applied_runnable_keys
  end

  @doc false
  @spec dispatchable_runnable_keys(t()) :: MapSet.t(String.t())
  def dispatchable_runnable_keys(%__MODULE__{} = projection) do
    ready_node_ids = ready_dependency_node_ids(projection)

    projection
    |> planned_runnables()
    |> Enum.reduce(MapSet.new(), fn runnable, keys ->
      if dispatchable_runnable?(projection.graph, runnable, ready_node_ids) do
        MapSet.put(keys, runnable_key(runnable))
      else
        keys
      end
    end)
  end

  @doc false
  @spec terminal_runnable?(t(), map()) :: boolean()
  def terminal_runnable?(%__MODULE__{graph: graph}, runnable) when is_map(runnable) do
    node_id = map_value(runnable, :step)

    case Map.get(graph.provenance.nodes, node_id) do
      :dependency_ordered -> MapSet.member?(graph.active_node_ids, node_id)
      _declared_or_legacy -> true
    end
  end

  @doc false
  @spec applied_results(t()) :: %{optional(String.t()) => map() | nil}
  def applied_results(%__MODULE__{} = projection) do
    Map.get(projection, :applied_results, %{})
  end

  @doc false
  @spec applied_result(t(), String.t()) :: {:ok, map() | nil} | :error
  def applied_result(%__MODULE__{} = projection, runnable_key) when is_binary(runnable_key) do
    Map.fetch(applied_results(projection), runnable_key)
  end

  defp dispatchable_runnable?(graph, runnable, ready_node_ids) do
    node_id = map_value(runnable, :step)

    case Map.get(graph.provenance.nodes, node_id) do
      :dependency_ordered ->
        MapSet.member?(graph.active_node_ids, node_id) and
          MapSet.member?(ready_node_ids, node_id)

      _declared_or_legacy ->
        true
    end
  end

  defp ready_dependency_node_ids(%__MODULE__{} = projection) do
    applied_node_ids = applied_node_ids(projection)
    predecessors = active_predecessors(projection.graph)

    Enum.reduce(predecessors, MapSet.new(), fn {node_id, dependencies}, ready ->
      if not MapSet.member?(applied_node_ids, node_id) and
           MapSet.subset?(dependencies, applied_node_ids) do
        MapSet.put(ready, node_id)
      else
        ready
      end
    end)
  end

  defp applied_node_ids(%__MODULE__{} = projection) do
    projection
    |> planned_runnables()
    |> Enum.reduce(MapSet.new(), fn runnable, node_ids ->
      runnable_key = runnable_key(runnable)
      node_id = map_value(runnable, :step)

      if is_binary(node_id) and MapSet.member?(projection.applied_runnable_keys, runnable_key) do
        MapSet.put(node_ids, node_id)
      else
        node_ids
      end
    end)
  end

  defp active_predecessors(graph) do
    predecessors =
      graph.active_node_ids
      |> Enum.filter(&(Map.get(graph.provenance.nodes, &1) == :dependency_ordered))
      |> Map.new(&{&1, MapSet.new()})

    Enum.reduce(graph.topology.edges, predecessors, fn {edge_id, edge}, grouped ->
      target = map_value(edge, :to)
      source = map_value(edge, :from)

      if MapSet.member?(graph.active_edge_ids, edge_id) and Map.has_key?(grouped, target) do
        Map.update!(grouped, target, &MapSet.put(&1, source))
      else
        grouped
      end
    end)
  end

  @doc false
  @spec applied_execution_opts(t(), String.t()) :: keyword()
  def applied_execution_opts(%__MODULE__{} = projection, runnable_key)
      when is_binary(runnable_key) do
    projection
    |> Map.get(:applied_execution_opts, %{})
    |> Map.get(runnable_key, [])
  end

  @doc false
  @spec applied_at(t(), String.t()) :: DateTime.t() | nil
  def applied_at(%__MODULE__{} = projection, runnable_key) when is_binary(runnable_key) do
    projection
    |> Map.get(:applied_at, %{})
    |> Map.get(runnable_key)
  end

  @doc false
  @spec applied_runnable_key_for_step(t(), String.t()) :: {:ok, String.t()} | :error
  def applied_runnable_key_for_step(%__MODULE__{} = projection, step) when is_binary(step) do
    applied_keys = applied_runnable_keys(projection)

    projection.planned_runnables
    |> Map.values()
    |> Enum.find_value(fn runnable ->
      runnable_key = map_value(runnable, :runnable_key)
      runnable_step = map_value(runnable, :step)

      if runnable_step == step and is_binary(runnable_key) and
           MapSet.member?(applied_keys, runnable_key) do
        runnable_key
      end
    end)
    |> case do
      runnable_key when is_binary(runnable_key) -> {:ok, runnable_key}
      nil -> :error
    end
  end

  @doc false
  @spec child_runs(t()) :: [map()]
  def child_runs(%__MODULE__{} = projection) do
    child_runs = Map.get(projection, :child_runs, [])

    child_runs
    |> Enum.reverse()
    |> Enum.sort_by(&{Map.get(&1, :child_key), Map.get(&1, :child_run_id)})
  end

  @doc false
  @spec continuation(t()) :: %{
          required(:continued_from) => map() | nil,
          required(:continued_to) => map() | nil
        }
  def continuation(%__MODULE__{} = projection) do
    %{
      continued_from:
        continuation_edge(
          Map.get(projection, :continued_from_run_id),
          Map.get(projection, :continued_from_key)
        ),
      continued_to:
        continuation_edge(
          Map.get(projection, :continued_to_run_id),
          Map.get(projection, :continued_to_key)
        )
    }
  end

  @doc false
  @spec dynamic_work(t()) :: [map()]
  def dynamic_work(%__MODULE__{} = projection) do
    dynamic_work = Map.get(projection, :dynamic_work, [])

    dynamic_work
    |> Enum.reverse()
    |> Enum.sort_by(&{Map.get(&1, :dynamic_key), Map.get(&1, :recorded_at)})
  end

  @doc false
  @spec command_history(t()) :: [map()]
  def command_history(%__MODULE__{} = projection) do
    projection
    |> Map.get(:command_history, [])
    |> Enum.reverse()
  end

  @doc false
  @spec jido_outbox(t()) :: map()
  def jido_outbox(%__MODULE__{} = projection) do
    Map.get(projection, @jido_outbox_key, Outbox.new_projection())
  end

  @doc false
  @spec checkpoint_compatible?(term()) :: boolean()
  def checkpoint_compatible?(%__MODULE__{} = projection) do
    checkpoint_schema_compatible?(projection) and
      Enum.all?(
        [
          :terminal_status,
          :continued_from_run_id,
          :continued_from_key,
          :continued_to_run_id,
          :continued_to_key,
          :continuation_request,
          :continuation_origin
        ],
        &Map.has_key?(projection, &1)
      ) and
      not stale_failed_checkpoint_missing_terminal_error?(projection)
  end

  def checkpoint_compatible?(_projection) do
    false
  end

  @doc false
  @spec upgrade(t()) :: t()
  def upgrade(%__MODULE__{} = projection) do
    dynamic_work =
      projection
      |> Map.get(:dynamic_work, [])
      |> Enum.map(&upgrade_legacy_dynamic_work/1)

    legacy_graph_state = legacy_graph_state(dynamic_work)
    graph = upgrade_graph_state(Map.get(projection, :graph), legacy_graph_state)

    projection
    |> Map.put_new(:command_history, [])
    |> Map.put_new(:child_runs, [])
    |> Map.put_new(:continued_from_run_id, nil)
    |> Map.put_new(:continued_from_key, nil)
    |> Map.put_new(:continued_to_run_id, nil)
    |> Map.put_new(:continued_to_key, nil)
    |> Map.put_new(:continuation_request, nil)
    |> Map.put_new(:continuation_origin, nil)
    |> Map.put(:dynamic_work, dynamic_work)
    |> Map.put(:graph, graph)
    |> Map.put_new(:terminal_error, nil)
    |> Map.put_new(:trace, nil)
  end

  @doc false
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: Enum.reverse(anomalies)

  defp stale_failed_checkpoint_missing_terminal_error?(%__MODULE__{} = projection) do
    Map.get(projection, :terminal_status) == :failed and
      not Map.has_key?(projection, :terminal_error)
  end

  defp apply_entry(
         %Entry{type: :run_terminal, data: data} = entry,
         %__MODULE__{terminal_status: terminal_status} = projection
       )
       when terminal_status != nil do
    if required_present?(data, [:run_id, :status]) do
      if duplicate_terminal?(projection, entry, data) do
        projection
      else
        add_anomaly(projection, entry, :conflicting_terminal)
      end
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :jido_signal_delivery_acknowledged} = entry,
         %__MODULE__{terminal_status: terminal_status} = projection
       )
       when terminal_status != nil do
    apply_outbox_entry(projection, entry)
  end

  defp apply_entry(
         %Entry{} = entry,
         %__MODULE__{terminal_status: terminal_status} = projection
       )
       when terminal_status != nil do
    add_anomaly(projection, entry, :terminal_run)
  end

  defp apply_entry(
         %Entry{type: :run_signal_received, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if required_present?(data, [:run_id, :signal_type]) do
      put_command_history(projection, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(%Entry{type: :run_started, data: data} = entry, %__MODULE__{} = projection) do
    if required_present?(data, [:run_id, :workflow]) do
      projection
      |> Map.put(:run_id, Map.fetch!(data, :run_id))
      |> Map.put(:workflow, Map.fetch!(data, :workflow))
      |> Map.put(:trigger, Map.get(data, :trigger))
      |> Map.put(:input, Map.get(data, :input))
      |> Map.put(:context, Map.get(data, :context, %{}))
      |> Map.put(:trace, Map.get(data, :trace))
      |> Map.put(:started_at, Map.get(data, :occurred_at, entry.occurred_at))
      |> Map.put(:replayed_from_run_id, Map.get(data, :replayed_from_run_id))
      |> Map.put(:definition_version, definition_metadata_value(data, :definition_version))
      |> Map.put(
        :definition_fingerprint,
        definition_metadata_value(data, :definition_fingerprint)
      )
      |> refresh_status()
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :runnables_planned, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if required_present?(data, [:run_id, :runnables]) and is_list(Map.fetch!(data, :runnables)) do
      projection
      |> Map.put(:planned_runnables, add_planned_runnables(projection.planned_runnables, data))
      |> Map.put(:run_id, projection.run_id || Map.fetch!(data, :run_id))
      |> validate_runnable_intent_fingerprints(entry, data)
      |> refresh_status()
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :runnable_applied, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if required_present?(data, [:run_id, :runnable_key]) do
      runnable_key = Map.fetch!(data, :runnable_key)
      apply_runnable_result(projection, entry, data, runnable_key)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :child_run_started, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if child_run_data?(data) do
      put_child_run(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :run_continuation_requested, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if continuation_request_data?(data) do
      put_continuation_request(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :run_continued_from, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if continuation_origin_data?(data) do
      put_continuation_origin(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :dynamic_work_recorded, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if dynamic_work_data?(data) do
      put_dynamic_work(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :dynamic_graph_mutated, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if dynamic_graph_mutation_data?(data) do
      put_graph_mutation(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :manual_step_paused, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if manual_pause_data?(data) do
      pause_manual_step(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: :manual_step_resolved, data: data} = entry,
         %__MODULE__{} = projection
       ) do
    if manual_resolution_data?(data) do
      resolve_manual_step(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(
         %Entry{type: type} = entry,
         %__MODULE__{} = projection
       )
       when type in [:jido_signal_enqueued, :jido_signal_delivery_acknowledged] do
    apply_outbox_entry(projection, entry)
  end

  defp apply_entry(%Entry{type: :run_terminal, data: data} = entry, %__MODULE__{} = projection) do
    if required_present?(data, [:run_id, :status]) do
      status = Map.fetch!(data, :status)

      %__MODULE__{
        projection
        | run_id: projection.run_id || Map.fetch!(data, :run_id),
          status: status,
          manual_state: nil,
          terminal_status: status,
          terminal_at: Map.get(data, :occurred_at, entry.occurred_at),
          terminal_error: terminal_error_from_data(data)
      }
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(%Entry{}, %__MODULE__{} = projection), do: projection

  defp put_command_history(%__MODULE__{} = projection, data) do
    command =
      %{
        signal_type: Map.fetch!(data, :signal_type),
        payload: Map.get(data, :payload, %{}),
        metadata: Map.get(data, :metadata, %{}),
        occurred_at: Map.get(data, :occurred_at)
      }
      |> maybe_put(:source, Map.get(data, :source))
      |> maybe_put(:signal_id, Map.get(data, :signal_id))
      |> maybe_put(:trace, Map.get(data, :trace))
      |> maybe_put(:idempotency_key, Map.get(data, :idempotency_key))
      |> maybe_put(:actor, Map.get(data, :actor))
      |> maybe_put(:comment, Map.get(data, :comment))

    projection
    |> Map.update(:command_history, [command], &[command | &1])
    |> Map.put(@checkpoint_version_key, @checkpoint_version)
    |> Map.update(@command_history_count_key, 1, &(&1 + 1))
  end

  defp checkpoint_schema_compatible?(projection) do
    current_checkpoint_schema?(projection)
  end

  defp current_checkpoint_schema?(projection) do
    Map.get(projection, @checkpoint_version_key) == @checkpoint_version and
      valid_command_history_count?(projection) and
      Outbox.valid_projection?(Map.get(projection, @jido_outbox_key)) and
      outbox_anomalies_consistent?(projection)
  end

  defp outbox_anomalies_consistent?(%{anomalies: anomalies} = projection)
       when is_list(anomalies) do
    outbox = Map.get(projection, @jido_outbox_key)

    if Enum.all?(anomalies, &is_map/1) do
      projected_anomalies =
        anomalies
        |> Enum.reverse()
        |> Enum.filter(&(Map.get(&1, :component) == :jido_outbox))

      projected_anomalies == Outbox.projection_anomalies(outbox)
    else
      false
    end
  end

  defp outbox_anomalies_consistent?(_projection) do
    false
  end

  defp valid_command_history_count?(projection) do
    count = Map.get(projection, @command_history_count_key)
    history = Map.get(projection, :command_history)

    is_integer(count) and count >= 0 and is_list(history) and count == length(history)
  end

  defp terminal_error_from_data(data) when is_map(data) do
    case definition_metadata_value(data, :error) do
      error when is_map(error) -> error
      _other -> nil
    end
  end

  defp duplicate_terminal?(%__MODULE__{} = projection, %Entry{} = entry, data)
       when is_map(data) do
    projection.run_id == definition_metadata_value(data, :run_id) and
      projection.terminal_status == definition_metadata_value(data, :status) and
      projection.terminal_at ==
        (definition_metadata_value(data, :occurred_at) || entry.occurred_at) and
      projection.terminal_error == terminal_error_from_data(data)
  end

  defp definition_metadata_value(data, key) when is_map(data) and is_atom(key) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end

  defp add_planned_runnables(planned_runnables, data) do
    data
    |> Map.fetch!(:runnables)
    |> Enum.reduce(planned_runnables, &put_planned_runnable/2)
  end

  defp put_planned_runnable(runnable, acc) do
    case runnable_key(runnable) do
      key when is_binary(key) and key != "" -> Map.put_new(acc, key, normalize_runnable(runnable))
      _missing_key -> acc
    end
  end

  defp validate_runnable_intent_fingerprints(projection, entry, data) do
    data
    |> Map.get(:runnables, [])
    |> Enum.reduce(projection, fn runnable, current_projection ->
      validate_runnable_intent_fingerprint(current_projection, entry, runnable)
    end)
  end

  defp validate_runnable_intent_fingerprint(projection, entry, runnable) do
    case map_value(runnable, :graph_mutation) do
      nil ->
        projection

      metadata when is_map(metadata) ->
        validate_graph_mutation_runnable(projection, entry, runnable, metadata)

      _invalid_metadata ->
        add_runnable_intent_anomaly(projection, entry, runnable, nil, nil)
    end
  end

  defp validate_graph_mutation_runnable(projection, entry, runnable, metadata) do
    mutation_id = map_value(metadata, :mutation_id)
    node_id = map_value(metadata, :node_id)
    actual_fingerprint = map_value(metadata, :intent_fingerprint)

    with history when is_map(history) <-
           Map.get(projection.graph.mutation_history, mutation_id),
         fingerprints when is_map(fingerprints) <-
           Map.get(history, :runnable_intent_fingerprints),
         expected_fingerprint when is_binary(expected_fingerprint) <-
           Map.get(fingerprints, node_id),
         true <- actual_fingerprint == expected_fingerprint do
      projection
    else
      _mismatch ->
        add_runnable_intent_anomaly(projection, entry, runnable, mutation_id, node_id)
    end
  end

  defp apply_runnable_result(projection, entry, data, runnable_key) do
    if Map.has_key?(projection.planned_runnables, runnable_key) do
      projection
      |> Map.put(
        :applied_runnable_keys,
        MapSet.put(projection.applied_runnable_keys, runnable_key)
      )
      |> Map.put(
        :applied_results,
        Map.put(applied_results(projection), runnable_key, Map.get(data, :result))
      )
      |> Map.put(
        :applied_execution_opts,
        Map.put(applied_execution_opts(projection), runnable_key, execution_opts(data))
      )
      |> Map.put(
        :applied_at,
        Map.put(applied_at(projection), runnable_key, effective_applied_at(data, entry))
      )
      |> refresh_status()
    else
      add_anomaly(projection, entry, :unknown_runnable_intent)
    end
  end

  defp child_run_data?(data) do
    required_present?(
      data,
      [:run_id, :child_run_id, :child_workflow, :child_trigger, :child_key, :origin]
    ) and is_map(Map.fetch!(data, :origin)) and is_map(Map.get(data, :metadata, %{}))
  end

  defp continuation_request_data?(data) do
    required_present?(data, [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint
    ]) and
      Map.has_key?(data, :definition_version) and
      valid_continuation_request_identifiers?(data) and
      is_map(Map.fetch!(data, :input)) and
      valid_continuation_definition?(data)
  end

  defp continuation_origin_data?(data) do
    required_present?(data, [:run_id, :predecessor_run_id, :continuation_key]) and
      non_empty_binary?(Map.fetch!(data, :run_id)) and
      non_empty_binary?(Map.fetch!(data, :predecessor_run_id)) and
      non_empty_binary?(Map.fetch!(data, :continuation_key))
  end

  defp valid_continuation_request_identifiers?(data) do
    non_empty_binary?(Map.fetch!(data, :run_id)) and
      non_empty_binary?(Map.fetch!(data, :successor_run_id)) and
      non_empty_binary?(Map.fetch!(data, :continuation_key)) and
      non_empty_binary?(Map.fetch!(data, :workflow)) and
      non_empty_binary?(Map.fetch!(data, :trigger))
  end

  defp valid_continuation_definition?(data) do
    Map.fetch!(data, :definition) == :current and
      optional_non_empty_binary?(Map.fetch!(data, :definition_version)) and
      non_empty_binary?(Map.fetch!(data, :definition_fingerprint))
  end

  defp put_continuation_request(%__MODULE__{} = projection, entry, data) do
    request =
      Map.take(data, [
        :run_id,
        :successor_run_id,
        :continuation_key,
        :workflow,
        :trigger,
        :input,
        :definition,
        :definition_version,
        :definition_fingerprint
      ])

    case Map.get(projection, :continuation_request) do
      nil ->
        projection
        |> Map.put(:run_id, projection.run_id || data.run_id)
        |> Map.put(:continued_to_run_id, data.successor_run_id)
        |> Map.put(:continued_to_key, data.continuation_key)
        |> Map.put(:continuation_request, request)

      ^request ->
        projection

      _conflict ->
        add_anomaly(projection, entry, :conflicting_continuation)
    end
  end

  defp put_continuation_origin(%__MODULE__{} = projection, entry, data) do
    origin = Map.take(data, [:run_id, :predecessor_run_id, :continuation_key])

    case Map.get(projection, :continuation_origin) do
      nil ->
        projection
        |> Map.put(:run_id, projection.run_id || data.run_id)
        |> Map.put(:continued_from_run_id, data.predecessor_run_id)
        |> Map.put(:continued_from_key, data.continuation_key)
        |> Map.put(:continuation_origin, origin)

      ^origin ->
        projection

      _conflict ->
        add_anomaly(projection, entry, :conflicting_continuation)
    end
  end

  defp continuation_edge(nil, _continuation_key) do
    nil
  end

  defp continuation_edge(run_id, continuation_key)
       when is_binary(run_id) and is_binary(continuation_key) do
    %{run_id: run_id, continuation_key: continuation_key}
  end

  defp continuation_edge(_run_id, _continuation_key) do
    nil
  end

  defp optional_non_empty_binary?(nil) do
    true
  end

  defp optional_non_empty_binary?(value) do
    non_empty_binary?(value)
  end

  defp non_empty_binary?(value) when is_binary(value) do
    value != ""
  end

  defp non_empty_binary?(_value) do
    false
  end

  defp put_child_run(%__MODULE__{} = projection, entry, data) do
    child_run =
      maybe_put(
        %{
          child_run_id: data.child_run_id,
          child_workflow: data.child_workflow,
          child_trigger: data.child_trigger,
          child_key: data.child_key,
          origin: data.origin,
          metadata: Map.get(data, :metadata, %{}),
          started_at: child_started_at(data, entry)
        },
        :trace,
        Map.get(data, :trace)
      )

    child_runs = Map.get(projection, :child_runs, [])

    cond do
      Enum.any?(child_runs, &conflicting_child_run?(&1, child_run)) ->
        add_child_run_anomaly(projection, entry, child_run.child_run_id)

      Enum.any?(child_runs, &same_child_run?(&1, child_run)) ->
        projection

      true ->
        %__MODULE__{
          projection
          | run_id: projection.run_id || data.run_id,
            child_runs: [child_run | child_runs]
        }
    end
  end

  defp same_child_run?(left, right) do
    Map.take(left, [:child_run_id, :child_workflow, :child_trigger, :child_key]) ==
      Map.take(right, [:child_run_id, :child_workflow, :child_trigger, :child_key])
  end

  defp conflicting_child_run?(left, right) do
    Map.get(left, :child_run_id) == Map.get(right, :child_run_id) and
      Map.take(left, [:child_workflow, :child_trigger, :child_key, :metadata]) !=
        Map.take(right, [:child_workflow, :child_trigger, :child_key, :metadata])
  end

  defp child_started_at(data, entry) do
    case Map.get(data, :started_at) do
      %DateTime{} = started_at -> started_at
      _missing -> entry.occurred_at
    end
  end

  defp dynamic_work_data?(data) do
    required_present?(data, [:run_id, :dynamic_key, :origin, :nodes]) and
      is_map(Map.fetch!(data, :origin)) and is_list(Map.fetch!(data, :nodes)) and
      is_map(Map.get(data, :metadata, %{})) and is_list(Map.get(data, :edges, []))
  end

  defp put_dynamic_work(%__MODULE__{} = projection, entry, data) do
    dynamic_work = normalize_dynamic_work(data, entry)
    existing_work = Map.get(projection, :dynamic_work, [])

    cond do
      Enum.any?(existing_work, &conflicting_dynamic_work?(&1, dynamic_work)) ->
        add_anomaly(projection, entry, :conflicting_dynamic_work)

      Enum.any?(existing_work, &same_dynamic_work?(&1, dynamic_work)) ->
        projection

      true ->
        projection =
          %__MODULE__{
            projection
            | run_id: projection.run_id || data.run_id,
              dynamic_work: [dynamic_work | existing_work]
          }

        put_legacy_graph_state(projection, dynamic_work)
    end
  end

  defp normalize_dynamic_work(data, entry) do
    nodes =
      data.nodes
      |> Enum.map(&normalize_dynamic_node/1)
      |> Enum.reject(&is_nil/1)

    compact(%{
      dynamic_key: data.dynamic_key,
      status: Map.get(data, :status, :recorded),
      reason: Map.get(data, :reason),
      origin: data.origin,
      nodes: nodes,
      edges: dynamic_edges(data, nodes),
      metadata: Map.get(data, :metadata, %{}),
      provenance: :legacy_eager,
      trace: Map.get(data, :trace),
      recorded_at: dynamic_recorded_at(data, entry)
    })
  end

  defp upgrade_legacy_dynamic_work(work) when is_map(work) do
    Map.put_new(work, :provenance, :legacy_eager)
  end

  defp upgrade_legacy_dynamic_work(work) do
    work
  end

  defp upgrade_graph_state(%GraphState{} = graph, _legacy_graph_state) do
    Map.put_new(graph, :topology, %{nodes: %{}, edges: %{}})
  end

  defp upgrade_graph_state(_missing_graph, legacy_graph_state) do
    legacy_graph_state
  end

  defp legacy_graph_state(dynamic_work) do
    Enum.reduce(dynamic_work, empty_legacy_graph_state(), fn work, state ->
      if legacy_dynamic_work?(work) do
        merge_legacy_graph_state(state, work)
      else
        state
      end
    end)
  end

  defp empty_legacy_graph_state do
    %GraphState{}
  end

  defp legacy_dynamic_work?(work) when is_map(work) do
    is_binary(Map.get(work, :dynamic_key)) and is_list(Map.get(work, :nodes)) and
      is_list(Map.get(work, :edges))
  end

  defp legacy_dynamic_work?(_work) do
    false
  end

  defp merge_legacy_graph_state(%GraphState{} = state, work) do
    node_ids = legacy_identity_ids(Map.get(work, :nodes, []))
    edge_ids = legacy_identity_ids(Map.get(work, :edges, []))

    %GraphState{
      state
      | version: state.version + 1,
        provenance: %{
          nodes: put_legacy_provenance(state.provenance.nodes, node_ids),
          edges: put_legacy_provenance(state.provenance.edges, edge_ids)
        },
        active_node_ids: MapSet.union(state.active_node_ids, node_ids),
        active_edge_ids: MapSet.union(state.active_edge_ids, edge_ids),
        reserved_node_ids: MapSet.union(state.reserved_node_ids, node_ids),
        reserved_edge_ids: MapSet.union(state.reserved_edge_ids, edge_ids)
    }
  end

  defp put_legacy_graph_state(%__MODULE__{} = projection, work) do
    graph = merge_legacy_graph_state(projection.graph, work)

    %__MODULE__{
      projection
      | graph: graph
    }
  end

  defp legacy_identity_ids(items) do
    Enum.reduce(items, MapSet.new(), fn item, ids ->
      case item do
        %{id: id} when is_binary(id) and id != "" -> MapSet.put(ids, id)
        _invalid_item -> ids
      end
    end)
  end

  defp put_legacy_provenance(provenance, ids) do
    Enum.reduce(ids, provenance, fn id, entries ->
      Map.put(entries, id, :legacy_eager)
    end)
  end

  defp dynamic_graph_mutation_data?(data) when is_map(data) do
    required_present?(data, [
      :run_id,
      :mutation_id,
      :expected_version,
      :result_version,
      :origin
    ]) and is_integer(Map.get(data, :result_version)) and Map.get(data, :result_version) >= 0
  end

  defp dynamic_graph_mutation_data?(_data) do
    false
  end

  defp put_graph_mutation(%__MODULE__{} = projection, entry, data) do
    with true <- is_map(Map.get(data, :runnable_intent_fingerprints, %{})),
         {:ok, mutation} <- normalize_graph_mutation(data) do
      replay_graph_mutation(projection, entry, data, mutation)
    else
      _invalid -> add_anomaly(projection, entry, :malformed_graph_operations)
    end
  end

  defp normalize_graph_mutation(data) do
    data
    |> Map.take([:mutation_id, :expected_version, :origin, :additions, :removals])
    |> GraphMutation.normalize()
  end

  defp replay_graph_mutation(projection, entry, data, mutation) do
    fingerprint = GraphMutation.fingerprint(mutation)

    case Map.get(projection.graph.mutation_history, mutation.mutation_id) do
      %{fingerprint: ^fingerprint} ->
        projection

      nil ->
        apply_new_graph_mutation(projection, entry, data, mutation, fingerprint)

      _conflicting_history ->
        add_anomaly(projection, entry, :conflicting_graph_mutation)
    end
  end

  defp apply_new_graph_mutation(
         %__MODULE__{} = projection,
         entry,
         data,
         mutation,
         fingerprint
       ) do
    if continuous_graph_version?(projection.graph, data, mutation) do
      graph =
        projection.graph
        |> apply_graph_operations(mutation)
        |> put_mutation_history(entry, data, mutation, fingerprint)

      %__MODULE__{
        projection
        | run_id: projection.run_id || Map.fetch!(data, :run_id),
          graph: graph
      }
    else
      add_anomaly(projection, entry, :discontinuous_graph_version)
    end
  end

  defp continuous_graph_version?(graph, data, mutation) do
    mutation.expected_version == graph.version and
      Map.get(data, :result_version) == graph.version + 1
  end

  defp apply_graph_operations(%GraphState{} = graph, mutation) do
    graph = Enum.reduce(mutation.additions, graph, &add_graph_operation/2)
    Enum.reduce(mutation.removals, graph, &remove_graph_operation/2)
  end

  defp add_graph_operation(%Operation{kind: :node} = operation, %GraphState{} = graph) do
    id = operation.id
    topology = Map.update!(graph.topology, :nodes, &Map.put(&1, id, Operation.to_map(operation)))
    provenance = Map.update!(graph.provenance, :nodes, &Map.put(&1, id, :dependency_ordered))

    %GraphState{
      graph
      | topology: topology,
        provenance: provenance,
        active_node_ids: MapSet.put(graph.active_node_ids, id),
        reserved_node_ids: MapSet.put(graph.reserved_node_ids, id)
    }
  end

  defp add_graph_operation(%Operation{kind: :edge} = operation, %GraphState{} = graph) do
    id = operation.id
    topology = Map.update!(graph.topology, :edges, &Map.put(&1, id, Operation.to_map(operation)))
    provenance = Map.update!(graph.provenance, :edges, &Map.put(&1, id, :dependency_ordered))

    %GraphState{
      graph
      | topology: topology,
        provenance: provenance,
        active_edge_ids: MapSet.put(graph.active_edge_ids, id),
        reserved_edge_ids: MapSet.put(graph.reserved_edge_ids, id)
    }
  end

  defp remove_graph_operation(%Operation{kind: :node, id: id}, %GraphState{} = graph) do
    %GraphState{
      graph
      | topology: Map.update!(graph.topology, :nodes, &Map.delete(&1, id)),
        active_node_ids: MapSet.delete(graph.active_node_ids, id),
        reserved_node_ids: MapSet.put(graph.reserved_node_ids, id),
        tombstoned_node_ids: MapSet.put(graph.tombstoned_node_ids, id)
    }
  end

  defp remove_graph_operation(%Operation{kind: :edge, id: id}, %GraphState{} = graph) do
    %GraphState{
      graph
      | topology: Map.update!(graph.topology, :edges, &Map.delete(&1, id)),
        active_edge_ids: MapSet.delete(graph.active_edge_ids, id),
        reserved_edge_ids: MapSet.put(graph.reserved_edge_ids, id),
        tombstoned_edge_ids: MapSet.put(graph.tombstoned_edge_ids, id)
    }
  end

  defp put_mutation_history(%GraphState{} = graph, entry, data, mutation, fingerprint) do
    result_version = Map.fetch!(data, :result_version)

    history =
      mutation
      |> GraphMutation.to_map()
      |> Map.merge(%{
        result_version: result_version,
        fingerprint: fingerprint,
        runnable_intent_fingerprints: Map.get(data, :runnable_intent_fingerprints, %{}),
        occurred_at: entry.occurred_at
      })

    %GraphState{
      graph
      | version: result_version,
        mutation_history: Map.put(graph.mutation_history, mutation.mutation_id, history)
    }
  end

  defp normalize_dynamic_node(node) when is_map(node) do
    case Map.get(node, :id) do
      id when is_binary(id) ->
        compact(%{
          id: id,
          action: Map.get(node, :action),
          input: Map.get(node, :input),
          retry: Map.get(node, :retry),
          status: Map.get(node, :status, :recorded),
          metadata: Map.get(node, :metadata, %{})
        })

      _missing_id ->
        nil
    end
  end

  defp normalize_dynamic_node(_node), do: nil

  defp dynamic_edges(data, nodes) do
    case Map.get(data, :edges, []) do
      [] ->
        data.origin
        |> inferred_dynamic_edges(nodes)
        |> normalize_dynamic_edges()

      edges ->
        normalize_dynamic_edges(edges)
    end
  end

  defp inferred_dynamic_edges(origin, nodes) when is_map(origin) do
    origin_step = Map.get(origin, :step)

    nodes
    |> Enum.map(&Map.get(&1, :id))
    |> Enum.reject(&is_nil/1)
    |> Enum.map(fn node_id ->
      DynamicEdge.attrs(
        Enum.join([origin_step, "dynamic", node_id], ":"),
        origin_step,
        node_id,
        :dynamic,
        :pending
      )
    end)
  end

  defp inferred_dynamic_edges(_origin, _nodes), do: []

  defp normalize_dynamic_edge(edge) when is_map(edge) do
    with id when is_binary(id) <- Map.get(edge, :id),
         from when is_binary(from) <- Map.get(edge, :from),
         to when is_binary(to) <- Map.get(edge, :to) do
      compact(
        DynamicEdge.attrs(
          id,
          from,
          to,
          Map.get(edge, :type, :dynamic),
          Map.get(edge, :status, :pending)
        )
      )
    else
      _missing_required_field -> nil
    end
  end

  defp normalize_dynamic_edge(_edge), do: nil

  defp normalize_dynamic_edges(edges) do
    edges
    |> Enum.map(&normalize_dynamic_edge/1)
    |> Enum.reject(&is_nil/1)
  end

  defp same_dynamic_work?(left, right) do
    Map.take(left, [:dynamic_key, :status, :reason, :origin, :nodes, :edges, :metadata]) ==
      Map.take(right, [:dynamic_key, :status, :reason, :origin, :nodes, :edges, :metadata])
  end

  defp conflicting_dynamic_work?(left, right) do
    Map.get(left, :dynamic_key) == Map.get(right, :dynamic_key) and
      not same_dynamic_work?(left, right)
  end

  defp dynamic_recorded_at(data, entry) do
    case Map.get(data, :recorded_at) do
      %DateTime{} = recorded_at -> recorded_at
      _missing -> entry.occurred_at
    end
  end

  defp pause_manual_step(
         %__MODULE__{terminal_status: nil, manual_state: nil} = projection,
         entry,
         data
       ) do
    manual_state =
      compact(%{
        step: data.step,
        kind: data.kind,
        reason: Map.get(data, :reason),
        paused_at: manual_paused_at(data, entry),
        metadata: Map.get(data, :metadata, %{}),
        deadline: manual_deadline(data)
      })

    projection
    |> Map.put(:run_id, projection.run_id || data.run_id)
    |> Map.put(:manual_state, manual_state)
    |> refresh_status()
  end

  defp pause_manual_step(
         %__MODULE__{terminal_status: nil, manual_state: manual_state} = projection,
         entry,
         data
       ) do
    duplicate_state =
      compact(%{
        step: data.step,
        kind: data.kind,
        reason: Map.get(data, :reason),
        paused_at: manual_paused_at(data, entry),
        metadata: Map.get(data, :metadata, %{}),
        deadline: manual_deadline(data)
      })

    if manual_state == duplicate_state do
      projection
    else
      add_anomaly(projection, entry, :active_manual_step)
    end
  end

  defp pause_manual_step(%__MODULE__{} = projection, entry, _data) do
    add_anomaly(projection, entry, :terminal_run)
  end

  defp resolve_manual_step(
         %__MODULE__{terminal_status: nil, manual_state: %{step: step}} = projection,
         _entry,
         data
       )
       when step == data.step do
    projection
    |> Map.put(:manual_state, nil)
    |> refresh_status()
  end

  defp resolve_manual_step(%__MODULE__{terminal_status: nil} = projection, entry, _data) do
    add_anomaly(projection, entry, :stale_manual_resolution)
  end

  defp resolve_manual_step(%__MODULE__{} = projection, entry, _data) do
    add_anomaly(projection, entry, :terminal_run)
  end

  defp applied_execution_opts(%__MODULE__{} = projection) do
    Map.get(projection, :applied_execution_opts, %{})
  end

  defp applied_at(%__MODULE__{} = projection) do
    Map.get(projection, :applied_at, %{})
  end

  defp execution_opts(data) when is_map(data) do
    case Map.get(data, :execution_opts) do
      opts when is_list(opts) -> opts
      _missing_or_invalid -> []
    end
  end

  defp effective_applied_at(data, entry) when is_map(data) do
    case Map.get(data, :applied_at) do
      %DateTime{} = applied_at -> applied_at
      _missing_or_invalid -> entry.occurred_at
    end
  end

  defp manual_paused_at(data, entry) when is_map(data) do
    case Map.get(data, :paused_at) do
      %DateTime{} = paused_at -> paused_at
      _missing_or_invalid -> entry.occurred_at
    end
  end

  defp manual_deadline(data) when is_map(data) do
    map_value(data, :deadline)
  end

  defp refresh_status(%__MODULE__{terminal_status: terminal_status} = projection)
       when terminal_status != nil do
    %__MODULE__{projection | status: terminal_status}
  end

  defp refresh_status(
         %__MODULE__{
           manual_state: nil,
           terminal_status: nil,
           planned_runnables: planned_runnables
         } =
           projection
       )
       when map_size(planned_runnables) == 0 do
    %__MODULE__{projection | status: :started}
  end

  defp refresh_status(%__MODULE__{manual_state: nil} = projection) do
    planned_keys =
      projection.planned_runnables
      |> Map.keys()
      |> MapSet.new()

    if MapSet.subset?(planned_keys, projection.applied_runnable_keys) do
      %__MODULE__{projection | status: :idle}
    else
      %__MODULE__{projection | status: :running}
    end
  end

  defp refresh_status(%__MODULE__{} = projection) do
    %__MODULE__{projection | status: :paused}
  end

  defp runnable_key(runnable) when is_map(runnable) do
    map_value(runnable, :runnable_key) || map_value(runnable, :key)
  end

  defp runnable_key(_runnable), do: nil

  defp map_value(map, key, default \\ nil), do: Squidie.MapField.get(map, key, default)

  defp normalize_runnable(runnable) when is_map(runnable), do: Map.new(runnable)

  defp manual_pause_data?(data) when is_map(data) do
    required_present?(data, [:run_id, :step, :kind]) and is_map(Map.get(data, :metadata, %{}))
  end

  defp manual_pause_data?(_data), do: false

  defp manual_resolution_data?(data) when is_map(data) do
    required_present?(data, [:run_id, :step, :action]) and is_map(Map.get(data, :result, %{}))
  end

  defp manual_resolution_data?(_data), do: false

  defp apply_outbox_entry(%__MODULE__{} = projection, %Entry{} = entry) do
    {outbox, anomaly} =
      projection
      |> jido_outbox()
      |> Outbox.apply_entry_observed(entry)

    projection = Map.put(projection, @jido_outbox_key, outbox)

    case anomaly do
      nil -> projection
      anomaly -> %{projection | anomalies: [anomaly | projection.anomalies]}
    end
  end

  defp add_anomaly(%__MODULE__{} = projection, %Entry{} = entry, reason) do
    data = data_map(entry)

    anomaly =
      %{
        reason: reason,
        entry_type: entry.type
      }
      |> maybe_put_run_id(Map.get(data, :run_id))
      |> maybe_put_mutation_id(Map.get(data, :mutation_id))
      |> maybe_put_runnable_key(Map.get(data, :runnable_key))
      |> maybe_put_step(Map.get(data, :step))

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp add_runnable_intent_anomaly(
         %__MODULE__{} = projection,
         %Entry{} = entry,
         runnable,
         mutation_id,
         node_id
       ) do
    data = data_map(entry)

    anomaly =
      %{
        reason: :runnable_intent_fingerprint_mismatch,
        entry_type: entry.type
      }
      |> maybe_put_run_id(Map.get(data, :run_id))
      |> maybe_put_mutation_id(mutation_id)
      |> maybe_put_node_id(node_id)
      |> maybe_put_runnable_key(runnable_key(runnable))

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp add_child_run_anomaly(%__MODULE__{} = projection, %Entry{} = entry, child_run_id) do
    data = data_map(entry)

    anomaly =
      %{
        reason: :conflicting_child_run,
        entry_type: entry.type
      }
      |> maybe_put_run_id(Map.get(data, :run_id))
      |> maybe_put_child_run_id(child_run_id)

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp required_present?(data, fields) when is_map(data) do
    Enum.all?(fields, &(Map.has_key?(data, &1) and not is_nil(Map.fetch!(data, &1))))
  end

  defp required_present?(_data, _fields), do: false

  defp data_map(%Entry{data: data}) when is_map(data), do: data
  defp data_map(%Entry{}), do: %{}

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_run_id(anomaly, nil), do: anomaly
  defp maybe_put_run_id(anomaly, run_id), do: Map.put(anomaly, :run_id, run_id)

  defp maybe_put_child_run_id(anomaly, nil), do: anomaly

  defp maybe_put_child_run_id(anomaly, child_run_id) do
    Map.put(anomaly, :child_run_id, child_run_id)
  end

  defp maybe_put_mutation_id(anomaly, nil) do
    anomaly
  end

  defp maybe_put_mutation_id(anomaly, mutation_id) do
    Map.put(anomaly, :mutation_id, mutation_id)
  end

  defp maybe_put_node_id(anomaly, nil) do
    anomaly
  end

  defp maybe_put_node_id(anomaly, node_id) do
    Map.put(anomaly, :node_id, node_id)
  end

  defp maybe_put_runnable_key(anomaly, nil), do: anomaly

  defp maybe_put_runnable_key(anomaly, runnable_key) do
    Map.put(anomaly, :runnable_key, runnable_key)
  end

  defp maybe_put_step(anomaly, nil), do: anomaly

  defp maybe_put_step(anomaly, step) do
    Map.put(anomaly, :step, step)
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
