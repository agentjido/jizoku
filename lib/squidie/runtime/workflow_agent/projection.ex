defmodule Squidie.Runtime.WorkflowAgent.Projection do
  @moduledoc """
  Rebuildable workflow-agent projection over one run-thread journal.

  Dispatch completion is not treated as workflow progress here. A runnable is
  applied only after the run thread records `:runnable_applied`, preserving the
  durable ordering between dispatch results and workflow state transitions.
  """

  alias Squidie.Runtime.DispatchProtocol.Entry

  @type anomaly :: %{
          required(:reason) => atom(),
          required(:entry_type) => atom(),
          optional(:child_run_id) => String.t(),
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
          dynamic_work: [map()],
          manual_state: manual_state() | nil,
          terminal_status: atom() | nil,
          terminal_error: map() | nil,
          anomalies: [anomaly()]
        }

  defstruct run_id: nil,
            workflow: nil,
            trigger: nil,
            input: nil,
            context: %{},
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
            dynamic_work: [],
            manual_state: nil,
            terminal_status: nil,
            terminal_error: nil,
            anomalies: []

  @doc "Internal API."
  @spec new() :: t()
  def new do
    %__MODULE__{applied_runnable_keys: MapSet.new()}
  end

  @doc "Internal API."
  @spec rebuild([Entry.t()]) :: t()
  def rebuild(entries) when is_list(entries) do
    replay(new(), entries)
  end

  @doc "Internal API."
  @spec replay(t(), [Entry.t()]) :: t()
  def replay(%__MODULE__{} = projection, entries) when is_list(entries) do
    Enum.reduce(entries, projection, &apply_entry/2)
  end

  @doc "Internal API."
  @spec status(t()) :: atom()
  def status(%__MODULE__{status: status}), do: status

  @doc "Internal API."
  @spec terminal?(t()) :: boolean()
  def terminal?(%__MODULE__{terminal_status: nil}), do: false
  def terminal?(%__MODULE__{}), do: true

  @doc "Internal API."
  @spec terminal_status(t()) :: atom() | nil
  def terminal_status(%__MODULE__{terminal_status: terminal_status}), do: terminal_status

  @doc "Internal API."
  @spec terminal_error(t()) :: map() | nil
  def terminal_error(%__MODULE__{terminal_error: terminal_error}), do: terminal_error

  @doc "Internal API."
  @spec manual_state(t()) :: manual_state() | nil
  def manual_state(%__MODULE__{manual_state: manual_state}), do: manual_state

  @doc "Internal API."
  @spec planned_runnable_keys(t()) :: [String.t()]
  def planned_runnable_keys(%__MODULE__{planned_runnables: planned_runnables}) do
    planned_runnables
    |> Map.keys()
    |> Enum.sort()
  end

  @doc "Internal API."
  @spec planned_runnables(t()) :: [map()]
  def planned_runnables(%__MODULE__{planned_runnables: planned_runnables}) do
    planned_runnables
    |> Map.values()
    |> Enum.sort_by(&runnable_key/1)
  end

  @doc "Internal API."
  @spec planned_runnable(t(), String.t()) :: {:ok, map()} | :error
  def planned_runnable(%__MODULE__{planned_runnables: planned_runnables}, runnable_key)
      when is_binary(runnable_key) do
    Map.fetch(planned_runnables, runnable_key)
  end

  @doc "Internal API."
  @spec planned_runnable_key?(t(), String.t()) :: boolean()
  def planned_runnable_key?(%__MODULE__{planned_runnables: planned_runnables}, runnable_key)
      when is_binary(runnable_key) do
    Map.has_key?(planned_runnables, runnable_key)
  end

  @doc "Internal API."
  @spec applied_runnable_keys(t()) :: MapSet.t(String.t())
  def applied_runnable_keys(%__MODULE__{applied_runnable_keys: applied_runnable_keys}) do
    applied_runnable_keys
  end

  @doc "Internal API."
  @spec applied_results(t()) :: %{optional(String.t()) => map() | nil}
  def applied_results(%__MODULE__{} = projection) do
    Map.get(projection, :applied_results, %{})
  end

  @doc "Internal API."
  @spec applied_result(t(), String.t()) :: {:ok, map() | nil} | :error
  def applied_result(%__MODULE__{} = projection, runnable_key) when is_binary(runnable_key) do
    Map.fetch(applied_results(projection), runnable_key)
  end

  @doc "Internal API."
  @spec applied_execution_opts(t(), String.t()) :: keyword()
  def applied_execution_opts(%__MODULE__{} = projection, runnable_key)
      when is_binary(runnable_key) do
    projection
    |> Map.get(:applied_execution_opts, %{})
    |> Map.get(runnable_key, [])
  end

  @doc "Internal API."
  @spec applied_at(t(), String.t()) :: DateTime.t() | nil
  def applied_at(%__MODULE__{} = projection, runnable_key) when is_binary(runnable_key) do
    projection
    |> Map.get(:applied_at, %{})
    |> Map.get(runnable_key)
  end

  @doc "Internal API."
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

  @doc "Internal API."
  @spec child_runs(t()) :: [map()]
  def child_runs(%__MODULE__{} = projection) do
    child_runs = Map.get(projection, :child_runs, [])

    child_runs
    |> Enum.reverse()
    |> Enum.sort_by(&{Map.get(&1, :child_key), Map.get(&1, :child_run_id)})
  end

  @doc "Internal API."
  @spec dynamic_work(t()) :: [map()]
  def dynamic_work(%__MODULE__{} = projection) do
    dynamic_work = Map.get(projection, :dynamic_work, [])

    dynamic_work
    |> Enum.reverse()
    |> Enum.sort_by(&{Map.get(&1, :dynamic_key), Map.get(&1, :recorded_at)})
  end

  @doc "Internal API."
  @spec command_history(t()) :: [map()]
  def command_history(%__MODULE__{} = projection) do
    projection
    |> Map.get(:command_history, [])
    |> Enum.reverse()
  end

  @doc "Internal API."
  @spec upgrade(t()) :: t()
  def upgrade(%__MODULE__{} = projection) do
    projection
    |> Map.put_new(:command_history, [])
    |> Map.put_new(:child_runs, [])
    |> Map.put_new(:dynamic_work, [])
    |> Map.put_new(:terminal_error, nil)
  end

  @doc "Internal API."
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: Enum.reverse(anomalies)

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

  defp apply_entry(%Entry{type: :run_terminal, data: data} = entry, %__MODULE__{} = projection) do
    if required_present?(data, [:run_id, :status]) do
      status = Map.fetch!(data, :status)

      %__MODULE__{
        projection
        | run_id: projection.run_id || Map.fetch!(data, :run_id),
          status: status,
          manual_state: nil,
          terminal_status: status,
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
      |> maybe_put(:idempotency_key, Map.get(data, :idempotency_key))
      |> maybe_put(:actor, Map.get(data, :actor))
      |> maybe_put(:comment, Map.get(data, :comment))

    Map.update(projection, :command_history, [command], &[command | &1])
  end

  defp terminal_error_from_data(data) when is_map(data) do
    case definition_metadata_value(data, :error) do
      error when is_map(error) -> error
      _other -> nil
    end
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

  defp put_child_run(%__MODULE__{} = projection, entry, data) do
    child_run = %{
      child_run_id: data.child_run_id,
      child_workflow: data.child_workflow,
      child_trigger: data.child_trigger,
      child_key: data.child_key,
      origin: data.origin,
      metadata: Map.get(data, :metadata, %{}),
      started_at: child_started_at(data, entry)
    }

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
        %__MODULE__{
          projection
          | run_id: projection.run_id || data.run_id,
            dynamic_work: [dynamic_work | existing_work]
        }
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
      recorded_at: dynamic_recorded_at(data, entry)
    })
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
      %{
        id: Enum.join([origin_step, "dynamic", node_id], ":"),
        from: origin_step,
        to: node_id,
        type: :dynamic,
        status: :pending
      }
    end)
  end

  defp inferred_dynamic_edges(_origin, _nodes), do: []

  defp normalize_dynamic_edge(edge) when is_map(edge) do
    with id when is_binary(id) <- Map.get(edge, :id),
         from when is_binary(from) <- Map.get(edge, :from),
         to when is_binary(to) <- Map.get(edge, :to) do
      compact(%{
        id: id,
        from: from,
        to: to,
        type: Map.get(edge, :type, :dynamic),
        status: Map.get(edge, :status, :pending)
      })
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
       when terminal_status in [:completed, :failed, :cancelled] do
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

  defp normalize_runnable(runnable) when is_map(runnable), do: Map.new(runnable)

  defp manual_pause_data?(data) when is_map(data) do
    required_present?(data, [:run_id, :step, :kind]) and is_map(Map.get(data, :metadata, %{}))
  end

  defp manual_pause_data?(_data), do: false

  defp manual_resolution_data?(data) when is_map(data) do
    required_present?(data, [:run_id, :step, :action]) and is_map(Map.get(data, :result, %{}))
  end

  defp manual_resolution_data?(_data), do: false

  defp add_anomaly(%__MODULE__{} = projection, %Entry{} = entry, reason) do
    data = data_map(entry)

    anomaly =
      %{
        reason: reason,
        entry_type: entry.type
      }
      |> maybe_put_run_id(Map.get(data, :run_id))
      |> maybe_put_runnable_key(Map.get(data, :runnable_key))
      |> maybe_put_step(Map.get(data, :step))

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
