# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.DynamicWork do
  @moduledoc false

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.DynamicEdge
  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.Definition

  @type dynamic_work_error ::
          {:invalid_dynamic_work,
           {:attrs, :invalid}
           | {:dynamic_key, :invalid}
           | {:run, :terminal}
           | {:origin,
              :invalid
              | :missing_runnable_key
              | :missing_step
              | :missing_attempt
              | :unknown_runnable
              | :unapplied_runnable}
           | {:nodes,
              :invalid
              | :empty
              | {:node, non_neg_integer(), term()}
              | {:duplicate_id, String.t()}
              | {:duplicate_existing_id, String.t()}}
           | {:edges,
              :invalid
              | {:edge, non_neg_integer(), term()}
              | {:duplicate_id, String.t()}
              | {:unknown_node, String.t()}}
           | {:metadata, :invalid}
           | {:reason, :invalid}
           | {:status, :invalid}
           | {:action_registry, :required}
           | {:definition, term()}}

  @type validation_context :: %{
          required(:terminal?) => boolean(),
          required(:planned_runnables) => [map()],
          required(:dynamic_work) => [map()],
          required(:definition) => Definition.t() | nil,
          optional(:action_registry) => ActionRegistry.registry()
        }

  @spec new_entry(String.t(), map() | keyword(), DateTime.t(), validation_context()) ::
          {:ok, Entry.t() | :duplicate} | {:error, dynamic_work_error() | term()}
  @doc false
  def new_entry(run_id, attrs, %DateTime{} = occurred_at, context) when is_binary(run_id) do
    with {:ok, %{entry: entry, duplicate?: duplicate?}} <-
           preview(run_id, attrs, occurred_at, context) do
      if duplicate? do
        {:ok, :duplicate}
      else
        {:ok, entry}
      end
    end
  end

  def new_entry(_run_id, _attrs, %DateTime{}, _context), do: invalid(:attrs, :invalid)

  @spec preview(String.t(), map() | keyword(), DateTime.t(), validation_context()) ::
          {:ok, %{entry: Entry.t(), dynamic_work: map(), duplicate?: boolean()}}
          | {:error, dynamic_work_error() | term()}
  @doc false
  def preview(run_id, attrs, %DateTime{} = occurred_at, context) when is_binary(run_id) do
    with {:ok, attrs} <- validate_attrs(attrs),
         attrs <- Map.put(attrs, :run_id, run_id),
         attrs <- Map.put(attrs, :occurred_at, occurred_at),
         {:ok, entry} <- DispatchProtocol.new_entry(:dynamic_work_recorded, attrs),
         {:ok, duplicate?} <- validate_preview(entry, context) do
      {:ok,
       %{
         entry: entry,
         dynamic_work: projected_dynamic_work(entry.data),
         duplicate?: duplicate?
       }}
    end
  end

  def preview(_run_id, _attrs, %DateTime{}, _context), do: invalid(:attrs, :invalid)

  defp validate_attrs(attrs) when is_map(attrs) do
    attrs = Map.new(attrs)

    with {:ok, dynamic_key} <- non_empty_binary(attrs, :dynamic_key),
         {:ok, origin} <- origin(value(attrs, :origin)),
         {:ok, nodes} <- nodes(value(attrs, :nodes)),
         {:ok, edges} <- edges(value(attrs, :edges, []), origin, nodes),
         {:ok, metadata} <- metadata(value(attrs, :metadata, %{})),
         {:ok, reason} <- optional_dynamic_value(attrs, :reason),
         {:ok, status} <- optional_dynamic_value(attrs, :status) do
      {:ok,
       compact(%{
         dynamic_key: dynamic_key,
         origin: origin,
         reason: reason,
         status: status,
         nodes: nodes,
         edges: edges,
         metadata: metadata
       })}
    end
  end

  defp validate_attrs(attrs) when is_list(attrs) do
    if Keyword.keyword?(attrs) do
      validate_attrs(Map.new(attrs))
    else
      invalid(:attrs, :invalid)
    end
  end

  defp validate_attrs(_attrs), do: invalid(:attrs, :invalid)

  defp validate_preview(%Entry{}, %{terminal?: true}) do
    invalid(:run, :terminal)
  end

  defp validate_preview(%Entry{data: data} = entry, context) do
    with :ok <- allowed_node_actions(data.nodes, context) do
      validate_non_duplicate_preview(entry, context, duplicate_dynamic_work?(data, context))
    end
  end

  defp validate_non_duplicate_preview(_entry, _context, true), do: {:ok, true}

  defp validate_non_duplicate_preview(entry, context, false) do
    with {:ok, _entry} <- validate_new_dynamic_work(entry, context) do
      {:ok, false}
    end
  end

  defp validate_new_dynamic_work(%Entry{data: data} = entry, context) when is_map(context) do
    with :ok <- known_origin(data.origin, context),
         :ok <- available_node_ids(data.nodes, context) do
      {:ok, entry}
    end
  end

  defp duplicate_dynamic_work?(data, context) do
    candidate = projected_dynamic_work(data)

    context
    |> Map.get(:dynamic_work, [])
    |> Enum.any?(&same_dynamic_work?(&1, candidate))
  end

  defp same_dynamic_work?(left, right) when is_map(left) and is_map(right) do
    Map.take(left, comparable_dynamic_work_fields()) ==
      Map.take(right, comparable_dynamic_work_fields())
  end

  defp same_dynamic_work?(_left, _right), do: false

  defp comparable_dynamic_work_fields do
    [:dynamic_key, :status, :reason, :origin, :nodes, :edges, :metadata]
  end

  defp projected_dynamic_work(data) do
    nodes = Enum.map(data.nodes, &projected_dynamic_node/1)

    compact(%{
      dynamic_key: data.dynamic_key,
      status: Map.get(data, :status, :recorded),
      reason: Map.get(data, :reason),
      origin: data.origin,
      nodes: nodes,
      edges: projected_dynamic_edges(data, nodes),
      metadata: Map.get(data, :metadata, %{}),
      recorded_at: projected_recorded_at(data)
    })
  end

  defp projected_recorded_at(data) do
    case Map.get(data, :recorded_at) do
      %DateTime{} = recorded_at -> recorded_at
      _missing -> Map.get(data, :occurred_at)
    end
  end

  defp projected_dynamic_node(node) do
    compact(%{
      id: Map.fetch!(node, :id),
      action: Map.get(node, :action),
      input: Map.get(node, :input),
      status: Map.get(node, :status, :recorded),
      metadata: Map.get(node, :metadata, %{})
    })
  end

  defp projected_dynamic_edges(data, nodes) do
    case Map.get(data, :edges, []) do
      [] -> inferred_dynamic_edges(data.origin, nodes)
      edges -> edges
    end
  end

  defp inferred_dynamic_edges(origin, nodes) do
    origin_step = Map.fetch!(origin, :step)

    Enum.map(nodes, fn node ->
      node_id = Map.fetch!(node, :id)

      DynamicEdge.attrs(
        Enum.join([origin_step, "dynamic", node_id], ":"),
        origin_step,
        node_id,
        :dynamic,
        :pending
      )
    end)
  end

  defp known_origin(origin, context) do
    if Enum.any?(Map.get(context, :planned_runnables, []), &same_origin?(&1, origin)) do
      :ok
    else
      invalid(:origin, :unknown_runnable)
    end
  end

  defp same_origin?(runnable, origin) when is_map(runnable) and is_map(origin) do
    value(runnable, :runnable_key) == origin.runnable_key and
      value(runnable, :step) == origin.step and
      runnable_attempt(runnable) == origin.attempt
  end

  defp same_origin?(_runnable, _origin), do: false

  defp runnable_attempt(runnable) do
    value(runnable, :attempt_number) || value(runnable, :attempt)
  end

  defp available_node_ids(nodes, context) do
    existing_ids = existing_node_ids(context)

    case Enum.find_value(nodes, &existing_node_id(&1, existing_ids)) do
      nil -> :ok
      id -> invalid(:nodes, {:duplicate_existing_id, id})
    end
  end

  defp allowed_node_actions(nodes, %{action_registry: registry}) do
    nodes
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {node, index}, :ok ->
      case ActionRegistry.validate_action(Map.get(node, :action), registry) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, invalid(:nodes, {:node, index, {:action, reason}})}
      end
    end)
  end

  defp allowed_node_actions(_nodes, _context), do: :ok

  defp existing_node_id(node, existing_ids) when is_map(node) do
    id = Map.fetch!(node, :id)

    if MapSet.member?(existing_ids, id), do: id
  end

  defp existing_node_ids(context) do
    context
    |> declared_node_ids()
    |> MapSet.union(dynamic_node_ids(Map.get(context, :dynamic_work, [])))
  end

  defp declared_node_ids(context) do
    MapSet.union(
      context_declared_node_ids(context),
      planned_runnable_node_ids(Map.get(context, :planned_runnables, []))
    )
  end

  defp context_declared_node_ids(%{definition: definition}) when is_map(definition) do
    definition
    |> Definition.inspect_steps()
    |> Enum.reduce(MapSet.new(), fn step, ids ->
      put_step_id(step.step, ids)
    end)
  end

  defp context_declared_node_ids(_context), do: MapSet.new()

  defp planned_runnable_node_ids(runnables) do
    Enum.reduce(runnables, MapSet.new(), fn
      runnable, ids when is_map(runnable) ->
        runnable
        |> value(:step)
        |> put_step_id(ids)

      _runnable, ids ->
        ids
    end)
  end

  defp put_step_id(step, ids) when is_atom(step), do: MapSet.put(ids, Atom.to_string(step))
  defp put_step_id(step, ids) when is_binary(step) and step != "", do: MapSet.put(ids, step)
  defp put_step_id(_step, ids), do: ids

  defp dynamic_node_ids(dynamic_work) when is_list(dynamic_work) do
    Enum.reduce(dynamic_work, MapSet.new(), fn
      dynamic, ids when is_map(dynamic) ->
        Enum.reduce(value(dynamic, :nodes, []), ids, &put_dynamic_node_id/2)

      _dynamic, ids ->
        ids
    end)
  end

  defp dynamic_node_ids(_dynamic_work), do: MapSet.new()

  defp put_dynamic_node_id(node, node_ids) when is_map(node) do
    case value(node, :id) do
      id when is_binary(id) and id != "" -> MapSet.put(node_ids, id)
      _missing -> node_ids
    end
  end

  defp put_dynamic_node_id(_node, node_ids), do: node_ids

  defp origin(origin) when is_map(origin) do
    with {:ok, runnable_key} <- origin_binary(origin, :runnable_key, :missing_runnable_key),
         {:ok, step} <- origin_binary(origin, :step, :missing_step),
         {:ok, attempt} <- origin_attempt(origin) do
      {:ok,
       %{
         runnable_key: runnable_key,
         step: step,
         attempt: attempt
       }}
    end
  end

  defp origin(_origin), do: invalid(:origin, :invalid)

  defp origin_binary(origin, field, missing_reason) do
    case non_empty_binary(origin, field) do
      {:ok, value} -> {:ok, value}
      {:error, {:invalid_dynamic_work, {^field, :invalid}}} -> invalid(:origin, missing_reason)
    end
  end

  defp origin_attempt(origin) do
    case value(origin, :attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> {:ok, attempt}
      _invalid -> invalid(:origin, :missing_attempt)
    end
  end

  defp nodes(nodes) when is_list(nodes) do
    with :ok <- non_empty_nodes(nodes),
         {:ok, nodes} <- indexed_map(nodes, &node/2),
         :ok <- unique_nodes(nodes) do
      {:ok, nodes}
    end
  end

  defp nodes(_nodes), do: invalid(:nodes, :invalid)

  defp non_empty_nodes([]), do: invalid(:nodes, :empty)
  defp non_empty_nodes([_node | _rest]), do: :ok

  defp node(node, index) when is_map(node) do
    with {:ok, id} <- node_binary(node, :id, index),
         {:ok, action} <- optional_node_value(node, :action, index),
         {:ok, input} <- node_input(node, index),
         {:ok, retry} <- node_retry(node, index),
         {:ok, status} <- optional_node_value(node, :status, index),
         {:ok, metadata} <-
           metadata(value(node, :metadata, %{}), {:nodes, {:node, index, :metadata}}) do
      {:ok,
       compact(%{
         id: id,
         action: action,
         input: input,
         retry: retry,
         status: status,
         metadata: metadata
       })}
    end
  end

  defp node(_node, index), do: invalid(:nodes, {:node, index, :invalid})

  defp node_binary(node, field, index) do
    case non_empty_binary(node, field) do
      {:ok, value} ->
        {:ok, value}

      {:error, {:invalid_dynamic_work, {^field, :invalid}}} ->
        invalid(:nodes, {:node, index, field})
    end
  end

  defp unique_nodes(nodes) do
    case duplicate_value(nodes, :id) do
      nil -> :ok
      id -> invalid(:nodes, {:duplicate_id, id})
    end
  end

  defp edges(nil, _origin, _nodes), do: {:ok, []}

  defp edges(edges, origin, nodes) when is_list(edges) do
    with {:ok, edges} <- indexed_map(edges, &edge(&1, &2, origin, nodes)),
         :ok <- unique_edges(edges) do
      {:ok, edges}
    end
  end

  defp edges(_edges, _origin, _nodes), do: invalid(:edges, :invalid)

  defp edge(edge, index, origin, nodes) when is_map(edge) do
    with {:ok, id} <- edge_binary(edge, :id, index),
         {:ok, from} <- edge_binary(edge, :from, index),
         {:ok, to} <- edge_binary(edge, :to, index),
         :ok <- known_edge_from(from, origin, nodes),
         :ok <- known_edge_to(to, nodes),
         {:ok, type} <- optional_edge_value(edge, :type, index),
         {:ok, status} <- optional_edge_value(edge, :status, index) do
      {:ok, compact(DynamicEdge.attrs(id, from, to, type, status))}
    end
  end

  defp edge(_edge, index, _origin, _nodes), do: invalid(:edges, {:edge, index, :invalid})

  defp edge_binary(edge, field, index) do
    case non_empty_binary(edge, field) do
      {:ok, value} ->
        {:ok, value}

      {:error, {:invalid_dynamic_work, {^field, :invalid}}} ->
        invalid(:edges, {:edge, index, field})
    end
  end

  defp known_edge_from(from, origin, nodes) do
    if from == origin.step or dynamic_node_id?(nodes, from) do
      :ok
    else
      invalid(:edges, {:unknown_node, from})
    end
  end

  defp known_edge_to(to, nodes) do
    if dynamic_node_id?(nodes, to) do
      :ok
    else
      invalid(:edges, {:unknown_node, to})
    end
  end

  defp dynamic_node_id?(nodes, id) do
    Enum.any?(nodes, &(&1.id == id))
  end

  defp unique_edges(edges) do
    case duplicate_value(edges, :id) do
      nil -> :ok
      id -> invalid(:edges, {:duplicate_id, id})
    end
  end

  defp indexed_map(values, mapper) do
    values
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {value, index}, {:ok, acc} ->
      case mapper.(value, index) do
        {:ok, mapped} -> {:cont, {:ok, [mapped | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, mapped} -> {:ok, Enum.reverse(mapped)}
      {:error, _reason} = error -> error
    end
  end

  defp optional_node_value(node, field, index) do
    case optional_dynamic_value(node, field) do
      {:ok, value} ->
        {:ok, value}

      {:error, {:invalid_dynamic_work, {^field, :invalid}}} ->
        invalid(:nodes, {:node, index, field})
    end
  end

  defp node_input(node, index) do
    case value(node, :input, :__missing_input__) do
      :__missing_input__ -> {:ok, nil}
      input when is_map(input) -> {:ok, input}
      _invalid -> invalid(:nodes, {:node, index, :input})
    end
  end

  defp node_retry(node, index) do
    case value(node, :retry, nil) do
      nil ->
        {:ok, nil}

      retry when is_map(retry) ->
        validate_node_retry(Map.new(retry), index)

      retry when is_list(retry) ->
        if Keyword.keyword?(retry) do
          retry
          |> Map.new()
          |> validate_node_retry(index)
        else
          invalid(:nodes, {:node, index, :retry})
        end

      _invalid ->
        invalid(:nodes, {:node, index, :retry})
    end
  end

  defp validate_node_retry(retry, index) do
    case value(retry, :max_attempts) do
      max_attempts when is_integer(max_attempts) and max_attempts > 0 ->
        {:ok, %{max_attempts: max_attempts}}

      _invalid ->
        invalid(:nodes, {:node, index, {:retry, :invalid_max_attempts}})
    end
  end

  defp optional_edge_value(edge, field, index) do
    case optional_dynamic_value(edge, field) do
      {:ok, value} ->
        {:ok, value}

      {:error, {:invalid_dynamic_work, {^field, :invalid}}} ->
        invalid(:edges, {:edge, index, field})
    end
  end

  defp optional_dynamic_value(map, field) do
    case value(map, field) do
      nil -> {:ok, nil}
      value when is_atom(value) or is_binary(value) -> {:ok, value}
      _invalid -> invalid(field, :invalid)
    end
  end

  defp metadata(metadata, error \\ {:metadata, :invalid})
  defp metadata(metadata, _error) when is_map(metadata), do: {:ok, metadata}
  defp metadata(_metadata, error), do: {:error, {:invalid_dynamic_work, error}}

  defp non_empty_binary(map, field) do
    case value(map, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> invalid(field, :invalid)
    end
  end

  defp duplicate_value(values, field) do
    values
    |> Enum.map(&Map.fetch!(&1, field))
    |> Enum.frequencies()
    |> Enum.find_value(fn {value, count} ->
      if count > 1, do: value
    end)
  end

  defp value(map, field, default \\ nil) when is_map(map) and is_atom(field) do
    case Map.fetch(map, field) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(field), default)
    end
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp invalid(field, reason), do: {:error, {:invalid_dynamic_work, {field, reason}}}
end
