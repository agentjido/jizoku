# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.ChildStarter do
  @moduledoc false

  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.ScheduleIdentity
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Trace
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Step.Context
  alias Squidie.Workflow.Definition

  @max_link_retries 25

  @type start_error ::
          {:invalid_option, term()}
          | {:invalid_parent_context, term()}
          | {:invalid_parent_run, :terminal}
          | Starter.start_error()
          | term()

  @doc false
  @spec start_child_run(Context.t(), module(), atom(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, start_error()}
  def start_child_run(
        %Context{} = parent_context,
        child_workflow,
        child_trigger,
        payload,
        opts
      )
      when is_atom(child_workflow) and is_atom(child_trigger) and is_map(payload) and
             is_list(opts) do
    with {:ok, opts} <- child_partition_options(parent_context, opts),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, child_key} <- child_key(opts),
         {:ok, metadata} <- metadata(opts),
         {:ok, origin} <- origin(parent_context),
         {:ok, parent_run_id} <- parent_run_id(parent_context),
         {:ok, parent_trace} <- durable_parent_trace(storage, parent_run_id, parent_context),
         {:ok, child_trace} <- child_trace(parent_trace, origin),
         {:ok, resolved_payload} <- validate_child_start(child_workflow, child_trigger, payload),
         {:ok, child_run_id} <-
           child_run_id(
             parent_run_id,
             origin.step,
             child_workflow,
             child_trigger,
             child_key
           ),
         parent = parent_context(parent_run_id, origin, child_key, metadata),
         child = %{
           child_run_id: child_run_id,
           child_workflow: Definition.serialize_workflow(child_workflow),
           child_trigger: Definition.serialize_trigger(child_trigger),
           child_key: child_key,
           origin: origin,
           metadata: metadata,
           trace: child_trace
         },
         :ok <- ensure_child_startable(storage, child_run_id, child, parent, resolved_payload),
         {:ok, linked_parent} <-
           ensure_parent_linked(storage, parent_context, parent_run_id, child, now),
         :ok <- ensure_parent_active(storage, parent_context, parent_run_id),
         :ok <-
           ensure_child_startable(storage, child_run_id, child, linked_parent, resolved_payload),
         {:ok, command_signal} <-
           child_command_signal(
             child_run_id,
             child_workflow,
             child_trigger,
             payload,
             linked_parent,
             now
           ),
         {:ok, %Inspection.Snapshot{} = child_snapshot} <-
           Starter.start_run(
             child_workflow,
             child_trigger,
             payload,
             opts
             |> Keyword.put(:run_id, child_run_id)
             |> Keyword.put(:command_signal, command_signal)
             |> Keyword.put(:initial_context, %{parent: Map.delete(linked_parent, :child_trace)})
           ) do
      Inspection.snapshot(storage, child_snapshot.run_id, queue: queue, now: now)
    end
  end

  def start_child_run(%Context{}, _child_workflow, _child_trigger, _payload, opts)
      when is_list(opts) do
    {:error, {:invalid_payload, :expected_map}}
  end

  def start_child_run(_parent_context, _child_workflow, _child_trigger, _payload, _opts) do
    {:error, {:invalid_parent_context, :expected_step_context}}
  end

  defp child_partition_options(%Context{partition: parent_partition}, opts) do
    with {:ok, parent_partition} <- Options.partition(parent_partition),
         {:ok, option_partition} <- child_option_partition(opts) do
      reconcile_child_partition(opts, parent_partition, option_partition)
    end
  end

  defp child_option_partition(opts) do
    case Keyword.fetch(opts, :partition) do
      {:ok, partition} -> normalize_child_option_partition(partition)
      :error -> {:ok, :absent}
    end
  end

  defp normalize_child_option_partition(partition) do
    case Options.partition(partition) do
      {:ok, partition} -> {:ok, {:present, partition}}
      {:error, _reason} = error -> error
    end
  end

  defp reconcile_child_partition(opts, nil, :absent),
    do: {:ok, Keyword.put(opts, :partition, nil)}

  defp reconcile_child_partition(opts, parent_partition, :absent)
       when is_binary(parent_partition),
       do: {:ok, Keyword.put(opts, :partition, parent_partition)}

  defp reconcile_child_partition(opts, partition, {:present, partition}),
    do: {:ok, Keyword.put(opts, :partition, partition)}

  defp reconcile_child_partition(_opts, _parent_partition, _option_partition),
    do: {:error, {:partition_mismatch, :child}}

  defp child_key(opts) do
    case Keyword.fetch(opts, :child_key) do
      {:ok, child_key} when is_binary(child_key) ->
        Options.thread_part(child_key, :child_key)

      {:ok, child_key} when is_atom(child_key) ->
        Options.thread_part(Atom.to_string(child_key), :child_key)

      {:ok, _child_key} ->
        {:error, {:invalid_option, {:child_key, :invalid}}}

      :error ->
        {:error, {:invalid_option, {:child_key, :missing}}}
    end
  end

  defp metadata(opts) do
    case Keyword.get(opts, :metadata, %{}) do
      metadata when is_map(metadata) -> validate_metadata(metadata)
      _invalid -> {:error, {:invalid_option, {:metadata, :invalid}}}
    end
  end

  defp validate_metadata(metadata) do
    if Options.storage_safe_value?(metadata) do
      {:ok, metadata}
    else
      {:error, {:invalid_option, {:metadata, :invalid}}}
    end
  end

  defp parent_run_id(%Context{run_id: run_id}) when is_binary(run_id) do
    Options.thread_part(run_id, :parent_run_id)
  end

  defp parent_run_id(%Context{}), do: {:error, {:invalid_parent_context, :run_id}}

  defp origin(%Context{runnable_key: runnable_key, step: step, attempt: attempt})
       when is_binary(runnable_key) and is_atom(step) and is_integer(attempt) do
    {:ok,
     %{
       runnable_key: runnable_key,
       step: Definition.serialize_step(step),
       attempt: attempt
     }}
  end

  defp origin(%Context{runnable_key: runnable_key, step: step})
       when is_binary(runnable_key) and is_atom(step) do
    {:error, {:invalid_parent_context, :attempt}}
  end

  defp origin(%Context{}), do: {:error, {:invalid_parent_context, :origin}}

  defp durable_parent_trace(storage, parent_run_id, %Context{} = context) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, parent_run_id}),
         projection = Projection.rebuild(entries),
         runnable when is_map(runnable) <-
           Enum.find(
             Projection.planned_runnables(projection),
             &(runnable_value(&1, :runnable_key) == context.runnable_key)
           ) do
      {:ok, runnable_value(runnable, :trace)}
    else
      nil -> {:error, {:invalid_parent_context, :runnable_key}}
      {:error, _reason} = error -> error
    end
  end

  defp child_trace(nil, _origin), do: {:ok, nil}

  defp child_trace(trace, %{runnable_key: runnable_key}) when is_map(trace) do
    case Trace.child_of(trace, runnable_key) do
      {:ok, child_trace} -> {:ok, child_trace}
      {:error, _reason} -> {:ok, nil}
    end
  end

  defp child_trace(_legacy_untraced, _origin), do: {:ok, nil}

  defp child_command_signal(
         child_run_id,
         child_workflow,
         child_trigger,
         payload,
         linked_parent,
         %DateTime{} = now
       ) do
    Signal.start_run(child_workflow, child_trigger, payload,
      id: child_run_id,
      trace: Map.get(linked_parent, :child_trace),
      occurred_at: now
    )
  end

  defp validate_child_start(child_workflow, child_trigger, payload) do
    with {:ok, definition} <- Definition.load(child_workflow),
         {:ok, trigger} <- Definition.trigger(definition, child_trigger) do
      Definition.resolve_payload(trigger, payload)
    end
  end

  defp ensure_child_startable(storage, child_run_id, child, parent, resolved_payload) do
    case Journal.load_thread(storage, {:run, child_run_id}) do
      {:ok, %{entries: entries}} ->
        validate_existing_child(entries, child, parent, resolved_payload)

      {:error, :not_found} ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_existing_child(entries, child, parent, resolved_payload) do
    projection = Projection.rebuild(entries)

    if existing_child_matches?(projection, child, parent, resolved_payload) do
      :ok
    else
      {:error, :conflict}
    end
  end

  defp existing_child_matches?(%Projection{} = projection, child, parent, resolved_payload) do
    projection.workflow == child.child_workflow and
      projection.trigger == child.child_trigger and
      projection.input == resolved_payload and
      same_logical_parent?(parent_context(projection.context), parent)
  end

  defp same_logical_parent?(existing_parent, expected_parent)
       when is_map(existing_parent) and is_map(expected_parent) do
    parent_value(existing_parent, :run_id) == parent_value(expected_parent, :run_id) and
      parent_value(existing_parent, :step) == parent_value(expected_parent, :step) and
      parent_value(existing_parent, :child_key) == parent_value(expected_parent, :child_key) and
      parent_value(existing_parent, :metadata) == parent_value(expected_parent, :metadata)
  end

  defp same_logical_parent?(_existing_parent, _expected_parent), do: false

  defp parent_context(context) when is_map(context) do
    case Map.fetch(context, :parent) do
      {:ok, parent} -> parent
      :error -> Map.get(context, "parent")
    end
  end

  defp parent_value(parent, key) when is_map(parent) do
    case Map.fetch(parent, key) do
      {:ok, value} -> value
      :error -> Map.get(parent, Atom.to_string(key))
    end
  end

  defp child_run_id(parent_run_id, parent_step, child_workflow, child_trigger, child_key) do
    child_workflow = Definition.serialize_workflow(child_workflow)
    child_trigger = Definition.serialize_trigger(child_trigger)
    signal_id = Enum.join([parent_run_id, parent_step, child_key], "|")

    ScheduleIdentity.run_id(child_workflow, child_trigger, signal_id)
  end

  defp parent_context(parent_run_id, origin, child_key, metadata) do
    Map.new(
      run_id: parent_run_id,
      runnable_key: origin.runnable_key,
      step: origin.step,
      attempt: origin.attempt,
      child_key: child_key,
      metadata: metadata
    )
  end

  defp ensure_parent_linked(storage, parent_context, parent_run_id, child, %DateTime{} = now) do
    ensure_parent_linked(storage, parent_context, parent_run_id, child, now, @max_link_retries)
  end

  defp ensure_parent_linked(_storage, _parent_context, _parent_run_id, _child, %DateTime{}, 0),
    do: {:error, :conflict}

  defp ensure_parent_linked(
         storage,
         parent_context,
         parent_run_id,
         child,
         %DateTime{} = now,
         retries_left
       ) do
    with {:ok, %{rev: rev, entries: entries}} <-
           Journal.load_thread(storage, {:run, parent_run_id}),
         projection = Projection.rebuild(entries),
         :ok <- ensure_not_terminal(projection),
         :ok <- validate_parent_context(projection, parent_context) do
      link_missing_parent(
        storage,
        parent_context,
        parent_run_id,
        child,
        now,
        rev,
        entries,
        retries_left
      )
    else
      {:error, _reason} = error -> error
    end
  end

  defp link_missing_parent(
         storage,
         parent_context,
         parent_run_id,
         child,
         now,
         rev,
         entries,
         retries_left
       ) do
    case child_link_state(entries, child) do
      {:linked, linked_parent} ->
        {:ok, linked_parent}

      :conflict ->
        {:error, :conflict}

      :missing ->
        append_parent_child_link(
          storage,
          parent_context,
          parent_run_id,
          child,
          now,
          rev,
          retries_left
        )
    end
  end

  defp append_parent_child_link(
         storage,
         parent_context,
         parent_run_id,
         child,
         now,
         rev,
         retries_left
       ) do
    with {:ok, entry} <- child_link_entry(parent_run_id, child, now) do
      case Journal.append_entries(storage, [entry], expected_rev: rev) do
        {:ok, _thread} ->
          parent_from_child_link(entry)

        {:error, :conflict} ->
          ensure_parent_linked(
            storage,
            parent_context,
            parent_run_id,
            child,
            now,
            retries_left - 1
          )

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp ensure_parent_active(storage, parent_context, parent_run_id) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, parent_run_id}),
         projection = Projection.rebuild(entries),
         :ok <- ensure_not_terminal(projection) do
      validate_parent_context(projection, parent_context)
    end
  end

  defp ensure_not_terminal(%Projection{} = projection) do
    if Projection.terminal?(projection) do
      {:error, {:invalid_parent_run, :terminal}}
    else
      :ok
    end
  end

  defp validate_parent_context(%Projection{} = projection, %Context{} = context) do
    case validate_parent_workflow(projection, context) do
      :ok -> validate_parent_runnable(projection, context)
      {:error, _reason} = error -> error
    end
  end

  defp validate_parent_workflow(%Projection{workflow: workflow}, %Context{
         workflow: workflow_module
       }) do
    if workflow == Definition.serialize_workflow(workflow_module) do
      :ok
    else
      {:error, {:invalid_parent_context, :workflow}}
    end
  end

  defp validate_parent_runnable(%Projection{} = projection, %Context{} = context) do
    projection
    |> Projection.planned_runnables()
    |> Enum.find(&(runnable_value(&1, :runnable_key) == context.runnable_key))
    |> case do
      nil ->
        {:error, {:invalid_parent_context, :runnable_key}}

      runnable ->
        validate_parent_step(runnable, context)
    end
  end

  defp validate_parent_step(runnable, %Context{} = context) do
    if runnable_value(runnable, :step) == Definition.serialize_step(context.step) do
      :ok
    else
      {:error, {:invalid_parent_context, :step}}
    end
  end

  defp runnable_value(runnable, key) when is_map(runnable) do
    Map.get(runnable, key) || Map.get(runnable, Atom.to_string(key))
  end

  defp child_link_state(entries, child) do
    link_state =
      Enum.reduce_while(entries, :missing, fn
        %{type: :child_run_started} = entry, state ->
          cond do
            same_child_link?(entry, child) -> {:halt, {:match, entry}}
            same_child_key?(entry, child) -> {:cont, :conflict}
            true -> {:cont, state}
          end

        _entry, state ->
          {:cont, state}
      end)

    case link_state do
      {:match, entry} ->
        case parent_from_child_link(entry) do
          {:ok, parent} -> {:linked, parent}
          {:error, :conflict} -> :conflict
        end

      state ->
        state
    end
  end

  defp same_child_link?(entry, child) do
    entry_value(entry, :child_run_id) == child.child_run_id and
      entry_value(entry, :child_key) == child.child_key
  end

  defp same_child_key?(entry, child) do
    entry_value(entry, :child_key) == child.child_key and
      origin_step(entry_value(entry, :origin)) == child.origin.step
  end

  defp entry_value(entry, key) do
    Map.get(entry.data, key) || Map.get(entry.data, Atom.to_string(key))
  end

  defp origin_step(origin) when is_map(origin), do: origin_value(origin, :step)
  defp origin_step(_origin), do: nil

  defp parent_from_child_link(entry) do
    with run_id when is_binary(run_id) <- entry_value(entry, :run_id),
         %{} = origin <- entry_value(entry, :origin),
         runnable_key when is_binary(runnable_key) <- origin_value(origin, :runnable_key),
         step when is_binary(step) <- origin_value(origin, :step),
         attempt when is_integer(attempt) <- origin_value(origin, :attempt),
         child_key when is_binary(child_key) <- entry_value(entry, :child_key),
         metadata when is_map(metadata) <- entry_value(entry, :metadata) || %{} do
      {:ok,
       run_id
       |> parent_context(runnable_key, step, attempt, child_key, metadata)
       |> Map.put(:child_trace, entry_value(entry, :trace))}
    else
      _invalid -> {:error, :conflict}
    end
  end

  defp parent_context(parent_run_id, runnable_key, step, attempt, child_key, metadata) do
    Map.new(
      run_id: parent_run_id,
      runnable_key: runnable_key,
      step: step,
      attempt: attempt,
      child_key: child_key,
      metadata: metadata
    )
  end

  defp origin_value(origin, key) when is_map(origin) do
    case Map.fetch(origin, key) do
      {:ok, value} -> value
      :error -> Map.get(origin, Atom.to_string(key))
    end
  end

  defp child_link_entry(parent_run_id, child, %DateTime{} = now) do
    DispatchProtocol.new_entry(:child_run_started, %{
      run_id: parent_run_id,
      child_run_id: child.child_run_id,
      child_workflow: child.child_workflow,
      child_trigger: child.child_trigger,
      child_key: child.child_key,
      origin: child.origin,
      metadata: child.metadata,
      trace: child.trace,
      occurred_at: now
    })
  end
end
