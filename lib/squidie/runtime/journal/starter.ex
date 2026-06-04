defmodule Squidie.Runtime.Journal.Starter do
  @moduledoc """
  Journal-backed workflow start boundary for the Jido-native runtime.

  This module resolves the public Squidie workflow contract, plans initial
  runnables through Runic, appends durable run and run-index facts to
  `Jido.Storage`, then uses `WorkflowAgent` and `DispatchAgent` as rebuildable
  coordinators to schedule dispatch attempts from the journal.

  The journal runtime can execute normal action steps, immediate built-in `:log`
  steps, built-in `:wait` steps in transition and dependency workflows, and
  manual `:pause` or `:approval` boundaries. Manual boundaries persist
  inspectable intervention state and can be resumed or reviewed through journal
  manual controls. Callers enter this path explicitly with `runtime: :journal`,
  `journal_storage:`, and optional queue or clock overrides. No Jido primitive
  is required in workflow authoring.
  """

  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.AgentRecovery
  alias Squidie.Runtime.Deadline
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.CommandReceipt
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.RunCatalogProjection
  alias Squidie.Runtime.RunIndexProjection
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.RunicPlanner
  alias Squidie.Workflow.Spec

  @dispatch_schedule_retries 25
  @spec_fields [
    :workflow,
    :definition_version,
    :triggers,
    :payload,
    :steps,
    :transitions,
    :retries,
    :entry_steps,
    :initial_step,
    :entry_step
  ]

  @type start_error ::
          {:invalid_payload, Definition.payload_error_details()}
          | Definition.load_error()
          | Definition.trigger_error()
          | {:invalid_option,
             {:journal_storage, nil} | {:now, term()} | {:queue, term()} | {:run_id, term()}}
          | term()

  @doc """
  Starts a workflow run by appending Jido journal facts and scheduling dispatch.

  The returned snapshot is rebuilt from the same journal-backed read model used
  by `Squidie.inspect_run/2` with `read_model: :read_model`.
  """
  @spec start_run(module(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Inspection.Snapshot.t()}}
          | {:error, start_error()}
  def start_run(workflow, trigger_name, payload, opts)
      when is_atom(workflow) and is_map(payload) and is_list(opts) do
    with :ok <- validate_command_signal(opts),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, definition} <- Definition.load(workflow),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(workflow),
         {:ok, _planned, runnables} <- RunicPlanner.plan(planner, resolved_payload),
         {:ok, run_id} <- run_id(opts),
         :ok <- validate_initial_context(opts),
         {:ok, journal_runnables} <- journal_runnables(definition, run_id, queue, runnables, now),
         {:ok, start_state} <-
           ensure_run_started(
             storage,
             %{
               workflow: workflow,
               definition: definition,
               trigger: trigger,
               input: resolved_payload,
               run_id: run_id
             },
             journal_runnables,
             now,
             opts
           ) do
      complete_started_run(storage, workflow, run_id, queue, now, start_state, opts)
    end
  end

  @doc """
  Starts a runtime-authored workflow spec by appending journal facts and
  scheduling dispatch.
  """
  @spec start_spec_run(Spec.t() | map(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Inspection.Snapshot.t()}}
          | {:error, start_error()}
  def start_spec_run(spec, trigger_name, payload, opts)
      when is_map(payload) and is_list(opts) do
    with :ok <- validate_command_signal(opts),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, spec} <- runtime_spec(spec, opts),
         definition <- definition_from_spec(spec),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(spec),
         {:ok, _planned, runnables} <- RunicPlanner.plan(planner, resolved_payload),
         {:ok, run_id} <- run_id(opts),
         :ok <- validate_initial_context(opts),
         {:ok, journal_runnables} <- journal_runnables(definition, run_id, queue, runnables, now),
         {:ok, start_state} <-
           ensure_run_started(
             storage,
             %{
               workflow: spec.workflow,
               definition: definition,
               definition_source: :runtime_spec,
               definition_spec: spec,
               trigger: trigger,
               input: resolved_payload,
               run_id: run_id
             },
             journal_runnables,
             now,
             opts
           ) do
      complete_started_run(storage, spec.workflow, run_id, queue, now, start_state, opts)
    end
  end

  def start_spec_run(_spec, _trigger_name, _payload, opts) when is_list(opts) do
    {:error, {:invalid_payload, :expected_map}}
  end

  defp runtime_spec(spec, opts) do
    with {:ok, resolved_spec} <- resolve_runtime_spec(spec, opts),
         resolved_spec <- to_spec_struct(resolved_spec),
         :ok <- Spec.validate(resolved_spec) do
      {:ok, resolved_spec}
    end
  end

  defp resolve_runtime_spec(spec, opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} -> ActionRegistry.resolve_spec(spec, registry)
      :error -> {:ok, spec}
    end
  end

  defp to_spec_struct(%Spec{} = spec), do: spec

  defp to_spec_struct(spec) when is_map(spec) do
    struct(Spec, spec_field_values(spec))
  end

  defp definition_from_spec(%Spec{} = spec), do: Map.from_struct(spec)

  defp spec_field_values(spec) when is_map(spec) do
    Map.new(@spec_fields, fn field ->
      {field, spec_field_value(spec, field)}
    end)
  end

  defp spec_field_value(spec, field) do
    case Map.fetch(spec, field) do
      {:ok, value} -> value
      :error -> Map.get(spec, Atom.to_string(field))
    end
  end

  defp trigger(definition, nil),
    do: Definition.trigger(definition, Definition.default_trigger(definition))

  defp trigger(definition, trigger_name), do: Definition.trigger(definition, trigger_name)

  defp ensure_run_started(
         storage,
         attrs,
         runnables,
         %DateTime{} = now,
         opts
       ) do
    attrs =
      attrs
      |> Map.put_new(:definition_source, :module)
      |> Map.put_new(:definition_spec, nil)

    do_ensure_run_started(storage, attrs, runnables, now, opts)
  end

  defp do_ensure_run_started(
         storage,
         %{
           workflow: workflow,
           definition: definition,
           definition_source: definition_source,
           definition_spec: definition_spec,
           trigger: trigger,
           input: input,
           run_id: run_id
         },
         runnables,
         %DateTime{} = now,
         opts
       ) do
    expected_fingerprint = Definition.fingerprint(definition)

    run_started_attrs =
      maybe_put_runtime_definition(
        %{
          run_id: run_id,
          workflow: Definition.serialize_workflow(workflow),
          trigger: Atom.to_string(Map.fetch!(trigger, :name)),
          input: input,
          context: initial_context(opts),
          replayed_from_run_id: replayed_from_run_id(opts),
          definition_version: definition.definition_version,
          definition_fingerprint: expected_fingerprint,
          occurred_at: now
        },
        definition_source,
        definition_spec
      )

    with {:ok, run_started} <-
           DispatchProtocol.new_entry(:run_started, run_started_attrs),
         {:ok, run_signal_received} <-
           CommandReceipt.new(
             start_signal_type(opts),
             %{
               run_id: run_id,
               payload: start_signal_payload(opts, workflow, trigger, input),
               metadata: start_signal_metadata(opts),
               idempotency_key: start_signal_idempotency_key(opts)
             },
             now
           ),
         {:ok, runnables_planned} <-
           DispatchProtocol.new_entry(:runnables_planned, %{
             run_id: run_id,
             runnables: runnables,
             occurred_at: now
           }),
         {:ok, _run_thread} <-
           Journal.append_entries(storage, [run_signal_received, run_started, runnables_planned],
             expected_rev: 0
           ) do
      {:ok, :created}
    else
      {:error, :conflict} ->
        rebuild_existing_start(storage, workflow, run_id, runnables, expected_fingerprint, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_put_runtime_definition(attrs, :runtime_spec, %Spec{} = spec) do
    attrs =
      attrs
      |> Map.put(:definition_source, :runtime_spec)
      |> Map.put(:definition_spec, spec)

    attrs
  end

  defp maybe_put_runtime_definition(attrs, _source, _spec), do: attrs

  defp start_signal_type(opts) do
    case start_command_signal(opts) do
      %Signal{type: type} ->
        type

      nil ->
        derived_start_signal_type(opts)
    end
  end

  defp start_signal_idempotency_key(opts) do
    case start_command_signal(opts) do
      %Signal{idempotency_key: idempotency_key} when is_binary(idempotency_key) ->
        idempotency_key

      _no_signal_key ->
        context = initial_context(opts)

        case schedule_idempotency_key(context) do
          nil -> schedule_signal_id(context)
          idempotency_key -> idempotency_key
        end
    end
  end

  defp start_signal_payload(opts, workflow, trigger, input) do
    case start_command_signal(opts) do
      %Signal{type: :start_run, payload: %{trigger: nil} = payload} ->
        Map.put(payload, :trigger, Atom.to_string(Map.fetch!(trigger, :name)))

      %Signal{payload: payload} ->
        payload

      nil ->
        case replayed_from_run_id(opts) do
          run_id when is_binary(run_id) ->
            %{run_id: run_id, allow_irreversible: replay_allow_irreversible?(opts)}

          _not_replay ->
            %{
              workflow: Definition.serialize_workflow(workflow),
              trigger: Atom.to_string(Map.fetch!(trigger, :name)),
              input: input
            }
        end
    end
  end

  defp start_signal_metadata(opts) do
    case start_command_signal(opts) do
      %Signal{metadata: metadata} -> metadata
      nil -> %{}
    end
  end

  defp command_signal(opts) do
    case Keyword.get(opts, :command_signal) do
      %Signal{} = signal -> signal
      _none -> nil
    end
  end

  defp validate_command_signal(opts) do
    case Keyword.fetch(opts, :command_signal) do
      :error ->
        :ok

      {:ok, %Signal{type: type}} when type in [:start_run, :start_cron, :replay_run] ->
        :ok

      {:ok, %Signal{type: type}} ->
        {:error, {:unsupported_command_signal, type}}

      {:ok, invalid} ->
        {:error, {:invalid_option, {:command_signal, invalid}}}
    end
  end

  defp start_command_signal(opts) do
    case command_signal(opts) do
      %Signal{type: type} = signal when type in [:start_run, :start_cron, :replay_run] -> signal
      _unsupported_or_missing -> nil
    end
  end

  defp derived_start_signal_type(opts) do
    cond do
      replayed_from_run_id(opts) -> :replay_run
      schedule_context?(initial_context(opts)) -> :start_cron
      true -> :start_run
    end
  end

  defp replay_allow_irreversible?(opts), do: Keyword.get(opts, :allow_irreversible) == true

  defp schedule_signal_id(context) when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:signal_id)
  end

  defp ensure_run_queued(storage, run_id, queue, %DateTime{} = now) do
    ensure_run_queued(storage, run_id, queue, now, @dispatch_schedule_retries)
  end

  defp ensure_run_queued(_storage, _run_id, _queue, %DateTime{}, 0), do: {:error, :conflict}

  defp ensure_run_queued(storage, run_id, queue, %DateTime{} = now, retries_left) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
      case DispatchAgent.ensure_run_queued(storage, dispatch_agent, run_id, now: now) do
        {:ok, _queued} ->
          :ok

        {:error, :conflict} ->
          ensure_run_queued(storage, run_id, queue, now, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp journal_runnables(definition, run_id, queue, runnables, %DateTime{} = now) do
    result =
      Enum.reduce_while(runnables, {:ok, []}, fn runnable, {:ok, acc} ->
        case journal_runnable(definition, run_id, queue, runnable, now) do
          {:ok, journal_runnable} -> {:cont, {:ok, [journal_runnable | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, journal_runnables} -> {:ok, Enum.reverse(journal_runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp journal_runnable(
         definition,
         run_id,
         queue,
         %{step: step, input: input},
         %DateTime{} = now
       )
       when is_atom(step) do
    attempt_number = 1
    step_name = Definition.serialize_step(step)
    runnable_key = "#{run_id}:#{step_name}:#{attempt_number}"

    with {:ok, recovery} <- replay_recovery_policy(definition, step),
         {:ok, deadline} <- Deadline.from_definition(definition, step, now) do
      runnable = %{
        run_id: run_id,
        runnable_key: runnable_key,
        idempotency_key: runnable_key,
        attempt_number: attempt_number,
        queue: queue,
        step: step_name,
        input: input || %{},
        recovery: recovery,
        visible_at: now
      }

      {:ok, maybe_put(runnable, :deadline, deadline)}
    end
  end

  defp journal_runnable(_definition, _run_id, _queue, runnable, %DateTime{}) do
    {:error, {:invalid_runnable, runnable}}
  end

  defp replay_recovery_policy(definition, step) do
    with {:ok, recovery} <- Definition.step_recovery_policy(definition, step) do
      {:ok, Definition.serialize_recovery_policy(recovery)}
    end
  end

  defp put_checkpoints(storage, workflow_agent, dispatch_agent, %DateTime{} = now) do
    _checkpoint_result = WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: now)
    _checkpoint_result = DispatchAgent.put_checkpoint(storage, dispatch_agent, updated_at: now)

    :ok
  end

  defp ensure_run_indexed(storage, workflow, run_id, queue, %DateTime{} = now) do
    workflow = Definition.serialize_workflow(workflow)

    with :ok <- ensure_workflow_run_indexed(storage, workflow, run_id, queue, now) do
      ensure_global_run_cataloged(storage, workflow, run_id, queue, now)
    end
  end

  defp ensure_workflow_run_indexed(storage, workflow, run_id, queue, %DateTime{} = now) do
    with {:ok, entry} <-
           DispatchProtocol.new_entry(:run_indexed, %{
             run_id: run_id,
             workflow: workflow,
             queue: queue,
             occurred_at: now
           }) do
      ensure_run_index_entry(storage, workflow, run_id, entry, @dispatch_schedule_retries)
    end
  end

  defp ensure_global_run_cataloged(storage, workflow, run_id, queue, %DateTime{} = now) do
    with {:ok, entry} <-
           DispatchProtocol.new_entry(:run_cataloged, %{
             run_id: run_id,
             workflow: workflow,
             queue: queue,
             occurred_at: now
           }) do
      ensure_run_cataloged(storage, run_id, entry, @dispatch_schedule_retries)
    end
  end

  defp ensure_run_index_entry(storage, workflow, run_id, entry, retries_left) do
    case load_run_index(storage, workflow) do
      {:ok, %{rev: rev, projection: projection}} ->
        append_missing_run_index(storage, workflow, run_id, entry, rev, projection, retries_left)

      {:error, _reason} = error ->
        error
    end
  end

  defp append_missing_run_index(storage, workflow, run_id, entry, rev, projection, retries_left) do
    case run_index_state(projection, run_id, workflow, entry.data.queue) do
      :matching ->
        :ok

      :missing ->
        append_run_index_entry(storage, workflow, run_id, entry, rev, retries_left)

      {:conflicting, _summary} ->
        {:error, {:conflicting_run_index, run_id}}
    end
  end

  defp append_run_index_entry(storage, workflow, run_id, entry, rev, retries_left) do
    case Journal.append_entries(storage, [entry], expected_rev: rev) do
      {:ok, _thread} ->
        :ok

      {:error, :conflict} when retries_left > 0 ->
        ensure_run_index_entry(storage, workflow, run_id, entry, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp load_run_index(storage, workflow) do
    case Journal.load_thread(storage, {:run_index, workflow}) do
      {:ok, %{rev: rev, entries: entries}} ->
        projection =
          workflow
          |> RunIndexProjection.new()
          |> RunIndexProjection.replay(entries)

        {:ok, %{rev: rev, projection: projection}}

      {:error, :not_found} ->
        {:ok, %{rev: 0, projection: RunIndexProjection.new(workflow)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp run_index_state(%RunIndexProjection{} = projection, run_id, workflow, queue) do
    projection
    |> RunIndexProjection.runs()
    |> Enum.find(&(&1.run_id == run_id))
    |> case do
      nil ->
        :missing

      %{workflow: ^workflow, queue: ^queue} ->
        :matching

      summary ->
        {:conflicting, summary}
    end
  end

  defp ensure_run_cataloged(storage, run_id, entry, retries_left) do
    case load_run_catalog(storage) do
      {:ok, %{rev: rev, projection: projection}} ->
        append_missing_run_catalog(storage, run_id, entry, rev, projection, retries_left)

      {:error, _reason} = error ->
        error
    end
  end

  defp append_missing_run_catalog(storage, run_id, entry, rev, projection, retries_left) do
    case run_catalog_state(projection, run_id, entry.data.workflow, entry.data.queue) do
      :matching ->
        :ok

      :missing ->
        append_run_catalog_entry(storage, run_id, entry, rev, retries_left)

      {:conflicting, _summary} ->
        {:error, {:conflicting_run_catalog, run_id}}
    end
  end

  defp append_run_catalog_entry(storage, run_id, entry, rev, retries_left) do
    case Journal.append_entries(storage, [entry], expected_rev: rev) do
      {:ok, _thread} ->
        :ok

      {:error, :conflict} when retries_left > 0 ->
        ensure_run_cataloged(storage, run_id, entry, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp load_run_catalog(storage) do
    case Journal.load_thread(storage, {:run_catalog, "all"}) do
      {:ok, %{rev: rev, entries: entries}} ->
        {:ok, %{rev: rev, projection: RunCatalogProjection.rebuild(entries)}}

      {:error, :not_found} ->
        {:ok, %{rev: 0, projection: RunCatalogProjection.new()}}

      {:error, _reason} = error ->
        error
    end
  end

  defp run_catalog_state(%RunCatalogProjection{} = projection, run_id, workflow, queue) do
    projection
    |> RunCatalogProjection.runs()
    |> Enum.find(&(&1.run_id == run_id))
    |> case do
      nil ->
        :missing

      %{workflow: ^workflow, queue: ^queue} ->
        :matching

      summary ->
        {:conflicting, summary}
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp complete_started_run(
         storage,
         workflow,
         run_id,
         queue,
         %DateTime{} = now,
         start_state,
         opts
       ) do
    queue = completion_queue(start_state, queue)

    case do_complete_started_run(storage, workflow, run_id, queue, now) do
      {:ok, %Inspection.Snapshot{} = snapshot} ->
        duplicate_start_result(snapshot, start_state, opts)

      {:error, reason} ->
        {:error, {:journal_start_committed, run_id, reason}}
    end
  end

  defp completion_queue({:existing_schedule_duplicate, queue}, _queue), do: queue
  defp completion_queue(_start_state, queue), do: queue

  defp duplicate_start_result(
         %Inspection.Snapshot{} = snapshot,
         {:existing_schedule_duplicate, _queue},
         _opts
       ) do
    {:ok, {:duplicate_schedule_start, snapshot}}
  end

  defp duplicate_start_result(%Inspection.Snapshot{} = snapshot, :existing, opts) do
    if Keyword.get(opts, :duplicate_schedule_start, false) do
      {:ok, {:duplicate_schedule_start, snapshot}}
    else
      {:ok, snapshot}
    end
  end

  defp duplicate_start_result(%Inspection.Snapshot{} = snapshot, _start_state, _opts) do
    {:ok, snapshot}
  end

  defp do_complete_started_run(storage, workflow, run_id, queue, %DateTime{} = now) do
    with :ok <- ensure_run_indexed(storage, workflow, run_id, queue, now),
         :ok <- ensure_run_queued(storage, run_id, queue, now),
         {:ok, %{workflow_agent: workflow_agent, dispatch_agent: dispatch_agent}} <-
           recover_or_continue(storage, run_id, queue, now),
         :ok <- put_checkpoints(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  defp rebuild_existing_start(
         storage,
         workflow,
         run_id,
         expected_runnables,
         expected_fingerprint,
         opts
       ) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, existing_fingerprint} <- persisted_definition_fingerprint(storage, run_id) do
      mode = existing_start_mode(opts)

      validate_existing_start_mode(
        mode,
        workflow_agent,
        workflow,
        expected_runnables,
        expected_fingerprint,
        existing_fingerprint,
        opts
      )
    end
  end

  defp existing_start_mode(opts) do
    if Keyword.get(opts, :duplicate_schedule_start, false) do
      :schedule_duplicate
    else
      :strict
    end
  end

  defp validate_existing_start_mode(
         :schedule_duplicate,
         workflow_agent,
         workflow,
         _expected_runnables,
         _expected_fingerprint,
         _existing_fingerprint,
         opts
       ) do
    validate_existing_schedule_start(workflow_agent, workflow, opts)
  end

  defp validate_existing_start_mode(
         :strict,
         workflow_agent,
         workflow,
         expected_runnables,
         expected_fingerprint,
         existing_fingerprint,
         _opts
       ) do
    with :ok <-
           validate_existing_start(
             workflow_agent,
             workflow,
             expected_runnables,
             expected_fingerprint,
             existing_fingerprint
           ) do
      {:ok, :existing}
    end
  end

  defp validate_existing_schedule_start(workflow_agent, workflow, opts) do
    existing_workflow = workflow_agent.state.workflow
    expected_workflow = Definition.serialize_workflow(workflow)

    expected_idempotency_key =
      schedule_idempotency_key(initial_context(opts)) || start_signal_idempotency_key(opts)

    existing_idempotency_key =
      existing_schedule_idempotency_key(workflow_agent) ||
        existing_start_signal_idempotency_key(workflow_agent, start_signal_type(opts))

    cond do
      existing_workflow != expected_workflow ->
        {:error, :conflict}

      is_nil(expected_idempotency_key) ->
        {:error, :conflict}

      existing_idempotency_key != expected_idempotency_key ->
        {:error, :conflict}

      not equivalent_start_signal_payload?(workflow_agent, workflow, opts) ->
        {:error, :conflict}

      true ->
        existing_queue(workflow_agent)
    end
  end

  defp existing_schedule_idempotency_key(%{
         state: %{projection: %Projection{context: context}}
       })
       when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:idempotency_key)
  end

  defp existing_schedule_idempotency_key(_workflow_agent), do: nil

  defp existing_start_signal_idempotency_key(
         %{state: %{projection: %Projection{} = projection}},
         signal_type
       )
       when is_atom(signal_type) do
    serialized_signal_type = Atom.to_string(signal_type)

    projection
    |> Projection.command_history()
    |> Enum.find_value(fn
      %{signal_type: ^serialized_signal_type, idempotency_key: idempotency_key}
      when is_binary(idempotency_key) ->
        idempotency_key

      _command ->
        false
    end)
  end

  defp existing_start_signal_idempotency_key(_workflow_agent, _signal_type), do: nil

  defp equivalent_start_signal_payload?(workflow_agent, workflow, opts) do
    case start_command_signal(opts) do
      %Signal{type: type, idempotency_key: idempotency_key, payload: payload}
      when type in [:start_run, :start_cron, :replay_run] and is_binary(idempotency_key) ->
        existing_start_signal_payload_matches?(
          workflow_agent,
          workflow,
          Atom.to_string(type),
          idempotency_key,
          payload
        )

      _no_command_signal ->
        true
    end
  end

  defp existing_start_signal_payload_matches?(
         %{state: %{projection: %Projection{} = projection}},
         workflow,
         signal_type,
         idempotency_key,
         payload
       ) do
    with {:ok, expected_payload} <- canonical_start_signal_payload(signal_type, payload, workflow),
         {:ok, existing_payload} <-
           existing_start_signal_payload(projection, signal_type, idempotency_key, workflow) do
      existing_payload == expected_payload
    else
      _missing_or_invalid -> false
    end
  end

  defp existing_start_signal_payload_matches?(
         _workflow_agent,
         _workflow,
         _signal_type,
         _idempotency_key,
         _payload
       ),
       do: false

  defp existing_start_signal_payload(projection, signal_type, idempotency_key, workflow) do
    projection
    |> Projection.command_history()
    |> Enum.find_value(fn
      %{
        signal_type: ^signal_type,
        idempotency_key: ^idempotency_key,
        payload: existing_payload
      } ->
        canonical_start_signal_payload(signal_type, existing_payload, workflow)

      _command ->
        nil
    end) || :error
  end

  defp canonical_start_signal_payload("start_run", payload, workflow) when is_map(payload) do
    with {:ok, trigger} <- canonical_start_run_trigger(payload_value(payload, :trigger), workflow) do
      {:ok,
       %{
         workflow: payload_value(payload, :workflow),
         trigger: trigger,
         input: payload_value(payload, :input)
       }}
    end
  end

  defp canonical_start_signal_payload("start_cron", payload, _workflow) when is_map(payload) do
    {:ok,
     %{
       workflow: payload_value(payload, :workflow),
       trigger: payload_value(payload, :trigger),
       input: payload_value(payload, :input)
     }}
  end

  defp canonical_start_signal_payload(_signal_type, payload, _workflow) when is_map(payload),
    do: {:ok, payload}

  defp canonical_start_signal_payload(_signal_type, _payload, _workflow), do: :error

  defp canonical_start_run_trigger(nil, workflow) do
    with {:ok, definition} <- Definition.load(workflow) do
      {:ok, Atom.to_string(Definition.default_trigger(definition))}
    end
  end

  defp canonical_start_run_trigger(trigger, _workflow) when is_binary(trigger), do: {:ok, trigger}
  defp canonical_start_run_trigger(_trigger, _workflow), do: :error

  defp payload_value(payload, key) when is_map(payload) and is_atom(key) do
    string_key = Atom.to_string(key)

    cond do
      Map.has_key?(payload, key) -> Map.fetch!(payload, key)
      Map.has_key?(payload, string_key) -> Map.fetch!(payload, string_key)
      true -> nil
    end
  end

  defp schedule_idempotency_key(context) when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:idempotency_key)
  end

  defp schedule_context(context) do
    case Map.fetch(context, :schedule) do
      {:ok, schedule} -> schedule
      :error -> Map.get(context, "schedule", %{})
    end
  end

  defp schedule_context?(context) when is_map(context) do
    case schedule_context(context) do
      schedule when is_map(schedule) -> map_size(schedule) > 0
      _missing -> false
    end
  end

  defp schedule_value(schedule, key) when is_map(schedule) do
    case Map.fetch(schedule, key) do
      {:ok, value} -> value
      :error -> Map.get(schedule, Atom.to_string(key))
    end
  end

  defp schedule_value(_schedule, _key), do: nil

  defp existing_queue(workflow_agent) do
    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.find_value(&runnable_value(&1, :queue))
    |> case do
      queue when is_binary(queue) -> {:ok, {:existing_schedule_duplicate, queue}}
      _missing_queue -> {:error, :conflict}
    end
  end

  defp validate_existing_start(
         workflow_agent,
         workflow,
         expected_runnables,
         expected_fingerprint,
         existing_fingerprint
       ) do
    existing_workflow = workflow_agent.state.workflow
    expected_workflow = Definition.serialize_workflow(workflow)
    existing_runnables = WorkflowAgent.planned_runnables(workflow_agent)

    cond do
      existing_workflow != expected_workflow ->
        {:error, :conflict}

      not equivalent_definition_fingerprint?(existing_fingerprint, expected_fingerprint) ->
        {:error, :conflict}

      equivalent_runnables?(existing_runnables, expected_runnables) ->
        :ok

      true ->
        {:error, :conflict}
    end
  end

  defp persisted_definition_fingerprint(storage, run_id) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, run_id}) do
      fingerprint =
        Enum.find_value(entries, fn
          %{type: :run_started, data: data} ->
            definition_metadata_value(data, :definition_fingerprint)

          _entry ->
            nil
        end)

      {:ok, fingerprint}
    end
  end

  defp definition_metadata_value(data, key) when is_map(data) and is_atom(key) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end

  defp equivalent_definition_fingerprint?(existing_fingerprint, expected_fingerprint),
    do: existing_fingerprint == expected_fingerprint

  defp equivalent_runnables?(left, right) do
    if length(left) != length(right) do
      false
    else
      left =
        left
        |> Enum.map(&stable_runnable/1)
        |> Enum.sort()

      right =
        right
        |> Enum.map(&stable_runnable/1)
        |> Enum.sort()

      left == right
    end
  end

  defp stable_runnable(runnable) when is_map(runnable) do
    %{
      run_id: runnable_value(runnable, :run_id),
      runnable_key: runnable_value(runnable, :runnable_key),
      idempotency_key: runnable_value(runnable, :idempotency_key),
      attempt_number: runnable_value(runnable, :attempt_number),
      queue: runnable_value(runnable, :queue),
      step: runnable_value(runnable, :step),
      input: runnable_value(runnable, :input)
    }
  end

  defp recover_or_continue(storage, run_id, queue, %DateTime{} = now) do
    case recover(storage, run_id, queue, now, @dispatch_schedule_retries) do
      {:ok, recovery} ->
        {:ok, recovery}

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
             {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
          {:ok,
           %{
             workflow_agent: workflow_agent,
             dispatch_agent: dispatch_agent,
             scheduled_runnables: [],
             applied_attempts: []
           }}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp recover(storage, run_id, queue, now, retries_left) do
    case AgentRecovery.recover(storage, run_id, queue, now: now) do
      {:ok, recovery} ->
        {:ok, recovery}

      {:error, :conflict} when retries_left > 0 ->
        recover(storage, run_id, queue, now, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp journal_storage(opts) do
    opts
    |> Keyword.get(:journal_storage)
    |> Options.storage()
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp now(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp run_id(opts) do
    case Keyword.get(opts, :run_id, Ecto.UUID.generate()) do
      run_id -> Options.uuid(run_id)
    end
  end

  defp validate_initial_context(opts) do
    opts
    |> Keyword.get(:initial_context, %{})
    |> validate_initial_parent_context()
  end

  defp validate_initial_parent_context(context) when is_map(context) do
    case initial_parent_context(context) do
      nil -> :ok
      parent when is_map(parent) -> validate_reserved_parent_context(parent)
      _parent -> {:error, {:invalid_initial_context, {:parent, :invalid}}}
    end
  end

  defp validate_initial_parent_context(_context), do: :ok

  defp initial_parent_context(context) do
    case Map.fetch(context, :parent) do
      {:ok, parent} -> parent
      :error -> Map.get(context, "parent")
    end
  end

  defp validate_reserved_parent_context(parent) do
    cond do
      parent_extra_keys?(parent) ->
        {:error, {:invalid_initial_context, {:parent, :invalid}}}

      not valid_parent_identity?(parent) ->
        {:error, {:invalid_initial_context, {:parent, :invalid}}}

      not storage_safe_parent_metadata?(parent_value(parent, :metadata)) ->
        {:error, {:invalid_initial_context, {:parent, :invalid}}}

      true ->
        :ok
    end
  end

  defp parent_extra_keys?(parent) do
    Enum.any?(Map.keys(parent), fn key ->
      key not in [:run_id, :runnable_key, :step, :attempt, :child_key, :metadata] and
        key not in ["run_id", "runnable_key", "step", "attempt", "child_key", "metadata"]
    end)
  end

  defp valid_parent_identity?(parent) do
    is_binary(parent_value(parent, :run_id)) and
      is_binary(parent_value(parent, :runnable_key)) and
      is_binary(parent_value(parent, :step)) and
      is_integer(parent_value(parent, :attempt)) and
      is_binary(parent_value(parent, :child_key))
  end

  defp storage_safe_parent_metadata?(metadata) when is_map(metadata),
    do: storage_safe_value?(metadata)

  defp storage_safe_parent_metadata?(nil), do: true

  defp storage_safe_parent_metadata?(_metadata), do: false

  defp storage_safe_value?(value) when is_binary(value) or is_number(value) or is_boolean(value),
    do: true

  defp storage_safe_value?(nil), do: true

  defp storage_safe_value?(values) when is_list(values),
    do: Enum.all?(values, &storage_safe_value?/1)

  defp storage_safe_value?(%{} = map) when not is_struct(map) do
    Enum.all?(map, fn
      {key, value} when is_binary(key) or is_atom(key) -> storage_safe_value?(value)
      {_key, _value} -> false
    end)
  end

  defp storage_safe_value?(_value), do: false

  defp parent_value(parent, key) when is_map(parent) do
    case Map.fetch(parent, key) do
      {:ok, value} -> value
      :error -> Map.get(parent, Atom.to_string(key))
    end
  end

  defp initial_context(opts) do
    opts
    |> Keyword.get(:initial_context, %{})
    |> pick_reserved_context()
  end

  defp pick_reserved_context(context) when is_map(context) do
    context
    |> Map.take([:schedule, "schedule", :parent, "parent"])
    |> normalize_schedule_context()
    |> normalize_parent_context(context)
  end

  defp pick_reserved_context(_context), do: %{}

  defp normalize_schedule_context(%{schedule: nil}), do: %{}

  defp normalize_schedule_context(%{schedule: schedule}) do
    %{schedule: schedule}
  end

  defp normalize_schedule_context(%{"schedule" => nil}), do: %{}

  defp normalize_schedule_context(%{"schedule" => schedule}) do
    %{schedule: schedule}
  end

  defp normalize_schedule_context(_context), do: %{}

  defp normalize_parent_context(context, source) do
    case Map.fetch(source, :parent) do
      {:ok, parent} when is_map(parent) ->
        Map.put(context, :parent, normalize_reserved_parent_context(parent))

      _missing ->
        normalize_string_parent_context(context, source)
    end
  end

  defp normalize_string_parent_context(context, source) do
    case Map.fetch(source, "parent") do
      {:ok, parent} when is_map(parent) ->
        Map.put(context, :parent, normalize_reserved_parent_context(parent))

      _missing ->
        context
    end
  end

  defp normalize_reserved_parent_context(parent) do
    %{
      run_id: parent_value(parent, :run_id),
      runnable_key: parent_value(parent, :runnable_key),
      step: parent_value(parent, :step),
      attempt: parent_value(parent, :attempt),
      child_key: parent_value(parent, :child_key),
      metadata: parent_value(parent, :metadata) || %{}
    }
  end

  defp replayed_from_run_id(opts) do
    Keyword.get(opts, :replayed_from_run_id)
  end

  defp runnable_value(runnable, key) when is_map(runnable) and is_atom(key) do
    Map.get(runnable, key) || Map.get(runnable, Atom.to_string(key))
  end
end
