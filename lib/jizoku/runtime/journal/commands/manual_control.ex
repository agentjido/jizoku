defmodule Jizoku.Runtime.Journal.Commands.ManualControl do
  @moduledoc """
  Journal-backed manual intervention controls.

  This module resolves manual pause boundaries by appending run-thread facts.
  The dispatch thread is updated only after the run thread contains the durable
  resolution and any successor runnable intent.
  """

  alias Jido.Agent
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.CommandReceipt
  alias Jizoku.Runtime.Journal.DispatchScheduler
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Runtime.ManualAction
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.StepInput
  alias Jizoku.Runtime.Trace
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  @run_append_retries 25
  @dispatch_append_retries 25

  @type control_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:invalid_resume, map()}
          | {:invalid_transition, atom(), atom()}
          | {:invalid_step, atom() | String.t()}
          | {:invalid_workflow, String.t()}
          | term()

  @doc """
  Resumes a journal-paused `:pause` step and schedules its successor.
  """
  @spec resume(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def resume(run_id, attrs, opts \\ [])

  def resume(run_id, attrs, opts)
      when is_binary(run_id) and is_map(attrs) and is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         :ok <- validate_resume(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, command} <- direct_command(:resume_run, run_id, attrs, now),
         {:ok, workflow_agent} <-
           resolve_or_repair(storage, run_id, queue, attrs, command, @run_append_retries),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           DispatchScheduler.schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def resume(_run_id, _attrs, _opts), do: {:error, :invalid_run_id}

  @doc """
  Approves a journal-paused `:approval` step and schedules its success path.
  """
  @spec approve(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def approve(run_id, attrs, opts \\ []), do: review(run_id, :approved, attrs, opts)

  @doc """
  Rejects a journal-paused `:approval` step and schedules its rejection path.
  """
  @spec reject(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def reject(run_id, attrs, opts \\ []), do: review(run_id, :rejected, attrs, opts)

  @doc """
  Applies a normalized manual-control signal to the journal runtime.
  """
  @spec apply_signal(Signal.t(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error() | {:invalid_signal, term()}}
  def apply_signal(
        %Signal{
          type: :resume_run,
          payload: %{run_id: run_id, attributes: attrs},
          occurred_at: %DateTime{} = now
        } = signal,
        opts
      )
      when is_binary(run_id) and is_map(attrs) and is_list(opts) do
    with :ok <- validate_resume(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, workflow_agent} <-
           resolve_or_repair(storage, run_id, queue, attrs, signal, @run_append_retries),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           DispatchScheduler.schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def apply_signal(
        %Signal{
          type: type,
          payload: %{run_id: run_id, attributes: attrs},
          occurred_at: %DateTime{} = now
        } = signal,
        opts
      )
      when type in [:approve_run, :reject_run] and is_binary(run_id) and is_map(attrs) and
             is_list(opts) do
    decision = signal_decision(type)

    with :ok <- validate_review(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, workflow_agent} <-
           resolve_or_repair_review(
             storage,
             run_id,
             queue,
             decision,
             attrs,
             signal,
             @run_append_retries
           ),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           DispatchScheduler.schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def apply_signal(%Signal{}, opts) when not is_list(opts),
    do: {:error, {:invalid_option, {:opts, :invalid}}}

  def apply_signal(%Signal{type: type}, _opts)
      when type in [:resume_run, :approve_run, :reject_run],
      do: {:error, {:invalid_signal, type}}

  def apply_signal(%Signal{type: type}, _opts), do: {:error, {:unsupported_signal, type}}
  def apply_signal(_signal, _opts), do: {:error, :invalid_signal}

  defp review(run_id, decision, attrs, opts)
       when is_binary(run_id) and decision in [:approved, :rejected] and is_map(attrs) and
              is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         :ok <- validate_review(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, command} <- direct_command(review_signal_type(decision), run_id, attrs, now),
         {:ok, workflow_agent} <-
           resolve_or_repair_review(
             storage,
             run_id,
             queue,
             decision,
             attrs,
             command,
             @run_append_retries
           ),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           DispatchScheduler.schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  defp review(_run_id, _decision, _attrs, _opts), do: {:error, :invalid_run_id}

  defp resolve_or_repair(_storage, _run_id, _queue, _attrs, _now, 0), do: {:error, :conflict}

  defp resolve_or_repair(storage, run_id, queue, attrs, command, retries_left) do
    now = command_occurred_at(command)

    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id),
         {:ok, resolution} <- resolution_target(storage, workflow_agent, command) do
      append_resolution(storage, run_id, queue, attrs, command, now, retries_left, resolution)
    end
  end

  defp resolve_or_repair_review(_storage, _run_id, _queue, _decision, _attrs, _now, 0),
    do: {:error, :conflict}

  defp resolve_or_repair_review(
         storage,
         run_id,
         queue,
         decision,
         attrs,
         command,
         retries_left
       ) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id),
         {:ok, resolution} <- review_resolution_target(storage, workflow_agent, decision, command) do
      append_review_resolution(
        storage,
        run_id,
        queue,
        decision,
        attrs,
        command,
        retries_left,
        resolution
      )
    end
  end

  defp resolution_target(
         storage,
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         command
       ) do
    case active_pause_state(projection) do
      {:ok, manual_state} ->
        {:ok, {:append, workflow_agent, manual_state}}

      {:error, {:invalid_transition, status, :running}} when status != :paused ->
        if resumed_pause_recorded?(storage, workflow_agent.state.run_id, command) do
          {:ok, {:repair, workflow_agent}}
        else
          {:error, {:invalid_transition, status, :running}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp review_resolution_target(
         storage,
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         decision,
         command
       ) do
    case active_approval_state(projection) do
      {:ok, manual_state} ->
        {:ok, {:append, workflow_agent, manual_state}}

      {:error, {:invalid_transition, status, :running}} when status != :paused ->
        if manual_resolution_recorded?(storage, workflow_agent.state.run_id, decision, command) do
          {:ok, {:repair, workflow_agent}}
        else
          {:error, {:invalid_transition, status, :running}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_resolution(_storage, _run_id, _queue, _attrs, _command, _now, _retries_left, {
         :repair,
         workflow_agent
       }) do
    {:ok, workflow_agent}
  end

  defp append_resolution(
         storage,
         run_id,
         queue,
         attrs,
         command,
         %DateTime{} = _now,
         retries_left,
         {:append, workflow_agent, manual_state}
       ) do
    with {:ok, entries} <-
           resolution_entries(workflow_agent, storage, queue, attrs, command, manual_state) do
      case Journal.append_entries(storage, entries,
             expected_rev: workflow_agent.state.thread_rev,
             telemetry_projection: workflow_agent.state.projection
           ) do
        {:ok, _thread} ->
          WorkflowAgent.rebuild(storage, run_id)

        {:error, :conflict} ->
          resolve_or_repair(storage, run_id, queue, attrs, command, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp append_review_resolution(
         _storage,
         _run_id,
         _queue,
         _decision,
         _attrs,
         _command,
         _retries_left,
         {
           :repair,
           workflow_agent
         }
       ) do
    {:ok, workflow_agent}
  end

  defp append_review_resolution(
         storage,
         run_id,
         queue,
         decision,
         attrs,
         command,
         retries_left,
         {:append, workflow_agent, manual_state}
       ) do
    now = command_occurred_at(command)

    with {:ok, entries} <-
           review_resolution_entries(
             workflow_agent,
             storage,
             queue,
             decision,
             attrs,
             command,
             now,
             manual_state
           ) do
      case Journal.append_entries(storage, entries,
             expected_rev: workflow_agent.state.thread_rev,
             telemetry_projection: workflow_agent.state.projection
           ) do
        {:ok, _thread} ->
          WorkflowAgent.rebuild(storage, run_id)

        {:error, :conflict} ->
          resolve_or_repair_review(
            storage,
            run_id,
            queue,
            decision,
            attrs,
            command,
            retries_left - 1
          )

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp resolution_entries(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         storage,
         queue,
         attrs,
         command,
         manual_state
       ) do
    now = command_occurred_at(command)

    with {:ok, _workflow, definition, step_name} <-
           resolved_pause_definition(storage, workflow_agent, manual_state),
         {:ok, target} <- manual_target(definition, manual_state),
         {:ok, result} <- manual_result(manual_state, projection),
         {:ok, progression_entries} <-
           resolution_progression_entries(
             workflow_agent,
             definition,
             target,
             result,
             manual_input(manual_state, projection),
             command,
             queue,
             now
           ),
         {:ok, command_receipt} <-
           manual_command_receipt(:resume_run, workflow_agent.state.run_id, attrs, command) do
      {:ok,
       [
         command_receipt,
         manual_step_resolved_entry!(
           workflow_agent.state.run_id,
           step_name,
           :resumed,
           result,
           ManualAction.build(:resumed, attrs, now),
           command_trace(command),
           now
         )
         | progression_entries
       ]}
    end
  end

  defp review_resolution_entries(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         storage,
         queue,
         decision,
         attrs,
         command,
         %DateTime{} = now,
         manual_state
       ) do
    with {:ok, _workflow, definition, step_name} <-
           resolved_approval_definition(storage, workflow_agent, manual_state),
         {:ok, result, target} <-
           review_result_and_target(definition, step_name, manual_state, decision, attrs, now),
         {:ok, progression_entries} <-
           resolution_progression_entries(
             workflow_agent,
             definition,
             target,
             result,
             manual_input(manual_state, projection),
             command,
             queue,
             now
           ),
         {:ok, command_receipt} <-
           manual_command_receipt(
             review_signal_type(decision),
             workflow_agent.state.run_id,
             attrs,
             command
           ) do
      {:ok,
       [
         command_receipt,
         manual_step_resolved_entry!(
           workflow_agent.state.run_id,
           step_name,
           decision,
           result,
           ManualAction.build(decision, attrs, now),
           command_trace(command),
           now
         )
         | progression_entries
       ]}
    end
  end

  defp active_pause_state(%Projection{} = projection) do
    case Projection.manual_state(projection) do
      %{kind: "pause"} = manual_state ->
        {:ok, manual_state}

      %{step: step, kind: kind} ->
        {:error, {:unsupported_journal_manual_step, step, kind}}

      nil ->
        {:error, {:invalid_transition, Projection.status(projection), :running}}
    end
  end

  defp active_approval_state(%Projection{} = projection) do
    case Projection.manual_state(projection) do
      %{kind: "approval"} = manual_state ->
        {:ok, manual_state}

      %{step: step, kind: kind} ->
        {:error, {:unsupported_journal_manual_step, step, kind}}

      nil ->
        {:error, {:invalid_transition, Projection.status(projection), :running}}
    end
  end

  defp resolved_pause_definition(storage, workflow_agent, %{step: step}) do
    with {:ok, workflow, definition} <-
           WorkflowDefinitionLoader.load(
             storage,
             workflow_agent.state.run_id,
             workflow_agent.state.workflow
           ),
         step_name when is_atom(step_name) <- Definition.deserialize_step(definition, step),
         {:ok, %{module: :pause}} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name}
    else
      step_name when is_binary(step_name) -> {:error, {:unknown_step, step_name}}
      {:ok, _other_step} -> {:error, {:invalid_step, step}}
      {:error, _reason} = error -> error
    end
  end

  defp resolved_approval_definition(storage, workflow_agent, %{step: step}) do
    with {:ok, workflow, definition} <-
           WorkflowDefinitionLoader.load(
             storage,
             workflow_agent.state.run_id,
             workflow_agent.state.workflow
           ),
         step_name when is_atom(step_name) <- Definition.deserialize_step(definition, step),
         {:ok, %{module: :approval}} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name}
    else
      step_name when is_binary(step_name) -> {:error, {:unknown_step, step_name}}
      {:ok, _other_step} -> {:error, {:invalid_step, step}}
      {:error, _reason} = error -> error
    end
  end

  defp manual_target(definition, %{step: step, metadata: metadata}) when is_map(metadata) do
    case map_value(metadata, :target) do
      "__complete__" ->
        {:ok, :complete}

      target when is_binary(target) ->
        case Definition.deserialize_step(definition, target) do
          target_step when is_atom(target_step) -> {:ok, target_step}
          _unknown -> {:error, {:unknown_step, target}}
        end

      _missing ->
        case Definition.deserialize_step(definition, step) do
          step_name when is_atom(step_name) ->
            Definition.transition_target(definition, step_name, :ok)

          _unknown ->
            {:error, {:unknown_step, step}}
        end
    end
  end

  defp manual_result(%{step: step, metadata: metadata}, %Projection{} = projection)
       when is_map(metadata) do
    case map_value(metadata, :output) do
      output when is_map(output) ->
        {:ok, output}

      _missing_or_invalid ->
        with {:ok, runnable_key} <- Projection.applied_runnable_key_for_step(projection, step),
             {:ok, output} when is_map(output) <-
               Projection.applied_result(projection, runnable_key) do
          {:ok, output}
        else
          _missing -> {:ok, %{}}
        end
    end
  end

  defp review_result_and_target(
         definition,
         step_name,
         %{metadata: metadata},
         decision,
         attrs,
         now
       )
       when is_map(metadata) and decision in [:approved, :rejected] do
    with {:ok, output_key} <- review_output_key(definition, step_name, metadata),
         {:ok, target} <- review_target(definition, step_name, metadata, decision) do
      {:ok, map_review_output(attrs, decision, output_key, now), target}
    end
  end

  defp review_output_key(definition, step_name, metadata) do
    case map_value(metadata, :output_key) do
      nil ->
        Definition.step_output_mapping(definition, step_name)

      output_key when is_binary(output_key) ->
        {:ok, deserialize_output_key(output_key)}

      output_key when is_atom(output_key) ->
        {:ok, output_key}

      _invalid ->
        {:error, {:invalid_resume_metadata, step_name}}
    end
  end

  defp review_target(definition, step_name, metadata, decision) do
    target_key = decision_metadata_target_key(decision)
    target = Map.get(metadata, target_key) || Map.get(metadata, Atom.to_string(target_key))

    resolve_review_target(definition, step_name, decision, target)
  end

  defp resolve_review_target(definition, step_name, decision, nil) do
    with {:ok, targets} <- Definition.approval_transition_targets(definition, step_name) do
      {:ok, Map.fetch!(targets, decision_target_key(decision))}
    end
  end

  defp resolve_review_target(_definition, _step_name, _decision, "__complete__"),
    do: {:ok, :complete}

  defp resolve_review_target(definition, _step_name, _decision, target) when is_binary(target) do
    case Definition.deserialize_step(definition, target) do
      target_step when is_atom(target_step) -> {:ok, target_step}
      _unknown -> {:error, {:unknown_step, target}}
    end
  end

  defp resolve_review_target(_definition, _step_name, _decision, target) when is_atom(target) do
    {:ok, target}
  end

  defp resolve_review_target(_definition, step_name, _decision, _target) do
    {:error, {:invalid_resume_metadata, step_name}}
  end

  defp decision_metadata_target_key(:approved), do: :ok_target
  defp decision_metadata_target_key(:rejected), do: :error_target

  defp decision_target_key(:approved), do: :ok
  defp decision_target_key(:rejected), do: :error

  defp review_signal_type(:approved), do: :approve_run
  defp review_signal_type(:rejected), do: :reject_run

  defp signal_decision(:approve_run), do: :approved
  defp signal_decision(:reject_run), do: :rejected

  defp manual_command_receipt(_signal_type, run_id, attrs, %Signal{} = signal) do
    CommandReceipt.new(
      signal.type,
      %{
        run_id: run_id,
        payload: %{run_id: run_id, attributes: command_attributes(attrs)},
        metadata: command_metadata(signal, attrs),
        source: signal.source,
        signal_id: signal.id,
        trace: signal.trace,
        idempotency_key: signal.idempotency_key,
        actor: Map.get(attrs, :actor),
        comment: Map.get(attrs, :comment)
      },
      signal.occurred_at
    )
  end

  defp manual_command_receipt(signal_type, run_id, attrs, %DateTime{} = now) do
    CommandReceipt.new(
      signal_type,
      %{
        run_id: run_id,
        payload: %{run_id: run_id, attributes: command_attributes(attrs)},
        metadata: Map.get(attrs, :metadata, %{}),
        actor: Map.get(attrs, :actor),
        comment: Map.get(attrs, :comment)
      },
      now
    )
  end

  defp command_occurred_at(%Signal{occurred_at: %DateTime{} = now}), do: now
  defp command_occurred_at(%DateTime{} = now), do: now

  defp direct_command(type, run_id, attrs, %DateTime{} = now) do
    with {:ok, trace} <- Trace.new_root() do
      apply(Signal, type, [run_id, attrs, [occurred_at: now, trace: trace]])
    end
  end

  defp command_trace(%Signal{trace: trace}), do: trace
  defp command_trace(%DateTime{}), do: nil

  defp trace_manual_runnable(runnable, %Signal{id: signal_id, trace: trace})
       when is_map(runnable) and is_map(trace) do
    case Trace.child_of(trace, signal_id) do
      {:ok, child_trace} -> {:ok, Map.put(runnable, :trace, child_trace)}
      {:error, _reason} = error -> error
    end
  end

  defp trace_manual_runnable(runnable, _command) when is_map(runnable), do: {:ok, runnable}

  defp command_metadata(%Signal{metadata: metadata}, _attrs) when map_size(metadata) > 0 do
    metadata
  end

  defp command_metadata(%Signal{}, attrs), do: Map.get(attrs, :metadata, %{})

  defp command_attributes(attrs) do
    Map.take(attrs, [:actor, :comment])
  end

  defp map_review_output(attrs, decision, output_key, %DateTime{} = now) do
    review_output =
      %{
        decision: serialize_decision(decision),
        actor: Map.fetch!(attrs, :actor),
        decided_at: DateTime.to_iso8601(now)
      }
      |> maybe_put(:comment, Map.get(attrs, :comment))
      |> maybe_put(:metadata, Map.get(attrs, :metadata))

    case output_key do
      nil ->
        review_output

      mapped_key ->
        %{mapped_key => review_output}
    end
  end

  defp serialize_decision(:approved), do: "approved"
  defp serialize_decision(:rejected), do: "rejected"

  defp deserialize_output_key(output_key) do
    String.to_existing_atom(output_key)
  rescue
    ArgumentError -> output_key
  end

  defp resolution_progression_entries(
         %Agent{agent_module: WorkflowAgent} = workflow_agent,
         _definition,
         :complete,
         _result,
         _input,
         command,
         _queue,
         %DateTime{} = now
       ) do
    {:ok,
     [run_terminal_entry!(workflow_agent.state.run_id, :completed, command_trace(command), now)]}
  end

  defp resolution_progression_entries(
         %Agent{agent_module: WorkflowAgent} = workflow_agent,
         definition,
         next_step,
         result,
         input,
         command,
         queue,
         %DateTime{} = now
       )
       when is_atom(next_step) do
    context =
      workflow_agent
      |> applied_result_context()
      |> Map.merge(input || %{})
      |> Map.merge(result)

    with {:ok, input} <- successor_input(context, definition, next_step),
         {:ok, runnable} <-
           journal_runnable(
             workflow_agent.state.run_id,
             queue,
             definition,
             next_step,
             input,
             1,
             now
           ),
         {:ok, runnable} <- trace_manual_runnable(runnable, command) do
      {:ok,
       [
         runnables_planned_entry!(
           workflow_agent.state.run_id,
           [runnable],
           now
         )
       ]}
    end
  end

  defp validate_resume(attrs) do
    case ManualAction.validate(attrs) do
      :ok -> :ok
      {:error, {:invalid_manual_action, details}} -> {:error, {:invalid_resume, details}}
    end
  end

  defp validate_review(attrs) do
    case ManualAction.validate(attrs, require_actor: true) do
      :ok -> :ok
      {:error, {:invalid_manual_action, details}} -> {:error, {:invalid_review, details}}
    end
  end

  defp rebuild_workflow_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _workflow_agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp resumed_pause_recorded?(storage, run_id, command) do
    manual_resolution_recorded?(storage, run_id, :resumed, command)
  end

  defp manual_resolution_recorded?(storage, run_id, action, %Signal{
         type: signal_type,
         idempotency_key: idempotency_key
       })
       when is_atom(action) and is_binary(idempotency_key) do
    serialized_action = Atom.to_string(action)
    serialized_signal_type = Atom.to_string(signal_type)

    case Journal.load_entries(storage, {:run, run_id}) do
      {:ok, entries} ->
        action_recorded? =
          Enum.any?(entries, fn
            %{type: :manual_step_resolved, data: %{action: ^serialized_action}} -> true
            _entry -> false
          end)

        command_recorded? =
          Enum.any?(entries, fn
            %{
              type: :run_signal_received,
              data: %{signal_type: ^serialized_signal_type, idempotency_key: ^idempotency_key}
            } ->
              true

            _entry ->
              false
          end)

        action_recorded? and command_recorded?

      {:error, _reason} ->
        false
    end
  end

  defp manual_resolution_recorded?(_storage, _run_id, action, %Signal{}) when is_atom(action),
    do: false

  defp manual_resolution_recorded?(storage, run_id, action, _command) when is_atom(action) do
    serialized_action = Atom.to_string(action)

    case Journal.load_entries(storage, {:run, run_id}) do
      {:ok, entries} ->
        Enum.any?(entries, fn
          %{type: :manual_step_resolved, data: %{action: ^serialized_action}} -> true
          _entry -> false
        end)

      {:error, _reason} ->
        false
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp applied_result_context(%Agent{
         agent_module: WorkflowAgent,
         state: %{projection: projection}
       }) do
    projection
    |> Projection.applied_results()
    |> Enum.filter(fn {_key, value} -> is_map(value) end)
    |> Enum.map(fn {_key, value} -> value end)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp manual_input(%{step: step}, %Projection{} = projection) do
    projection
    |> Projection.planned_runnables()
    |> Enum.find_value(%{}, fn runnable ->
      runnable_step = map_value(runnable, :step)

      if runnable_step == step do
        map_value(runnable, :input, %{})
      end
    end)
  end

  defp map_value(map, key, default \\ nil), do: Jizoku.MapField.get(map, key, default)

  defp successor_input(context, definition, next_step) do
    case Definition.step_input_mapping(definition, next_step) do
      {:ok, input_mapping} -> StepInput.apply_input_mapping(context, input_mapping)
      {:error, _reason} = error -> error
    end
  end

  defp manual_step_resolved_entry!(
         run_id,
         step_name,
         action,
         result,
         metadata,
         trace,
         %DateTime{} = now
       )
       when is_binary(run_id) and is_atom(step_name) and is_atom(action) and is_map(result) and
              is_map(metadata) do
    entry!(:manual_step_resolved, %{
      run_id: run_id,
      step: Definition.serialize_step(step_name),
      action: Atom.to_string(action),
      result: result,
      metadata: metadata,
      trace: trace,
      occurred_at: now
    })
  end

  defp runnables_planned_entry!(run_id, runnables, %DateTime{} = now) do
    Jizoku.Runtime.Journal.EntryBuilder.runnables_planned!(run_id, runnables, now)
  end

  defp run_terminal_entry!(run_id, status, trace, %DateTime{} = now) do
    Jizoku.Runtime.Journal.EntryBuilder.traced_run_terminal!(run_id, status, trace, now)
  end

  defp entry!(type, attrs) do
    {:ok, %Entry{} = entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp journal_runnable(
         run_id,
         queue,
         definition,
         step_name,
         input,
         attempt_number,
         %DateTime{} = now
       ) do
    Jizoku.Runtime.Journal.EntryBuilder.runnable(
      definition,
      run_id,
      queue,
      step_name,
      input,
      attempt_number,
      now
    )
  end

  defp run_id(run_id) do
    Options.uuid_run_id(run_id)
  end

  defp journal_storage(opts) do
    Options.storage_from_opts(opts)
  end

  defp queue(opts) do
    Options.queue_from_opts(opts)
  end

  defp now(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now(:microsecond))

    if match?(%DateTime{}, now) do
      {:ok, now}
    else
      {:error, {:invalid_option, {:now, :invalid}}}
    end
  end
end
