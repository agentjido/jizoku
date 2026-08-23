# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Journal.Commands.Starter do
  @moduledoc """
  Journal-backed workflow start boundary for the Jido-native runtime.

  This module resolves the public Jizoku workflow contract, plans initial
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

  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.AgentRecovery
  alias Jizoku.Runtime.DispatchAgent
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.CommandReceipt
  alias Jizoku.Runtime.Journal.EntryBuilder
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.RunCatalogProjection
  alias Jizoku.Runtime.RunIndexProjection
  alias Jizoku.Runtime.SearchAttributes
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Trace
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.ActionRegistry
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.GuardrailRegistry
  alias Jizoku.Workflow.RunicPlanner
  alias Jizoku.Workflow.Spec

  @dispatch_schedule_retries 25

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
  by `Jizoku.inspect_run/2` with `read_model: :read_model`.
  """
  @spec start_run(module(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Inspection.Snapshot.t()}}
          | {:error, start_error()}
  def start_run(workflow, trigger_name, payload, opts)
      when is_atom(workflow) and is_map(payload) and is_list(opts) do
    with :ok <- validate_command_signal(opts),
         :ok <- validate_continuation_options(opts),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, search_attributes} <- start_search_attributes(opts),
         {:ok, definition} <- Definition.load(workflow),
         :ok <- validate_continuation_definition(definition, opts),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(workflow),
         {:ok, _planned, runnables} <- RunicPlanner.plan(planner, resolved_payload),
         {:ok, run_id} <- run_id(opts),
         :ok <- validate_initial_context(opts),
         {:ok, journal_runnables} <- journal_runnables(definition, run_id, queue, runnables, now),
         {:ok, journal_runnables, opts} <- traced_start_runnables(journal_runnables, opts),
         {:ok, start_state} <-
           ensure_run_started(
             storage,
             %{
               workflow: workflow,
               definition: definition,
               trigger: trigger,
               input: resolved_payload,
               search_attributes: search_attributes,
               run_id: run_id
             },
             journal_runnables,
             now,
             opts
           ) do
      complete_started_run(storage, workflow, run_id, queue, now, start_state, opts)
    end
  end

  @doc false
  @spec start_continuation_from_intent(module(), atom(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, start_error()}
  def start_continuation_from_intent(workflow, trigger_name, resolved_input, opts)
      when is_atom(workflow) and is_atom(trigger_name) and is_map(resolved_input) and
             is_list(opts) do
    with :ok <- validate_command_signal(opts),
         :ok <- validate_required_continuation_options(opts),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, run_id} <- run_id(opts) do
      case Journal.load_thread(storage, {:run, run_id}) do
        {:ok, _thread} ->
          repair_existing_continuation(
            storage,
            workflow,
            trigger_name,
            resolved_input,
            run_id,
            queue,
            now,
            opts
          )

        {:error, :not_found} ->
          start_run(workflow, trigger_name, resolved_input, opts)

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc false
  @spec repair_existing_continuation_from_intent(String.t(), String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, start_error() | :not_found}
  def repair_existing_continuation_from_intent(
        workflow_name,
        trigger_name,
        resolved_input,
        opts
      )
      when is_binary(workflow_name) and workflow_name != "" and is_binary(trigger_name) and
             trigger_name != "" and
             is_map(resolved_input) and is_list(opts) do
    with :ok <- validate_command_signal(opts),
         :ok <- validate_required_continuation_options(opts),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, run_id} <- run_id(opts) do
      case Journal.load_thread(storage, {:run, run_id}) do
        {:ok, _thread} ->
          repair_existing_continuation(
            storage,
            workflow_name,
            trigger_name,
            resolved_input,
            run_id,
            queue,
            now,
            opts
          )

        {:error, :not_found} ->
          {:error, :not_found}

        {:error, _reason} = error ->
          error
      end
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
         :ok <- validate_continuation_options(opts),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, search_attributes} <- start_search_attributes(opts),
         {:ok, spec} <- runtime_spec(spec, opts),
         definition <- definition_from_spec(spec),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(spec),
         {:ok, _planned, runnables} <- RunicPlanner.plan(planner, resolved_payload),
         :ok <- validate_initial_guardrails(definition, runnables, opts),
         :ok <- validate_planned_action_inputs(definition, runnables),
         persisted_spec <- persisted_runtime_spec(spec),
         persisted_definition <- definition_from_spec(persisted_spec),
         :ok <- validate_continuation_definition(persisted_definition, opts),
         {:ok, run_id} <- run_id(opts),
         :ok <- validate_initial_context(opts),
         {:ok, journal_runnables} <-
           journal_runnables(persisted_definition, run_id, queue, runnables, now),
         {:ok, journal_runnables, opts} <- traced_start_runnables(journal_runnables, opts),
         {:ok, start_state} <-
           ensure_run_started(
             storage,
             %{
               workflow: persisted_spec.workflow,
               definition: persisted_definition,
               definition_source: :runtime_spec,
               definition_spec: persisted_spec,
               trigger: trigger,
               input: resolved_payload,
               search_attributes: search_attributes,
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
         :ok <- Spec.validate(resolved_spec),
         :ok <- validate_runtime_spec_guardrails(resolved_spec, opts) do
      {:ok, resolved_spec}
    end
  end

  defp validate_runtime_spec_guardrails(%Spec{} = spec, opts) do
    GuardrailRegistry.validate_spec_option(spec, opts)
  end

  defp validate_planned_action_inputs(definition, runnables) when is_list(runnables) do
    runnables
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {runnable, index}, :ok ->
      case validate_planned_action_input(definition, runnable, index) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_initial_guardrails(definition, runnables, opts) when is_list(runnables) do
    case Keyword.fetch(opts, :guardrail_registry) do
      {:ok, registry} ->
        reduce_initial_guardrails(definition, runnables, registry)

      :error ->
        :ok
    end
  end

  defp reduce_initial_guardrails(definition, runnables, registry) do
    runnables
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {runnable, index}, :ok ->
      case validate_initial_guardrail(definition, runnable, index, registry) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_initial_guardrail(definition, %{step: step, input: input}, index, registry)
       when is_atom(step) and is_map(input) do
    with {:ok, step_definition} <- Definition.step(definition, step) do
      case GuardrailRegistry.evaluate_step(step_definition, index, :input, input, registry, %{
             phase: :run_start
           }) do
        {:ok, _decisions} ->
          :ok

        {:error, error, _decisions} ->
          {:error, {:invalid_workflow_spec, [error]}}
      end
    end
  end

  defp validate_initial_guardrail(_definition, _runnable, _index, _registry), do: :ok

  defp persisted_runtime_spec(%Spec{} = spec) do
    %Spec{spec | steps: persisted_steps(spec.steps)}
  end

  defp persisted_steps(steps) when is_list(steps), do: Enum.map(steps, &persisted_step/1)

  defp persisted_step(step) when is_map(step) do
    module = Map.get(step, :module)

    if is_atom(module) and function_exported?(module, :persisted_action_opts, 1) do
      Map.update(step, :opts, [], &persisted_step_opts(module, &1))
    else
      step
    end
  end

  defp persisted_step(step), do: step

  defp persisted_step_opts(module, opts) when is_list(opts) do
    action_opts =
      opts
      |> Keyword.get(:action_opts, [])
      |> module.persisted_action_opts()

    Keyword.put(opts, :action_opts, action_opts)
  end

  defp persisted_step_opts(_module, opts), do: opts

  defp validate_planned_action_input(definition, %{step: step, input: input}, index)
       when is_atom(step) and is_map(input) do
    with {:ok, step_definition} <- Definition.step(definition, step) do
      module = Map.get(step_definition, :module)
      opts = Map.get(step_definition, :opts, [])

      if is_atom(module) and function_exported?(module, :validate_action_input, 2) do
        validate_action_input(module, step, input, opts, index)
      else
        :ok
      end
    end
  end

  defp validate_planned_action_input(_definition, _runnable, _index), do: :ok

  defp validate_action_input(module, step, input, opts, index) do
    case module.validate_action_input(input, Keyword.get(opts, :action_opts, [])) do
      :ok ->
        :ok

      {:error, error} ->
        {:error,
         {:invalid_workflow_spec,
          [
            %{
              path: [:steps, index, :input],
              code: :invalid_action_input,
              message: "step #{inspect(step)} action input is invalid",
              details: %{
                step: step,
                validation_errors: Map.get(error, :validation_errors, %{})
              }
            }
          ]}}
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

  defp definition_from_spec(%Spec{} = spec), do: Jizoku.Workflow.SpecData.from_struct(spec)

  defp spec_field_values(spec) when is_map(spec),
    do: Jizoku.Workflow.SpecData.struct_fields(spec)

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
           search_attributes: search_attributes,
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
          search_attributes: search_attributes,
          context: initial_context(opts),
          replayed_from_run_id: replayed_from_run_id(opts),
          definition_version: definition.definition_version,
          definition_fingerprint: expected_fingerprint,
          trace: start_run_trace(opts),
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
               source: start_signal_source(opts),
               signal_id: start_signal_id(opts),
               trace: start_run_trace(opts),
               idempotency_key: start_signal_idempotency_key(opts)
             },
             now
           ),
         {:ok, runnables_planned} <-
           EntryBuilder.runnables_planned(run_id, runnables, now),
         {:ok, continuation_entries} <- continuation_entries(run_id, now, opts),
         {:ok, _run_thread} <-
           Journal.append_entries(
             storage,
             Enum.concat([
               [run_signal_received, run_started],
               continuation_entries,
               [runnables_planned]
             ]),
             expected_rev: 0
           ) do
      {:ok, :created}
    else
      {:error, :conflict} ->
        rebuild_existing_start(
          storage,
          run_id,
          %{
            workflow: workflow,
            trigger: trigger,
            input: input,
            search_attributes: search_attributes,
            runnables: runnables,
            definition_version: definition.definition_version,
            definition_fingerprint: expected_fingerprint
          },
          opts
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_put_runtime_definition(attrs, :runtime_spec, %Spec{} = spec) do
    attrs
    |> Map.put(:definition_source, :runtime_spec)
    |> Map.put(:definition_spec, spec)
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

  defp start_signal_source(opts) do
    case start_command_signal(opts) do
      %Signal{source: source} -> source
      nil -> nil
    end
  end

  defp traced_start_runnables(runnables, opts) when is_list(runnables) and is_list(opts) do
    with {:ok, signal_id, run_trace, opts} <- start_trace_context(opts),
         {:ok, runnables} <- trace_initial_runnables(runnables, run_trace, signal_id) do
      lineage = %{signal_id: signal_id, run_trace: run_trace}
      {:ok, runnables, Keyword.put(opts, :start_trace_lineage, lineage)}
    end
  end

  defp start_trace_context(opts) do
    case start_command_signal(opts) do
      %Signal{} = signal ->
        with {:ok, signal_id} <- start_trace_signal_id(signal.id),
             {:ok, run_trace} <- normalized_or_new_trace(signal.trace) do
          signal = %Signal{signal | id: signal_id, trace: run_trace}
          {:ok, signal_id, run_trace, Keyword.put(opts, :command_signal, signal)}
        end

      nil ->
        with {:ok, run_trace} <- Trace.new_root() do
          {:ok, Ecto.UUID.generate(), run_trace, opts}
        end
    end
  end

  defp start_trace_signal_id(id) when is_binary(id) and id != "", do: {:ok, id}
  defp start_trace_signal_id(_id), do: {:ok, Ecto.UUID.generate()}

  defp normalized_or_new_trace(nil), do: Trace.new_root()
  defp normalized_or_new_trace(trace), do: Trace.normalize(trace)

  defp trace_initial_runnables(runnables, run_trace, signal_id) do
    result =
      Enum.reduce_while(runnables, {:ok, []}, fn runnable, {:ok, traced_runnables} ->
        case Trace.child_of(run_trace, signal_id) do
          {:ok, trace} -> {:cont, {:ok, [Map.put(runnable, :trace, trace) | traced_runnables]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, traced_runnables} -> {:ok, Enum.reverse(traced_runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp start_signal_id(opts) do
    opts
    |> Keyword.fetch!(:start_trace_lineage)
    |> Map.fetch!(:signal_id)
  end

  defp start_run_trace(opts) do
    opts
    |> Keyword.fetch!(:start_trace_lineage)
    |> Map.fetch!(:run_trace)
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

  defp validate_continuation_options(opts) do
    with {:ok, origin} <- continuation_origin(opts),
         {:ok, identity} <- continuation_definition_identity(opts) do
      case {origin, identity} do
        {nil, nil} ->
          :ok

        {%{}, %{}} ->
          validate_continuation_run_id(opts)

        {%{}, nil} ->
          {:error, {:invalid_option, {:continuation_definition_identity, :invalid}}}

        {nil, %{}} ->
          {:error, {:invalid_option, {:continuation_origin, :invalid}}}
      end
    end
  end

  defp validate_required_continuation_options(opts) do
    with :ok <- validate_continuation_options(opts) do
      if Keyword.has_key?(opts, :continuation_origin) do
        :ok
      else
        {:error, {:invalid_option, {:continuation_origin, :required}}}
      end
    end
  end

  defp validate_continuation_run_id(opts) do
    if Keyword.has_key?(opts, :run_id) do
      :ok
    else
      {:error, {:invalid_option, {:run_id, :required}}}
    end
  end

  defp validate_continuation_definition(definition, opts) do
    with {:ok, expected} <- continuation_definition_identity(opts) do
      validate_continuation_definition_match(definition, expected)
    end
  end

  defp validate_continuation_definition_match(_definition, nil) do
    :ok
  end

  defp validate_continuation_definition_match(definition, expected) do
    cond do
      definition.definition_version != expected.definition_version ->
        {:error, {:continuation_definition_mismatch, :version}}

      Definition.fingerprint(definition) != expected.definition_fingerprint ->
        {:error, {:continuation_definition_mismatch, :fingerprint}}

      true ->
        :ok
    end
  end

  defp continuation_definition_identity(opts) do
    case Keyword.fetch(opts, :continuation_definition_identity) do
      :error ->
        {:ok, nil}

      {:ok, %{definition_version: version, definition_fingerprint: fingerprint}}
      when (is_nil(version) or (is_binary(version) and version != "")) and
             is_binary(fingerprint) and fingerprint != "" ->
        {:ok, %{definition_version: version, definition_fingerprint: fingerprint}}

      {:ok, _invalid} ->
        {:error, {:invalid_option, {:continuation_definition_identity, :invalid}}}
    end
  end

  defp continuation_entries(run_id, %DateTime{} = now, opts) do
    case continuation_origin(opts) do
      {:ok, nil} ->
        {:ok, []}

      {:ok, %{predecessor_run_id: predecessor_run_id, continuation_key: continuation_key}} ->
        with {:ok, entry} <-
               DispatchProtocol.new_entry(:run_continued_from, %{
                 run_id: run_id,
                 predecessor_run_id: predecessor_run_id,
                 continuation_key: continuation_key,
                 occurred_at: now
               }) do
          {:ok, [entry]}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp continuation_origin(opts) do
    case Keyword.fetch(opts, :continuation_origin) do
      :error ->
        {:ok, nil}

      {:ok,
       %{
         predecessor_run_id: predecessor_run_id,
         continuation_key: continuation_key
       }}
      when is_binary(predecessor_run_id) and predecessor_run_id != "" and
             is_binary(continuation_key) and continuation_key != "" ->
        with {:ok, predecessor_run_id} <- canonical_predecessor_run_id(predecessor_run_id),
             {:ok, continuation_key} <-
               Options.thread_part(continuation_key, :continuation_key) do
          {:ok,
           %{
             predecessor_run_id: predecessor_run_id,
             continuation_key: continuation_key
           }}
        else
          {:error, _reason} ->
            {:error, {:invalid_option, {:continuation_origin, :invalid}}}
        end

      {:ok, _invalid} ->
        {:error, {:invalid_option, {:continuation_origin, :invalid}}}
    end
  end

  defp canonical_predecessor_run_id(predecessor_run_id) do
    case Ecto.UUID.cast(predecessor_run_id) do
      {:ok, predecessor_run_id} -> {:ok, predecessor_run_id}
      :error -> {:error, :invalid_predecessor_run_id}
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
    EntryBuilder.runnable(definition, run_id, queue, step, input || %{}, 1, now)
  end

  defp journal_runnable(_definition, _run_id, _queue, runnable, %DateTime{}) do
    {:error, {:invalid_runnable, runnable}}
  end

  defp put_checkpoints(storage, workflow_agent, dispatch_agent, %DateTime{} = now) do
    _checkpoint_result = WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: now)
    _checkpoint_result = DispatchAgent.put_checkpoint(storage, dispatch_agent, updated_at: now)

    :ok
  end

  defp ensure_run_indexed(storage, workflow, run_id, queue, %DateTime{} = now) do
    workflow = serialized_workflow(workflow)

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

  defp serialized_workflow(workflow) when is_atom(workflow) do
    Definition.serialize_workflow(workflow)
  end

  defp serialized_workflow(workflow) when is_binary(workflow) and workflow != "" do
    workflow
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
    case RunCatalogProjection.load(storage) do
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
         run_id,
         expected,
         opts
       ) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, existing_fingerprint} <- persisted_definition_fingerprint(storage, run_id) do
      mode = existing_start_mode(opts)

      validate_existing_start_mode(
        mode,
        workflow_agent,
        expected,
        existing_fingerprint,
        opts
      )
    end
  end

  defp existing_start_mode(opts) do
    cond do
      Keyword.has_key?(opts, :continuation_origin) ->
        :continuation

      Keyword.get(opts, :duplicate_schedule_start, false) ->
        :schedule_duplicate

      true ->
        :strict
    end
  end

  defp validate_existing_start_mode(
         :continuation,
         workflow_agent,
         expected,
         existing_fingerprint,
         opts
       ) do
    projection = workflow_agent.state.projection

    with :ok <-
           validate_existing_start(
             workflow_agent,
             expected.workflow,
             expected.runnables,
             expected.definition_fingerprint,
             existing_fingerprint,
             expected.search_attributes
           ),
         {:ok, expected_identity} <- continuation_definition_identity(opts),
         :ok <-
           validate_persisted_definition_identity(
             projection,
             existing_fingerprint,
             expected_identity
           ),
         :ok <- validate_existing_continuation(workflow_agent, expected, opts) do
      {:ok, :existing}
    end
  end

  defp validate_existing_start_mode(
         :schedule_duplicate,
         workflow_agent,
         expected,
         _existing_fingerprint,
         opts
       ) do
    with :ok <- validate_existing_continuation(workflow_agent, expected, opts) do
      validate_existing_schedule_start(workflow_agent, expected.workflow, opts)
    end
  end

  defp validate_existing_start_mode(
         :strict,
         workflow_agent,
         expected,
         existing_fingerprint,
         opts
       ) do
    with :ok <-
           validate_existing_start(
             workflow_agent,
             expected.workflow,
             expected.runnables,
             expected.definition_fingerprint,
             existing_fingerprint,
             expected.search_attributes
           ),
         :ok <- validate_existing_continuation(workflow_agent, expected, opts) do
      {:ok, :existing}
    end
  end

  defp validate_existing_continuation(workflow_agent, expected, opts) do
    with {:ok, expected_origin} <- continuation_origin(opts) do
      projection = workflow_agent.state.projection

      if continuation_identity_matches?(projection, expected, expected_origin) do
        :ok
      else
        {:error, :conflict}
      end
    end
  end

  defp continuation_identity_matches?(%Projection{} = projection, _expected, nil) do
    projection
    |> Projection.continuation()
    |> Map.fetch!(:continued_from)
    |> is_nil()
  end

  defp continuation_identity_matches?(
         %Projection{} = projection,
         expected,
         expected_origin
       ) do
    existing_origin =
      projection
      |> Projection.continuation()
      |> Map.fetch!(:continued_from)

    existing_origin == public_continuation_origin(expected_origin) and
      projection.trigger == Definition.serialize_trigger(Map.fetch!(expected.trigger, :name)) and
      projection.input == expected.input
  end

  defp public_continuation_origin(%{
         predecessor_run_id: predecessor_run_id,
         continuation_key: continuation_key
       }) do
    %{run_id: predecessor_run_id, continuation_key: continuation_key}
  end

  defp repair_existing_continuation(
         storage,
         workflow,
         trigger_name,
         input,
         run_id,
         queue,
         %DateTime{} = now,
         opts
       ) do
    workflow_name = serialized_workflow(workflow)

    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, existing_fingerprint} <- persisted_definition_fingerprint(storage, run_id),
         :ok <-
           validate_persisted_continuation(
             workflow_agent.state.projection,
             existing_fingerprint,
             workflow_name,
             trigger_name,
             input,
             queue,
             opts
           ) do
      complete_started_run(storage, workflow_name, run_id, queue, now, :existing, opts)
    end
  end

  defp validate_persisted_continuation(
         %Projection{} = projection,
         existing_fingerprint,
         workflow_name,
         trigger_name,
         input,
         queue,
         opts
       ) do
    with {:ok, expected_identity} <- continuation_definition_identity(opts),
         {:ok, expected_origin} <- continuation_origin(opts),
         :ok <-
           validate_persisted_definition_identity(
             projection,
             existing_fingerprint,
             expected_identity
           ),
         :ok <- validate_persisted_continuation_queue(projection, queue) do
      expected = %{trigger: %{name: trigger_name}, input: input}

      cond do
        projection.workflow != workflow_name ->
          {:error, :conflict}

        not continuation_identity_matches?(projection, expected, expected_origin) ->
          {:error, :conflict}

        true ->
          :ok
      end
    end
  end

  defp validate_persisted_definition_identity(
         %Projection{} = projection,
         existing_fingerprint,
         expected_identity
       ) do
    if is_map(expected_identity) and
         projection.definition_version == expected_identity.definition_version and
         existing_fingerprint == expected_identity.definition_fingerprint do
      :ok
    else
      {:error, :conflict}
    end
  end

  defp validate_persisted_continuation_queue(%Projection{} = projection, queue) do
    queues =
      projection
      |> Projection.planned_runnables()
      |> Enum.map(&runnable_value(&1, :queue))
      |> Enum.uniq()

    if queues == [queue] do
      :ok
    else
      {:error, :conflict}
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
       Signal.start_payload(
         payload_value(payload, :workflow),
         trigger,
         payload_value(payload, :input)
       )}
    end
  end

  defp canonical_start_signal_payload("start_cron", payload, _workflow) when is_map(payload) do
    {:ok,
     Signal.start_payload(
       payload_value(payload, :workflow),
       payload_value(payload, :trigger),
       payload_value(payload, :input)
     )}
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
         existing_fingerprint,
         expected_search_attributes
       ) do
    existing_workflow = workflow_agent.state.workflow
    expected_workflow = Definition.serialize_workflow(workflow)
    existing_runnables = WorkflowAgent.planned_runnables(workflow_agent)

    cond do
      existing_workflow != expected_workflow ->
        {:error, :conflict}

      not equivalent_definition_fingerprint?(existing_fingerprint, expected_fingerprint) ->
        {:error, :conflict}

      Projection.search_attributes(workflow_agent.state.projection) != expected_search_attributes ->
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

  defp start_search_attributes(opts) do
    SearchAttributes.normalize(
      Keyword.get(opts, :search_attributes, %{}),
      Keyword.get(opts, :search_attribute_schema)
    )
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
    do: Options.storage_safe_value?(metadata)

  defp storage_safe_parent_metadata?(nil), do: true

  defp storage_safe_parent_metadata?(_metadata), do: false

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
