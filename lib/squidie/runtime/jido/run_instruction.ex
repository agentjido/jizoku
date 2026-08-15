# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Jido.RunInstruction do
  @moduledoc false

  alias Jido.Agent
  alias Jido.Agent.Directive
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Jido.Instruction
  alias Squidie.Runtime.Journal.DynamicWork
  alias Squidie.Runtime.Journal.EntryBuilder
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Trace
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.ActionRegistry

  @max_term_bytes 65_536

  @type error ::
          {:invalid_jido_run_instruction,
           :invalid
           | :multiple
           | {:result_action, :invalid | :unsupported}
           | {:meta, :invalid | :unsupported}
           | term()}

  @doc false
  @spec prepare(
          Directive.RunInstruction.t(),
          ActionAttempt.t(),
          Agent.t(),
          map(),
          String.t(),
          DateTime.t(),
          ActionRegistry.registry()
        ) :: {:ok, map()} | {:error, error() | term()}
  def prepare(
        %Directive.RunInstruction{
          instruction: instruction,
          result_action: result_action,
          meta: meta
        },
        %ActionAttempt{} = attempt,
        workflow_agent,
        definition,
        queue,
        %DateTime{} = now,
        registry
      )
      when is_binary(queue) do
    origin =
      Map.new(
        runnable_key: attempt.runnable_key,
        step: attempt.step,
        attempt: attempt.attempt_number
      )

    with :ok <- validate_result_action(result_action),
         :ok <- validate_meta(meta),
         {:ok, attrs} <- Instruction.dynamic_work(instruction, origin, registry),
         {:ok, %Entry{} = entry} <-
           dynamic_work_entry(attrs, attempt, workflow_agent, definition, registry, now),
         {:ok, runnable} <- dynamic_runnable(entry.data, attempt, queue, now, registry),
         {:ok, _planned_entry} <- EntryBuilder.runnables_planned(attempt.run_id, [runnable], now) do
      {:ok,
       %{
         "dynamic_work" => entry.data,
         "runnables" => [runnable]
       }}
    else
      {:ok, :duplicate} -> invalid(:invalid)
      {:error, {:invalid_jido_run_instruction, _reason}} = error -> error
      {:error, reason} -> invalid(reason)
    end
  end

  def prepare(_directive, _attempt, _workflow_agent, _definition, _queue, _now, _registry) do
    invalid(:invalid)
  end

  @doc false
  @spec durable_entries(map(), ActionAttempt.t(), Agent.t(), map(), DateTime.t()) ::
          {:ok, [Squidie.Runtime.DispatchProtocol.Entry.t()]}
          | {:error, error() | term()}
  def durable_entries(
        %{"dynamic_work" => dynamic_work, "runnables" => runnables},
        %ActionAttempt{} = attempt,
        workflow_agent,
        definition,
        %DateTime{} = now
      )
      when is_map(dynamic_work) and is_list(runnables) and runnables != [] do
    occurred_at = Map.get(dynamic_work, :occurred_at, now)

    with :ok <- validate_persisted_origin(dynamic_work, attempt),
         {:ok, entry} <-
           DynamicWork.new_entry(
             attempt.run_id,
             dynamic_work,
             occurred_at,
             dynamic_context(workflow_agent, definition, nil)
           ),
         true <- not match?(:duplicate, entry),
         :ok <- validate_runnables(runnables, attempt, dynamic_work),
         {:ok, planned_entry} <- EntryBuilder.runnables_planned(attempt.run_id, runnables, now) do
      {:ok, [entry, planned_entry]}
    else
      false -> invalid(:invalid)
      {:error, reason} -> invalid(reason)
    end
  end

  def durable_entries(_plan, _attempt, _workflow_agent, _definition, _now) do
    invalid(:invalid)
  end

  @doc false
  @spec recorded?(map(), term(), ActionAttempt.t(), map()) :: boolean()
  def recorded?(
        %{"dynamic_work" => dynamic_work, "runnables" => runnables},
        workflow_agent,
        attempt,
        definition
      )
      when is_map(dynamic_work) and is_list(runnables) do
    dynamic_recorded? =
      match?(
        {:ok, :duplicate},
        DynamicWork.new_entry(
          attempt.run_id,
          dynamic_work,
          Map.get(dynamic_work, :occurred_at, attempt.completed_at),
          dynamic_context(workflow_agent, definition, nil)
        )
      )

    MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) and
      dynamic_recorded? and recorded_runnables?(runnables, workflow_agent)
  end

  def recorded?(_plan, _workflow_agent, _attempt, _definition), do: false

  defp dynamic_work_entry(
         attrs,
         attempt,
         workflow_agent,
         definition,
         registry,
         %DateTime{} = now
       ) do
    with {:ok, entry} <-
           DynamicWork.new_entry(
             attempt.run_id,
             attrs,
             now,
             dynamic_context(workflow_agent, definition, registry)
           ) do
      trace_dynamic_work(entry, attempt)
    end
  end

  defp dynamic_context(workflow_agent, definition, registry) do
    context =
      %{definition: definition}
      |> Map.put(:terminal?, Projection.terminal?(workflow_agent.state.projection))
      |> Map.put(:planned_runnables, WorkflowAgent.planned_runnables(workflow_agent))
      |> Map.put(:dynamic_work, Projection.dynamic_work(workflow_agent.state.projection))

    if is_nil(registry), do: context, else: Map.put(context, :action_registry, registry)
  end

  defp trace_dynamic_work(:duplicate, _attempt), do: {:ok, :duplicate}

  defp trace_dynamic_work(entry, %ActionAttempt{} = attempt) do
    case Trace.child_of(attempt.trace, entry.data.dynamic_key) do
      {:ok, trace} -> {:ok, %{entry | data: Map.put(entry.data, :trace, trace)}}
      {:error, _reason} -> {:ok, entry}
    end
  end

  defp dynamic_runnable(dynamic_work, attempt, queue, %DateTime{} = now, registry) do
    [node] = dynamic_work.nodes

    with {:ok, module} <- ActionRegistry.resolve_action(node.action, registry),
         {:ok, action_opts} <- ActionRegistry.resolve_action_opts(node.action, registry),
         :ok <- validate_action_input(module, node.input, action_opts) do
      runnable_key = Enum.join([attempt.run_id, node.id, 1], ":")

      {:ok,
       %{
         run_id: attempt.run_id,
         runnable_key: runnable_key,
         idempotency_key: runnable_key,
         attempt_number: 1,
         queue: queue,
         step: node.id,
         input: node.input,
         visible_at: now,
         trace: child_trace(dynamic_work, node.id),
         recovery: dynamic_recovery(node.action),
         dynamic?: true,
         dynamic_work:
           put_instruction_metadata(
             %{
               dynamic_key: dynamic_work.dynamic_key,
               action: node.action,
               module: module,
               action_opts: persisted_action_opts(module, action_opts),
               retry: Map.get(node, :retry),
               origin: dynamic_work.origin
             },
             node
           )
       }}
    end
  end

  defp child_trace(%{trace: trace}, node_id) when is_map(trace) do
    case Trace.child_of(trace, node_id) do
      {:ok, child} -> child
      {:error, _reason} -> nil
    end
  end

  defp child_trace(_dynamic_work, _node_id), do: nil

  defp dynamic_recovery(action) do
    Map.new(
      irreversible?: true,
      compensatable?: false,
      replay: :manual_review_required,
      recovery: :manual_intervention,
      dynamic?: true,
      action: action
    )
  end

  defp put_instruction_metadata(dynamic_work, node) do
    instruction = Squidie.MapField.get(node.metadata, :jido_instruction)
    Map.put(dynamic_work, "jido_instruction", instruction)
  end

  defp persisted_action_opts(module, action_opts) do
    if function_exported?(module, :persisted_action_opts, 1) do
      module.persisted_action_opts(action_opts)
    else
      action_opts
    end
  end

  defp validate_action_input(module, input, action_opts) do
    if function_exported?(module, :validate_action_input, 2) do
      case module.validate_action_input(input, action_opts) do
        :ok -> :ok
        {:error, error} -> {:error, {:action_input, Map.get(error, :validation_errors, %{})}}
      end
    else
      :ok
    end
  end

  defp validate_result_action(:instruction_result), do: :ok

  defp validate_result_action(result_action) when is_atom(result_action) do
    invalid({:result_action, :unsupported})
  end

  defp validate_result_action(_result_action), do: invalid_field(:result_action)

  defp validate_meta(meta) when meta == %{}, do: :ok

  defp validate_meta(meta) when is_map(meta) do
    if Options.storage_safe_value?(meta) and bounded?(meta) do
      invalid({:meta, :unsupported})
    else
      invalid_field(:meta)
    end
  end

  defp validate_meta(_meta), do: invalid_field(:meta)

  defp validate_persisted_origin(dynamic_work, attempt) do
    origin = Map.get(dynamic_work, :origin, %{})

    if is_map(origin) and Map.get(origin, :runnable_key) == attempt.runnable_key and
         Map.get(origin, :step) == attempt.step and
         Map.get(origin, :attempt) == attempt.attempt_number do
      :ok
    else
      invalid(:invalid)
    end
  end

  defp validate_runnables([runnable], attempt, dynamic_work) when is_map(runnable) do
    [node] = dynamic_work.nodes

    if Map.get(runnable, :run_id) == attempt.run_id and
         Map.get(runnable, :step) == node.id and
         Map.get(runnable, :input) == node.input and
         Map.get(runnable, :dynamic?) == true do
      :ok
    else
      invalid(:invalid)
    end
  end

  defp validate_runnables(_runnables, _attempt, _dynamic_work), do: invalid(:invalid)

  defp recorded_runnables?(runnables, workflow_agent) do
    persisted_by_key =
      workflow_agent
      |> WorkflowAgent.planned_runnables()
      |> Map.new(&{Map.get(&1, :runnable_key), &1})

    Enum.all?(runnables, fn runnable ->
      key = Map.get(runnable, :runnable_key)

      case Map.get(persisted_by_key, key) do
        persisted when is_map(persisted) ->
          Map.take(persisted, comparable_runnable_fields()) ==
            Map.take(runnable, comparable_runnable_fields())

        _missing ->
          false
      end
    end)
  end

  defp comparable_runnable_fields do
    [
      :run_id,
      :runnable_key,
      :idempotency_key,
      :attempt_number,
      :queue,
      :step,
      :input,
      :visible_at,
      :trace,
      :recovery,
      :dynamic?,
      :dynamic_work
    ]
  end

  defp bounded?(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> byte_size()
    |> Kernel.<=(@max_term_bytes)
  end

  defp invalid_field(field), do: invalid({field, :invalid})
  defp invalid(reason), do: {:error, {:invalid_jido_run_instruction, reason}}
end
