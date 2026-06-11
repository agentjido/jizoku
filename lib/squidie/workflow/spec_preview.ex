defmodule Squidie.Workflow.SpecPreview do
  @moduledoc false

  alias Squidie.Runtime.StepInput
  alias Squidie.Step
  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.RunicPlanner
  alias Squidie.Workflow.RunicPlanner.Runnable
  alias Squidie.Workflow.Spec

  @doc false
  @spec preview(Spec.t() | map() | term(), atom() | nil, map(), keyword()) ::
          {:ok, Squidie.Runs.SpecPreview.t()} | {:error, term()}
  def preview(spec, trigger_name, payload, opts)
      when is_map(payload) and is_list(opts) do
    with {:ok, registry} <- action_registry(opts),
         {:ok, spec} <- runtime_spec(spec, registry),
         definition <- Map.from_struct(spec),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(spec),
         {:ok, _planner, runnables} <- RunicPlanner.plan(planner, resolved_payload) do
      {:ok, build_preview(spec, definition, trigger, registry, runnables)}
    end
  end

  def preview(_spec, _trigger_name, _payload, opts) when is_list(opts) do
    {:error, {:invalid_payload, :expected_map}}
  end

  defp action_registry(opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} -> {:ok, registry}
      :error -> {:error, {:invalid_option, {:action_registry, :required}}}
    end
  end

  defp runtime_spec(spec, registry) do
    with {:ok, resolved_spec} <- ActionRegistry.resolve_spec(spec, registry),
         spec <- to_spec_struct(resolved_spec),
         :ok <- Spec.validate(spec) do
      {:ok, spec}
    end
  end

  defp to_spec_struct(%Spec{} = spec), do: spec

  defp to_spec_struct(spec) when is_map(spec) do
    struct(Spec, spec_field_values(spec))
  end

  defp spec_field_values(spec) when is_map(spec) do
    fields =
      Spec.__struct__()
      |> Map.from_struct()
      |> Map.keys()

    Map.new(fields, fn field ->
      {field, value(spec, field)}
    end)
  end

  defp trigger(definition, nil),
    do: Definition.trigger(definition, Definition.default_trigger(definition))

  defp trigger(definition, trigger_name), do: Definition.trigger(definition, trigger_name)

  defp build_preview(%Spec{} = spec, definition, trigger, registry, runnables) do
    {nodes, status} = preview_runnables(spec, definition, registry, runnables, [], :completed)
    errors = Enum.flat_map(nodes, &node_errors/1)

    Squidie.Runs.SpecPreview.new(
      workflow: spec.workflow,
      definition_version: spec.definition_version,
      trigger: Map.get(trigger, :name),
      status: final_status(status, errors),
      nodes: nodes,
      errors: errors
    )
  end

  defp preview_runnables(%Spec{}, _definition, _registry, [], nodes, status) do
    {Enum.reverse(nodes), status}
  end

  defp preview_runnables(%Spec{} = spec, definition, registry, runnables, nodes, status) do
    case preview_batch(spec, definition, registry, runnables, nodes, status) do
      {:halt, nodes, status} ->
        {Enum.reverse(nodes), status}

      {:cont, next_runnables, nodes, status} ->
        preview_runnables(spec, definition, registry, next_runnables, nodes, status)
    end
  end

  defp preview_batch(spec, definition, registry, runnables, nodes, status) do
    Enum.reduce_while(runnables, {:cont, [], nodes, status}, fn runnable, acc ->
      node = preview_runnable(spec, registry, runnable)
      handle_preview_node(spec, definition, runnable, node, acc)
    end)
  end

  defp handle_preview_node(
         spec,
         definition,
         runnable,
         %{status: :completed} = node,
         {:cont, next_runnables, current_nodes, current_status}
       ) do
    continue_after_node(
      spec,
      definition,
      runnable,
      node,
      :ok,
      node.output,
      {next_runnables, current_nodes, current_status}
    )
  end

  defp handle_preview_node(
         spec,
         definition,
         runnable,
         %{status: :failed} = node,
         {:cont, next_runnables, current_nodes, _current_status}
       ) do
    continue_after_node(
      spec,
      definition,
      runnable,
      node,
      :error,
      node.error,
      {next_runnables, current_nodes, :failed}
    )
  end

  defp handle_preview_node(
         _spec,
         _definition,
         _runnable,
         %{status: :validation_error} = node,
         acc
       ) do
    halt_after_node(node, acc, :invalid)
  end

  defp handle_preview_node(_spec, _definition, _runnable, %{status: :unsupported} = node, acc) do
    halt_after_node(node, acc, :blocked)
  end

  defp continue_after_node(spec, definition, runnable, node, outcome, result, acc) do
    {next_runnables, current_nodes, current_status} = acc

    case next_runnable(spec, definition, runnable, outcome, result) do
      {:ok, nil} ->
        {:cont, {:cont, next_runnables, [node | current_nodes], current_status}}

      {:ok, next_runnable} ->
        {:cont, {:cont, [next_runnable | next_runnables], [node | current_nodes], current_status}}

      {:error, error} ->
        failed = error_node(error)
        {:halt, {:halt, [failed, node | current_nodes], :failed}}
    end
  end

  defp halt_after_node(node, {:cont, _next_runnables, current_nodes, _current_status}, status) do
    {:halt, {:halt, [node | current_nodes], status}}
  end

  defp next_runnable(%Spec{} = spec, definition, %Runnable{} = runnable, outcome, result) do
    context = next_context(spec, runnable.step, runnable.input, outcome, result)

    case Definition.transition(definition, runnable.step, outcome, context) do
      {:ok, %{to: :complete}} ->
        {:ok, nil}

      {:ok, %{to: target}} when is_atom(target) ->
        build_runnable(spec, target, context)

      {:error, {:unknown_transition, _step, _outcome}} ->
        {:ok, nil}

      {:error, {:no_matching_transition, _step, _outcome}} ->
        {:ok, nil}
    end
  end

  defp next_context(%Spec{} = spec, step_name, input, :ok, result) do
    input
    |> normalize_context()
    |> Map.merge(mapped_step_output(spec, step_name, result))
  end

  defp next_context(%Spec{}, _step_name, input, :error, _result), do: normalize_context(input)

  defp mapped_step_output(%Spec{} = spec, step_name, result) when is_map(result) do
    step = step!(spec, step_name)
    opts = value(step, :opts, [])

    case Keyword.get(opts, :output) do
      nil -> result
      output_key when is_atom(output_key) -> %{output_key => result}
    end
  end

  defp mapped_step_output(%Spec{}, _step_name, _result), do: %{}

  defp normalize_context(context) when is_map(context), do: context
  defp normalize_context(_context), do: %{}

  defp build_runnable(%Spec{} = spec, step_name, context) when is_atom(step_name) do
    with step when is_map(step) <- step!(spec, step_name),
         {:ok, input} <-
           StepInput.apply_input_mapping(context, Keyword.get(value(step, :opts, []), :input)) do
      {:ok, %Runnable{step: step_name, input: input, metadata: value(step, :metadata, %{})}}
    else
      nil ->
        {:error,
         preview_error(:unknown_step, "workflow preview referenced an unknown step", %{
           step: step_name
         })}

      {:error, reason} ->
        {:error,
         preview_error(:invalid_step_input_mapping, "workflow preview input mapping failed", %{
           step: step_name,
           reason: reason
         })}
    end
  end

  defp preview_runnable(%Spec{} = spec, registry, %Runnable{} = runnable) do
    step = step!(spec, runnable.step)
    action = value(step, :action)
    module = value(step, :module)
    opts = value(step, :opts, [])
    action_opts = Keyword.get(opts, :action_opts, [])

    with :ok <- validate_action_input(module, runnable, action_opts),
         {:ok, dry_run} <- ActionRegistry.resolve_dry_run(action, registry),
         {:ok, output} <- execute_dry_run(dry_run, spec, step, runnable, action_opts) do
      node(runnable, action, :completed, output: output)
    else
      {:error, :unsupported_preview} ->
        node(runnable, action, :unsupported,
          error:
            preview_error(
              :unsupported_preview,
              "step #{inspect(runnable.step)} does not support dry-run preview",
              %{
                step: runnable.step,
                action: action
              }
            )
        )

      {:error, {:invalid_action_input, error}} ->
        node(runnable, action, :validation_error,
          error:
            preview_error(
              :invalid_action_input,
              "step #{inspect(runnable.step)} action input is invalid",
              %{
                step: runnable.step,
                validation_errors: Map.get(error, :validation_errors, %{})
              }
            )
        )

      {:error, error} when is_map(error) ->
        node(runnable, action, :failed, error: error)
    end
  end

  defp validate_action_input(module, %Runnable{input: input}, action_opts)
       when is_atom(module) do
    cond do
      function_exported?(module, :validate_action_input, 2) ->
        normalize_action_input_validation(module.validate_action_input(input, action_opts))

      Step.native_step?(module) ->
        normalize_native_input_validation(Step.validate_input(module, input))

      true ->
        :ok
    end
  end

  defp validate_action_input(_module, %Runnable{}, _action_opts), do: :ok

  defp normalize_action_input_validation(:ok), do: :ok

  defp normalize_action_input_validation({:error, error}) do
    {:error, {:invalid_action_input, error}}
  end

  defp normalize_native_input_validation({:ok, _input}), do: :ok

  defp normalize_native_input_validation({:error, error}) do
    {:error, {:invalid_action_input, error}}
  end

  defp execute_dry_run(
         {module, function},
         %Spec{} = spec,
         step,
         %Runnable{} = runnable,
         action_opts
       ) do
    # Dry-run previews intentionally do not expose durable run state. If previews
    # later need state, build the callback context through Step.Context.from_map/1.
    context = %{
      preview?: true,
      workflow: spec.workflow,
      step: runnable.step,
      action: value(step, :action),
      attempt: 1,
      step_opts: action_opts,
      state: %{}
    }

    result = apply(module, function, [runnable.input, context])

    case Step.normalize_result(result) do
      {:ok, output, _opts} -> {:ok, output}
      {:error, error} -> {:error, error}
    end
  rescue
    exception ->
      {:error,
       preview_error(:dry_run_failed, "step #{inspect(runnable.step)} dry-run preview failed", %{
         exception: inspect(exception.__struct__)
       })}
  end

  defp node(%Runnable{} = runnable, action, status, attrs) do
    output = Keyword.get(attrs, :output)
    error = Keyword.get(attrs, :error)

    compact(%{
      id: Atom.to_string(runnable.step),
      step: runnable.step,
      action: action,
      status: status,
      input: runnable.input,
      output: output,
      error: error,
      debug: %{input: runnable.input, output: output, error: error}
    })
  end

  defp error_node(error) do
    %{
      id: nil,
      step: nil,
      action: nil,
      status: :failed,
      input: %{},
      output: nil,
      error: error,
      debug: %{error: error}
    }
  end

  defp final_status(_status, errors) when errors == [], do: :completed
  defp final_status(:completed, _errors), do: :failed
  defp final_status(status, _errors), do: status

  defp node_errors(%{error: error}) when is_map(error), do: [error]
  defp node_errors(_node), do: []

  defp step!(%Spec{} = spec, step_name) do
    Enum.find(spec.steps, &(value(&1, :name) == step_name))
  end

  defp preview_error(code, message, details) do
    %{code: code, message: message, details: details}
  end

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp value(map, key, default \\ nil)

  defp value(map, key, default) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key), default)
    end
  end

  defp value(_map, _key, default), do: default
end
