defmodule Squidie.Workflow.ActionRegistry do
  @moduledoc """
  Host-owned trust boundary for runtime-authored workflow actions.

  Runtime-authored specs should reference stable action keys rather than raw
  module atoms. The host application owns the registry and maps those keys to
  approved `Squidie.Step` or explicit `Jido.Action` modules before a spec can
  be activated.
  """

  alias Squidie.Step
  alias Squidie.Workflow.Spec

  @built_in_step_kinds [:wait, :log, :pause, :approval]

  @type action_key :: atom() | String.t()
  @type action_validation_error ::
          :missing_action_key
          | :invalid_action_key
          | :unknown_action_key
          | :disabled_action_key
          | :incompatible_action_module
  @type registry_entry ::
          module()
          | keyword()
          | %{optional(:module) => module(), optional(:enabled?) => boolean()}
          | %{optional(String.t()) => term()}
  @type registry :: %{optional(action_key()) => registry_entry()} | keyword(registry_entry())
  @type validation_error :: Spec.validation_error()

  @doc """
  Resolves `:action` step keys in a workflow spec to approved executable modules.

  The resolved spec preserves the stable action key in both `:action` and step
  `:metadata` so later planner and inspection surfaces can expose identity
  without trusting user-provided module values.
  """
  @spec resolve_spec(Spec.t() | map() | term(), registry()) ::
          {:ok, Spec.t() | map()} | {:error, {:invalid_workflow_spec, [validation_error()]}}
  def resolve_spec(%Spec{} = spec, registry) do
    spec
    |> Map.from_struct()
    |> resolve_spec_map(registry)
    |> case do
      {:ok, resolved} -> {:ok, struct(spec, resolved)}
      {:error, _reason} = error -> error
    end
  end

  def resolve_spec(spec, registry) when is_map(spec), do: resolve_spec_map(spec, registry)

  def resolve_spec(spec, _registry) do
    {:error,
     {:invalid_workflow_spec,
      [
        error([], :invalid_spec, "workflow spec must be a map", %{spec: spec})
      ]}}
  end

  @doc """
  Resolves action keys and validates the resulting executable spec shape.
  """
  @spec validate_spec(Spec.t() | map() | term(), registry()) ::
          :ok | {:error, {:invalid_workflow_spec, [validation_error()]}}
  def validate_spec(spec, registry) do
    with {:ok, resolved} <- resolve_spec(spec, registry) do
      Spec.validate(resolved)
    end
  end

  @doc false
  @spec validate_action(action_key() | term(), registry()) ::
          :ok | {:error, action_validation_error()}
  def validate_action(action, registry) do
    cond do
      is_nil(action) ->
        {:error, :missing_action_key}

      not valid_action_key?(action) ->
        {:error, :invalid_action_key}

      not has_registry_key?(registry, action) ->
        {:error, :unknown_action_key}

      true ->
        registry
        |> fetch_registry_entry(action)
        |> validate_action_entry()
    end
  end

  @doc false
  @spec resolve_action(action_key() | term(), registry()) ::
          {:ok, module()} | {:error, action_validation_error()}
  def resolve_action(action, registry) do
    with :ok <- validate_action(action, registry),
         {:ok, entry} <- fetch_registry_entry(registry, action) do
      {module, _enabled?} = registry_entry_module(entry)
      {:ok, module}
    end
  end

  defp resolve_spec_map(spec, registry) do
    case spec_steps(spec) do
      {steps_key, steps} when is_list(steps) ->
        case resolve_steps(steps, registry) do
          {:ok, steps} -> {:ok, put_resolved_steps(spec, steps_key, steps)}
          {:error, _reason} = error -> error
        end

      _missing_or_invalid ->
        {:ok, spec}
    end
  end

  defp resolve_steps(steps, registry) when is_list(steps) do
    {steps, errors} =
      steps
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {step, index}, {resolved_steps, errors} ->
        case resolve_step(step, index, registry) do
          {:ok, resolved_step} -> {[resolved_step | resolved_steps], errors}
          {:error, error} -> {[step | resolved_steps], [error | errors]}
        end
      end)

    case errors do
      [] -> {:ok, Enum.reverse(steps)}
      errors -> {:error, {:invalid_workflow_spec, Enum.reverse(errors)}}
    end
  end

  defp resolve_step(step, index, registry) when is_map(step) do
    case step_action(step) do
      :missing -> resolve_step_without_action(step, index)
      {action_key, action} -> resolve_step_action(step, index, action_key, action, registry)
    end
  end

  defp resolve_step(step, _index, _registry), do: {:ok, step}

  defp resolve_step_without_action(step, index) do
    module = Map.get(step, :module)

    cond do
      module in @built_in_step_kinds ->
        {:ok, step}

      is_atom(module) and module?(module) ->
        {:error,
         error(
           [:steps, index, :action],
           :missing_action_key,
           "step #{inspect(Map.get(step, :name))} must reference an action key",
           %{step: Map.get(step, :name), module: module}
         )}

      true ->
        {:ok, step}
    end
  end

  defp resolve_step_action(step, index, action_key, action, registry) do
    name = Map.get(step, :name)

    cond do
      not valid_action_key?(action) ->
        {:error,
         error(
           [:steps, index, :action],
           :invalid_action_key,
           "step #{inspect(name)} must reference an atom or non-empty string action key",
           %{step: name, action: action}
         )}

      not has_registry_key?(registry, action) ->
        {:error,
         error(
           [:steps, index, :action],
           :unknown_action_key,
           "step #{inspect(name)} references unknown action key",
           %{step: name, action: action}
         )}

      true ->
        registry
        |> fetch_registry_entry(action)
        |> validate_registry_entry(step, index, action_key, action)
    end
  end

  defp validate_registry_entry({:ok, entry}, step, index, action_key, action) do
    name = Map.get(step, :name)
    {module, enabled?} = registry_entry_module(entry)

    cond do
      enabled? == false ->
        {:error,
         error(
           [:steps, index, :action],
           :disabled_action_key,
           "step #{inspect(name)} references disabled action key",
           %{step: name, action: action}
         )}

      not executable_action_module?(module) ->
        {:error,
         error(
           [:steps, index, :action],
           :incompatible_action_module,
           "step #{inspect(name)} references an incompatible action module",
           %{step: name, action: action, module: module}
         )}

      true ->
        {:ok,
         step
         |> normalize_step_action(action_key, action)
         |> Map.put(:module, module)
         |> put_action_metadata(action)}
    end
  end

  defp validate_action_entry({:ok, entry}) do
    {module, enabled?} = registry_entry_module(entry)

    cond do
      enabled? == false -> {:error, :disabled_action_key}
      not executable_action_module?(module) -> {:error, :incompatible_action_module}
      true -> :ok
    end
  end

  defp registry_entry_module(module) when is_atom(module), do: {module, true}

  defp registry_entry_module(entry) when is_list(entry) do
    {Keyword.get(entry, :module), Keyword.get(entry, :enabled?, true)}
  end

  defp registry_entry_module(entry) when is_map(entry) do
    module = map_value(entry, :module)
    enabled? = map_value(entry, :enabled?, true)

    {module, enabled?}
  end

  defp registry_entry_module(_entry), do: {nil, true}

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

  defp put_action_metadata(step, action) do
    metadata = Map.get(step, :metadata, %{})

    if is_map(metadata) do
      Map.put(step, :metadata, Map.put(metadata, :action, action))
    else
      step
    end
  end

  defp executable_action_module?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and
      (Step.native_step?(module) or jido_action?(module))
  end

  defp executable_action_module?(_module), do: false

  defp jido_action?(module) when is_atom(module) do
    function_exported?(module, :__action_metadata__, 0) and
      function_exported?(module, :run, 2) and
      function_exported?(module, :validate_params, 1) and
      function_exported?(module, :validate_output, 1)
  end

  defp has_registry_key?(registry, action) do
    match?({:ok, _entry}, fetch_registry_entry(registry, action))
  end

  defp fetch_registry_entry(registry, action) when is_map(registry),
    do: Map.fetch(registry, action)

  defp fetch_registry_entry(registry, action) when is_list(registry) do
    if Keyword.keyword?(registry) and is_atom(action) do
      Keyword.fetch(registry, action)
    else
      :error
    end
  end

  defp fetch_registry_entry(_registry, _action), do: :error

  defp spec_steps(spec) do
    cond do
      Map.has_key?(spec, :steps) -> {:steps, Map.get(spec, :steps)}
      Map.has_key?(spec, "steps") -> {"steps", Map.get(spec, "steps")}
      true -> :missing
    end
  end

  defp put_resolved_steps(spec, :steps, steps), do: Map.put(spec, :steps, steps)

  defp put_resolved_steps(spec, "steps", steps) do
    spec
    |> Map.delete("steps")
    |> Map.put(:steps, steps)
  end

  defp step_action(step) do
    cond do
      Map.has_key?(step, :action) -> {:action, Map.get(step, :action)}
      Map.has_key?(step, "action") -> {"action", Map.get(step, "action")}
      true -> :missing
    end
  end

  defp normalize_step_action(step, :action, action), do: Map.put(step, :action, action)

  defp normalize_step_action(step, "action", action) do
    step
    |> Map.delete("action")
    |> Map.put(:action, action)
  end

  defp valid_action_key?(action) when is_atom(action), do: true
  defp valid_action_key?(action) when is_binary(action), do: action != ""
  defp valid_action_key?(_action), do: false

  defp module?(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  defp module?(_module), do: false

  defp error(path, code, message, details) do
    %{
      path: path,
      code: code,
      message: message,
      details: details
    }
  end
end
