# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
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
  @type catalog_entry :: %{
          key: action_key(),
          display_name: String.t(),
          category: String.t() | nil,
          description: String.t(),
          enabled?: boolean(),
          input_contract: term(),
          output_contract: term(),
          credential_requirements: term()
        }
  @type catalog_error :: %{
          path: [atom() | action_key()],
          code: atom(),
          message: String.t(),
          details: map()
        }
  @type validation_error :: Spec.validation_error()

  @doc """
  Projects a host-owned action registry into editor-safe catalog metadata.

  The catalog intentionally omits executable modules and credential values. It
  exposes stable action keys, display metadata, contracts, and credential
  requirements so editor clients can build palettes while `validate_action/2`
  and `resolve_action/2` remain the execution trust boundary.
  """
  @spec catalog(term()) ::
          {:ok, [catalog_entry()]} | {:error, {:invalid_action_catalog, [catalog_error()]}}
  def catalog(registry) do
    case registry_pairs(registry) do
      {:ok, pairs} -> catalog_entries(pairs)
      {:error, error} -> {:error, {:invalid_action_catalog, [error]}}
    end
  end

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

  @doc false
  @spec resolve_action_opts(action_key() | term(), registry()) ::
          {:ok, keyword()} | {:error, action_validation_error()}
  def resolve_action_opts(action, registry) do
    with :ok <- validate_action(action, registry),
         {:ok, entry} <- fetch_registry_entry(registry, action) do
      case entry_value(entry, :action_opts, []) do
        opts when is_list(opts) -> {:ok, opts}
        _invalid -> {:ok, []}
      end
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
         |> delete_user_action_opts()
         |> put_action_opts(entry)
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

  defp catalog_entries(pairs) do
    {entries, errors} =
      Enum.reduce(pairs, {[], []}, fn {action, entry}, {entries, errors} ->
        case catalog_entry(action, entry) do
          {:ok, catalog_entry} -> {[catalog_entry | entries], errors}
          {:error, error} -> {entries, [error | errors]}
        end
      end)

    case errors do
      [] -> {:ok, Enum.reverse(entries)}
      errors -> {:error, {:invalid_action_catalog, Enum.reverse(errors)}}
    end
  end

  defp registry_pairs(registry) when is_map(registry), do: {:ok, Enum.to_list(registry)}

  defp registry_pairs(registry) when is_list(registry) do
    if Keyword.keyword?(registry) do
      {:ok, registry}
    else
      {:error, invalid_registry_error(registry)}
    end
  end

  defp registry_pairs(registry), do: {:error, invalid_registry_error(registry)}

  defp invalid_registry_error(registry) do
    catalog_error(
      [:action_registry],
      :invalid_action_registry,
      "action registry must be a map or keyword list",
      %{registry: registry}
    )
  end

  defp catalog_entry(action, entry) do
    {module, enabled?} = registry_entry_module(entry)

    cond do
      not valid_action_key?(action) ->
        {:error,
         catalog_error(
           [:actions, action],
           :invalid_action_key,
           "action #{inspect(action)} must use an atom or non-empty string key",
           %{action: action}
         )}

      not executable_action_module?(module) ->
        {:error,
         catalog_error(
           [:actions, action],
           :incompatible_action_module,
           "action #{inspect(action)} references an incompatible action module",
           %{action: action}
         )}

      true ->
        catalog_metadata(action, entry, module, enabled?)
    end
  end

  defp catalog_metadata(action, entry, module, enabled?) do
    metadata = module_metadata(module)

    with {:ok, category} <-
           entry
           |> entry_value(:category, metadata_value(metadata, :category))
           |> catalog_json_value(action, :category),
         {:ok, input_contract} <-
           entry
           |> entry_value(:input_contract, metadata_input_contract(metadata))
           |> catalog_json_value(action, :input_contract),
         {:ok, output_contract} <-
           entry
           |> entry_value(:output_contract, metadata_output_contract(metadata))
           |> catalog_json_value(action, :output_contract),
         {:ok, credential_requirements} <-
           entry
           |> entry_value(:credential_requirements, [])
           |> catalog_json_value(action, :credential_requirements) do
      {:ok,
       %{
         key: action,
         display_name: display_name(entry, metadata, action),
         category: category,
         description: description(entry, metadata),
         enabled?: enabled? != false,
         input_contract: input_contract,
         output_contract: output_contract,
         credential_requirements: credential_requirements
       }}
    end
  end

  defp module_metadata(module) do
    cond do
      Step.native_step?(module) ->
        Step.metadata(module)

      jido_action?(module) ->
        module.__action_metadata__()

      true ->
        %{}
    end
  end

  defp display_name(entry, metadata, action) do
    name =
      entry_value(entry, :display_name, nil) ||
        metadata_value(metadata, :display_name) ||
        metadata_value(metadata, :title) ||
        metadata_value(metadata, :name)

    humanize_action_name(name, action)
  end

  defp humanize_action_name(nil, action), do: humanize_name(to_string(action))
  defp humanize_action_name(name, _action), do: humanize_name(to_string(name))

  defp humanize_name(name) do
    name
    |> String.replace(".", " ")
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp description(entry, metadata) do
    entry_value(entry, :description, nil) ||
      metadata_value(metadata, :description) ||
      ""
  end

  defp metadata_input_contract(metadata) do
    metadata_value(metadata, :input_schema) || metadata_value(metadata, :schema) || []
  end

  defp metadata_output_contract(metadata), do: metadata_value(metadata, :output_schema) || []

  defp metadata_value(metadata, key), do: map_value(metadata, key)

  defp entry_value(entry, key, default) when is_list(entry) do
    Keyword.get(entry, key, default)
  end

  defp entry_value(entry, key, default) when is_map(entry), do: map_value(entry, key, default)
  defp entry_value(_entry, _key, default), do: default

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

  defp catalog_json_value(value, action, field) do
    case json_value(value, [field]) do
      {:ok, value} ->
        {:ok, value}

      {:error, path} ->
        {:error,
         catalog_error(
           [:actions, action | Enum.reverse(path)],
           :unsupported_json_value,
           "action catalog metadata must be JSON-safe",
           %{action: action, field: field}
         )}
    end
  end

  defp json_value(nil, _path), do: {:ok, nil}
  defp json_value(value, _path) when is_boolean(value), do: {:ok, value}
  defp json_value(value, _path) when is_integer(value), do: {:ok, value}
  defp json_value(value, _path) when is_float(value), do: {:ok, value}
  defp json_value(value, _path) when is_binary(value), do: {:ok, value}
  defp json_value(value, _path) when is_atom(value), do: {:ok, Atom.to_string(value)}

  defp json_value(value, path) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> json_value(path)
  end

  defp json_value([], _path), do: {:ok, []}

  defp json_value(value, path) when is_list(value) do
    if Keyword.keyword?(value), do: json_map(value, path), else: json_list(value, path)
  end

  defp json_value(value, path) when is_map(value), do: json_map(Map.to_list(value), path)

  defp json_value(_value, path), do: {:error, path}

  defp json_map(pairs, path) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn {key, item}, {:ok, acc} ->
      with {:ok, key} <- json_key(key, path),
           {:ok, item} <- json_value(item, [key | path]) do
        {:cont, {:ok, Map.put(acc, key, item)}}
      else
        {:error, path} -> {:halt, {:error, path}}
      end
    end)
  end

  defp json_key(key, _path) when is_atom(key), do: {:ok, Atom.to_string(key)}
  defp json_key(key, _path) when is_binary(key), do: {:ok, key}
  defp json_key(key, _path) when is_integer(key), do: {:ok, Integer.to_string(key)}
  defp json_key(_key, path), do: {:error, path}

  defp json_list(list, path) do
    list
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {item, index}, {:ok, acc} ->
      case json_value(item, [index | path]) do
        {:ok, item} -> {:cont, {:ok, [item | acc]}}
        {:error, path} -> {:halt, {:error, path}}
      end
    end)
    |> case do
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, path} -> {:error, path}
    end
  end

  defp put_action_metadata(step, action) do
    metadata = Map.get(step, :metadata, %{})

    if is_map(metadata) do
      Map.put(step, :metadata, Map.put(metadata, :action, action))
    else
      step
    end
  end

  defp put_action_opts(step, entry) do
    case entry_value(entry, :action_opts, nil) do
      opts when is_list(opts) ->
        put_host_action_opts(step, opts)

      _missing_or_invalid ->
        step
    end
  end

  defp delete_user_action_opts(step) do
    case Map.fetch(step, :opts) do
      {:ok, opts} when is_list(opts) -> Map.put(step, :opts, Keyword.delete(opts, :action_opts))
      _missing_or_invalid -> step
    end
  end

  defp put_host_action_opts(step, opts) do
    case Map.fetch(step, :opts) do
      {:ok, step_opts} when is_list(step_opts) ->
        Map.put(step, :opts, Keyword.put(step_opts, :action_opts, opts))

      :error ->
        Map.put(step, :opts, action_opts: opts)

      {:ok, _invalid} ->
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

  defp catalog_error(path, code, message, details), do: error(path, code, message, details)
end
