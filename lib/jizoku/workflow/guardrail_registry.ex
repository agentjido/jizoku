# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Workflow.GuardrailRegistry do
  @moduledoc """
  Host-owned trust boundary for runtime-authored workflow guardrails.

  Runtime-authored specs reference stable guardrail keys under step
  `opts[:guardrails]`. Host applications own the registry that maps those keys
  to validator modules before drafts are published, previewed, or executed.
  """

  defmodule Decision do
    @moduledoc false

    @enforce_keys [:key, :placement, :policy, :status]
    defstruct [:key, :placement, :policy, :status, result: %{}]

    @type t :: %__MODULE__{
            key: atom() | String.t(),
            placement: :input | :action | :output,
            policy: :block_publish | :block_run_start | :route_error | :audit,
            status: :passed | :failed,
            result: map()
          }
  end

  alias __MODULE__.Decision
  alias Jizoku.Workflow.RegistryHelpers
  alias Jizoku.Workflow.Spec

  @placements [:input, :action, :output]

  @type guardrail_key :: atom() | String.t()
  @type placement :: :input | :action | :output
  @type policy :: :block_publish | :block_run_start | :route_error | :audit
  @type guardrail_validation_error ::
          :missing_guardrail_key
          | :invalid_guardrail_key
          | :unknown_guardrail_key
          | :disabled_guardrail_key
          | :incompatible_guardrail_module
  @type registry_entry ::
          module()
          | keyword()
          | %{optional(:module) => module(), optional(:enabled?) => boolean()}
          | %{optional(String.t()) => term()}
  @type registry :: %{optional(guardrail_key()) => registry_entry()} | keyword(registry_entry())
  @type guardrail_ref :: %{
          key: guardrail_key(),
          placement: placement(),
          policy: policy(),
          config: map()
        }
  @type decision :: Decision.t()
  @type catalog_entry :: %{
          key: guardrail_key(),
          display_name: String.t(),
          category: String.t() | nil,
          description: String.t(),
          enabled?: boolean(),
          input_contract: term(),
          config_schema: term()
        }
  @type catalog_error :: Spec.validation_error()
  @type validation_error :: Spec.validation_error()

  @doc """
  Projects host-owned guardrails into editor-safe catalog metadata.
  """
  @spec catalog(term()) ::
          {:ok, [catalog_entry()]} | {:error, {:invalid_guardrail_catalog, [catalog_error()]}}
  def catalog(registry) do
    case RegistryHelpers.registry_pairs(registry, &invalid_registry_error/1) do
      {:ok, pairs} -> catalog_entries(pairs)
      {:error, error} -> {:error, {:invalid_guardrail_catalog, [error]}}
    end
  end

  @doc false
  @spec validate_guardrail(guardrail_key() | term(), registry()) ::
          :ok | {:error, guardrail_validation_error()}
  def validate_guardrail(guardrail, registry) do
    cond do
      is_nil(guardrail) ->
        {:error, :missing_guardrail_key}

      not valid_guardrail_key?(guardrail) ->
        {:error, :invalid_guardrail_key}

      not has_registry_key?(registry, guardrail) ->
        {:error, :unknown_guardrail_key}

      true ->
        registry
        |> RegistryHelpers.fetch_registry_entry(guardrail)
        |> validate_guardrail_entry()
    end
  end

  @doc false
  @spec validate_spec(Spec.t() | map() | term(), registry()) ::
          :ok | {:error, {:invalid_workflow_spec, [validation_error()]}}
  def validate_spec(%Spec{} = spec, registry), do: validate_spec(Map.from_struct(spec), registry)

  def validate_spec(spec, registry) when is_map(spec) do
    errors =
      spec
      |> spec_steps()
      |> guardrail_reference_errors(registry)

    case errors do
      [] -> :ok
      errors -> {:error, {:invalid_workflow_spec, errors}}
    end
  end

  def validate_spec(spec, _registry) do
    {:error,
     {:invalid_workflow_spec,
      [error([], :invalid_spec, "workflow spec must be a map", %{spec: spec})]}}
  end

  @doc false
  @spec validate_spec_option(Spec.t() | map() | term(), keyword()) ::
          :ok | {:error, {:invalid_workflow_spec, [validation_error()]}}
  def validate_spec_option(spec, opts) when is_list(opts) do
    case {uses_guardrails?(spec), Keyword.fetch(opts, :guardrail_registry)} do
      {true, {:ok, registry}} ->
        validate_spec(spec, registry)

      {true, :error} ->
        {:error,
         {:invalid_workflow_spec,
          [
            error(
              [:guardrail_registry],
              :missing_guardrail_registry,
              "guardrail registry is required when a spec references guardrails",
              %{}
            )
          ]}}

      {false, _registry} ->
        :ok
    end
  end

  @doc false
  @spec uses_guardrails?(Spec.t() | map() | term()) :: boolean()
  def uses_guardrails?(%Spec{} = spec), do: uses_guardrails?(Map.from_struct(spec))

  def uses_guardrails?(spec) when is_map(spec) do
    spec
    |> spec_steps()
    |> Enum.any?(fn
      step when is_map(step) ->
        step
        |> step_opts()
        |> guardrails()
        |> present_guardrails?()

      _other ->
        false
    end)
  end

  def uses_guardrails?(_spec), do: false

  @doc false
  @spec step_guardrails(map(), placement()) :: [guardrail_ref()]
  def step_guardrails(step, placement) when is_map(step) and placement in @placements do
    step
    |> step_opts()
    |> guardrails()
    |> refs_for_placement(placement)
    |> Enum.flat_map(fn {ref, _index} ->
      case normalize_ref(ref, placement) do
        {:ok, normalized} -> [normalized]
        {:error, _reason} -> []
      end
    end)
  end

  def step_guardrails(_step, _placement), do: []

  @doc false
  @spec public_step_guardrails(map()) :: [map()]
  def public_step_guardrails(step) when is_map(step) do
    @placements
    |> Enum.flat_map(&step_guardrails(step, &1))
    |> Enum.map(&public_ref/1)
  end

  @doc false
  @spec evaluate_step(map(), non_neg_integer(), placement(), map(), registry(), map()) ::
          {:ok, [decision()]} | {:error, validation_error(), [decision()]}
  def evaluate_step(step, index, placement, value, registry, context)
      when is_map(step) and placement in @placements and is_map(value) and is_map(context) do
    step
    |> step_guardrails(placement)
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, decisions} ->
      decision = evaluate_ref(ref, value, registry, step, Map.put(context, :step_index, index))

      if blocking_failure?(decision) do
        error = failure_error(step_name(step), index, ref, decision)
        {:halt, {:error, error, Enum.reverse([decision | decisions])}}
      else
        {:cont, {:ok, [decision | decisions]}}
      end
    end)
    |> case do
      {:ok, decisions} -> {:ok, Enum.reverse(decisions)}
      {:error, _error, _decisions} = error -> error
    end
  end

  @doc false
  @spec runtime_error(atom(), decision()) :: map()
  def runtime_error(step_name, %{key: key, placement: placement} = decision) do
    %{
      code: "guardrail_failed",
      message: failure_message(step_name, placement, key),
      retryable?: false,
      guardrail: public_decision(decision)
    }
  end

  @doc false
  @spec public_decision(decision()) :: map()
  def public_decision(
        %{key: key, placement: placement, policy: policy, status: status} = decision
      ) do
    %{
      key: key,
      placement: placement,
      policy: policy,
      status: status,
      result: Map.get(decision, :result, %{})
    }
  end

  defp guardrail_reference_errors(steps, registry) when is_list(steps) do
    steps
    |> Stream.with_index()
    |> Enum.flat_map(fn
      {step, index} when is_map(step) -> step_reference_errors(step, index, registry)
      _other -> []
    end)
  end

  defp step_reference_errors(step, index, registry) do
    Enum.flat_map(@placements, &placement_reference_errors(step, index, &1, registry))
  end

  defp placement_reference_errors(step, index, placement, registry) do
    step
    |> step_opts()
    |> guardrails()
    |> refs_for_placement(placement)
    |> Enum.flat_map(fn {ref, ref_index} ->
      case normalize_ref(ref, placement) do
        {:ok, normalized} ->
          validate_normalized_ref(step, index, ref_index, normalized, registry)

        {:error, {code, field, details}} ->
          [
            error(
              [:steps, index, :opts, :guardrails, placement, ref_index | List.wrap(field)],
              code,
              invalid_ref_message(step_name(step), placement, code),
              Map.merge(%{step: step_name(step), placement: placement}, details)
            )
          ]
      end
    end)
  end

  defp validate_normalized_ref(step, index, ref_index, ref, registry) do
    key = Map.fetch!(ref, :key)

    case validate_guardrail(key, registry) do
      :ok ->
        validate_policy(step, index, ref_index, ref)

      {:error, code} ->
        [
          error(
            [:steps, index, :opts, :guardrails, ref.placement, ref_index, :key],
            code,
            guardrail_key_error_message(step_name(step), ref.placement, code),
            %{step: step_name(step), guardrail: key, placement: ref.placement}
          )
        ]
    end
  end

  defp validate_policy(step, index, ref_index, %{policy: policy} = ref) do
    if policy in allowed_policies(ref.placement) do
      []
    else
      [
        error(
          [:steps, index, :opts, :guardrails, ref.placement, ref_index, :policy],
          :invalid_guardrail_policy,
          "step #{inspect(step_name(step))} defines an invalid #{ref.placement} guardrail policy",
          %{
            step: step_name(step),
            guardrail: ref.key,
            policy: policy
          }
        )
      ]
    end
  end

  defp allowed_policies(:input), do: [:block_run_start, :audit]

  defp allowed_policies(placement) when placement in [:action, :output],
    do: [:route_error, :audit]

  defp allowed_policies(_placement), do: []

  defp evaluate_ref(ref, value, registry, step, context) do
    result =
      with :ok <- validate_guardrail(ref.key, registry),
           {:ok, entry} <- RegistryHelpers.fetch_registry_entry(registry, ref.key),
           {module, _enabled?} <- registry_entry_module(entry) do
        module
        |> apply_guardrail(value, guardrail_context(ref, step, context))
        |> normalize_guardrail_result()
      end

    case result do
      {:ok, result} -> decision(ref, :passed, result)
      {:error, result} -> decision(ref, :failed, result)
    end
  end

  defp apply_guardrail(module, value, context) do
    module.validate_guardrail(value, context)
  rescue
    exception in [
      ArgumentError,
      BadMapError,
      CaseClauseError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      RuntimeError,
      UndefinedFunctionError
    ] ->
      {:error, %{message: "guardrail validator failed", exception: inspect(exception.__struct__)}}
  end

  defp guardrail_context(ref, step, context) do
    context
    |> Map.put(:step, step_name(step))
    |> Map.put(:guardrail, ref.key)
    |> Map.put(:placement, ref.placement)
    |> Map.put(:policy, ref.policy)
    |> Map.put(:config, ref.config)
  end

  defp normalize_guardrail_result(:ok), do: {:ok, %{}}
  defp normalize_guardrail_result(true), do: {:ok, %{}}
  defp normalize_guardrail_result({:ok, result}) when is_map(result), do: {:ok, result}
  defp normalize_guardrail_result({:ok, _result}), do: {:ok, %{}}
  defp normalize_guardrail_result(false), do: {:error, %{message: "guardrail returned false"}}
  defp normalize_guardrail_result({:error, result}) when is_map(result), do: {:error, result}
  defp normalize_guardrail_result({:fail, result}) when is_map(result), do: {:error, result}

  defp normalize_guardrail_result({:error, result}) when is_binary(result) do
    {:error, %{message: result}}
  end

  defp normalize_guardrail_result(_result),
    do: {:error, %{message: "unsupported guardrail result"}}

  defp decision(ref, status, result) do
    %Decision{
      key: ref.key,
      placement: ref.placement,
      policy: ref.policy,
      status: status,
      result: result
    }
  end

  defp blocking_failure?(%{status: :failed, policy: :audit}), do: false
  defp blocking_failure?(%{status: :failed}), do: true
  defp blocking_failure?(_decision), do: false

  defp failure_error(step_name, index, ref, decision) do
    error(
      [:steps, index, :opts, :guardrails, ref.placement, 0],
      :guardrail_failed,
      failure_message(step_name, ref.placement, ref.key),
      %{
        step: step_name,
        guardrail: ref.key,
        placement: ref.placement,
        policy: ref.policy,
        result: Map.get(decision, :result, %{})
      }
    )
  end

  defp failure_message(step_name, placement, key) do
    "step #{inspect(step_name)} #{placement} guardrail #{inspect(key)} failed"
  end

  defp catalog_entries(pairs) do
    {entries, errors} =
      Enum.reduce(pairs, {[], []}, fn {guardrail, entry}, {entries, errors} ->
        case catalog_entry(guardrail, entry) do
          {:ok, entry} -> {[entry | entries], errors}
          {:error, error} -> {entries, [error | errors]}
        end
      end)

    case errors do
      [] -> {:ok, Enum.reverse(entries)}
      errors -> {:error, {:invalid_guardrail_catalog, Enum.reverse(errors)}}
    end
  end

  defp catalog_entry(guardrail, entry) do
    {module, enabled?} = registry_entry_module(entry)

    cond do
      not valid_guardrail_key?(guardrail) ->
        {:error,
         error(
           [:guardrails, guardrail],
           :invalid_guardrail_key,
           "guardrail #{inspect(guardrail)} must use an atom or non-empty string key",
           %{guardrail: guardrail}
         )}

      not validator_module?(module) ->
        {:error,
         error(
           [:guardrails, guardrail],
           :incompatible_guardrail_module,
           "guardrail #{inspect(guardrail)} references an incompatible validator module",
           %{guardrail: guardrail}
         )}

      true ->
        catalog_metadata(guardrail, entry, enabled?)
    end
  end

  defp catalog_metadata(guardrail, entry, enabled?) do
    with {:ok, input_contract} <-
           entry
           |> entry_value(:input_contract, [])
           |> catalog_json_value(guardrail, :input_contract),
         {:ok, config_schema} <-
           entry
           |> entry_value(:config_schema, %{})
           |> catalog_json_value(guardrail, :config_schema) do
      {:ok,
       %{
         key: guardrail,
         display_name: humanize_name(entry_value(entry, :display_name, guardrail)),
         category: entry_value(entry, :category, nil),
         description: entry_value(entry, :description, ""),
         enabled?: enabled? != false,
         input_contract: input_contract,
         config_schema: config_schema
       }}
    end
  end

  defp invalid_registry_error(registry) do
    error(
      [:guardrail_registry],
      :invalid_guardrail_registry,
      "guardrail registry must be a map or keyword list",
      %{registry: registry}
    )
  end

  defp validate_guardrail_entry({:ok, entry}) do
    {module, enabled?} = registry_entry_module(entry)

    cond do
      enabled? == false -> {:error, :disabled_guardrail_key}
      not validator_module?(module) -> {:error, :incompatible_guardrail_module}
      true -> :ok
    end
  end

  defp validate_guardrail_entry(:error), do: {:error, :unknown_guardrail_key}

  defp validator_module?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :validate_guardrail, 2)
  end

  defp validator_module?(_module), do: false

  defp refs_for_placement(guardrails, placement) when is_list(guardrails) do
    guardrails
    |> Keyword.get(placement, [])
    |> List.wrap()
    |> Enum.with_index()
  end

  defp refs_for_placement(guardrails, placement) when is_map(guardrails) do
    guardrails
    |> map_value(placement, [])
    |> List.wrap()
    |> Enum.with_index()
  end

  defp refs_for_placement(_guardrails, _placement), do: []

  defp normalize_ref(key, placement) when is_atom(key) or is_binary(key) do
    normalize_ref(%{key: key}, placement)
  end

  defp normalize_ref(ref, placement) when is_list(ref) do
    if Keyword.keyword?(ref) do
      normalize_ref(Map.new(ref), placement)
    else
      {:error, {:invalid_guardrail_ref, nil, %{guardrail: ref}}}
    end
  end

  defp normalize_ref(ref, placement) when is_map(ref) do
    key = map_value(ref, :key)

    cond do
      is_nil(key) ->
        {:error, {:missing_guardrail_key, :key, %{guardrail: ref}}}

      not valid_guardrail_key?(key) ->
        {:error, {:invalid_guardrail_key, :key, %{guardrail: key}}}

      true ->
        case config(ref) do
          {:ok, config} ->
            {:ok,
             %{
               key: key,
               placement: placement,
               policy: map_value(ref, :policy, default_policy(placement)),
               config: config
             }}

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  defp normalize_ref(ref, _placement),
    do: {:error, {:invalid_guardrail_ref, nil, %{guardrail: ref}}}

  defp default_policy(:input), do: :block_run_start
  defp default_policy(_placement), do: :route_error

  defp config(ref) do
    case map_value(ref, :config, %{}) do
      config when is_map(config) ->
        {:ok, config}

      invalid ->
        {:error, {:invalid_guardrail_config, :config, %{guardrail: ref, config: invalid}}}
    end
  end

  defp guardrails(opts) when is_list(opts), do: Keyword.get(opts, :guardrails, [])
  defp guardrails(_opts), do: []

  defp present_guardrails?(guardrails) when guardrails in [nil, []], do: false
  defp present_guardrails?(guardrails) when is_map(guardrails), do: map_size(guardrails) > 0
  defp present_guardrails?(_guardrails), do: true

  defp step_opts(step) when is_map(step), do: map_value(step, :opts, [])

  defp spec_steps(spec) when is_map(spec) do
    case map_value(spec, :steps, []) do
      steps when is_list(steps) -> steps
      _invalid -> []
    end
  end

  defp step_name(step), do: map_value(step, :name)

  defp public_ref(%{key: key, placement: placement, policy: policy}) do
    %{key: key, placement: placement, policy: policy}
  end

  defp guardrail_key_error_message(step, placement, :unknown_guardrail_key) do
    "step #{inspect(step)} references unknown #{placement} guardrail key"
  end

  defp guardrail_key_error_message(step, placement, :disabled_guardrail_key) do
    "step #{inspect(step)} references disabled #{placement} guardrail key"
  end

  defp guardrail_key_error_message(step, placement, :incompatible_guardrail_module) do
    "step #{inspect(step)} references incompatible #{placement} guardrail key"
  end

  defp guardrail_key_error_message(step, placement, _code) do
    "step #{inspect(step)} references invalid #{placement} guardrail key"
  end

  defp invalid_ref_message(step, placement, _code) do
    "step #{inspect(step)} defines an invalid #{placement} guardrail"
  end

  defp valid_guardrail_key?(guardrail) when is_atom(guardrail), do: true
  defp valid_guardrail_key?(guardrail) when is_binary(guardrail), do: guardrail != ""
  defp valid_guardrail_key?(_guardrail), do: false

  defp registry_entry_module(module) when is_atom(module), do: {module, true}

  defp registry_entry_module(entry) when is_list(entry) do
    {Keyword.get(entry, :module), Keyword.get(entry, :enabled?, true)}
  end

  defp registry_entry_module(entry) when is_map(entry) do
    {map_value(entry, :module), map_value(entry, :enabled?, true)}
  end

  defp registry_entry_module(_entry), do: {nil, true}

  defp has_registry_key?(registry, guardrail) do
    match?({:ok, _entry}, RegistryHelpers.fetch_registry_entry(registry, guardrail))
  end

  defp entry_value(entry, key, default) when is_list(entry), do: Keyword.get(entry, key, default)
  defp entry_value(entry, key, default) when is_map(entry), do: map_value(entry, key, default)
  defp entry_value(_entry, _key, default), do: default

  defp catalog_json_value(value, guardrail, field) do
    case RegistryHelpers.json_value(value, [field]) do
      {:ok, value} ->
        {:ok, value}

      {:error, path} ->
        {:error,
         error(
           [:guardrails, guardrail | Enum.reverse(path)],
           :unsupported_json_value,
           "guardrail catalog metadata must be JSON-safe",
           %{guardrail: guardrail, field: field}
         )}
    end
  end

  defp humanize_name(name) do
    name
    |> to_string()
    |> String.replace(".", " ")
    |> String.replace("_", " ")
    |> String.capitalize()
  end

  defp map_value(map, key, default \\ nil), do: Jizoku.MapField.get(map, key, default)

  defp error(path, code, message, details) do
    Map.new(path: path, code: code, message: message, details: details)
  end
end
