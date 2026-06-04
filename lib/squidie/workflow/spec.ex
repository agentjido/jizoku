# credo:disable-for-next-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Workflow.Spec do
  @moduledoc """
  Serializable, normalized workflow specification used to rebuild planner state.

  The public workflow DSL compiles to the runtime definition used by durable
  execution. This struct captures the same workflow shape as plain data so
  planner state can be reconstructed without exposing Runic structs as public
  API.
  """

  alias Squidie.Runtime.Deadline
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.InputMapping

  @built_in_step_kinds [:wait, :log, :pause, :approval]
  @collection_fields [:triggers, :payload, :steps, :transitions, :retries, :entry_steps]
  @recognized_top_level_fields [
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
  @recognized_nested_fields %{
    triggers: [:name, :type, :config, :payload],
    payload: [:name, :type, :opts],
    steps: [:name, :action, :module, :opts, :metadata],
    transitions: [:from, :on, :to, :condition, :recovery],
    retries: [:step, :opts]
  }
  @allowed_trigger_types [:manual, :cron]
  @allowed_cron_idempotency_strategies [:return_existing_run, :skip_duplicate]
  @log_levels [:debug, :info, :warning, :error]
  @supported_recovery_markers [:compensation, :undo]
  @supported_transaction_boundaries [:repo]
  @terminal_transition_targets [:complete]
  @transition_outcomes [:ok, :error]

  @type t :: %__MODULE__{
          workflow: module(),
          definition_version: String.t() | nil,
          triggers: [Definition.trigger()],
          payload: [Definition.payload_field()],
          steps: [Definition.step()],
          transitions: [Definition.transition()],
          retries: [Definition.retry()],
          entry_steps: [atom()],
          initial_step: atom(),
          entry_step: atom() | nil
        }

  defstruct [
    :workflow,
    :definition_version,
    triggers: [],
    payload: [],
    steps: [],
    transitions: [],
    retries: [],
    entry_steps: [],
    initial_step: nil,
    entry_step: nil
  ]

  @type validation_error :: %{
          path: [atom() | non_neg_integer()],
          code: atom(),
          message: String.t(),
          details: map()
        }

  @doc false
  @spec from_definition(module(), Definition.t()) :: t()
  def from_definition(workflow, definition) when is_atom(workflow) and is_map(definition) do
    %__MODULE__{
      workflow: workflow,
      definition_version: definition.definition_version,
      triggers: definition.triggers,
      payload: definition.payload,
      steps: definition.steps,
      transitions: definition.transitions,
      retries: definition.retries,
      entry_steps: definition.entry_steps,
      initial_step: definition.initial_step,
      entry_step: definition.entry_step
    }
  end

  @doc """
  Validates a workflow spec as data without loading workflow or step modules.
  """
  @spec validate(t() | map() | term()) ::
          :ok | {:error, {:invalid_workflow_spec, [validation_error()]}}
  def validate(%__MODULE__{} = spec), do: validate(Map.from_struct(spec))

  def validate(spec) when is_map(spec) do
    case validation_errors(spec) do
      [] -> :ok
      errors -> {:error, {:invalid_workflow_spec, errors}}
    end
  end

  def validate(spec) do
    {:error,
     {:invalid_workflow_spec,
      [
        error([], :invalid_spec, "workflow spec must be a map", %{spec: spec})
      ]}}
  end

  defp validation_errors(spec) do
    steps = list_field(spec, :steps)
    transitions = list_field(spec, :transitions)
    retries = list_field(spec, :retries)
    entry_steps = list_field(spec, :entry_steps)
    step_names = step_names(steps)

    []
    |> validate_ambiguous_keys(spec, [], @recognized_top_level_fields)
    |> validate_nested_ambiguous_keys(spec)
    |> validate_collections(spec)
    |> validate_required_declarations(spec)
    |> validate_definition_version(spec)
    |> validate_workflow(spec)
    |> validate_triggers(list_field(spec, :triggers))
    |> validate_unique_trigger_names(list_field(spec, :triggers))
    |> validate_trigger_payload_conflicts(list_field(spec, :triggers))
    |> validate_payload_contract(spec)
    |> validate_payload_fields(list_field(spec, :payload), [:payload])
    |> validate_steps(steps)
    |> validate_unique_step_names(steps)
    |> validate_step_mappings(steps)
    |> validate_step_retries(steps)
    |> validate_step_recovery_markers(steps)
    |> validate_built_in_steps(steps, transitions)
    |> validate_dependency_graph(steps, step_names)
    |> validate_transitions(transitions, step_names)
    |> validate_transition_graph(steps, transitions)
    |> validate_dependency_transitions(steps, transitions)
    |> validate_retries(retries, step_names)
    |> validate_retry_derivation(spec, steps)
    |> validate_entry_steps(entry_steps, step_names)
    |> validate_expected_entry_metadata(spec, steps, transitions)
    |> validate_step_reference(spec, :initial_step, step_names)
    |> validate_step_reference(spec, :entry_step, step_names)
    |> Enum.reverse()
  end

  defp validate_definition_version(errors, spec) do
    case field(spec, :definition_version) do
      nil ->
        errors

      version when is_binary(version) ->
        errors

      version ->
        [
          error(
            [:definition_version],
            :invalid_definition_version,
            "definition_version must be a string",
            %{definition_version: version}
          )
          | errors
        ]
    end
  end

  defp validate_workflow(errors, spec) do
    workflow = field(spec, :workflow)

    if module?(workflow) do
      errors
    else
      [
        error(
          [:workflow],
          :invalid_workflow,
          "workflow must be a module atom",
          %{workflow: workflow}
        )
        | errors
      ]
    end
  end

  defp validate_collections(errors, spec) do
    Enum.reduce(@collection_fields, errors, fn
      field_name, acc ->
        value = field(spec, field_name)

        if is_list(value) do
          acc
        else
          [
            error(
              [field_name],
              :invalid_collection,
              "#{field_name} must be a list",
              %{field: field_name, value: value}
            )
            | acc
          ]
        end
    end)
  end

  defp validate_required_declarations(errors, spec) do
    errors
    |> maybe_error(
      list_field(spec, :triggers) != [],
      [:triggers],
      :missing_triggers,
      "at least one trigger is required",
      %{}
    )
    |> maybe_error(
      list_field(spec, :steps) != [],
      [:steps],
      :missing_steps,
      "at least one step is required",
      %{}
    )
  end

  defp validate_triggers(errors, triggers) do
    triggers
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {trigger, index}, acc ->
      acc
      |> validate_trigger_shape(trigger, index)
      |> validate_trigger_payload(trigger, index)
    end)
  end

  defp validate_trigger_shape(errors, trigger, index) when is_map(trigger) do
    name = field(trigger, :name)
    type = field(trigger, :type)
    config = field(trigger, :config)

    errors
    |> maybe_error(
      atom_name?(name),
      [:triggers, index, :name],
      :invalid_trigger_name,
      "trigger name must be an atom",
      %{trigger: name}
    )
    |> maybe_error(
      type in @allowed_trigger_types,
      [:triggers, index, :type],
      :invalid_trigger_type,
      "trigger #{inspect(name)} defines unsupported type #{inspect(type)}",
      %{trigger: name, type: type}
    )
    |> maybe_error(
      is_map(config),
      [:triggers, index, :config],
      :invalid_trigger_config,
      "trigger #{inspect(name)} config must be a map",
      %{trigger: name, config: config}
    )
    |> validate_cron_trigger(trigger, index)
  end

  defp validate_trigger_shape(errors, trigger, index) do
    [
      error([:triggers, index], :invalid_trigger, "trigger must be a map", %{trigger: trigger})
      | errors
    ]
  end

  defp validate_trigger_payload(errors, trigger, index) when is_map(trigger) do
    payload = field(trigger, :payload)

    if is_list(payload) do
      validate_payload_fields(errors, payload, [:triggers, index, :payload])
    else
      [
        error(
          [:triggers, index, :payload],
          :invalid_collection,
          "trigger #{inspect(field(trigger, :name))} payload must be a list",
          %{field: :payload, value: payload}
        )
        | errors
      ]
    end
  end

  defp validate_trigger_payload(errors, _trigger, _index), do: errors

  defp validate_cron_trigger(errors, trigger, index) do
    if field(trigger, :type) == :cron and is_map(field(trigger, :config)) do
      config = field(trigger, :config)
      name = field(trigger, :name)

      errors
      |> maybe_error(
        non_empty_binary?(field(config, :expression)),
        [:triggers, index, :config, :expression],
        :invalid_cron_expression,
        "trigger #{inspect(name)} must define a cron expression and timezone",
        %{trigger: name, expression: field(config, :expression)}
      )
      |> maybe_error(
        non_empty_binary?(field(config, :timezone)),
        [:triggers, index, :config, :timezone],
        :invalid_cron_timezone,
        "trigger #{inspect(name)} must define a cron expression and timezone",
        %{trigger: name, timezone: field(config, :timezone)}
      )
      |> validate_cron_idempotency(name, config, index)
    else
      errors
    end
  end

  defp validate_cron_idempotency(errors, name, config, index) do
    strategy = field(config, :idempotency)

    if is_nil(strategy) or strategy in @allowed_cron_idempotency_strategies do
      errors
    else
      [
        error(
          [:triggers, index, :config, :idempotency],
          :invalid_cron_idempotency,
          "trigger #{inspect(name)} defines invalid cron idempotency strategy #{inspect(strategy)}",
          %{trigger: name, idempotency: strategy}
        )
        | errors
      ]
    end
  end

  defp validate_unique_trigger_names(errors, triggers) do
    {_seen, duplicate_errors} =
      triggers
      |> Enum.with_index()
      |> Enum.reduce({MapSet.new(), []}, fn {trigger, index}, {seen, acc} ->
        validate_unique_trigger_name(seen, acc, trigger, index)
      end)

    duplicate_errors ++ errors
  end

  defp validate_unique_trigger_name(seen, errors, trigger, index) when is_map(trigger) do
    name = field(trigger, :name)

    cond do
      not atom_name?(name) ->
        {seen, errors}

      MapSet.member?(seen, name) ->
        {
          seen,
          [
            error(
              [:triggers, index, :name],
              :duplicate_trigger_name,
              "duplicate trigger name: #{inspect(name)}",
              %{trigger: name}
            )
            | errors
          ]
        }

      true ->
        {MapSet.put(seen, name), errors}
    end
  end

  defp validate_unique_trigger_name(seen, errors, _trigger, _index), do: {seen, errors}

  defp validate_trigger_payload_conflicts(errors, triggers) do
    triggers
    |> Enum.flat_map(&trigger_payload_fields/1)
    |> Enum.group_by(&field(&1, :name))
    |> Enum.reduce(errors, fn {name, payload_fields}, acc ->
      validate_trigger_payload_conflict(acc, name, payload_fields)
    end)
  end

  defp trigger_payload_fields(trigger) when is_map(trigger) do
    case field(trigger, :payload) do
      payload when is_list(payload) -> payload
      _payload -> []
    end
  end

  defp trigger_payload_fields(_trigger), do: []

  defp validate_trigger_payload_conflict(errors, name, payload_fields) do
    types =
      payload_fields
      |> Enum.map(&field(&1, :type))
      |> Enum.uniq()
      |> Enum.sort_by(&inspect/1)

    if atom_name?(name) do
      case types do
        [_single] ->
          errors

        [_first, _second | _rest] ->
          [
            error(
              [:triggers],
              :conflicting_payload_field,
              "payload field #{inspect(name)} defines conflicting types across triggers: #{inspect(types)}",
              %{field: name, types: types}
            )
            | errors
          ]

        _other ->
          errors
      end
    else
      errors
    end
  end

  defp validate_payload_contract(errors, spec) do
    payload = field(spec, :payload)
    expected_payload = workflow_payload_fields(list_field(spec, :triggers))

    if payload == expected_payload do
      errors
    else
      [
        error(
          [:payload],
          :invalid_payload_contract,
          "payload must match trigger payload fields",
          %{payload: payload, expected: expected_payload}
        )
        | errors
      ]
    end
  end

  defp workflow_payload_fields(triggers) do
    triggers
    |> Enum.flat_map(&trigger_payload_fields/1)
    |> Enum.uniq_by(&field(&1, :name))
  end

  defp validate_payload_fields(errors, payload_fields, path) do
    payload_fields
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {payload_field, index}, acc ->
      validate_payload_field(acc, payload_field, append_path(path, index))
    end)
  end

  defp validate_payload_field(errors, payload_field, path) when is_map(payload_field) do
    name = field(payload_field, :name)
    type = field(payload_field, :type)
    opts = field(payload_field, :opts)

    errors
    |> maybe_error(
      atom_name?(name),
      append_path(path, :name),
      :invalid_payload_field_name,
      "payload field name must be an atom",
      %{field: name}
    )
    |> maybe_error(
      atom_name?(type),
      append_path(path, :type),
      :invalid_payload_field_type,
      "payload field #{inspect(name)} type must be an atom",
      %{field: name, type: type}
    )
    |> maybe_error(
      Keyword.keyword?(opts),
      append_path(path, :opts),
      :invalid_payload_field_opts,
      "payload field #{inspect(name)} opts must be a keyword list",
      %{field: name, opts: opts}
    )
    |> validate_payload_default(payload_field, opts, path)
  end

  defp validate_payload_field(errors, payload_field, path) do
    [
      error(path, :invalid_payload_field, "payload field must be a map", %{field: payload_field})
      | errors
    ]
  end

  defp validate_payload_default(errors, payload_field, opts, path) when is_list(opts) do
    name = field(payload_field, :name)
    type = field(payload_field, :type)

    case Keyword.fetch(opts, :default) do
      {:ok, default} ->
        if valid_payload_default?(type, default) do
          errors
        else
          [
            error(
              append_path(path, [:opts, :default]),
              :invalid_payload_default,
              "payload field #{inspect(name)} defines an invalid default for type #{inspect(type)}",
              %{field: name, type: type, default: default}
            )
            | errors
          ]
        end

      :error ->
        errors
    end
  end

  defp validate_payload_default(errors, _payload_field, _opts, _path), do: errors

  defp validate_steps(errors, steps) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      acc
      |> validate_step_name(step, index)
      |> validate_step_module(step, index)
      |> validate_step_opts(step, index)
    end)
  end

  defp validate_step_name(errors, step, index) when is_map(step) do
    name = field(step, :name)

    if atom_name?(name) do
      errors
    else
      [
        error(
          [:steps, index, :name],
          :invalid_step_name,
          "step name must be an atom",
          %{step: name}
        )
        | errors
      ]
    end
  end

  defp validate_step_name(errors, step, index) do
    [
      error([:steps, index], :invalid_step, "step must be a map", %{step: step})
      | errors
    ]
  end

  defp validate_step_module(errors, step, index) when is_map(step) do
    name = field(step, :name)
    module = field(step, :module)

    if valid_step_module?(module) do
      errors
    else
      [
        error(
          [:steps, index, :module],
          :invalid_step_module,
          "step #{inspect(name)} must use a module atom or built-in step kind",
          %{step: name, module: module}
        )
        | errors
      ]
    end
  end

  defp validate_step_module(errors, _step, _index), do: errors

  defp validate_step_opts(errors, step, index) when is_map(step) do
    opts = field(step, :opts)

    if Keyword.keyword?(opts) do
      errors
    else
      [
        error(
          [:steps, index, :opts],
          :invalid_step_opts,
          "step #{inspect(field(step, :name))} opts must be a keyword list",
          %{step: field(step, :name), opts: opts}
        )
        | errors
      ]
    end
  end

  defp validate_step_opts(errors, _step, _index), do: errors

  defp validate_step_mappings(errors, steps) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      if is_map(step) and Keyword.keyword?(field(step, :opts) || []) do
        name = field(step, :name)
        opts = field(step, :opts) || []

        acc
        |> validate_step_input_mapping(name, opts, index)
        |> validate_step_output_mapping(name, opts, index)
        |> validate_step_transaction_boundary(name, opts, index)
        |> validate_step_deadline(name, opts, index)
      else
        acc
      end
    end)
  end

  defp validate_step_input_mapping(errors, name, opts, index) do
    input = Keyword.get(opts, :input)

    cond do
      is_nil(input) ->
        errors

      InputMapping.valid?(input) ->
        errors

      true ->
        [
          error(
            [:steps, index, :opts, :input],
            :invalid_step_input_mapping,
            "step #{inspect(name)} defines an invalid :input mapping",
            %{step: name, input: input}
          )
          | errors
        ]
    end
  end

  defp validate_step_output_mapping(errors, name, opts, index) do
    output = Keyword.get(opts, :output)

    if is_nil(output) or is_atom(output) do
      errors
    else
      [
        error(
          [:steps, index, :opts, :output],
          :invalid_step_output_mapping,
          "step #{inspect(name)} defines an invalid :output mapping",
          %{step: name, output: output}
        )
        | errors
      ]
    end
  end

  defp validate_step_transaction_boundary(errors, name, opts, index) do
    boundary = Keyword.get(opts, :transaction)

    if is_nil(boundary) or boundary in @supported_transaction_boundaries do
      errors
    else
      [
        error(
          [:steps, index, :opts, :transaction],
          :invalid_step_transaction_boundary,
          "step #{inspect(name)} defines an invalid :transaction boundary",
          %{step: name, transaction: boundary}
        )
        | errors
      ]
    end
  end

  defp validate_step_deadline(errors, name, opts, index) do
    case Deadline.policy_from_opts(opts) do
      {:ok, _deadline} ->
        errors

      {:error, _reason} ->
        [
          error(
            [:steps, index, :opts, :deadline],
            :invalid_step_deadline,
            "step #{inspect(name)} defines an invalid :deadline policy",
            %{step: name, deadline: Keyword.get(opts, :deadline)}
          )
          | errors
        ]
    end
  end

  defp validate_step_retries(errors, steps) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      if is_map(step) and Keyword.keyword?(field(step, :opts)) do
        validate_step_retry(acc, field(step, :name), field(step, :opts), index)
      else
        acc
      end
    end)
  end

  defp validate_step_retry(errors, name, opts, index) do
    case Keyword.fetch(opts, :retry) do
      {:ok, retry_opts} ->
        validate_retry_policy(
          errors,
          name,
          retry_opts,
          [:steps, index, :opts, :retry],
          :invalid_step_retry_opts,
          :invalid_step_retry_max_attempts,
          :invalid_step_retry_backoff
        )

      :error ->
        errors
    end
  end

  defp validate_step_recovery_markers(errors, steps) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      if is_map(step) and Keyword.keyword?(field(step, :opts) || []) do
        name = field(step, :name)
        opts = field(step, :opts) || []

        acc
        |> validate_boolean_step_option(name, opts, index, :irreversible)
        |> validate_boolean_step_option(name, opts, index, :compensatable)
        |> validate_compensate_callback(name, opts, index)
        |> validate_step_recovery_marker_conflict(name, opts, index)
      else
        acc
      end
    end)
  end

  defp validate_boolean_step_option(errors, name, opts, index, option) do
    case Keyword.fetch(opts, option) do
      {:ok, value} when is_boolean(value) ->
        errors

      {:ok, value} ->
        [
          error(
            [:steps, index, :opts, option],
            :invalid_step_recovery_marker,
            "step #{inspect(name)} defines an invalid #{inspect(option)} marker",
            %{step: name, option: option, value: value}
          )
          | errors
        ]

      :error ->
        errors
    end
  end

  defp validate_compensate_callback(errors, name, opts, index) do
    case Keyword.fetch(opts, :compensate) do
      {:ok, callback} when is_atom(callback) ->
        if module?(callback) and callback not in @built_in_step_kinds do
          errors
        else
          invalid_compensate_callback_error(errors, name, callback, index)
        end

      {:ok, callback} ->
        invalid_compensate_callback_error(errors, name, callback, index)

      :error ->
        errors
    end
  end

  defp invalid_compensate_callback_error(errors, name, callback, index) do
    [
      error(
        [:steps, index, :opts, :compensate],
        :invalid_step_compensate_callback,
        "step #{inspect(name)} defines an invalid :compensate callback",
        %{step: name, compensate: callback}
      )
      | errors
    ]
  end

  defp validate_step_recovery_marker_conflict(errors, name, opts, index) do
    cond do
      Keyword.get(opts, :irreversible) == true and Keyword.get(opts, :compensatable) == true ->
        [
          error(
            [:steps, index, :opts],
            :conflicting_step_recovery_markers,
            "step #{inspect(name)} cannot be both irreversible and compensatable",
            %{step: name}
          )
          | errors
        ]

      Keyword.has_key?(opts, :compensate) and
          (Keyword.get(opts, :irreversible) == true or Keyword.get(opts, :compensatable) == false) ->
        [
          error(
            [:steps, index, :opts, :compensate],
            :conflicting_step_compensation,
            "step #{inspect(name)} cannot declare :compensate when it is irreversible or non-compensatable",
            %{step: name}
          )
          | errors
        ]

      true ->
        errors
    end
  end

  defp validate_built_in_steps(errors, steps, transitions) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      if is_map(step) and field(step, :module) in @built_in_step_kinds and
           Keyword.keyword?(field(step, :opts) || []) do
        acc
        |> validate_built_in_step(step, index)
        |> validate_manual_step_transition_conditions(step, index, transitions)
        |> validate_approval_transitions(step, index, transitions)
      else
        acc
      end
    end)
    |> validate_dependency_manual_step_kinds(steps)
  end

  defp validate_built_in_step(errors, step, index) do
    name = field(step, :name)
    opts = field(step, :opts) || []

    errors =
      if Keyword.has_key?(opts, :transaction) do
        [
          error(
            [:steps, index, :opts, :transaction],
            :built_in_step_transaction,
            "built-in step #{inspect(name)} cannot declare a :transaction boundary",
            %{step: name}
          )
          | errors
        ]
      else
        errors
      end

    case field(step, :module) do
      :wait -> validate_wait_step(errors, name, opts, index)
      :log -> validate_log_step(errors, name, opts, index)
      _other -> errors
    end
  end

  defp validate_wait_step(errors, name, opts, index) do
    duration = Keyword.get(opts, :duration)

    if is_integer(duration) and duration > 0 do
      errors
    else
      [
        error(
          [:steps, index, :opts, :duration],
          :invalid_wait_duration,
          "built-in step #{inspect(name)} requires a positive :duration option",
          %{step: name, duration: duration}
        )
        | errors
      ]
    end
  end

  defp validate_log_step(errors, name, opts, index) do
    message = Keyword.get(opts, :message)
    level = Keyword.get(opts, :level, :info)

    errors
    |> maybe_error(
      non_empty_binary?(message),
      [:steps, index, :opts, :message],
      :invalid_log_message,
      "built-in step #{inspect(name)} requires a non-empty :message option",
      %{step: name, message: message}
    )
    |> maybe_error(
      level in @log_levels,
      [:steps, index, :opts, :level],
      :invalid_log_level,
      "built-in step #{inspect(name)} defines unsupported :level",
      %{step: name, level: level}
    )
  end

  defp validate_approval_transitions(errors, step, index, transitions) do
    if field(step, :module) == :approval do
      name = field(step, :name)

      if has_transition?(transitions, name, :ok) and has_transition?(transitions, name, :error) do
        errors
      else
        [
          error(
            [:steps, index],
            :missing_approval_transitions,
            "approval step #{inspect(name)} must define both :ok and :error transitions",
            %{step: name}
          )
          | errors
        ]
      end
    else
      errors
    end
  end

  defp validate_dependency_manual_step_kinds(errors, steps) do
    if dependency_mode?(steps) do
      steps
      |> Enum.with_index()
      |> Enum.reduce(errors, fn {step, index}, acc ->
        validate_dependency_manual_step_kind(acc, step, index)
      end)
    else
      errors
    end
  end

  defp validate_dependency_manual_step_kind(errors, step, index) do
    if is_map(step) and field(step, :module) in [:pause, :approval] do
      [
        error(
          [:steps, index, :module],
          :manual_step_in_dependency_workflow,
          "dependency-based workflows cannot declare built-in #{inspect(field(step, :module))} steps",
          %{step: field(step, :name), module: field(step, :module)}
        )
        | errors
      ]
    else
      errors
    end
  end

  defp validate_manual_step_transition_conditions(errors, step, index, transitions) do
    if field(step, :module) in [:pause, :approval] do
      name = field(step, :name)

      conditioned? =
        Enum.any?(transitions, fn transition ->
          is_map(transition) and field(transition, :from) == name and
            not is_nil(field(transition, :condition))
        end)

      if conditioned? do
        [
          error(
            [:steps, index],
            :manual_step_transition_condition,
            "transition from built-in manual step #{inspect(name)} cannot define a condition",
            %{step: name}
          )
          | errors
        ]
      else
        errors
      end
    else
      errors
    end
  end

  defp validate_unique_step_names(errors, steps) do
    {_seen, duplicate_errors} =
      steps
      |> Enum.with_index()
      |> Enum.reduce({MapSet.new(), []}, fn {step, index}, {seen, acc} ->
        name = field(step, :name)

        cond do
          not atom_name?(name) ->
            {seen, acc}

          MapSet.member?(seen, name) ->
            {
              seen,
              [
                error(
                  [:steps, index, :name],
                  :duplicate_step_name,
                  "duplicate step name: #{inspect(name)}",
                  %{step: name}
                )
                | acc
              ]
            }

          true ->
            {MapSet.put(seen, name), acc}
        end
      end)

    duplicate_errors ++ errors
  end

  defp validate_dependency_graph(errors, steps, step_names) do
    errors
    |> validate_step_dependencies(steps, step_names)
    |> validate_dependency_cycle(steps)
  end

  defp validate_step_dependencies(errors, steps, step_names) do
    steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      if is_map(step) and Keyword.keyword?(field(step, :opts) || []) do
        validate_step_dependency_list(acc, step, index, step_names)
      else
        acc
      end
    end)
  end

  defp validate_step_dependency_list(errors, step, index, step_names) do
    name = field(step, :name)

    case dependency_list(field(step, :opts) || []) do
      {:ok, dependencies} ->
        Enum.reduce(dependencies, errors, fn dependency, acc ->
          validate_step_dependency(acc, name, dependency, index, step_names)
        end)

      :error ->
        [
          error(
            [:steps, index, :opts, :after],
            :invalid_step_dependencies,
            "step #{inspect(name)} defines an invalid :after dependency list",
            %{step: name, after: Keyword.get(field(step, :opts) || [], :after)}
          )
          | errors
        ]

      :absent ->
        errors
    end
  end

  defp validate_step_dependency(errors, name, dependency, index, step_names) do
    cond do
      dependency == name ->
        [
          error(
            [:steps, index, :opts, :after],
            :self_step_dependency,
            "step #{inspect(name)} cannot depend on itself",
            %{step: name}
          )
          | errors
        ]

      dependency in step_names ->
        errors

      true ->
        [
          error(
            [:steps, index, :opts, :after],
            :unknown_step_dependency,
            "step #{inspect(name)} depends on unknown step #{inspect(dependency)}",
            %{step: name, dependency: dependency}
          )
          | errors
        ]
    end
  end

  defp validate_dependency_cycle(errors, steps) do
    if dependency_mode?(steps) and not dependency_graph_acyclic?(steps) do
      [
        error(
          [:steps],
          :dependency_cycle,
          "workflow dependency graph must be acyclic",
          %{}
        )
        | errors
      ]
    else
      errors
    end
  end

  defp validate_transitions(errors, transitions, step_names) do
    errors
    |> validate_duplicate_transitions(transitions)
    |> then(fn acc ->
      transitions
      |> Enum.with_index()
      |> Enum.reduce(acc, fn {transition, index}, reduce_acc ->
        reduce_acc
        |> validate_transition_source(transition, index, step_names)
        |> validate_transition_outcome(transition, index)
        |> validate_transition_condition(transition, index)
        |> validate_transition_recovery(transition, index)
        |> validate_transition_target(transition, index, step_names)
      end)
    end)
  end

  defp validate_duplicate_transitions(errors, transitions) do
    {_seen, duplicate_errors} =
      transitions
      |> Enum.with_index()
      |> Enum.reduce({MapSet.new(), []}, fn {transition, index}, {seen, acc} ->
        validate_duplicate_transition(seen, acc, transition, index)
      end)

    duplicate_errors ++ errors
  end

  defp validate_duplicate_transition(seen, errors, transition, index) when is_map(transition) do
    key =
      {field(transition, :from), field(transition, :on), transition_condition_key(transition)}

    if MapSet.member?(seen, key) do
      {from, on, _condition} = key

      {
        seen,
        [
          error(
            [:transitions, index],
            :duplicate_transition,
            "duplicate transition declared from #{inspect(from)} on outcome #{inspect(on)}",
            %{from: from, on: on}
          )
          | errors
        ]
      }
    else
      {MapSet.put(seen, key), errors}
    end
  end

  defp validate_duplicate_transition(seen, errors, _transition, _index), do: {seen, errors}

  defp transition_condition_key(transition) do
    case field(transition, :condition) do
      nil -> :unconditional
      condition -> {:condition, Squidie.Workflow.TransitionCondition.serialize(condition)}
    end
  end

  defp validate_dependency_transitions(errors, steps, transitions) do
    if dependency_mode?(steps) and transitions != [] do
      [
        error(
          [:transitions],
          :dependency_transitions,
          "dependency-based workflows cannot declare transitions",
          %{}
        )
        | errors
      ]
    else
      errors
    end
  end

  defp validate_transition_graph(errors, steps, transitions) do
    if not dependency_mode?(steps) and not transition_graph_acyclic?(steps, transitions) do
      [
        error(
          [:transitions],
          :transition_cycle,
          "workflow transition graph must be acyclic",
          %{}
        )
        | errors
      ]
    else
      errors
    end
  end

  defp validate_transition_recovery(errors, transition, index) when is_map(transition) do
    recovery = field(transition, :recovery)
    from = field(transition, :from)
    on = field(transition, :on)

    cond do
      is_nil(recovery) ->
        errors

      on != :error ->
        [
          error(
            [:transitions, index, :recovery],
            :invalid_transition_recovery,
            "transition from #{inspect(from)} can only define recovery markers for :error outcomes",
            %{from: from, on: on, recovery: recovery}
          )
          | errors
        ]

      recovery in @supported_recovery_markers ->
        errors

      true ->
        [
          error(
            [:transitions, index, :recovery],
            :invalid_transition_recovery,
            "transition from #{inspect(from)} defines unsupported recovery marker #{inspect(recovery)}",
            %{from: from, on: on, recovery: recovery}
          )
          | errors
        ]
    end
  end

  defp validate_transition_recovery(errors, _transition, _index), do: errors

  defp validate_transition_condition(errors, transition, index) when is_map(transition) do
    condition = field(transition, :condition)
    from = field(transition, :from)

    cond do
      is_nil(condition) ->
        errors

      match?({:ok, _condition}, Squidie.Workflow.TransitionCondition.normalize(condition)) ->
        errors

      true ->
        [
          error(
            [:transitions, index, :condition],
            :invalid_transition_condition,
            "transition from #{inspect(from)} defines an invalid condition",
            %{from: from, condition: condition}
          )
          | errors
        ]
    end
  end

  defp validate_transition_condition(errors, _transition, _index), do: errors

  defp has_transition?(transitions, from_step, outcome) do
    Enum.any?(transitions, fn
      transition when is_map(transition) ->
        field(transition, :from) == from_step and field(transition, :on) == outcome

      _transition ->
        false
    end)
  end

  defp validate_retries(errors, retries, step_names) do
    retries
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {retry, index}, acc ->
      acc
      |> validate_retry_step(retry, index, step_names)
      |> validate_retry_opts(retry, index)
    end)
  end

  defp validate_retry_step(errors, retry, index, step_names) when is_map(retry) do
    step = field(retry, :step)

    if step in step_names do
      errors
    else
      [
        error(
          [:retries, index, :step],
          :unknown_retry_step,
          "retry targets unknown step: #{inspect(step)}",
          %{step: step}
        )
        | errors
      ]
    end
  end

  defp validate_retry_step(errors, retry, index, _step_names) do
    [
      error([:retries, index], :invalid_retry, "retry must be a map", %{retry: retry})
      | errors
    ]
  end

  defp validate_retry_opts(errors, retry, index) when is_map(retry) do
    step = field(retry, :step)
    opts = field(retry, :opts)

    validate_retry_policy(
      errors,
      step,
      opts,
      [:retries, index, :opts],
      :invalid_retry_opts,
      :invalid_retry_max_attempts,
      :invalid_retry_backoff
    )
  end

  defp validate_retry_opts(errors, _retry, _index), do: errors

  defp validate_retry_policy(
         errors,
         step,
         opts,
         path,
         invalid_opts_code,
         invalid_max_attempts_code,
         invalid_backoff_code
       ) do
    max_attempts = if Keyword.keyword?(opts), do: Keyword.get(opts, :max_attempts)

    cond do
      not Keyword.keyword?(opts) ->
        [
          error(
            path,
            invalid_opts_code,
            "retry for #{inspect(step)} must define a positive :max_attempts",
            %{step: step, opts: opts}
          )
          | errors
        ]

      not (is_integer(max_attempts) and max_attempts > 0) ->
        [
          error(
            append_path(path, :max_attempts),
            invalid_max_attempts_code,
            "retry for #{inspect(step)} must define a positive :max_attempts",
            %{step: step, max_attempts: max_attempts}
          )
          | errors
        ]

      true ->
        validate_retry_backoff(errors, step, opts, path, invalid_backoff_code)
    end
  end

  defp validate_retry_backoff(errors, step, opts, path, invalid_backoff_code) do
    backoff = Keyword.get(opts, :backoff)

    cond do
      is_nil(backoff) ->
        errors

      Keyword.keyword?(backoff) and valid_retry_backoff?(backoff) ->
        errors

      true ->
        [
          error(
            append_path(path, :backoff),
            invalid_backoff_code,
            "retry for #{inspect(step)} defines an invalid :backoff option",
            %{step: step, backoff: backoff}
          )
          | errors
        ]
    end
  end

  defp validate_retry_derivation(errors, spec, steps) do
    retries = field(spec, :retries)
    expected_retries = derive_retries(steps)

    if retries == expected_retries do
      errors
    else
      [
        error(
          [:retries],
          :invalid_retry_derivation,
          "retries must match step retry options",
          %{retries: retries, expected: expected_retries}
        )
        | errors
      ]
    end
  end

  defp valid_retry_backoff?(backoff) do
    min_delay = Keyword.get(backoff, :min)
    max_delay = Keyword.get(backoff, :max)

    Keyword.get(backoff, :type) == :exponential and
      is_integer(min_delay) and min_delay > 0 and
      is_integer(max_delay) and max_delay >= min_delay
  end

  defp validate_transition_source(errors, transition, index, step_names)
       when is_map(transition) do
    from = field(transition, :from)

    if from in step_names do
      errors
    else
      [
        error(
          [:transitions, index, :from],
          :unknown_transition_source,
          "transition starts from unknown step: #{inspect(from)}",
          %{from: from}
        )
        | errors
      ]
    end
  end

  defp validate_transition_source(errors, transition, index, _step_names) do
    [
      error(
        [:transitions, index],
        :invalid_transition,
        "transition must be a map",
        %{transition: transition}
      )
      | errors
    ]
  end

  defp validate_transition_outcome(errors, transition, index) when is_map(transition) do
    outcome = field(transition, :on)

    if outcome in @transition_outcomes do
      errors
    else
      [
        error(
          [:transitions, index, :on],
          :invalid_transition_outcome,
          "transition outcome must be :ok or :error",
          %{on: outcome}
        )
        | errors
      ]
    end
  end

  defp validate_transition_outcome(errors, _transition, _index), do: errors

  defp validate_transition_target(errors, transition, index, step_names)
       when is_map(transition) do
    to = field(transition, :to)

    if to in @terminal_transition_targets or to in step_names do
      errors
    else
      [
        error(
          [:transitions, index, :to],
          :unknown_transition_target,
          "transition targets unknown step: #{inspect(to)}",
          %{to: to}
        )
        | errors
      ]
    end
  end

  defp validate_transition_target(errors, _transition, _index, _step_names), do: errors

  defp validate_entry_steps(errors, entry_steps, step_names) do
    entry_steps
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {entry_step, index}, acc ->
      if entry_step in step_names do
        acc
      else
        [
          error(
            [:entry_steps, index],
            :unknown_entry_step,
            "entry step is unknown: #{inspect(entry_step)}",
            %{entry_step: entry_step}
          )
          | acc
        ]
      end
    end)
  end

  defp validate_step_reference(errors, spec, field_name, step_names) do
    step = field(spec, field_name)

    if is_nil(step) or step in step_names do
      errors
    else
      [
        error(
          [field_name],
          :unknown_step_reference,
          "#{field_name} references unknown step: #{inspect(step)}",
          %{field_name => step}
        )
        | errors
      ]
    end
  end

  defp validate_expected_entry_metadata(errors, spec, steps, transitions) do
    expected_entry_steps = expected_entry_steps(steps, transitions)
    expected_initial_step = List.first(expected_entry_steps)
    entry_steps = field(spec, :entry_steps)
    initial_step = field(spec, :initial_step)
    entry_step = field(spec, :entry_step)

    cond do
      expected_entry_steps == [] and steps != [] ->
        [
          error(
            [:entry_steps],
            :missing_entry_steps,
            "workflow must define at least one entry step",
            %{}
          )
          | errors
        ]

      expected_entry_steps == [] ->
        errors

      true ->
        errors
        |> maybe_error(
          entry_steps == expected_entry_steps,
          [:entry_steps],
          :invalid_entry_steps,
          "entry_steps must match workflow roots",
          %{entry_steps: entry_steps, expected: expected_entry_steps}
        )
        |> maybe_error(
          initial_step == expected_initial_step,
          [:initial_step],
          :invalid_initial_step,
          "initial_step must be the first workflow root",
          %{initial_step: initial_step, expected: expected_initial_step}
        )
        |> validate_expected_entry_step(entry_step, expected_entry_steps, dependency_mode?(steps))
    end
  end

  defp validate_expected_entry_step(errors, entry_step, _expected_entry_steps, true) do
    if is_nil(entry_step) do
      errors
    else
      [
        error(
          [:entry_step],
          :invalid_entry_step,
          "entry_step must be nil for dependency-based workflows",
          %{entry_step: entry_step, expected: nil}
        )
        | errors
      ]
    end
  end

  defp validate_expected_entry_step(errors, entry_step, [expected_entry_step], false) do
    if entry_step == expected_entry_step do
      errors
    else
      [
        error(
          [:entry_step],
          :invalid_entry_step,
          "entry_step must match the transition workflow root",
          %{entry_step: entry_step, expected: expected_entry_step}
        )
        | errors
      ]
    end
  end

  defp validate_expected_entry_step(errors, entry_step, expected_entry_steps, false) do
    [
      error(
        [:entry_step],
        :invalid_entry_step,
        "entry_step must match the transition workflow root",
        %{entry_step: entry_step, expected: expected_entry_steps}
      )
      | errors
    ]
  end

  defp valid_step_module?(module) when is_atom(module) do
    module in @built_in_step_kinds or module?(module)
  end

  defp valid_step_module?(_module), do: false

  defp module?(module) when is_atom(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  defp module?(_module), do: false

  defp dependency_mode?(steps) when is_list(steps) do
    Enum.any?(steps, fn step ->
      is_map(step) and Keyword.keyword?(field(step, :opts) || []) and
        match?(
          {:ok, dependencies} when dependencies != [],
          dependency_list(field(step, :opts) || [])
        )
    end)
  end

  defp dependency_list(opts) do
    case Keyword.fetch(opts, :after) do
      {:ok, []} ->
        :error

      {:ok, dependencies} when is_list(dependencies) ->
        if Enum.all?(dependencies, &is_atom/1) do
          {:ok, Enum.uniq(dependencies)}
        else
          :error
        end

      {:ok, _other} ->
        :error

      :error ->
        :absent
    end
  end

  defp dependency_graph_acyclic?(steps) do
    adjacency = dependency_map(steps)
    graph_acyclic?(adjacency)
  end

  defp graph_acyclic?(adjacency) do
    {result, _state} =
      Enum.reduce_while(
        Map.keys(adjacency),
        {:ok, %{visiting: MapSet.new(), visited: MapSet.new()}},
        fn step_name, {:ok, state} ->
          case visit_dependency(step_name, adjacency, state) do
            {:ok, next_state} -> {:cont, {:ok, next_state}}
            {:error, :cycle} -> {:halt, {:error, :cycle}}
          end
        end
      )

    result == :ok
  end

  defp transition_graph_acyclic?(steps, transitions) do
    steps
    |> transition_dependency_map(transitions)
    |> graph_acyclic?()
  end

  defp transition_dependency_map(steps, transitions) do
    step_names =
      steps
      |> Enum.map(&field(&1, :name))
      |> MapSet.new()

    transition_parents =
      Enum.reduce(transitions, %{}, fn transition, acc ->
        put_transition_parent(acc, transition, step_names)
      end)

    Map.new(steps, fn step ->
      {field(step, :name), Map.get(transition_parents, field(step, :name), [])}
    end)
  end

  defp put_transition_parent(acc, transition, step_names) when is_map(transition) do
    to = field(transition, :to)

    if MapSet.member?(step_names, to) do
      Map.update(acc, to, [field(transition, :from)], fn parents ->
        List.insert_at(parents, -1, field(transition, :from))
      end)
    else
      acc
    end
  end

  defp put_transition_parent(acc, _transition, _step_names), do: acc

  defp dependency_map(steps) do
    Map.new(steps, &dependency_map_entry/1)
  end

  defp dependency_map_entry(step) do
    {field(step, :name), dependency_map_values(step)}
  end

  defp dependency_map_values(step) when is_map(step) do
    opts = field(step, :opts) || []

    if Keyword.keyword?(opts) do
      case dependency_list(opts) do
        {:ok, values} -> values
        _other -> []
      end
    else
      []
    end
  end

  defp dependency_map_values(_step), do: []

  defp derive_retries(steps) do
    Enum.flat_map(steps, &derive_step_retries/1)
  end

  defp derive_step_retries(step) when is_map(step) do
    opts = field(step, :opts)

    if Keyword.keyword?(opts) do
      retry_from_step_opts(step, opts)
    else
      []
    end
  end

  defp derive_step_retries(_step), do: []

  defp retry_from_step_opts(step, opts) do
    case Keyword.fetch(opts, :retry) do
      {:ok, retry_opts} -> [%{step: field(step, :name), opts: retry_opts}]
      :error -> []
    end
  end

  defp visit_dependency(step_name, adjacency, %{visited: visited} = state) do
    cond do
      MapSet.member?(visited, step_name) ->
        {:ok, state}

      MapSet.member?(state.visiting, step_name) ->
        {:error, :cycle}

      true ->
        state = %{state | visiting: MapSet.put(state.visiting, step_name)}

        adjacency
        |> Map.get(step_name, [])
        |> visit_dependencies(adjacency, state)
        |> case do
          {:ok, next_state} ->
            {:ok,
             %{
               next_state
               | visiting: MapSet.delete(next_state.visiting, step_name),
                 visited: MapSet.put(next_state.visited, step_name)
             }}

          {:error, :cycle} ->
            {:error, :cycle}
        end
    end
  end

  defp visit_dependencies(dependencies, adjacency, state) do
    Enum.reduce_while(dependencies, {:ok, state}, fn dependency, {:ok, acc} ->
      case visit_dependency(dependency, adjacency, acc) do
        {:ok, next_acc} -> {:cont, {:ok, next_acc}}
        {:error, :cycle} -> {:halt, {:error, :cycle}}
      end
    end)
  end

  defp expected_entry_steps(steps, transitions) do
    if dependency_mode?(steps) do
      incoming_dependencies = dependency_map(steps)

      steps
      |> Enum.map(&field(&1, :name))
      |> Enum.reject(fn step_name ->
        incoming_dependencies
        |> Map.get(step_name, [])
        |> Enum.any?()
      end)
    else
      transition_targets =
        transitions
        |> Enum.map(&field(&1, :to))
        |> MapSet.new()

      steps
      |> Enum.map(&field(&1, :name))
      |> Enum.reject(&MapSet.member?(transition_targets, &1))
    end
  end

  defp step_names(steps) do
    steps
    |> Enum.map(&field(&1, :name))
    |> Enum.filter(&atom_name?/1)
  end

  defp list_field(spec, key) do
    case field(spec, key) do
      values when is_list(values) -> values
      _other -> []
    end
  end

  defp validate_nested_ambiguous_keys(errors, spec) do
    errors =
      Enum.reduce(@recognized_nested_fields, errors, fn {collection, keys}, acc ->
        spec
        |> list_field(collection)
        |> Enum.with_index()
        |> Enum.reduce(acc, fn {value, index}, nested_acc ->
          validate_ambiguous_keys(nested_acc, value, [collection, index], keys)
        end)
      end)

    spec
    |> list_field(:triggers)
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {trigger, trigger_index}, acc ->
      acc
      |> validate_trigger_config_ambiguous_keys(trigger, trigger_index)
      |> validate_trigger_payload_ambiguous_keys(trigger, trigger_index)
    end)
  end

  defp validate_trigger_config_ambiguous_keys(errors, trigger, trigger_index) do
    validate_ambiguous_keys(
      errors,
      field(trigger, :config),
      [:triggers, trigger_index, :config],
      [:expression, :timezone, :idempotency]
    )
  end

  defp validate_trigger_payload_ambiguous_keys(errors, trigger, trigger_index) do
    payload =
      case field(trigger, :payload) do
        value when is_list(value) -> value
        _value -> []
      end

    payload
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {payload_field, payload_index}, acc ->
      validate_ambiguous_keys(
        acc,
        payload_field,
        [:triggers, trigger_index, :payload, payload_index],
        [:name, :type, :opts]
      )
    end)
  end

  defp field(map, key) when is_map(map), do: Map.get(map, key)
  defp field(_value, _key), do: nil

  defp validate_ambiguous_keys(errors, map, path, keys) when is_map(map) do
    Enum.reduce(keys, errors, fn key, acc ->
      if Map.has_key?(map, key) and Map.has_key?(map, to_string(key)) do
        [
          error(
            append_path(path, key),
            :ambiguous_key,
            "#{key} cannot be provided with both atom and string keys",
            %{key: key}
          )
          | acc
        ]
      else
        acc
      end
    end)
  end

  defp validate_ambiguous_keys(errors, _value, _path, _keys), do: errors

  defp maybe_error(errors, true, _path, _code, _message, _details), do: errors

  defp maybe_error(errors, false, path, code, message, details) do
    [error(path, code, message, details) | errors]
  end

  defp append_path(path, segments) when is_list(segments), do: Enum.concat(path, segments)
  defp append_path(path, segment), do: List.insert_at(path, -1, segment)

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp valid_payload_default?(:string, {:today, :iso8601}), do: true
  defp valid_payload_default?(:string, {:now, :iso8601}), do: true
  defp valid_payload_default?(type, default), do: input_matches_type?(default, type)

  defp input_matches_type?(value, :string), do: is_binary(value)
  defp input_matches_type?(value, :integer), do: is_integer(value)
  defp input_matches_type?(value, :float), do: is_float(value)
  defp input_matches_type?(value, :boolean), do: is_boolean(value)
  defp input_matches_type?(value, :map), do: is_map(value)
  defp input_matches_type?(value, :list), do: is_list(value)
  defp input_matches_type?(value, :atom), do: is_atom(value)
  defp input_matches_type?(_value, _unknown_type), do: true

  defp atom_name?(value), do: is_atom(value) and not is_nil(value)

  defp error(path, code, message, details) do
    %{path: path, code: code, message: message, details: details}
  end
end
