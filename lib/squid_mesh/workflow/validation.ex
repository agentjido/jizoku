defmodule SquidMesh.Workflow.Validation do
  @moduledoc """
  Compile-time validation and normalization for workflow modules.

  This module keeps contract enforcement in one place so the DSL in
  `SquidMesh.Workflow` can remain compact and declarative.
  """

  @terminal_transitions [:complete]
  @supported_transition_outcomes [:ok, :error]
  @supported_transition_recovery_markers [:compensation, :undo]
  @supported_transaction_boundaries [:repo]
  @allowed_trigger_types [:manual, :cron]
  @allowed_cron_idempotency_strategies [:return_existing_run, :skip_duplicate]
  @built_in_step_kinds [:wait, :log, :pause, :approval]
  @log_levels [:debug, :info, :warning, :error]

  alias SquidMesh.Runtime.Deadline
  alias SquidMesh.Workflow.InputMapping

  @doc """
  Validates a compiled workflow definition and raises a compile error when the
  declaration is invalid.
  """
  @spec validate!(map(), Macro.Env.t()) :: :ok
  def validate!(definition, env) do
    case validation_errors(definition) do
      [] ->
        :ok

      errors ->
        description =
          Enum.join(["workflow validation failed:" | Enum.map(errors, &"- #{&1}")], "\n")

        raise CompileError,
          file: env.file,
          line: env.line,
          description: description
    end
  end

  @doc """
  Returns the workflow entry steps or raises when the workflow declaration does
  not define a valid entry set.
  """
  @spec entry_steps!(map(), Macro.Env.t()) :: [atom()]
  def entry_steps!(definition, env) do
    case entry_steps(definition) do
      [] ->
        description =
          if dependency_mode?(definition.steps) do
            "workflow validation failed:\n- dependency-based workflow must define at least one root step"
          else
            "workflow validation failed:\n- workflow must define exactly one entry step"
          end

        raise CompileError,
          file: env.file,
          line: env.line,
          description: description

      [entry_step] ->
        [entry_step]

      entry_steps ->
        if dependency_mode?(definition.steps) do
          entry_steps
        else
          raise CompileError,
            file: env.file,
            line: env.line,
            description:
              "workflow validation failed:\n- workflow must define exactly one entry step"
        end
    end
  end

  @doc """
  Returns the single workflow entry step for transition-based workflows.

  Dependency-based workflows return `nil` because they may declare multiple root
  steps instead of one singular entry step.
  """
  @spec entry_step!(map(), Macro.Env.t()) :: atom() | nil
  def entry_step!(definition, env) do
    if dependency_mode?(definition.steps) do
      nil
    else
      definition
      |> entry_steps!(env)
      |> List.first()
    end
  end

  @doc """
  Returns the first step to schedule for runtime dispatch.
  """
  @spec initial_step!(map(), Macro.Env.t()) :: atom()
  def initial_step!(definition, env) do
    definition
    |> entry_steps!(env)
    |> List.first()
  end

  @doc """
  Converts trigger declarations into the normalized runtime trigger shape.
  """
  @spec normalize_triggers!(map()) :: [map()]
  def normalize_triggers!(definition) do
    Enum.map(definition.triggers, fn trigger ->
      definition_entry = List.first(trigger.definitions)

      %{
        name: trigger.name,
        type: definition_entry.type,
        config: definition_entry.config,
        payload: trigger.payload
      }
    end)
  end

  @doc """
  Returns the canonical workflow payload contract derived from the trigger set.
  """
  @spec workflow_payload!([map()]) :: [map()]
  def workflow_payload!(triggers) when is_list(triggers) do
    triggers
    |> Enum.flat_map(& &1.payload)
    |> Enum.uniq_by(& &1.name)
  end

  def workflow_payload!([]), do: []

  @doc """
  Derives workflow retry declarations from per-step retry configuration.
  """
  @spec derive_retries([map()]) :: [map()]
  def derive_retries(steps) do
    Enum.flat_map(steps, fn step ->
      case Keyword.get(step.opts, :retry) do
        nil ->
          []

        opts when is_list(opts) ->
          [%{step: step.name, opts: opts}]

        opts ->
          [%{step: step.name, opts: opts}]
      end
    end)
  end

  defp validation_errors(definition) do
    step_names = Enum.map(definition.steps, & &1.name)
    payload_fields = workflow_payload_fields(definition)

    []
    |> validate_triggers(definition.triggers)
    |> validate_payload_defaults(payload_fields)
    |> require_steps(step_names)
    |> validate_built_in_steps(definition.steps, definition.transitions)
    |> validate_step_mappings(definition.steps)
    |> validate_step_recovery_markers(definition.steps)
    |> validate_unique_step_names(step_names)
    |> validate_dependency_graph(definition.steps, step_names)
    |> validate_transitions(definition.transitions, step_names)
    |> validate_dependency_transitions(definition.steps, definition.transitions)
    |> validate_retries(definition.retries, step_names)
  end

  defp validate_dependency_graph(errors, steps, step_names) do
    errors
    |> validate_step_dependencies(steps, step_names)
    |> validate_dependency_cycles(steps)
  end

  defp validate_step_dependencies(errors, steps, step_names) do
    Enum.reduce(steps, errors, fn %{name: name, opts: opts}, acc ->
      case dependency_list(opts) do
        {:ok, dependencies} ->
          acc
          |> validate_known_dependencies(name, dependencies, step_names)
          |> validate_self_dependency(name, dependencies)

        :absent ->
          acc

        :error ->
          ["step #{inspect(name)} defines an invalid :after dependency list" | acc]
      end
    end)
  end

  defp validate_known_dependencies(errors, step_name, dependencies, step_names) do
    Enum.reduce(dependencies, errors, fn dependency, acc ->
      if dependency in step_names do
        acc
      else
        ["step #{inspect(step_name)} depends on unknown step #{inspect(dependency)}" | acc]
      end
    end)
  end

  defp validate_self_dependency(errors, step_name, dependencies) do
    if step_name in dependencies do
      ["step #{inspect(step_name)} cannot depend on itself" | errors]
    else
      errors
    end
  end

  defp validate_dependency_cycles(errors, steps) do
    if dependency_mode?(steps) and not dependency_graph_acyclic?(steps) do
      ["workflow dependency graph must be acyclic" | errors]
    else
      errors
    end
  end

  defp validate_triggers(errors, triggers) do
    errors
    |> validate_trigger_count(triggers)
    |> validate_unique_trigger_names(triggers)
    |> validate_trigger_payload_conflicts(triggers)
    |> validate_trigger_definitions(triggers)
  end

  defp validate_trigger_count(errors, []), do: ["at least one trigger is required" | errors]
  defp validate_trigger_count(errors, _triggers), do: errors

  defp validate_unique_trigger_names(errors, triggers) do
    duplicates =
      triggers
      |> Enum.map(& &1.name)
      |> Enum.frequencies()
      |> Enum.filter(fn {_name, count} -> count > 1 end)
      |> Enum.map_join(", ", fn {name, _count} -> inspect(name) end)

    case duplicates do
      "" -> errors
      names -> ["duplicate trigger names: #{names}" | errors]
    end
  end

  defp validate_trigger_payload_conflicts(errors, triggers) do
    triggers
    |> Enum.flat_map(& &1.payload)
    |> Enum.group_by(& &1.name)
    |> Enum.reduce(errors, fn {name, fields}, acc ->
      fields
      |> Enum.map(& &1.type)
      |> Enum.uniq()
      |> case do
        [_single_type] ->
          acc

        types ->
          [
            "payload field #{inspect(name)} defines conflicting types across triggers: #{inspect(types)}"
            | acc
          ]
      end
    end)
  end

  defp validate_trigger_definitions(errors, triggers) do
    Enum.reduce(triggers, errors, fn trigger, acc ->
      acc
      |> validate_trigger_type_count(trigger)
      |> validate_trigger_type_allowed(trigger)
      |> validate_trigger_config(trigger)
    end)
  end

  defp validate_trigger_type_count(errors, %{definitions: [_single_definition]}) do
    errors
  end

  defp validate_trigger_type_count(errors, %{name: name}) do
    ["trigger #{inspect(name)} must define exactly one type" | errors]
  end

  defp validate_trigger_type_allowed(errors, %{definitions: [%{type: type}]})
       when type in @allowed_trigger_types do
    errors
  end

  defp validate_trigger_type_allowed(errors, %{name: name, definitions: [%{type: type}]}) do
    ["trigger #{inspect(name)} defines unsupported type #{inspect(type)}" | errors]
  end

  defp validate_trigger_type_allowed(errors, _trigger), do: errors

  defp validate_trigger_config(errors, %{definitions: [%{type: :manual}]}) do
    errors
  end

  defp validate_trigger_config(errors, %{
         name: name,
         definitions: [%{type: :cron, config: config}]
       }) do
    expression = Map.get(config, :expression)
    timezone = Map.get(config, :timezone)

    errors
    |> validate_cron_schedule(name, expression, timezone)
    |> validate_cron_idempotency(name, Map.get(config, :idempotency))
  end

  defp validate_trigger_config(errors, _trigger), do: errors

  defp validate_cron_schedule(errors, _name, expression, timezone)
       when is_binary(expression) and expression != "" and is_binary(timezone) and timezone != "" do
    errors
  end

  defp validate_cron_schedule(errors, name, _expression, _timezone) do
    ["trigger #{inspect(name)} must define a cron expression and timezone" | errors]
  end

  defp validate_cron_idempotency(errors, _name, nil), do: errors

  defp validate_cron_idempotency(errors, _name, strategy)
       when strategy in @allowed_cron_idempotency_strategies do
    errors
  end

  defp validate_cron_idempotency(errors, name, strategy) do
    [
      "trigger #{inspect(name)} defines invalid cron idempotency strategy #{inspect(strategy)}"
      | errors
    ]
  end

  defp validate_payload_defaults(errors, payload_fields) do
    Enum.reduce(payload_fields, errors, &validate_payload_default/2)
  end

  defp validate_payload_default(field, acc) do
    case Keyword.fetch(field.opts, :default) do
      {:ok, default} -> maybe_invalid_payload_default(field, default, acc)
      :error -> acc
    end
  end

  defp maybe_invalid_payload_default(field, default, acc) do
    if valid_payload_default?(field.type, default) do
      acc
    else
      [
        "payload field #{inspect(field.name)} defines an invalid default for type #{inspect(field.type)}"
        | acc
      ]
    end
  end

  defp require_steps(errors, []), do: ["at least one step is required" | errors]
  defp require_steps(errors, _step_names), do: errors

  defp validate_built_in_steps(errors, steps, transitions) do
    errors =
      Enum.reduce(steps, errors, fn step, acc ->
        validate_built_in_step(acc, step)
      end)

    errors
    |> validate_dependency_manual_step_kind(steps, :pause)
    |> validate_dependency_manual_step_kind(steps, :approval)
    |> validate_manual_step_transition_conditions(steps, transitions)
    |> validate_approval_transitions(steps, transitions)
  end

  defp validate_built_in_step(errors, %{module: kind} = step) when kind in @built_in_step_kinds do
    errors =
      if Keyword.has_key?(step.opts, :transaction) do
        ["built-in step #{inspect(step.name)} cannot declare a :transaction boundary" | errors]
      else
        errors
      end

    case kind do
      :wait -> validate_wait_step(errors, step)
      :log -> validate_log_step(errors, step)
      :pause -> errors
      :approval -> errors
    end
  end

  defp validate_built_in_step(errors, _step), do: errors

  defp validate_dependency_manual_step_kind(errors, steps, kind) do
    if dependency_mode?(steps) and Enum.any?(steps, &(&1.module == kind)) do
      ["dependency-based workflows cannot declare built-in #{inspect(kind)} steps" | errors]
    else
      errors
    end
  end

  defp validate_manual_step_transition_conditions(errors, steps, transitions) do
    manual_steps =
      steps
      |> Enum.filter(&(&1.module in [:pause, :approval]))
      |> MapSet.new(& &1.name)

    Enum.reduce(transitions, errors, fn transition, acc ->
      if Map.has_key?(transition, :condition) and MapSet.member?(manual_steps, transition.from) do
        [
          "transition from built-in manual step #{inspect(transition.from)} cannot define a condition"
          | acc
        ]
      else
        acc
      end
    end)
  end

  defp validate_approval_transitions(errors, steps, transitions) do
    Enum.reduce(steps, errors, fn
      %{name: name, module: :approval}, acc ->
        if has_transition?(transitions, name, :ok) and has_transition?(transitions, name, :error) do
          acc
        else
          ["approval step #{inspect(name)} must define both :ok and :error transitions" | acc]
        end

      _step, acc ->
        acc
    end)
  end

  defp validate_step_mappings(errors, steps) do
    Enum.reduce(steps, errors, fn %{name: name, opts: opts}, acc ->
      acc
      |> validate_step_input_mapping(name, opts)
      |> validate_step_output_mapping(name, opts)
      |> validate_step_transaction_boundary(name, opts)
      |> validate_step_deadline(name, opts)
    end)
  end

  defp validate_step_input_mapping(errors, name, opts) do
    case Keyword.get(opts, :input) do
      nil ->
        errors

      input_mapping ->
        if InputMapping.valid?(input_mapping) do
          errors
        else
          ["step #{inspect(name)} defines an invalid :input mapping" | errors]
        end
    end
  end

  defp validate_step_output_mapping(errors, name, opts) do
    case Keyword.get(opts, :output) do
      nil ->
        errors

      output_mapping when is_atom(output_mapping) ->
        errors

      _other ->
        ["step #{inspect(name)} defines an invalid :output mapping" | errors]
    end
  end

  defp validate_step_transaction_boundary(errors, name, opts) do
    case Keyword.fetch(opts, :transaction) do
      {:ok, boundary} when boundary in @supported_transaction_boundaries ->
        errors

      {:ok, _boundary} ->
        ["step #{inspect(name)} defines an invalid :transaction boundary" | errors]

      :error ->
        errors
    end
  end

  defp validate_step_deadline(errors, name, opts) do
    case Deadline.policy_from_opts(opts) do
      {:ok, _deadline} -> errors
      {:error, _reason} -> ["step #{inspect(name)} defines an invalid :deadline policy" | errors]
    end
  end

  defp validate_step_recovery_markers(errors, steps) do
    Enum.reduce(steps, errors, fn %{name: name, opts: opts}, acc ->
      acc
      |> validate_boolean_step_option(name, opts, :irreversible)
      |> validate_boolean_step_option(name, opts, :compensatable)
      |> validate_step_compensation_callback(name, opts)
      |> validate_recovery_marker_conflict(name, opts)
      |> validate_compensation_marker_conflict(name, opts)
    end)
  end

  defp validate_boolean_step_option(errors, name, opts, option) do
    case Keyword.fetch(opts, option) do
      {:ok, value} when is_boolean(value) ->
        errors

      {:ok, _value} ->
        ["step #{inspect(name)} defines an invalid #{inspect(option)} marker" | errors]

      :error ->
        errors
    end
  end

  defp validate_recovery_marker_conflict(errors, name, opts) do
    if Keyword.get(opts, :irreversible) == true and Keyword.get(opts, :compensatable) == true do
      ["step #{inspect(name)} cannot be both irreversible and compensatable" | errors]
    else
      errors
    end
  end

  defp validate_step_compensation_callback(errors, name, opts) do
    case Keyword.fetch(opts, :compensate) do
      {:ok, callback} when is_atom(callback) ->
        if module_atom?(callback) and callback not in @built_in_step_kinds do
          errors
        else
          ["step #{inspect(name)} defines an invalid :compensate callback" | errors]
        end

      {:ok, _callback} ->
        ["step #{inspect(name)} defines an invalid :compensate callback" | errors]

      :error ->
        errors
    end
  end

  defp module_atom?(callback) when is_atom(callback) do
    callback
    |> Atom.to_string()
    |> String.starts_with?("Elixir.")
  end

  defp validate_compensation_marker_conflict(errors, name, opts) do
    if Keyword.has_key?(opts, :compensate) and
         (Keyword.get(opts, :irreversible) == true or Keyword.get(opts, :compensatable) == false) do
      [
        "step #{inspect(name)} cannot declare :compensate when it is irreversible or non-compensatable"
        | errors
      ]
    else
      errors
    end
  end

  defp validate_wait_step(errors, %{name: name, opts: opts}) do
    duration = Keyword.get(opts, :duration)

    if is_integer(duration) and duration > 0 do
      errors
    else
      ["built-in step #{inspect(name)} requires a positive :duration option" | errors]
    end
  end

  defp validate_log_step(errors, %{name: name, opts: opts}) do
    errors
    |> validate_log_message(name, opts)
    |> validate_log_level(name, opts)
  end

  defp validate_log_message(errors, name, opts) do
    case Keyword.get(opts, :message) do
      message when is_binary(message) and message != "" ->
        errors

      _other ->
        ["built-in step #{inspect(name)} requires a non-empty :message option" | errors]
    end
  end

  defp validate_log_level(errors, name, opts) do
    case Keyword.get(opts, :level, :info) do
      level when level in @log_levels ->
        errors

      _other ->
        ["built-in step #{inspect(name)} defines unsupported :level" | errors]
    end
  end

  defp validate_unique_step_names(errors, step_names) do
    duplicates =
      step_names
      |> Enum.frequencies()
      |> Enum.filter(fn {_name, count} -> count > 1 end)
      |> Enum.map_join(", ", fn {name, _count} -> inspect(name) end)

    case duplicates do
      "" -> errors
      names -> ["duplicate step names: #{names}" | errors]
    end
  end

  defp validate_transitions(errors, transitions, step_names) do
    errors
    |> validate_duplicate_transitions(transitions)
    |> then(fn acc ->
      Enum.reduce(transitions, acc, fn transition, reduce_acc ->
        reduce_acc
        |> validate_transition_from(transition, step_names)
        |> validate_transition_outcome(transition)
        |> validate_transition_condition(transition)
        |> validate_transition_recovery_marker(transition)
        |> validate_transition_to(transition, step_names)
      end)
    end)
  end

  defp validate_dependency_transitions(errors, steps, transitions) do
    if dependency_mode?(steps) and transitions != [] do
      ["dependency-based workflows cannot declare transitions" | errors]
    else
      errors
    end
  end

  defp validate_transition_from(errors, %{from: from}, step_names) do
    if from in step_names do
      errors
    else
      ["transition references unknown step: #{inspect(from)}" | errors]
    end
  end

  defp validate_transition_outcome(errors, %{from: from, on: outcome}) do
    if outcome in @supported_transition_outcomes do
      errors
    else
      [
        "transition from #{inspect(from)} defines unsupported outcome #{inspect(outcome)}"
        | errors
      ]
    end
  end

  defp validate_transition_recovery_marker(errors, %{recovery: _recovery, from: from, on: on})
       when on != :error do
    [
      "transition from #{inspect(from)} can only define recovery markers for :error outcomes"
      | errors
    ]
  end

  defp validate_transition_recovery_marker(errors, %{recovery: recovery, from: from}) do
    if recovery in @supported_transition_recovery_markers do
      errors
    else
      [
        "transition from #{inspect(from)} defines unsupported recovery marker #{inspect(recovery)}"
        | errors
      ]
    end
  end

  defp validate_transition_recovery_marker(errors, _transition), do: errors

  defp validate_transition_to(errors, %{to: to}, step_names) do
    if to in step_names or to in @terminal_transitions do
      errors
    else
      ["transition targets unknown step: #{inspect(to)}" | errors]
    end
  end

  defp validate_transition_condition(errors, %{condition: condition, from: from}) do
    case SquidMesh.Workflow.TransitionCondition.normalize(condition) do
      {:ok, _condition} ->
        errors

      {:error, :invalid_condition} ->
        ["transition from #{inspect(from)} defines an invalid condition" | errors]
    end
  end

  defp validate_transition_condition(errors, _transition), do: errors

  defp validate_duplicate_transitions(errors, transitions) do
    transitions
    |> Enum.group_by(fn %{from: from, on: outcome} -> {from, outcome} end)
    |> Enum.reduce(errors, fn
      {{from, outcome}, grouped}, acc ->
        validate_transition_group(acc, from, outcome, grouped)
    end)
  end

  defp validate_transition_group(errors, _from, _outcome, [_single_transition]), do: errors

  defp validate_transition_group(errors, from, outcome, transitions) do
    unconditional_count = Enum.count(transitions, &(not Map.has_key?(&1, :condition)))

    cond do
      unconditional_count > 1 ->
        [
          "duplicate transition declared from #{inspect(from)} on outcome #{inspect(outcome)}"
          | errors
        ]

      duplicate_transition_condition?(transitions) ->
        [
          "duplicate transition declared from #{inspect(from)} on outcome #{inspect(outcome)}"
          | errors
        ]

      true ->
        errors
    end
  end

  defp duplicate_transition_condition?(transitions) do
    conditions =
      transitions
      |> Enum.filter(&Map.has_key?(&1, :condition))
      |> Enum.map(&SquidMesh.Workflow.TransitionCondition.serialize(&1.condition))

    Enum.uniq(conditions) != conditions
  end

  defp has_transition?(transitions, from_step, outcome) do
    Enum.any?(transitions, &(&1.from == from_step and &1.on == outcome))
  end

  defp validate_retries(errors, retries, step_names) do
    Enum.reduce(retries, errors, fn retry, acc ->
      acc
      |> validate_retry_step(retry, step_names)
      |> validate_retry_opts(retry)
    end)
  end

  defp validate_retry_step(errors, %{step: step}, step_names) do
    if step in step_names do
      errors
    else
      ["retry references unknown step: #{inspect(step)}" | errors]
    end
  end

  defp validate_retry_opts(errors, %{step: step, opts: opts}) do
    if is_list(opts) do
      max_attempts = Keyword.get(opts, :max_attempts)

      if is_integer(max_attempts) and max_attempts > 0 do
        validate_retry_backoff(errors, step, opts)
      else
        ["retry for #{inspect(step)} must define a positive :max_attempts" | errors]
      end
    else
      ["retry for #{inspect(step)} must define a positive :max_attempts" | errors]
    end
  end

  defp validate_retry_backoff(errors, step, opts) do
    case Keyword.get(opts, :backoff) do
      nil ->
        errors

      backoff when is_list(backoff) ->
        if valid_retry_backoff?(backoff) do
          errors
        else
          ["retry for #{inspect(step)} defines an invalid :backoff option" | errors]
        end

      _other ->
        ["retry for #{inspect(step)} defines an invalid :backoff option" | errors]
    end
  end

  defp valid_retry_backoff?(backoff) do
    case Keyword.get(backoff, :type) do
      :exponential ->
        min_delay = Keyword.get(backoff, :min)
        max_delay = Keyword.get(backoff, :max)

        is_integer(min_delay) and min_delay > 0 and
          is_integer(max_delay) and max_delay >= min_delay

      _other ->
        false
    end
  end

  defp valid_payload_default?(:string, {:today, :iso8601}), do: true
  defp valid_payload_default?(:string, {:now, :iso8601}), do: true
  defp valid_payload_default?(type, default), do: input_matches_type?(default, type)

  defp workflow_payload_fields(%{payload: payload}) when is_list(payload), do: payload

  defp workflow_payload_fields(%{triggers: triggers}) when is_list(triggers) do
    Enum.flat_map(triggers, &Map.get(&1, :payload, []))
  end

  defp workflow_payload_fields(_definition), do: []

  defp entry_steps(definition) do
    if dependency_mode?(definition.steps) do
      incoming_dependencies = dependency_map(definition.steps)

      definition.steps
      |> Enum.map(& &1.name)
      |> Enum.reject(fn step_name ->
        incoming_dependencies
        |> Map.get(step_name, [])
        |> Enum.any?()
      end)
    else
      transition_targets =
        definition.transitions
        |> Enum.map(& &1.to)
        |> MapSet.new()

      definition.steps
      |> Enum.map(& &1.name)
      |> Enum.reject(&MapSet.member?(transition_targets, &1))
    end
  end

  defp dependency_mode?(steps) when is_list(steps) do
    Enum.any?(steps, fn step ->
      case Keyword.get(step.opts, :after) do
        dependencies when is_list(dependencies) -> dependencies != []
        _other -> false
      end
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

    {result, _state} =
      Enum.reduce_while(
        Map.keys(adjacency),
        {:ok, %{visiting: MapSet.new(), visited: MapSet.new()}},
        fn
          step_name, {:ok, state} ->
            case visit_dependency(step_name, adjacency, state) do
              {:ok, next_state} -> {:cont, {:ok, next_state}}
              {:error, :cycle} -> {:halt, {:error, :cycle}}
            end
        end
      )

    result == :ok
  end

  defp dependency_map(steps) do
    Map.new(steps, fn %{name: name, opts: opts} ->
      explicit_dependencies =
        case dependency_list(opts) do
          {:ok, dependencies} -> dependencies
          _other -> []
        end

      {name, explicit_dependencies}
    end)
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

  defp input_matches_type?(value, :string), do: is_binary(value)
  defp input_matches_type?(value, :integer), do: is_integer(value)
  defp input_matches_type?(value, :float), do: is_float(value)
  defp input_matches_type?(value, :boolean), do: is_boolean(value)
  defp input_matches_type?(value, :map), do: is_map(value)
  defp input_matches_type?(value, :list), do: is_list(value)
  defp input_matches_type?(value, :atom), do: is_atom(value)
  defp input_matches_type?(_value, _unknown_type), do: true
end
