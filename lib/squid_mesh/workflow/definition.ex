defmodule SquidMesh.Workflow.Definition do
  @moduledoc """
  Runtime-facing representation of a compiled workflow definition.

  `SquidMesh.Workflow` builds the declarative DSL at compile time. This module
  loads the compiled definition and applies the runtime operations needed for
  run creation, payload resolution, and persistence serialization.
  """

  @type built_in_step_kind :: :wait | :log | :pause | :approval
  @type transition_outcome :: :ok | :error
  @type transition_condition :: SquidMesh.Workflow.TransitionCondition.t()
  @type step_input_mapping :: [atom()] | keyword([atom()])
  @type step_output_mapping :: atom()
  @type step_transaction_boundary :: :repo
  @type payload_field :: %{name: atom(), type: atom(), opts: keyword()}
  @type trigger_type :: :manual | :cron
  @type transition_target :: atom() | :complete
  @type trigger :: %{
          name: atom(),
          type: trigger_type(),
          config: map(),
          payload: [payload_field()]
        }
  @type payload_contract :: t() | trigger()
  @type step :: %{name: atom(), module: module() | built_in_step_kind(), opts: keyword()}
  @type failure_recovery_strategy :: :compensation | :undo
  @type transition :: %{
          required(:from) => atom(),
          required(:on) => transition_outcome(),
          required(:to) => transition_target(),
          optional(:condition) => transition_condition(),
          optional(:recovery) => failure_recovery_strategy()
        }
  @type failure_recovery :: %{
          strategy: failure_recovery_strategy(),
          target: transition_target() | String.t()
        }
  @type retry :: %{step: atom(), opts: keyword()}
  @type recovery_policy :: %{
          optional(:compensation) => map(),
          optional(:failure) => failure_recovery(),
          required(:irreversible?) => boolean(),
          required(:compensatable?) => boolean(),
          required(:recovery) => :automatic | :manual_intervention,
          required(:replay) => :allowed | :manual_review_required
        }
  @type dependency_step_status :: :pending | :running | :completed | :failed
  @type inspect_step_status :: dependency_step_status() | :waiting
  @type dependency_progress ::
          :complete
          | {:dispatch, [atom()]}
          | {:wait, [atom()]}
          | {:error, {:no_runnable_step, [atom()]}}
  @type inspect_step :: %{
          step: atom(),
          depends_on: [atom()],
          status: inspect_step_status(),
          recovery: recovery_policy()
        }

  @type t :: %{
          definition_version: String.t() | nil,
          triggers: [trigger()],
          payload: [payload_field()],
          steps: [step()],
          transitions: [transition()],
          retries: [retry()],
          entry_steps: [atom()],
          initial_step: atom(),
          entry_step: atom() | nil
        }

  @type load_error :: {:invalid_workflow, module() | String.t()}
  @type trigger_error :: {:invalid_trigger, atom() | String.t()}
  @type payload_error_details :: %{
          optional(:missing_fields) => [atom()],
          optional(:unknown_fields) => [atom() | String.t()],
          optional(:invalid_types) => %{optional(atom()) => atom()}
        }
  @doc """
  Loads a compiled workflow definition from a workflow module.
  """
  @spec load(module()) :: {:ok, t()} | {:error, load_error()}
  def load(workflow) when is_atom(workflow) do
    case Code.ensure_loaded(workflow) do
      {:module, ^workflow} ->
        if function_exported?(workflow, :workflow_definition, 0) do
          {:ok, workflow.workflow_definition()}
        else
          {:error, {:invalid_workflow, workflow}}
        end

      {:error, _reason} ->
        {:error, {:invalid_workflow, workflow}}
    end
  end

  @doc """
  Loads a workflow definition from its persisted module name.
  """
  @spec load_serialized(String.t()) :: {:ok, module(), t()} | {:error, load_error()}
  def load_serialized(workflow_name) when is_binary(workflow_name) do
    with {:ok, workflow} <- deserialize_workflow_name(workflow_name),
         {:ok, definition} <- load(workflow) do
      {:ok, workflow, definition}
    else
      {:error, _reason} -> {:error, {:invalid_workflow, workflow_name}}
    end
  end

  @doc """
  Validates a payload map against the workflow payload contract.
  """
  @spec validate_payload(payload_contract(), map()) ::
          :ok | {:error, {:invalid_payload, payload_error_details()}}
  def validate_payload(definition, payload) when is_map(payload) do
    declared_fields = definition.payload
    declared_names = MapSet.new(Enum.map(declared_fields, & &1.name))
    provided_names = MapSet.new(Map.keys(payload))

    missing_fields =
      declared_fields
      |> Enum.filter(
        &(Keyword.get(&1.opts, :required, true) and not Map.has_key?(payload, &1.name))
      )
      |> Enum.map(& &1.name)

    unknown_fields =
      provided_names
      |> MapSet.to_list()
      |> Enum.reject(&MapSet.member?(declared_names, &1))
      |> Enum.sort_by(&to_string/1)

    invalid_types = Enum.reduce(declared_fields, %{}, &invalid_payload_type(payload, &1, &2))

    errors =
      %{}
      |> maybe_put(:missing_fields, missing_fields)
      |> maybe_put(:unknown_fields, unknown_fields)
      |> maybe_put(:invalid_types, invalid_types)

    case errors do
      %{} = empty when map_size(empty) == 0 -> :ok
      details -> {:error, {:invalid_payload, details}}
    end
  end

  @doc """
  Resolves payload defaults and validates the final payload for a new run.
  """
  @spec resolve_payload(payload_contract(), map()) ::
          {:ok, map()} | {:error, {:invalid_payload, payload_error_details()}}
  def resolve_payload(definition, payload) when is_map(payload) do
    resolved_payload = Enum.reduce(definition.payload, payload, &put_payload_default/2)

    case validate_payload(definition, resolved_payload) do
      :ok -> {:ok, resolved_payload}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Returns the workflow entry step.
  """
  @spec entry_step(t()) :: atom() | nil
  def entry_step(definition), do: definition.entry_step

  @doc """
  Returns the workflow entry steps in semantic execution order.
  """
  @spec entry_steps(t()) :: [atom()]
  def entry_steps(definition), do: definition.entry_steps

  @doc """
  Returns the first step scheduled when a run starts.
  """
  @spec initial_step(t()) :: atom()
  def initial_step(definition), do: definition.initial_step

  @doc """
  Returns the default trigger for the workflow definition.
  """
  @spec default_trigger(t()) :: atom()
  def default_trigger(definition) do
    definition.triggers
    |> List.first()
    |> Map.fetch!(:name)
  end

  @doc """
  Resolves one named trigger from the workflow definition.
  """
  @spec resolve_trigger(t(), atom()) :: {:ok, atom()} | {:error, trigger_error()}
  def resolve_trigger(definition, trigger_name) when is_atom(trigger_name) do
    with {:ok, trigger} <- trigger(definition, trigger_name) do
      {:ok, trigger.name}
    end
  end

  @doc """
  Fetches one declared workflow trigger by name.
  """
  @spec trigger(t(), atom()) :: {:ok, trigger()} | {:error, trigger_error()}
  def trigger(definition, trigger_name) when is_atom(trigger_name) do
    case Enum.find(definition.triggers, &(&1.name == trigger_name)) do
      %{} = trigger -> {:ok, trigger}
      nil -> {:error, {:invalid_trigger, trigger_name}}
    end
  end

  @doc """
  Fetches one declared workflow step by name.
  """
  @spec step(t(), atom()) :: {:ok, step()} | {:error, {:unknown_step, atom()}}
  def step(definition, step_name) when is_atom(step_name) do
    case Enum.find(definition.steps, &(&1.name == step_name)) do
      %{} = step -> {:ok, step}
      nil -> {:error, {:unknown_step, step_name}}
    end
  end

  @doc """
  Returns the explicit input mapping for one declared step, if any.
  """
  @spec step_input_mapping(t(), atom()) ::
          {:ok, step_input_mapping() | nil} | {:error, {:unknown_step, atom()}}
  def step_input_mapping(definition, step_name) when is_atom(step_name) do
    with {:ok, step} <- step(definition, step_name) do
      {:ok, Keyword.get(step.opts, :input)}
    end
  end

  @doc """
  Returns the explicit output mapping key for one declared step, if any.
  """
  @spec step_output_mapping(t(), atom()) ::
          {:ok, step_output_mapping() | nil} | {:error, {:unknown_step, atom()}}
  def step_output_mapping(definition, step_name) when is_atom(step_name) do
    with {:ok, step} <- step(definition, step_name) do
      {:ok, Keyword.get(step.opts, :output)}
    end
  end

  @doc """
  Returns the local transaction boundary for one declared step, if any.

  `:repo` wraps only the host action execution in the configured Ecto repo
  transaction. Squid Mesh persists attempt, step, and run progression in its
  normal durable phase after the action returns.
  """
  @spec step_transaction_boundary(t(), atom()) ::
          {:ok, step_transaction_boundary() | nil} | {:error, {:unknown_step, atom()}}
  def step_transaction_boundary(definition, step_name) when is_atom(step_name) do
    with {:ok, step} <- step(definition, step_name) do
      {:ok, Keyword.get(step.opts, :transaction)}
    end
  end

  @doc """
  Returns the recovery policy for one declared step.

  Irreversible steps are always treated as non-compensatable. Steps marked
  `compensatable: false` keep their reversibility marker but still require
  explicit operator review before replay.
  """
  @spec step_recovery_policy(t(), atom()) ::
          {:ok, recovery_policy()} | {:error, {:unknown_step, atom()}}
  def step_recovery_policy(definition, step_name) when is_atom(step_name) do
    with {:ok, step} <- step(definition, step_name) do
      {:ok, recovery_policy(step)}
    end
  end

  @doc """
  Returns the compensation callback for one declared step, if any.

  A callback means the step's completed side effect is reversible by a host
  application action. The runtime uses it only during saga rollback after a
  downstream terminal failure, never as a same-step fallback.
  """
  @spec step_compensation_callback(t(), atom()) ::
          {:ok, module() | nil} | {:error, {:unknown_step, atom()}}
  def step_compensation_callback(definition, step_name) when is_atom(step_name) do
    with {:ok, step} <- step(definition, step_name) do
      {:ok, Keyword.get(step.opts, :compensate)}
    end
  end

  @doc """
  Returns completed steps whose recovery policy makes replay unsafe by default.
  """
  @spec unsafe_replay_steps(t(), [atom() | String.t() | {atom() | String.t(), map() | nil}]) ::
          [map()]
  def unsafe_replay_steps(definition, completed_steps) when is_list(completed_steps) do
    Enum.flat_map(completed_steps, &unsafe_replay_step(definition, &1))
  end

  @doc false
  @spec normalize_recovery_policy(map()) :: recovery_policy()
  def normalize_recovery_policy(policy) when is_map(policy) do
    irreversible? = boolean_recovery_value(policy, :irreversible?, false)
    compensatable? = recovery_compensatable?(policy, irreversible?)
    replay = recovery_replay(policy, irreversible?, compensatable?)
    recovery = recovery_mode(policy, replay)

    %{
      irreversible?: irreversible?,
      compensatable?: compensatable?,
      replay: replay,
      recovery: recovery
    }
    |> maybe_put_compensation(normalize_compensation(recovery_value(policy, :compensation, nil)))
    |> maybe_put(:failure, normalize_failure_recovery(recovery_value(policy, :failure, nil)))
  end

  @doc false
  @spec serialize_recovery_policy(recovery_policy()) :: map()
  def serialize_recovery_policy(policy) when is_map(policy) do
    policy = normalize_recovery_policy(policy)

    %{
      "irreversible?" => policy.irreversible?,
      "compensatable?" => policy.compensatable?,
      "replay" => Atom.to_string(policy.replay),
      "recovery" => Atom.to_string(policy.recovery)
    }
    |> maybe_put_serialized_compensation(Map.get(policy, :compensation))
    |> maybe_put_serialized_failure(Map.get(policy, :failure))
  end

  @doc """
  Applies the declared output mapping for one step result.
  """
  @spec apply_output_mapping(t(), atom(), map()) ::
          {:ok, map()} | {:error, {:unknown_step, atom()}}
  def apply_output_mapping(definition, step_name, output)
      when is_atom(step_name) and is_map(output) do
    with {:ok, step} <- step(definition, step_name) do
      case Keyword.get(step.opts, :output) do
        nil -> {:ok, output}
        output_key when is_atom(output_key) -> {:ok, %{output_key => output}}
      end
    end
  end

  @doc """
  Resolves the transition target for a step outcome.
  """
  @spec transition_target(t(), atom(), transition_outcome()) ::
          {:ok, transition_target()} | {:error, {:unknown_transition, atom(), atom()}}
  def transition_target(definition, from_step, outcome)
      when is_atom(from_step) and is_atom(outcome) do
    case transition(definition, from_step, outcome) do
      {:ok, %{to: to_step}} -> {:ok, to_step}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Resolves the transition target for a step outcome using accumulated context.
  """
  @spec transition_target(t(), atom(), transition_outcome(), map()) ::
          {:ok, transition_target()}
          | {:error,
             {:unknown_transition, atom(), atom()} | {:no_matching_transition, atom(), atom()}}
  def transition_target(definition, from_step, outcome, context)
      when is_atom(from_step) and is_atom(outcome) and is_map(context) do
    case transition(definition, from_step, outcome, context) do
      {:ok, %{to: to_step}} -> {:ok, to_step}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Resolves the full transition metadata for one step outcome.
  """
  @spec transition(t(), atom(), transition_outcome()) ::
          {:ok, transition()} | {:error, {:unknown_transition, atom(), atom()}}
  def transition(definition, from_step, outcome)
      when is_atom(from_step) and is_atom(outcome) do
    case Enum.find(definition.transitions, &(&1.from == from_step and &1.on == outcome)) do
      %{} = transition -> {:ok, transition}
      nil -> {:error, {:unknown_transition, from_step, outcome}}
    end
  end

  @doc """
  Resolves the selected transition metadata for one step outcome and context.
  """
  @spec transition(t(), atom(), transition_outcome(), map()) ::
          {:ok, transition()}
          | {:error,
             {:unknown_transition, atom(), atom()} | {:no_matching_transition, atom(), atom()}}
  def transition(definition, from_step, outcome, context)
      when is_atom(from_step) and is_atom(outcome) and is_map(context) do
    matching_outcome =
      Enum.filter(definition.transitions, &(&1.from == from_step and &1.on == outcome))

    conditional_transitions = Enum.filter(matching_outcome, &Map.has_key?(&1, :condition))
    fallback_transition = Enum.find(matching_outcome, &(not Map.has_key?(&1, :condition)))

    selectable_transitions =
      case fallback_transition do
        nil -> conditional_transitions
        fallback -> Enum.concat(conditional_transitions, [fallback])
      end

    case Enum.find(selectable_transitions, &transition_matches?(&1, context)) do
      %{} = transition -> {:ok, transition}
      nil when matching_outcome == [] -> {:error, {:unknown_transition, from_step, outcome}}
      nil -> {:error, {:no_matching_transition, from_step, outcome}}
    end
  end

  @doc false
  @spec serialize_transition_decision(transition() | nil) :: map() | nil
  def serialize_transition_decision(nil), do: nil

  def serialize_transition_decision(%{} = transition) do
    %{
      "from" => serialize_step(Map.fetch!(transition, :from)),
      "on" => Atom.to_string(Map.fetch!(transition, :on)),
      "to" => serialize_transition_target(Map.fetch!(transition, :to))
    }
    |> maybe_put_serialized_condition(Map.get(transition, :condition))
    |> maybe_put_serialized_recovery(Map.get(transition, :recovery))
  end

  @doc false
  @spec deserialize_transition_decision(t() | nil, map() | nil) :: map() | nil
  def deserialize_transition_decision(_definition, nil), do: nil

  def deserialize_transition_decision(definition, transition) when is_map(transition) do
    from = Map.get(transition, :from) || Map.get(transition, "from")
    on = Map.get(transition, :on) || Map.get(transition, "on")
    to = Map.get(transition, :to) || Map.get(transition, "to")

    %{
      from: deserialize_step(definition, from),
      on: deserialize_transition_outcome(on),
      to: deserialize_transition_target(definition, to)
    }
    |> maybe_put_deserialized_condition(
      Map.get(transition, :condition) || Map.get(transition, "condition")
    )
    |> maybe_put_deserialized_recovery(
      Map.get(transition, :recovery) || Map.get(transition, "recovery")
    )
  end

  def deserialize_transition_decision(_definition, _transition), do: nil

  @doc """
  Returns the explicit failure recovery route for a step when one was declared.
  """
  @spec failure_recovery(t(), atom()) ::
          {:ok, failure_recovery() | nil} | {:error, {:unknown_transition, atom(), atom()}}
  def failure_recovery(definition, from_step) when is_atom(from_step) do
    case transition(definition, from_step, :error) do
      {:ok, %{recovery: strategy, to: target}} when strategy in [:compensation, :undo] ->
        {:ok, %{strategy: strategy, target: target}}

      {:ok, %{}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Resolves the success and rejection targets for an approval step.
  """
  @spec approval_transition_targets(t(), atom()) ::
          {:ok, %{ok: transition_target(), error: transition_target()}}
          | {:error, {:unknown_transition, atom(), atom()}}
  def approval_transition_targets(definition, step_name) when is_atom(step_name) do
    with {:ok, ok_target} <- transition_target(definition, step_name, :ok),
         {:ok, error_target} <- transition_target(definition, step_name, :error) do
      {:ok, %{ok: ok_target, error: error_target}}
    end
  end

  @doc """
  Resolves the next step after a successful execution.
  """
  @spec next_step_after_success(t(), atom(), [atom() | String.t()]) ::
          {:ok, transition_target()}
          | {:error, {:no_runnable_step, [atom()]}}
          | {:error, {:unknown_transition, atom(), atom()}}
  def next_step_after_success(definition, from_step, completed_steps)
      when is_atom(from_step) and is_list(completed_steps) do
    if dependency_mode?(definition) do
      next_dependency_step(definition, completed_steps)
    else
      transition_target(definition, from_step, :ok)
    end
  end

  @doc """
  Resolves dependency-mode progress from persisted per-step state.

  Ready steps are scheduled breadth-first by dependency phase so newly unlocked
  descendants do not bypass incomplete root or sibling steps.
  """
  @spec dependency_progress(
          t(),
          %{optional(atom() | String.t()) => dependency_step_status() | String.t()}
        ) :: dependency_progress()
  def dependency_progress(definition, step_statuses) when is_map(step_statuses) do
    normalized_statuses =
      Map.new(step_statuses, fn {step, status} ->
        {serialize_step(step), serialize_dependency_status(status)}
      end)

    completed_steps =
      normalized_statuses
      |> Enum.filter(fn {_step, status} -> status == "completed" end)
      |> Enum.map(fn {step, _status} -> step end)
      |> MapSet.new()

    remaining_steps =
      definition.steps
      |> Enum.map(& &1.name)
      |> Enum.reject(fn step_name ->
        Map.get(normalized_statuses, serialize_step(step_name)) == "completed"
      end)

    case remaining_steps do
      [] ->
        :complete

      _steps ->
        phases = dependency_phases(definition)

        current_phase =
          remaining_steps
          |> Enum.map(&Map.fetch!(phases, &1))
          |> Enum.min()

        phase_steps =
          definition
          |> dependency_step_order()
          |> Enum.filter(fn step_name ->
            step_name in remaining_steps and Map.fetch!(phases, step_name) == current_phase
          end)

        ready_steps =
          Enum.filter(phase_steps, fn step_name ->
            is_nil(Map.get(normalized_statuses, serialize_step(step_name))) and
              dependencies_satisfied?(definition, step_name, completed_steps)
          end)

        cond do
          ready_steps != [] ->
            {:dispatch, ready_steps}

          Enum.any?(phase_steps, fn step_name ->
            Map.get(normalized_statuses, serialize_step(step_name)) in [
              "pending",
              "running",
              "failed"
            ]
          end) ->
            {:wait, phase_steps}

          true ->
            {:error, {:no_runnable_step, phase_steps}}
        end
    end
  end

  @doc """
  Builds the public per-step inspection view from declared steps and persisted
  step statuses.
  """
  @spec inspect_steps(
          t(),
          %{optional(atom() | String.t()) => dependency_step_status() | inspect_step_status()}
        ) :: [inspect_step()]
  def inspect_steps(definition, step_statuses \\ %{}) when is_map(step_statuses) do
    normalized_statuses =
      Map.new(step_statuses, fn {step, status} ->
        {serialize_step(step), serialize_inspect_step_status(status)}
      end)

    dependencies = dependency_map(definition)

    definition
    |> inspect_step_order()
    |> Enum.map(fn step_name ->
      %{
        step: step_name,
        depends_on: Map.get(dependencies, step_name, []),
        status:
          normalized_statuses
          |> Map.get(serialize_step(step_name), "waiting")
          |> deserialize_inspect_step_status(),
        recovery: recovery_policy(step(definition, step_name))
      }
    end)
  end

  @doc """
  Deserializes persisted payload keys back to declared workflow field names.
  """
  @spec deserialize_payload(t() | nil, map()) :: map()
  def deserialize_payload(nil, payload), do: payload

  def deserialize_payload(definition, payload) when is_map(payload) do
    known_fields =
      definition.payload
      |> Enum.map(&{Atom.to_string(&1.name), &1.name})
      |> Map.new()

    Map.new(payload, fn
      {key, value} when is_binary(key) ->
        {Map.get(known_fields, key, key), value}

      entry ->
        entry
    end)
  end

  @doc """
  Serializes a workflow module name for persistence.
  """
  @spec serialize_workflow(module()) :: String.t()
  def serialize_workflow(workflow) when is_atom(workflow), do: Atom.to_string(workflow)

  @doc """
  Returns a stable fingerprint for runtime-significant workflow semantics.

  Journal-backed execution stores this value at start so later runtime workers can
  reject stale attempts when a deploy changes step modules, transitions,
  mappings, dependency declarations, or retry policy for already-planned work.
  """
  @spec fingerprint(t()) :: String.t()
  def fingerprint(definition) when is_map(definition) do
    definition
    |> fingerprint_terms()
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  @doc """
  Builds structured metadata for persisted/current workflow definition drift.
  """
  @spec compatibility_metadata(t(), map()) :: map()
  def compatibility_metadata(definition, persisted)
      when is_map(definition) and is_map(persisted) do
    %{
      persisted_definition_version: metadata_value(persisted, :definition_version),
      persisted_definition_fingerprint: metadata_value(persisted, :definition_fingerprint),
      current_definition_version: Map.get(definition, :definition_version),
      current_definition_fingerprint: fingerprint(definition)
    }
  end

  @doc """
  Builds the structured non-retryable runtime error for definition drift.
  """
  @spec incompatible_definition_error(t(), map()) :: map()
  def incompatible_definition_error(definition, persisted)
      when is_map(definition) and is_map(persisted) do
    definition
    |> compatibility_metadata(persisted)
    |> Map.merge(%{code: "incompatible_workflow_definition", retryable?: false})
  end

  @doc """
  Serializes a trigger identifier for persistence.
  """
  @spec serialize_trigger(atom() | String.t() | nil) :: String.t() | nil
  def serialize_trigger(nil), do: nil
  def serialize_trigger(trigger) when is_atom(trigger), do: Atom.to_string(trigger)
  def serialize_trigger(trigger) when is_binary(trigger), do: trigger

  @doc """
  Serializes a step identifier for persistence.
  """
  @spec serialize_step(atom() | String.t() | nil) :: String.t() | nil
  def serialize_step(nil), do: nil
  def serialize_step(step) when is_atom(step), do: Atom.to_string(step)
  def serialize_step(step) when is_binary(step), do: step

  @doc """
  Returns true when the workflow uses dependency-based step progression.
  """
  @spec dependency_mode?(t()) :: boolean()
  def dependency_mode?(definition) do
    Enum.any?(definition.steps, fn step ->
      case Keyword.get(step.opts, :after) do
        dependencies when is_list(dependencies) -> dependencies != []
        _other -> false
      end
    end)
  end

  @doc """
  Deserializes a persisted trigger name back to the declared workflow trigger.
  """
  @spec deserialize_trigger(t() | nil, String.t() | nil) :: atom() | String.t() | nil
  def deserialize_trigger(_definition, nil), do: nil
  def deserialize_trigger(nil, trigger_name) when is_binary(trigger_name), do: trigger_name

  def deserialize_trigger(definition, trigger_name) when is_binary(trigger_name) do
    Enum.find_value(definition.triggers, trigger_name, fn
      %{name: trigger} ->
        if Atom.to_string(trigger) == trigger_name, do: trigger, else: false
    end)
  end

  @doc """
  Deserializes a persisted step name back to the declared workflow step.
  """
  @spec deserialize_step(t() | nil, String.t() | nil) :: atom() | String.t() | nil
  def deserialize_step(_definition, nil), do: nil
  def deserialize_step(nil, step_name) when is_binary(step_name), do: step_name

  def deserialize_step(definition, step_name) when is_binary(step_name) do
    Enum.find_value(definition.steps, step_name, fn
      %{name: step} ->
        if Atom.to_string(step) == step_name, do: step, else: false
    end)
  end

  defp transition_matches?(%{condition: condition}, context) do
    SquidMesh.Workflow.TransitionCondition.matches?(context, condition)
  end

  defp transition_matches?(%{}, _context), do: true

  defp maybe_put_serialized_condition(serialized, nil), do: serialized

  defp maybe_put_serialized_condition(serialized, condition) do
    case SquidMesh.Workflow.TransitionCondition.serialize(condition) do
      nil -> serialized
      condition -> Map.put(serialized, "condition", condition)
    end
  end

  defp maybe_put_serialized_recovery(serialized, nil), do: serialized

  defp maybe_put_serialized_recovery(serialized, recovery) do
    Map.put(serialized, "recovery", Atom.to_string(recovery))
  end

  defp maybe_put_serialized_compensation(serialized, nil), do: serialized

  defp maybe_put_serialized_compensation(serialized, compensation) when is_map(compensation) do
    Map.put(serialized, "compensation", %{
      "callback" => serialize_recovery_callback(Map.get(compensation, :callback)),
      "status" => serialize_atom_field(Map.get(compensation, :status, :available))
    })
  end

  defp maybe_put_serialized_failure(serialized, nil), do: serialized

  defp maybe_put_serialized_failure(serialized, failure) when is_map(failure) do
    Map.put(serialized, "failure", %{
      "strategy" => serialize_atom_field(Map.get(failure, :strategy)),
      "target" => serialize_failure_target(Map.get(failure, :target))
    })
  end

  defp serialize_recovery_callback(nil), do: nil
  defp serialize_recovery_callback(callback) when is_atom(callback), do: Atom.to_string(callback)
  defp serialize_recovery_callback(callback), do: callback

  defp serialize_atom_field(nil), do: nil
  defp serialize_atom_field(value) when is_atom(value), do: Atom.to_string(value)
  defp serialize_atom_field(value), do: value

  defp serialize_failure_target(nil), do: nil

  defp serialize_failure_target(target) when is_atom(target),
    do: serialize_transition_target(target)

  defp serialize_failure_target(target), do: target

  defp serialize_transition_target(:complete), do: "__complete__"
  defp serialize_transition_target(step) when is_atom(step), do: serialize_step(step)

  defp deserialize_transition_outcome(outcome) when is_atom(outcome), do: outcome
  defp deserialize_transition_outcome("ok"), do: :ok
  defp deserialize_transition_outcome("error"), do: :error
  defp deserialize_transition_outcome(outcome), do: outcome

  defp metadata_value(metadata, key) when is_map(metadata) and is_atom(key) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp deserialize_transition_target(_definition, target)
       when target in [:complete, "__complete__", "complete"],
       do: :complete

  defp deserialize_transition_target(definition, target), do: deserialize_step(definition, target)

  defp maybe_put_deserialized_condition(transition, nil), do: transition

  defp maybe_put_deserialized_condition(transition, condition) do
    case SquidMesh.Workflow.TransitionCondition.deserialize(condition) do
      nil -> transition
      condition -> Map.put(transition, :condition, condition)
    end
  end

  defp maybe_put_deserialized_recovery(transition, nil), do: transition

  defp maybe_put_deserialized_recovery(transition, recovery) when is_binary(recovery) do
    Map.put(transition, :recovery, String.to_existing_atom(recovery))
  rescue
    ArgumentError -> transition
  end

  defp maybe_put_deserialized_recovery(transition, recovery) when is_atom(recovery) do
    Map.put(transition, :recovery, recovery)
  end

  defp maybe_put_deserialized_recovery(transition, _recovery), do: transition

  defp invalid_payload_type(payload, field, acc) do
    case Map.fetch(payload, field.name) do
      {:ok, value} -> maybe_put_invalid_payload_type(acc, field, value)
      :error -> acc
    end
  end

  defp fingerprint_terms(definition) do
    %{
      steps:
        Enum.map(definition.steps, fn step ->
          %{
            name: step.name,
            module: inspect(step.module),
            input: canonical_input_mapping(Keyword.get(step.opts, :input)),
            output: Keyword.get(step.opts, :output),
            after: canonical_dependency_list(Keyword.get(step.opts, :after)),
            retry: Keyword.get(step.opts, :retry)
          }
        end),
      transitions:
        Enum.map(definition.transitions, &Map.take(&1, [:from, :on, :to, :condition, :recovery])),
      retries: definition.retries
    }
  end

  defp canonical_dependency_list(nil), do: []
  defp canonical_dependency_list(dependencies), do: canonical_atom_list(dependencies)

  defp canonical_input_mapping(nil), do: nil

  defp canonical_input_mapping(input_mapping) when is_list(input_mapping) do
    if Keyword.keyword?(input_mapping) do
      Enum.sort_by(input_mapping, fn {target, _path} -> Atom.to_string(target) end)
    else
      canonical_atom_list(input_mapping)
    end
  end

  defp canonical_atom_list(nil), do: nil

  defp canonical_atom_list(values) when is_list(values) do
    Enum.sort_by(values, &Atom.to_string/1)
  end

  defp maybe_put_invalid_payload_type(acc, field, value) do
    if input_matches_type?(value, field.type), do: acc, else: Map.put(acc, field.name, field.type)
  end

  defp put_payload_default(field, acc) do
    if Map.has_key?(acc, field.name), do: acc, else: fetch_payload_default(field, acc)
  end

  defp fetch_payload_default(field, acc) do
    case Keyword.fetch(field.opts, :default) do
      {:ok, default} -> Map.put(acc, field.name, resolve_default!(default))
      :error -> acc
    end
  end

  defp recovery_compensatable?(_policy, true), do: false

  defp recovery_compensatable?(policy, false),
    do: boolean_recovery_value(policy, :compensatable?, true)

  defp recovery_replay(policy, irreversible?, compensatable?) do
    case recovery_value(policy, :replay, nil) do
      value when value in [:manual_review_required, "manual_review_required"] ->
        :manual_review_required

      _other ->
        if irreversible? or not compensatable?, do: :manual_review_required, else: :allowed
    end
  end

  defp recovery_mode(policy, replay) do
    case recovery_value(policy, :recovery, nil) do
      value when value in [:manual_intervention, "manual_intervention"] ->
        :manual_intervention

      _other ->
        if replay == :manual_review_required, do: :manual_intervention, else: :automatic
    end
  end

  defp recovery_policy({:ok, step}), do: recovery_policy(step)

  defp recovery_policy(%{opts: opts}) do
    irreversible? = Keyword.get(opts, :irreversible, false)
    compensatable? = Keyword.get(opts, :compensatable, not irreversible?)

    if irreversible? or not compensatable? do
      %{
        irreversible?: irreversible?,
        compensatable?: false,
        replay: :manual_review_required,
        recovery: :manual_intervention
      }
    else
      default_policy = %{
        irreversible?: false,
        compensatable?: true,
        replay: :allowed,
        recovery: :automatic
      }

      maybe_put_compensation(default_policy, compensation_policy(Keyword.get(opts, :compensate)))
    end
  end

  defp compensation_policy(nil), do: nil

  defp compensation_policy(callback) when is_atom(callback) do
    %{callback: callback, status: :available}
  end

  defp normalize_compensation(nil), do: nil

  defp normalize_compensation(compensation) when is_map(compensation) do
    compensation
    |> Map.new(fn {key, value} -> {deserialize_compensation_key(key), value} end)
    |> Map.update(:status, :available, &deserialize_compensation_status/1)
  end

  defp normalize_compensation(_compensation), do: nil

  defp maybe_put_compensation(policy, nil), do: policy

  defp maybe_put_compensation(policy, compensation),
    do: Map.put(policy, :compensation, compensation)

  defp deserialize_compensation_key(key) when is_binary(key) do
    case key do
      "callback" -> :callback
      "status" -> :status
      "output" -> :output
      "error" -> :error
      "started_at" -> :started_at
      "completed_at" -> :completed_at
      "failed_at" -> :failed_at
      other -> other
    end
  end

  defp deserialize_compensation_key(key), do: key

  defp deserialize_compensation_status(status) when is_binary(status) do
    case status do
      "available" -> :available
      "running" -> :running
      "completed" -> :completed
      "failed" -> :failed
      other -> other
    end
  end

  defp deserialize_compensation_status(status), do: status

  defp unsafe_replay_step(definition, {completed_step, recovery}) when is_map(recovery) do
    step_name = deserialize_completed_step(definition, completed_step)
    policy = normalize_recovery_policy(recovery)

    if is_atom(step_name) and policy.replay == :manual_review_required do
      [Map.put(policy, :step, step_name)]
    else
      []
    end
  end

  defp unsafe_replay_step(definition, {completed_step, _recovery}) do
    unsafe_replay_step(definition, completed_step)
  end

  defp unsafe_replay_step(definition, completed_step) do
    step_name = deserialize_completed_step(definition, completed_step)

    case step_name do
      step when is_atom(step) ->
        case step_recovery_policy(definition, step) do
          {:ok, %{replay: :manual_review_required} = policy} ->
            [Map.put(policy, :step, step)]

          _other ->
            []
        end

      _unknown ->
        []
    end
  end

  defp deserialize_completed_step(_definition, step) when is_atom(step), do: step

  defp deserialize_completed_step(definition, step) when is_binary(step),
    do: deserialize_step(definition, step)

  defp normalize_failure_recovery(%{} = failure) do
    strategy =
      case recovery_value(failure, :strategy, nil) do
        strategy when strategy in [:compensation, :undo] -> strategy
        "compensation" -> :compensation
        "undo" -> :undo
        _other -> nil
      end

    target = recovery_value(failure, :target, nil)

    if strategy && target do
      %{strategy: strategy, target: target}
    else
      %{}
    end
  end

  defp normalize_failure_recovery(_failure), do: %{}

  defp recovery_value(policy, key, default) do
    Map.get(policy, key, Map.get(policy, Atom.to_string(key), default))
  end

  defp boolean_recovery_value(policy, key, default) do
    case recovery_value(policy, key, default) do
      true -> true
      false -> false
      "true" -> true
      "false" -> false
      _other -> default
    end
  end

  defp next_dependency_step(definition, completed_steps) do
    completed_steps =
      completed_steps
      |> Enum.map(&serialize_step/1)
      |> MapSet.new()

    pending_steps =
      definition.steps
      |> Enum.map(& &1.name)
      |> Enum.reject(fn step_name ->
        MapSet.member?(completed_steps, serialize_step(step_name))
      end)

    if pending_steps == [] do
      {:ok, :complete}
    else
      definition
      |> dependency_step_order()
      |> Enum.find(fn step_name ->
        step_name in pending_steps and
          dependencies_satisfied?(definition, step_name, completed_steps)
      end)
      |> case do
        nil -> {:error, {:no_runnable_step, pending_steps}}
        step_name -> {:ok, step_name}
      end
    end
  end

  defp dependencies_satisfied?(definition, step_name, completed_steps) do
    definition
    |> dependency_map()
    |> Map.get(step_name, [])
    |> Enum.all?(fn dependency ->
      MapSet.member?(completed_steps, serialize_step(dependency))
    end)
  end

  defp dependency_step_order(definition) do
    phases = dependency_phases(definition)

    declaration_order =
      definition.steps
      |> Enum.map(& &1.name)
      |> Enum.with_index()
      |> Map.new()

    definition.steps
    |> Enum.map(& &1.name)
    |> Enum.sort_by(fn step_name ->
      {Map.fetch!(phases, step_name), Map.fetch!(declaration_order, step_name)}
    end)
  end

  defp inspect_step_order(definition) do
    if dependency_mode?(definition) do
      dependency_step_order(definition)
    else
      Enum.map(definition.steps, & &1.name)
    end
  end

  defp dependency_phases(definition) do
    dependencies = dependency_map(definition)
    step_names = Enum.map(definition.steps, & &1.name)

    {phases, _visiting} =
      Enum.reduce(step_names, {%{}, MapSet.new()}, fn step_name, {phases, visiting} ->
        {phase, phases, visiting} = dependency_phase(step_name, dependencies, phases, visiting)
        {Map.put(phases, step_name, phase), visiting}
      end)

    phases
  end

  defp dependency_phase(step_name, dependencies, phases, visiting) do
    case Map.fetch(phases, step_name) do
      {:ok, phase} ->
        {phase, phases, visiting}

      :error ->
        if MapSet.member?(visiting, step_name) do
          raise ArgumentError, "workflow dependency graph must be acyclic"
        end

        visiting = MapSet.put(visiting, step_name)

        {dependency_phases, phases, visiting} =
          Enum.reduce(Map.get(dependencies, step_name, []), {[], phases, visiting}, fn dependency,
                                                                                       {acc,
                                                                                        phases,
                                                                                        visiting} ->
            {phase, phases, visiting} =
              dependency_phase(dependency, dependencies, phases, visiting)

            {[phase | acc], phases, visiting}
          end)

        phase =
          case dependency_phases do
            [] -> 0
            phases -> Enum.max(phases) + 1
          end

        {phase, Map.put(phases, step_name, phase), MapSet.delete(visiting, step_name)}
    end
  end

  defp dependency_map(definition) do
    Map.new(definition.steps, fn %{name: name, opts: opts} ->
      explicit_dependencies =
        opts
        |> Keyword.get(:after, [])
        |> List.wrap()

      {name, explicit_dependencies}
    end)
  end

  defp maybe_put(map, _key, []), do: map
  defp maybe_put(map, _key, %{} = value) when map_size(value) == 0, do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp input_matches_type?(value, :string), do: is_binary(value)
  defp input_matches_type?(value, :integer), do: is_integer(value)
  defp input_matches_type?(value, :float), do: is_float(value)
  defp input_matches_type?(value, :boolean), do: is_boolean(value)
  defp input_matches_type?(value, :map), do: is_map(value)
  defp input_matches_type?(value, :list), do: is_list(value)
  defp input_matches_type?(value, :atom), do: is_atom(value)
  defp input_matches_type?(_value, _unknown_type), do: true

  defp serialize_dependency_status(status) when is_atom(status), do: Atom.to_string(status)
  defp serialize_dependency_status(status) when is_binary(status), do: status
  defp serialize_inspect_step_status(status) when is_atom(status), do: Atom.to_string(status)
  defp serialize_inspect_step_status(status) when is_binary(status), do: status

  defp deserialize_inspect_step_status("pending"), do: :pending
  defp deserialize_inspect_step_status("running"), do: :running
  defp deserialize_inspect_step_status("completed"), do: :completed
  defp deserialize_inspect_step_status("failed"), do: :failed
  defp deserialize_inspect_step_status("waiting"), do: :waiting

  defp deserialize_workflow_name(workflow_name) do
    {:ok, String.to_existing_atom(workflow_name)}
  rescue
    ArgumentError -> {:error, {:invalid_workflow, workflow_name}}
  end

  defp resolve_default!({:today, :iso8601}), do: Date.to_iso8601(Date.utc_today())
  defp resolve_default!({:now, :iso8601}), do: DateTime.to_iso8601(DateTime.utc_now())
  defp resolve_default!(default), do: default
end
