defmodule SquidMesh.Workflow.EditorSpec do
  @moduledoc """
  JSON-safe workflow spec projection for visual editors.

  This module keeps editor round-trips on the data side of the boundary. It does
  not load workflow modules, create atoms from input, resolve editor input into
  modules, or start runs. Runtime execution of validated specs remains a
  separate boundary.
  """

  alias SquidMesh.Workflow.ActionRegistry
  alias SquidMesh.Workflow.Spec

  @editor_fields [
    "workflow",
    "definition_version",
    "triggers",
    "payload",
    "steps",
    "transitions",
    "retries",
    "entry_steps",
    "initial_step",
    "entry_step"
  ]

  @collection_fields ["triggers", "payload", "steps", "transitions", "retries", "entry_steps"]
  @runtime_owned_fields [
    "run_id",
    "status",
    "terminal_status",
    "current_node_id",
    "current_node_ids",
    "definition_fingerprint",
    "fingerprint",
    "spec_fingerprint",
    "journal",
    "audit_history",
    "attempts",
    "dispatches",
    "history"
  ]
  @terminal_targets ["complete"]
  @transition_outcomes ["ok", "error"]

  @type editor_map :: %{String.t() => term()}
  @type validation_error :: %{
          path: [atom() | non_neg_integer()],
          code: atom(),
          message: String.t(),
          details: map()
        }
  @type validation_opts :: [action_registry: ActionRegistry.registry()]

  @doc """
  Converts a normalized workflow spec into a JSON-safe editor map.

  The projection keeps only editor-owned fields and serializes atoms, module
  atoms, keyword lists, nested maps, and lists into JSON-compatible values.
  """
  @spec to_map(Spec.t() | map()) :: editor_map()
  def to_map(%Spec{} = spec) do
    spec
    |> Map.from_struct()
    |> to_map()
  end

  def to_map(spec) when is_map(spec) do
    spec
    |> Map.new(fn {key, value} -> {string_key(key), json_value(value)} end)
    |> Map.take(@editor_fields)
  end

  @doc """
  Validates an editor spec map without starting a run.

  Without `:action_registry`, validation stays structural and does not load
  workflow modules. When `:action_registry` is supplied, editor-owned top-level
  action keys are checked against the host allowlist before the draft can be
  previewed or accepted.
  """
  @spec validate_map(term()) ::
          :ok | {:error, {:invalid_workflow_editor_spec, [validation_error()]}}
  def validate_map(value), do: validate_map(value, [])

  @spec validate_map(term(), validation_opts()) ::
          :ok | {:error, {:invalid_workflow_editor_spec, [validation_error()]}}
  def validate_map(map, opts) when is_map(map) and is_list(opts) do
    map = stringify_map(map)

    errors =
      []
      |> validate_runtime_owned_fields(map)
      |> validate_collections(map)
      |> validate_steps(map)
      |> validate_step_actions(map, opts)
      |> validate_transitions(map)
      |> validate_entry_metadata(map)
      |> Enum.reverse()

    case errors do
      [] -> :ok
      errors -> {:error, {:invalid_workflow_editor_spec, errors}}
    end
  end

  def validate_map(value, opts) when is_list(opts) do
    {:error,
     {:invalid_workflow_editor_spec,
      [
        error([], :invalid_editor_spec, "workflow editor spec must be a map", %{spec: value})
      ]}}
  end

  @doc """
  Builds a draft graph preview from a JSON-safe editor spec map.
  """
  @spec preview_graph(Spec.t() | map()) ::
          {:ok, editor_map()} | {:error, {:invalid_workflow_editor_spec, [validation_error()]}}
  def preview_graph(spec), do: preview_graph(spec, [])

  @spec preview_graph(Spec.t() | map(), validation_opts()) ::
          {:ok, editor_map()} | {:error, {:invalid_workflow_editor_spec, [validation_error()]}}
  def preview_graph(%Spec{} = spec, opts) when is_list(opts) do
    spec
    |> to_map()
    |> preview_graph(opts)
  end

  def preview_graph(map, opts) when is_map(map) and is_list(opts) do
    map = stringify_map(map)

    with :ok <- validate_map(map, opts) do
      {:ok,
       %{
         "source" => "workflow_spec",
         "status" => "draft",
         "workflow" => Map.get(map, "workflow"),
         "definition_version" => Map.get(map, "definition_version"),
         "current_node_id" => nil,
         "current_node_ids" => [],
         "terminal?" => false,
         "nodes" => preview_nodes(map),
         "edges" => preview_edges(map)
       }}
    end
  end

  def preview_graph(value, opts) when is_list(opts), do: validate_map(value, opts)

  defp validate_runtime_owned_fields(errors, map) do
    Enum.reduce(@runtime_owned_fields, errors, fn field, acc ->
      if Map.has_key?(map, field) do
        [
          error(
            [path_atom(field)],
            :runtime_owned_field,
            "#{field} is runtime-owned and cannot be edited",
            %{field: field}
          )
          | acc
        ]
      else
        acc
      end
    end)
  end

  defp validate_collections(errors, map) do
    Enum.reduce(@collection_fields, errors, fn field, acc ->
      if is_list(Map.get(map, field)) do
        acc
      else
        [
          error(
            [path_atom(field)],
            :invalid_collection,
            "#{field} must be a list",
            %{field: field, value: Map.get(map, field)}
          )
          | acc
        ]
      end
    end)
  end

  defp validate_steps(errors, map) do
    map
    |> list_field("steps")
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {step, index}, acc ->
      name = field(step, "name")

      if is_binary(name) and name != "" do
        acc
      else
        [
          error(
            [:steps, index, :name],
            :invalid_step_name,
            "step name must be a non-empty string",
            %{step: name}
          )
          | acc
        ]
      end
    end)
  end

  defp validate_step_actions(errors, map, opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} ->
        registry = editor_action_registry(registry)

        map
        |> list_field("steps")
        |> Enum.with_index()
        |> Enum.reduce(errors, fn {step, index}, acc ->
          validate_step_action(acc, step, index, registry)
        end)

      :error ->
        errors
    end
  end

  defp validate_step_action(errors, step, index, registry) when is_map(step) do
    case step_action(step) do
      {:ok, action} ->
        case ActionRegistry.validate_action(action, registry) do
          :ok ->
            errors

          {:error, reason} ->
            [
              error(
                [:steps, index, :action],
                reason,
                action_error_message(step, reason),
                %{step: field(step, "name"), action: action}
              )
              | errors
            ]
        end

      :missing ->
        errors
    end
  end

  defp validate_step_action(errors, _step, _index, _registry), do: errors

  defp step_action(step) when is_map(step) do
    if Map.has_key?(step, "action") do
      {:ok, field(step, "action")}
    else
      :missing
    end
  end

  defp editor_action_registry(registry) when is_map(registry) do
    Enum.reduce(registry, %{}, fn {key, entry}, acc ->
      acc
      |> Map.put(key, entry)
      |> Map.put(json_value(key), entry)
    end)
  end

  defp editor_action_registry(registry) when is_list(registry) do
    if Keyword.keyword?(registry) do
      Enum.reduce(registry, %{}, fn {key, entry}, acc ->
        acc
        |> Map.put(key, entry)
        |> Map.put(json_value(key), entry)
      end)
    else
      registry
    end
  end

  defp editor_action_registry(registry), do: registry

  defp action_error_message(step, :invalid_action_key) do
    "step #{inspect_name(field(step, "name"))} must reference an atom or non-empty string action key"
  end

  defp action_error_message(step, :unknown_action_key) do
    "step #{inspect_name(field(step, "name"))} references unknown action key"
  end

  defp action_error_message(step, :disabled_action_key) do
    "step #{inspect_name(field(step, "name"))} references disabled action key"
  end

  defp action_error_message(step, :incompatible_action_module) do
    "step #{inspect_name(field(step, "name"))} references an incompatible action module"
  end

  defp action_error_message(step, :missing_action_key) do
    "step #{inspect_name(field(step, "name"))} must reference an action key"
  end

  defp validate_transitions(errors, map) do
    step_names = step_names(map)

    map
    |> list_field("transitions")
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {transition, index}, acc ->
      acc
      |> validate_transition_endpoint(transition, index, "from", step_names)
      |> validate_transition_endpoint(transition, index, "to", step_names)
      |> validate_transition_outcome(transition, index)
    end)
  end

  defp validate_transition_endpoint(errors, transition, index, key, step_names) do
    value = field(transition, key)

    valid? =
      if key == "to" do
        value in step_names or value in @terminal_targets
      else
        value in step_names
      end

    if valid? do
      errors
    else
      code = if key == "to", do: :unknown_transition_target, else: :unknown_transition_source
      noun = if key == "to", do: "targets", else: "starts from"

      [
        error(
          [:transitions, index, path_atom(key)],
          code,
          "transition #{noun} unknown step: #{inspect_name(value)}",
          %{path_atom(key) => value}
        )
        | errors
      ]
    end
  end

  defp validate_transition_outcome(errors, transition, index) do
    outcome = field(transition, "on")

    if outcome in @transition_outcomes do
      errors
    else
      [
        error(
          [:transitions, index, :on],
          :invalid_transition_outcome,
          "transition outcome must be ok or error",
          %{on: outcome}
        )
        | errors
      ]
    end
  end

  defp validate_entry_metadata(errors, map) do
    step_names = step_names(map)

    errors
    |> validate_entry_steps(map, step_names)
    |> validate_step_reference(map, "initial_step", step_names)
    |> validate_step_reference(map, "entry_step", step_names)
  end

  defp validate_entry_steps(errors, map, step_names) do
    map
    |> list_field("entry_steps")
    |> Enum.with_index()
    |> Enum.reduce(errors, fn {entry_step, index}, acc ->
      if entry_step in step_names do
        acc
      else
        [
          error(
            [:entry_steps, index],
            :unknown_entry_step,
            "entry step is unknown: #{inspect_name(entry_step)}",
            %{entry_step: entry_step}
          )
          | acc
        ]
      end
    end)
  end

  defp validate_step_reference(errors, map, field, step_names) do
    value = field(map, field)

    cond do
      is_nil(value) ->
        errors

      value in step_names ->
        errors

      true ->
        [
          error(
            [path_atom(field)],
            :unknown_step_reference,
            "#{field} references unknown step: #{inspect_name(value)}",
            %{path_atom(field) => value}
          )
          | errors
        ]
    end
  end

  defp preview_nodes(map) do
    map
    |> list_field("steps")
    |> Enum.map(&preview_node/1)
  end

  defp preview_node(step) do
    compact(%{
      "id" => field(step, "name"),
      "action" => field(step, "action") || nested_field(step, ["metadata", "action"]),
      "status" => "draft",
      "current?" => false,
      "input" => nil,
      "output" => nil,
      "error" => nil,
      "recovery" => nil,
      "transition" => nil,
      "manual_state" => nil,
      "attempts" => []
    })
  end

  defp preview_edges(map) do
    transitions = list_field(map, "transitions")

    if transitions == [] do
      dependency_edges(map)
    else
      transitions
      |> Enum.with_index()
      |> Enum.map(fn {transition, index} -> transition_edge(transition, index) end)
    end
  end

  defp transition_edge(transition, index) do
    from = field(transition, "from")
    outcome = field(transition, "on")
    to = field(transition, "to")
    condition = field(transition, "condition")

    put_conditional_edge_id(
      %{
        "id" => Enum.join([from, outcome, to], ":"),
        "from" => from,
        "to" => to,
        "type" => "transition",
        "status" => "pending",
        "selected?" => false,
        "skipped?" => false,
        "pending?" => true,
        "blocked?" => false,
        "outcome" => outcome,
        "condition" => condition,
        "recovery" => field(transition, "recovery")
      },
      condition,
      index
    )
  end

  defp dependency_edges(map) do
    map
    |> list_field("steps")
    |> Enum.flat_map(&dependency_edges_for_step/1)
  end

  defp dependency_edges_for_step(step) do
    case nested_field(step, ["opts", "after"]) do
      dependencies when is_list(dependencies) ->
        Enum.map(dependencies, &dependency_edge(&1, step))

      _other ->
        []
    end
  end

  defp dependency_edge(dependency, step) do
    %{
      "id" => Enum.join([dependency, "dependency", field(step, "name")], ":"),
      "from" => dependency,
      "to" => field(step, "name"),
      "type" => "dependency",
      "status" => "pending",
      "selected?" => false,
      "skipped?" => false,
      "pending?" => true,
      "blocked?" => false,
      "outcome" => nil,
      "condition" => nil,
      "recovery" => nil
    }
  end

  defp put_conditional_edge_id(edge, nil, _index), do: edge

  defp put_conditional_edge_id(edge, _condition, index) do
    %{edge | "id" => edge["id"] <> ":condition:" <> Integer.to_string(index)}
  end

  defp json_value(nil), do: nil
  defp json_value(value) when is_boolean(value), do: value
  defp json_value(value) when is_atom(value), do: Atom.to_string(value)

  defp json_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> json_value()
  end

  defp json_value([]), do: []

  defp json_value(value) when is_list(value) do
    if Keyword.keyword?(value) do
      Map.new(value, fn {key, item} -> {string_key(key), json_value(item)} end)
    else
      Enum.map(value, &json_value/1)
    end
  end

  defp json_value(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {string_key(key), json_value(item)} end)
  end

  defp json_value(value), do: value

  defp stringify_map(map) when is_map(map) do
    Map.new(map, fn {key, value} -> {string_key(key), json_value(value)} end)
  end

  defp string_key(key) when is_atom(key), do: Atom.to_string(key)
  defp string_key(key) when is_binary(key), do: key
  defp string_key(key), do: inspect(key)

  defp path_atom("definition_fingerprint"), do: :definition_fingerprint
  defp path_atom("current_node_id"), do: :current_node_id
  defp path_atom("current_node_ids"), do: :current_node_ids
  defp path_atom("terminal_status"), do: :terminal_status
  defp path_atom("spec_fingerprint"), do: :spec_fingerprint
  defp path_atom("audit_history"), do: :audit_history
  defp path_atom("entry_steps"), do: :entry_steps
  defp path_atom("initial_step"), do: :initial_step
  defp path_atom("entry_step"), do: :entry_step
  defp path_atom("run_id"), do: :run_id
  defp path_atom("status"), do: :status
  defp path_atom("fingerprint"), do: :fingerprint
  defp path_atom("journal"), do: :journal
  defp path_atom("attempts"), do: :attempts
  defp path_atom("dispatches"), do: :dispatches
  defp path_atom("history"), do: :history
  defp path_atom("triggers"), do: :triggers
  defp path_atom("payload"), do: :payload
  defp path_atom("steps"), do: :steps
  defp path_atom("transitions"), do: :transitions
  defp path_atom("retries"), do: :retries
  defp path_atom("from"), do: :from
  defp path_atom("to"), do: :to

  defp list_field(map, field) do
    case Map.get(map, field) do
      values when is_list(values) -> values
      _other -> []
    end
  end

  defp step_names(map) do
    map
    |> list_field("steps")
    |> Enum.map(&field(&1, "name"))
    |> Enum.filter(&(is_binary(&1) and &1 != ""))
  end

  defp field(map, key) when is_map(map), do: Map.get(map, key)
  defp field(_value, _key), do: nil

  defp nested_field(value, []), do: value

  defp nested_field(value, [key | keys]) do
    value
    |> field(key)
    |> nested_field(keys)
  end

  defp inspect_name(name) when is_binary(name), do: name
  defp inspect_name(name), do: inspect(name)

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end

  defp error(path, code, message, details) do
    %{path: path, code: code, message: message, details: details}
  end
end
