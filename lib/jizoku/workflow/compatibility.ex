defmodule Jizoku.Workflow.Compatibility do
  @moduledoc "Compares normalized definitions conservatively without weakening execution fences."

  alias Jizoku.Workflow.Compatibility.Difference
  alias Jizoku.Workflow.Compatibility.Result
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.Spec

  defmodule Canonical do
    @moduledoc false

    @type t :: %__MODULE__{}

    defstruct [
      :definition_version,
      :initial_step,
      :entry_step,
      triggers: [],
      payload: [],
      steps: [],
      transitions: [],
      retries: [],
      entry_steps: []
    ]
  end

  @category_rank %{compatible: 0, migration_required: 1, incompatible: 2}
  @added_kinds %{
    payload: :payload_field_added,
    steps: :step_added,
    triggers: :trigger_added,
    retries: :retry_added
  }
  @removed_kinds %{
    payload: :payload_field_removed,
    steps: :step_removed,
    triggers: :trigger_removed,
    retries: :retry_removed
  }
  @changed_kinds %{
    {:steps, :action} => :action_changed,
    {:steps, :input} => :step_input_changed,
    {:steps, :output} => :step_output_changed,
    {:steps, :after} => :step_dependencies_changed,
    {:steps, :retry} => :step_retry_changed,
    {:steps, :deadline} => :step_deadline_changed,
    {:steps, :recovery} => :step_recovery_changed
  }
  @known_step_options [
    :input,
    :output,
    :after,
    :retry,
    :deadline,
    :compensate,
    :compensatable,
    :irreversible,
    :transaction
  ]

  @type input :: module() | Spec.t() | map()

  @doc """
  Classifies the structural differences between two workflow definitions.
  """
  @spec compare(input(), input()) ::
          {:ok, Result.t()}
          | {:error, Definition.load_error() | {:invalid_workflow_spec, [map()]}}
  def compare(old, new) do
    with {:ok, old} <- normalize(old),
         {:ok, new} <- normalize(new) do
      differences = differences(old, new)

      {:ok,
       %Result{
         category: overall_category(differences),
         differences: differences
       }}
    end
  end

  defp normalize(workflow) when is_atom(workflow) do
    with {:ok, spec} <- Jizoku.Workflow.to_spec(workflow) do
      normalize(spec)
    end
  end

  defp normalize(%Spec{} = spec) do
    normalize(Map.from_struct(spec))
  end

  defp normalize(spec) when is_map(spec) do
    case Spec.validate(spec) do
      :ok -> {:ok, canonical_spec(spec)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize(spec) do
    Spec.validate(spec)
  end

  defp differences(old, new) do
    []
    |> compare_value(
      [:definition_version],
      :definition_version_changed,
      :compatible,
      old.definition_version,
      new.definition_version
    )
    |> compare_named_collection(:payload, old.payload, new.payload)
    |> compare_named_collection(:steps, old.steps, new.steps)
    |> compare_named_collection(:triggers, old.triggers, new.triggers)
    |> compare_indexed_collection(:transitions, old.transitions, new.transitions)
    |> compare_named_collection(:retries, old.retries, new.retries)
    |> compare_value(
      [:entry_steps],
      :entry_steps_changed,
      :migration_required,
      old.entry_steps,
      new.entry_steps
    )
    |> compare_value(
      [:initial_step],
      :initial_step_changed,
      :migration_required,
      old.initial_step,
      new.initial_step
    )
    |> compare_value(
      [:entry_step],
      :entry_step_changed,
      :migration_required,
      old.entry_step,
      new.entry_step
    )
    |> Enum.sort_by(&path_sort_key/1)
  end

  defp compare_named_collection(differences, collection, old_items, new_items) do
    old_by_name = Map.new(old_items, &{item_name(&1), &1})
    new_by_name = Map.new(new_items, &{item_name(&1), &1})

    names =
      old_by_name
      |> Map.keys()
      |> Kernel.++(Map.keys(new_by_name))
      |> Enum.uniq()
      |> Enum.sort()

    Enum.reduce(names, differences, fn name, acc ->
      compare_named_item(
        acc,
        collection,
        name,
        Map.get(old_by_name, name),
        Map.get(new_by_name, name)
      )
    end)
  end

  defp compare_named_item(differences, collection, name, nil, new) do
    [
      difference(
        added_category(collection, new),
        added_kind(collection),
        [collection, name],
        nil,
        new
      )
      | differences
    ]
  end

  defp compare_named_item(differences, collection, name, old, nil) do
    [
      difference(:incompatible, removed_kind(collection), [collection, name], old, nil)
      | differences
    ]
  end

  defp compare_named_item(differences, collection, name, old, new) do
    fields = comparison_fields(collection, old, new)

    fields
    |> Enum.reduce(differences, fn field, acc ->
      compare_value(
        acc,
        [collection, name, field],
        changed_kind(collection, field),
        :migration_required,
        Map.get(old, field),
        Map.get(new, field)
      )
    end)
    |> compare_nested_fields(collection, name, old, new)
  end

  defp compare_nested_fields(differences, :triggers, trigger_name, old, new) do
    compare_nested_payload(
      differences,
      [:triggers, trigger_name, :payload],
      old.payload,
      new.payload
    )
  end

  defp compare_nested_fields(differences, :steps, step_name, old, new) do
    compare_step_options(differences, step_name, old.options, new.options)
  end

  defp compare_nested_fields(differences, _collection, _name, _old, _new) do
    differences
  end

  defp compare_nested_payload(differences, path, old_items, new_items) do
    old_by_name = Map.new(old_items, &{item_name(&1), &1})
    new_by_name = Map.new(new_items, &{item_name(&1), &1})

    old_by_name
    |> Map.keys()
    |> Kernel.++(Map.keys(new_by_name))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce(differences, fn name, acc ->
      compare_nested_payload_item(
        acc,
        append_path(path, name),
        Map.get(old_by_name, name),
        Map.get(new_by_name, name)
      )
    end)
  end

  defp compare_nested_payload_item(differences, path, nil, new) do
    [
      difference(
        added_category(:payload, new),
        :trigger_payload_field_added,
        path,
        nil,
        new
      )
      | differences
    ]
  end

  defp compare_nested_payload_item(differences, path, old, nil) do
    [difference(:incompatible, :trigger_payload_field_removed, path, old, nil) | differences]
  end

  defp compare_nested_payload_item(differences, path, old, new) do
    Enum.reduce([:type, :opts], differences, fn field, acc ->
      compare_value(
        acc,
        append_path(path, field),
        :trigger_payload_field_changed,
        :migration_required,
        Map.get(old, field),
        Map.get(new, field)
      )
    end)
  end

  defp compare_step_options(differences, step_name, old_options, new_options) do
    old_options
    |> Map.keys()
    |> Kernel.++(Map.keys(new_options))
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.reduce(differences, fn option_name, acc ->
      compare_value(
        acc,
        [:steps, step_name, :options, option_name],
        :step_option_changed,
        :migration_required,
        Map.get(old_options, option_name),
        Map.get(new_options, option_name)
      )
    end)
  end

  defp compare_indexed_collection(differences, collection, old_items, new_items) do
    count = max(length(old_items), length(new_items))
    old_items = List.to_tuple(old_items)
    new_items = List.to_tuple(new_items)

    Enum.reduce(0..max(count - 1, 0), differences, fn index, acc ->
      compare_indexed_item(
        acc,
        collection,
        index,
        tuple_item(old_items, index),
        tuple_item(new_items, index)
      )
    end)
  end

  defp compare_indexed_item(differences, _collection, _index, nil, nil) do
    differences
  end

  defp compare_indexed_item(differences, collection, index, nil, new) do
    [
      difference(:migration_required, :transition_added, [collection, index], nil, new)
      | differences
    ]
  end

  defp compare_indexed_item(differences, collection, index, old, nil) do
    [difference(:incompatible, :transition_removed, [collection, index], old, nil) | differences]
  end

  defp compare_indexed_item(differences, collection, index, old, new) do
    Enum.reduce([:from, :on, :to, :condition, :recovery], differences, fn field, acc ->
      compare_value(
        acc,
        [collection, index, field],
        :transition_changed,
        :migration_required,
        Map.get(old, field),
        Map.get(new, field)
      )
    end)
  end

  defp compare_value(differences, _path, _kind, _category, value, value) do
    differences
  end

  defp compare_value(differences, path, kind, category, old, new) do
    [difference(category, kind, path, old, new) | differences]
  end

  defp comparison_fields(:payload, _old, _new) do
    [:type, :opts]
  end

  defp comparison_fields(:triggers, _old, _new) do
    [:type, :config]
  end

  defp comparison_fields(:retries, _old, _new) do
    [:opts]
  end

  defp comparison_fields(:steps, old, new) do
    Enum.filter(
      [:action, :input, :output, :after, :retry, :deadline, :recovery],
      &(Map.has_key?(old, &1) or Map.has_key?(new, &1))
    )
  end

  defp added_category(:payload, %{opts: opts}) do
    if option(opts, :required, true), do: :migration_required, else: :compatible
  end

  defp added_category(:steps, _item) do
    :migration_required
  end

  defp added_category(:retries, _item) do
    :migration_required
  end

  defp added_category(_collection, _item) do
    :compatible
  end

  defp added_kind(collection) do
    Map.fetch!(@added_kinds, collection)
  end

  defp removed_kind(collection) do
    Map.fetch!(@removed_kinds, collection)
  end

  defp changed_kind(:steps, field) do
    Map.fetch!(@changed_kinds, {:steps, field})
  end

  defp changed_kind(:payload, _field) do
    :payload_field_changed
  end

  defp changed_kind(:triggers, _field) do
    :trigger_changed
  end

  defp changed_kind(:retries, _field) do
    :retry_changed
  end

  defp difference(category, kind, path, old, new) do
    %Difference{
      category: category,
      kind: kind,
      path: path,
      old: canonical_value(old),
      new: canonical_value(new)
    }
  end

  defp overall_category([]) do
    :compatible
  end

  defp overall_category(differences) do
    differences
    |> Enum.max_by(&Map.fetch!(@category_rank, &1.category))
    |> Map.fetch!(:category)
  end

  defp canonical_spec(spec) do
    %Canonical{
      definition_version: field(spec, :definition_version),
      triggers: canonical_named(spec, :triggers, &canonical_trigger/1),
      payload: canonical_named(spec, :payload, &canonical_payload/1),
      steps: canonical_named(spec, :steps, &canonical_step/1),
      transitions: canonical_transitions(spec),
      retries: canonical_named(spec, :retries, &canonical_retry/1),
      entry_steps: canonical_names(field(spec, :entry_steps, [])),
      initial_step: name(field(spec, :initial_step)),
      entry_step: name(field(spec, :entry_step))
    }
  end

  defp canonical_named(spec, key, mapper) do
    spec
    |> field(key, [])
    |> Enum.map(mapper)
    |> Enum.sort_by(&item_name/1)
  end

  defp canonical_list(spec, key, mapper) do
    spec
    |> field(key, [])
    |> Enum.map(mapper)
  end

  defp canonical_transitions(spec) do
    spec
    |> canonical_list(:transitions, &canonical_transition/1)
    |> Enum.sort_by(fn transition ->
      {transition.from, transition.on, transition.to}
    end)
  end

  defp canonical_trigger(trigger) do
    %{
      name: name(field(trigger, :name)),
      type: name(field(trigger, :type)),
      config: canonical_value(field(trigger, :config, %{})),
      payload:
        trigger
        |> field(:payload, [])
        |> Enum.map(&canonical_payload/1)
        |> Enum.sort_by(&item_name/1)
    }
  end

  defp canonical_payload(payload) do
    %{
      name: name(field(payload, :name)),
      type: name(field(payload, :type)),
      opts: canonical_options(field(payload, :opts, []))
    }
  end

  defp canonical_step(step) do
    opts = field(step, :opts, [])

    %{
      name: name(field(step, :name)),
      action: action_identity(step),
      input: canonical_value(option(opts, :input)),
      output: name(option(opts, :output)),
      after: canonical_names(option(opts, :after, [])),
      retry: canonical_value(option(opts, :retry)),
      deadline: canonical_value(option(opts, :deadline)),
      recovery: canonical_value(recovery_identity(opts)),
      options: additional_step_options(opts)
    }
  end

  defp canonical_transition(transition) do
    %{
      from: name(field(transition, :from)),
      on: name(field(transition, :on)),
      to: name(field(transition, :to)),
      condition: canonical_value(field(transition, :condition)),
      recovery: name(field(transition, :recovery))
    }
  end

  defp canonical_retry(retry) do
    %{name: name(field(retry, :step)), opts: canonical_options(field(retry, :opts, []))}
  end

  defp action_identity(step) do
    case field(step, :action) do
      nil -> inspect(field(step, :module))
      action -> name(action)
    end
  end

  defp recovery_identity(opts) do
    %{
      compensate: inspect(option(opts, :compensate)),
      compensatable: option(opts, :compensatable),
      irreversible: option(opts, :irreversible),
      transaction: name(option(opts, :transaction))
    }
  end

  defp canonical_options(opts) when is_list(opts) do
    opts
    |> Enum.map(fn {key, value} -> {name(key), canonical_value(value)} end)
    |> Enum.sort()
  end

  defp canonical_options(opts) do
    canonical_value(opts)
  end

  defp additional_step_options(opts) when is_list(opts) do
    opts
    |> Keyword.drop(@known_step_options)
    |> canonical_options()
    |> Map.new()
  end

  defp additional_step_options(_opts) do
    %{}
  end

  defp canonical_names(nil) do
    []
  end

  defp canonical_names(names) when is_list(names) do
    names
    |> Enum.map(&name/1)
    |> Enum.sort()
  end

  defp canonical_value(value) when is_boolean(value) or is_nil(value) do
    value
  end

  defp canonical_value(value) when is_atom(value) do
    name(value)
  end

  defp canonical_value(value) when is_list(value) do
    if Keyword.keyword?(value),
      do: canonical_options(value),
      else: Enum.map(value, &canonical_value/1)
  end

  defp canonical_value(%module{} = value) do
    fields =
      value
      |> Map.from_struct()
      |> canonical_value()

    %{
      "__struct__" => Atom.to_string(module),
      "fields" => fields
    }
  end

  defp canonical_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, item} -> {name(key), canonical_value(item)} end)
    |> Enum.sort()
    |> Map.new()
  end

  defp canonical_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&canonical_value/1)
  end

  defp canonical_value(value) do
    value
  end

  defp field(map, key, default \\ nil) do
    Jizoku.MapField.get(map, key, default)
  end

  defp option(options, key, default \\ nil)

  defp option(options, key, default) when is_list(options) do
    key_name = Atom.to_string(key)

    result =
      Enum.find_value(options, default, fn
        {option_key, value} ->
          if option_key == key or option_key == key_name, do: {:found, value}, else: false

        _option ->
          false
      end)

    case result do
      {:found, value} -> value
      value -> value
    end
  end

  defp option(options, key, default) when is_map(options) do
    field(options, key, default)
  end

  defp option(_options, _key, default) do
    default
  end

  defp item_name(item) do
    Map.fetch!(item, :name)
  end

  defp path_sort_key(%Difference{path: path}) do
    Enum.map_join(path, ".", &to_string/1)
  end

  defp append_path(path, item) do
    List.insert_at(path, -1, item)
  end

  defp tuple_item(items, index) when index < tuple_size(items) do
    elem(items, index)
  end

  defp tuple_item(_items, _index) do
    nil
  end

  defp name(nil) do
    nil
  end

  defp name(value) when is_atom(value) do
    Atom.to_string(value)
  end

  defp name(value) when is_binary(value) do
    value
  end

  defp name(value) do
    case String.Chars.impl_for(value) do
      nil -> inspect(value)
      _implementation -> to_string(value)
    end
  end
end
