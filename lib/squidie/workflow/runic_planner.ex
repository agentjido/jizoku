defmodule Squidie.Workflow.RunicPlanner do
  @moduledoc false

  alias Runic.Workflow
  alias Runic.Workflow.Events.ActivationConsumed
  alias Runic.Workflow.Events.FactProduced
  alias Runic.Workflow.Fact
  alias Runic.Workflow.Invokable
  alias Squidie.Runtime.StepInput
  alias Squidie.Workflow.Info
  alias Squidie.Workflow.RunicPlanner.Runnable
  alias Squidie.Workflow.Spec

  @max_phash 4_294_967_296
  @no_input {:squidie, :no_planner_input}

  @type t :: %__MODULE__{
          spec: Spec.t(),
          runic_workflow: Workflow.t()
        }

  defstruct [:spec, :runic_workflow]

  @doc """
  Builds a planner from a compiled Squidie workflow module.
  """
  @spec new(module() | Spec.t()) :: {:ok, t()} | {:error, term()}
  def new(workflow) when is_atom(workflow) do
    with {:ok, spec} <- Info.fetch_spec(workflow) do
      new(spec)
    end
  end

  def new(%Spec{} = spec) do
    {:ok, %__MODULE__{spec: spec, runic_workflow: build_runic_workflow(spec)}}
  end

  @doc """
  Plans from existing planner facts and returns externally executable steps.
  """
  @spec plan(t()) :: {:ok, t(), [Runnable.t()]} | {:error, term()}
  def plan(%__MODULE__{} = planner), do: plan(planner, @no_input)

  @doc """
  Adds a new durable input fact, plans through Runic, and returns runnable steps.
  """
  @spec plan(t(), term()) :: {:ok, t(), [Runnable.t()]} | {:error, term()}
  def plan(%__MODULE__{} = planner, input) do
    workflow =
      case input do
        @no_input -> Workflow.plan_eagerly(planner.runic_workflow)
        value -> Workflow.plan_eagerly(planner.runic_workflow, value)
      end

    prepare_external_runnables(%{planner | runic_workflow: workflow})
  end

  @doc """
  Applies the result of an externally executed Squidie runnable.
  """
  @spec apply_result(t(), Runnable.t(), {:ok, term()} | {:error, term()}) ::
          {:ok, t()}
  def apply_result(
        %__MODULE__{} = planner,
        %Runnable{runic_runnable: %Runic.Workflow.Runnable{} = runic_runnable},
        {:ok, result}
      ) do
    completed = complete_runic_runnable(planner.spec, runic_runnable, :ok, result)
    {:ok, %{planner | runic_workflow: Workflow.apply_runnable(planner.runic_workflow, completed)}}
  end

  def apply_result(
        %__MODULE__{} = planner,
        %Runnable{runic_runnable: %Runic.Workflow.Runnable{} = runic_runnable},
        {:error, reason}
      ) do
    completed = complete_runic_runnable(planner.spec, runic_runnable, :error, reason)
    {:ok, %{planner | runic_workflow: Workflow.apply_runnable(planner.runic_workflow, completed)}}
  end

  @doc false
  @spec external_step_result(term()) :: term()
  def external_step_result(input), do: input

  defp build_runic_workflow(%Spec{} = spec) do
    parent_map = parent_map(spec)
    step_map = Map.new(spec.steps, &{&1.name, &1})

    spec
    |> ordered_steps(parent_map, step_map)
    |> Enum.reduce(Workflow.new(spec.workflow), fn step, workflow ->
      add_runic_step(workflow, spec, step, Map.get(parent_map, step.name, []))
    end)
  end

  defp add_runic_step(workflow, spec, step, []) do
    Workflow.add(workflow, runic_step(spec, step), validate: :off)
  end

  defp add_runic_step(workflow, spec, step, [parent]) do
    Workflow.add(workflow, runic_step(spec, step), to: parent, validate: :off)
  end

  defp add_runic_step(workflow, spec, step, parents) when is_list(parents) do
    Workflow.add(workflow, runic_step(spec, step), to: parents, validate: :off)
  end

  defp runic_step(spec, step) do
    Runic.Workflow.Step.new(
      work: &__MODULE__.external_step_result/1,
      name: step.name,
      hash: stable_step_hash(spec, step),
      inputs: nil,
      outputs: nil
    )
  end

  defp stable_step_hash(spec, step) do
    :erlang.phash2({:squidie_step, spec.workflow, step.name, step.module}, @max_phash)
  end

  defp parent_map(%Spec{} = spec) do
    step_names =
      spec.steps
      |> Enum.map(& &1.name)
      |> MapSet.new()

    transition_parents =
      Enum.reduce(spec.transitions, %{}, fn
        %{from: from, to: to}, acc when is_atom(to) ->
          put_transition_parent(acc, step_names, from, to)

        _transition, acc ->
          acc
      end)

    Map.new(spec.steps, fn step ->
      parents =
        case Keyword.get(step.opts, :after) do
          dependencies when is_list(dependencies) -> dependencies
          _other -> Map.get(transition_parents, step.name, [])
        end

      {step.name, parents}
    end)
  end

  defp put_transition_parent(acc, step_names, from, to) do
    if MapSet.member?(step_names, to) do
      Map.update(acc, to, [from], &List.insert_at(&1, -1, from))
    else
      acc
    end
  end

  defp ordered_steps(%Spec{} = spec, parent_map, step_map) do
    {steps, _visiting, _visited} =
      Enum.reduce(spec.steps, {[], MapSet.new(), MapSet.new()}, fn step, acc ->
        visit_step(step.name, parent_map, step_map, acc)
      end)

    Enum.reverse(steps)
  end

  defp visit_step(step_name, parent_map, step_map, {ordered, visiting, visited})
       when is_atom(step_name) do
    if MapSet.member?(visited, step_name) or MapSet.member?(visiting, step_name) do
      {ordered, visiting, visited}
    else
      do_visit_step(step_name, parent_map, step_map, {ordered, visiting, visited})
    end
  end

  defp do_visit_step(step_name, parent_map, step_map, {ordered, visiting, visited}) do
    visiting_steps = MapSet.put(visiting, step_name)

    {ordered, ancestor_visits, visited_steps} =
      parent_map
      |> Map.get(step_name, [])
      |> Enum.reduce({ordered, visiting_steps, visited}, fn parent, acc ->
        visit_step(parent, parent_map, step_map, acc)
      end)

    step = Map.fetch!(step_map, step_name)

    {
      [step | ordered],
      MapSet.delete(ancestor_visits, step_name),
      MapSet.put(visited_steps, step_name)
    }
  end

  defp prepare_external_runnables(%__MODULE__{} = planner) do
    {workflow, runnables} = Workflow.prepare_for_dispatch(planner.runic_workflow)
    {external, internal} = Enum.split_with(runnables, &external_runnable?(&1, planner.spec))
    {allowed, blocked} = Enum.split_with(external, &runnable_allowed?(&1, planner.spec))

    prepared_workflow =
      Enum.reduce(blocked, workflow, fn runic_runnable, acc ->
        Workflow.apply_runnable(acc, consume_blocked_runnable(runic_runnable))
      end)

    if internal == [] and blocked == [] do
      prepared_planner = %{planner | runic_workflow: prepared_workflow}

      with {:ok, runnables} <- to_squidie_runnables(allowed, planner.spec) do
        {:ok, prepared_planner, runnables}
      end
    else
      workflow =
        Enum.reduce(internal, prepared_workflow, fn runic_runnable, acc ->
          executed = Invokable.execute(runic_runnable.node, runic_runnable)
          Workflow.apply_runnable(acc, executed)
        end)

      planner
      |> then(&%{&1 | runic_workflow: Workflow.plan_eagerly(workflow)})
      |> prepare_external_runnables()
    end
  end

  defp external_runnable?(%Runic.Workflow.Runnable{node: %{name: name}}, %Spec{} = spec) do
    Enum.any?(spec.steps, &(&1.name == name))
  end

  defp external_runnable?(_runnable, _spec), do: false

  defp runnable_allowed?(
         %Runic.Workflow.Runnable{input_fact: %Fact{ancestry: nil}},
         %Spec{}
       ) do
    true
  end

  defp runnable_allowed?(
         %Runic.Workflow.Runnable{
           input_fact: %Fact{ancestry: {producer_hash, _fact_hash}} = fact,
           node: %{name: target}
         },
         %Spec{} = spec
       ) do
    with {:ok, from} <- step_name_by_hash(spec, producer_hash),
         transitions when transitions != [] <- transitions_between(spec, from, target) do
      outcome = fact_outcome(fact)
      Enum.any?(transitions, &(&1.on == outcome))
    else
      _no_outcome_specific_transition -> true
    end
  end

  defp runnable_allowed?(_runnable, _spec), do: true

  defp to_squidie_runnables(runnables, %Spec{} = spec) do
    result =
      Enum.reduce_while(runnables, {:ok, []}, fn runic_runnable, {:ok, acc} ->
        case to_squidie_runnable(runic_runnable, spec) do
          {:ok, runnable} -> {:cont, {:ok, [runnable | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, mapped_runnables} -> {:ok, Enum.reverse(mapped_runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp to_squidie_runnable(%Runic.Workflow.Runnable{} = runic_runnable, %Spec{} = spec) do
    step = Enum.find(spec.steps, &(&1.name == runic_runnable.node.name))

    with {:ok, input} <-
           runic_runnable.input_fact
           |> fact_context()
           |> apply_input_mapping(step) do
      {:ok,
       %Runnable{
         id: runic_runnable.id,
         step: step.name,
         input: input,
         metadata: Map.get(step, :metadata, %{}),
         runic_runnable: runic_runnable
       }}
    end
  end

  defp consume_blocked_runnable(%Runic.Workflow.Runnable{} = runic_runnable) do
    event = %ActivationConsumed{
      fact_hash: runic_runnable.input_fact.hash,
      node_hash: runic_runnable.node.hash,
      from_label: :runnable
    }

    Runic.Workflow.Runnable.complete(runic_runnable, :blocked, [event])
  end

  defp fact_context(%Fact{meta: %{squidie_context: context}}) when is_map(context) do
    context
  end

  defp fact_context(%Fact{value: value}), do: normalize_step_input(value)

  defp fact_outcome(%Fact{meta: %{squidie_outcome: outcome}}) when outcome in [:ok, :error] do
    outcome
  end

  defp fact_outcome(%Fact{}), do: :ok

  defp apply_input_mapping(input, %{opts: opts}) do
    StepInput.apply_input_mapping(input, Keyword.get(opts, :input))
  end

  defp normalize_step_input(values) when is_list(values) do
    if Enum.all?(values, &is_map/1) do
      Enum.reduce(values, %{}, &Map.merge(&2, &1))
    else
      values
    end
  end

  defp normalize_step_input(value), do: value

  defp complete_runic_runnable(
         %Spec{} = spec,
         %Runic.Workflow.Runnable{} = runic_runnable,
         outcome,
         result
       ) do
    node = runic_runnable.node
    input_fact = runic_runnable.input_fact
    context = runic_runnable.context
    next_context = next_context(spec, node.name, input_fact, outcome, result)

    result_fact =
      Fact.new(
        value: next_context,
        ancestry: {node.hash, input_fact.hash},
        meta: %{squidie_context: next_context, squidie_outcome: outcome}
      )

    events = [
      %FactProduced{
        hash: result_fact.hash,
        value: result_fact.value,
        ancestry: result_fact.ancestry,
        producer_label: :produced,
        weight: context.ancestry_depth + 1,
        meta: result_fact.meta
      },
      %ActivationConsumed{
        fact_hash: input_fact.hash,
        node_hash: node.hash,
        from_label: :runnable
      }
    ]

    Runic.Workflow.Runnable.complete(runic_runnable, result_fact, events)
  end

  defp next_context(%Spec{} = spec, step_name, input_fact, :ok, result) do
    input_fact
    |> fact_context()
    |> Map.merge(mapped_step_output(spec, step_name, result))
  end

  defp next_context(%Spec{}, _step_name, input_fact, :error, _reason),
    do: fact_context(input_fact)

  defp mapped_step_output(%Spec{} = spec, step_name, result) when is_map(result) do
    step = Enum.find(spec.steps, &(&1.name == step_name))

    case Keyword.get(step.opts, :output) do
      nil -> result
      output_key when is_atom(output_key) -> %{output_key => result}
    end
  end

  defp mapped_step_output(%Spec{}, _step_name, _result), do: %{}

  defp transitions_between(%Spec{} = spec, from, to) do
    Enum.filter(spec.transitions, &(&1.from == from and &1.to == to))
  end

  defp step_name_by_hash(%Spec{} = spec, hash) do
    case Enum.find(spec.steps, &(stable_step_hash(spec, &1) == hash)) do
      %{name: name} -> {:ok, name}
      nil -> :error
    end
  end
end
