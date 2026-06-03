defmodule Squidie.Workflow do
  @moduledoc """
  Declarative workflow contract for Squidie workflow modules.

  ## Example

      defmodule Billing.InvoiceReminder do
        use Squidie.Workflow

        workflow do
          trigger :invoice_delivery do
            manual()

            payload do
              field :account_id, :string
              field :invoice_id, :string
            end
          end

          step :load_invoice, Billing.Steps.LoadInvoice
          step :send_email, Billing.Steps.SendReminderEmail, retry: [max_attempts: 3]

          transition :load_invoice, on: :ok, to: :send_email
        end
      end

  The contract defined here captures workflow structure. Validation and runtime
  execution behavior are added in subsequent slices.
  """

  @contract %{
    required: [:trigger, :step],
    optional: [:transition]
  }

  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.Info
  alias Squidie.Workflow.PayloadFieldSpec
  alias Squidie.Workflow.PayloadSpec
  alias Squidie.Workflow.Spec
  alias Squidie.Workflow.StepSpec
  alias Squidie.Workflow.TransitionSpec
  alias Squidie.Workflow.TriggerDefinitionSpec
  alias Squidie.Workflow.TriggerSpec
  alias Squidie.Workflow.Validation

  @doc """
  Injects the workflow DSL into a workflow module.
  """
  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(_opts) do
    quote do
      use Squidie.Workflow.Dsl

      @before_compile Squidie.Workflow
    end
  end

  @doc """
  Validates the collected workflow declarations and emits runtime accessors.
  """
  @spec __before_compile__(Macro.Env.t()) :: Macro.t()
  defmacro __before_compile__(env) do
    env
    |> build_definition!()
    |> quoted_definition()
  end

  @doc """
  Resolves late-bound runtime metadata for generated workflow definitions.

  Workflow modules store a compile-time definition, but step modules may expose
  Squidie step metadata that should be read when the workflow definition is
  consumed. This keeps generated workflow modules small while preserving the
  runtime contract used by dispatch and inspection.
  """
  @spec __resolve_runtime_definition__(map()) :: map()
  def __resolve_runtime_definition__(definition) when is_map(definition) do
    Map.update!(definition, :steps, fn steps ->
      Enum.map(steps, &resolve_step_metadata/1)
    end)
  end

  @doc """
  Converts a compiled workflow module into Squidie's normalized workflow spec.
  """
  @spec to_spec(module()) ::
          {:ok, Spec.t()} | {:error, Squidie.Workflow.Definition.load_error()}
  def to_spec(workflow) when is_atom(workflow), do: Info.fetch_spec(workflow)

  @doc """
  Validates a normalized workflow spec without resolving workflow or step modules.

  This validates the structural contract used by Squidie planner state. It
  does not prove that arbitrary module atoms are owned by the host application
  or executable as runtime-authored workflow code.
  """
  @spec validate_spec(Spec.t() | map() | term()) ::
          :ok | {:error, {:invalid_workflow_spec, [map()]}}
  def validate_spec(spec), do: Spec.validate(spec)

  @doc """
  Validates a normalized workflow spec after resolving host-approved action keys.

  Runtime-authored specs can use stable `:action` keys instead of raw module
  atoms when the host provides an `:action_registry`. Module-authored workflow
  specs without action keys keep using the normal validation path.
  """
  @spec validate_spec(Spec.t() | map() | term(), keyword()) ::
          :ok | {:error, {:invalid_workflow_spec, [map()]}}
  def validate_spec(spec, opts) when is_list(opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} ->
        if spec_uses_action_keys?(spec) do
          ActionRegistry.validate_spec(spec, registry)
        else
          validate_spec(spec)
        end

      :error ->
        validate_spec(spec)
    end
  end

  @doc """
  Resolves runtime-authored `:action` step keys to host-approved modules.
  """
  @spec resolve_spec_actions(Spec.t() | map() | term(), keyword()) ::
          {:ok, Spec.t() | map()} | {:error, {:invalid_workflow_spec, [map()]}}
  def resolve_spec_actions(spec, opts) when is_list(opts) do
    case Keyword.fetch(opts, :action_registry) do
      {:ok, registry} ->
        if spec_uses_action_keys?(spec) do
          ActionRegistry.resolve_spec(spec, registry)
        else
          {:ok, spec}
        end

      :error ->
        {:ok, spec}
    end
  end

  defp spec_uses_action_keys?(%Spec{} = spec), do: spec_uses_action_keys?(Map.from_struct(spec))

  defp spec_uses_action_keys?(spec) when is_map(spec) do
    steps = Map.get(spec, :steps) || Map.get(spec, "steps") || []

    case steps do
      steps when is_list(steps) ->
        Enum.any?(steps, fn
          step when is_map(step) -> Map.has_key?(step, :action) or Map.has_key?(step, "action")
          _other -> false
        end)

      _missing_or_invalid ->
        false
    end
  end

  defp spec_uses_action_keys?(_spec), do: false

  defp quoted_definition(definition) do
    quote do
      @doc false
      def workflow_definition do
        Squidie.Workflow.__resolve_runtime_definition__(unquote(Macro.escape(definition)))
      end

      @doc false
      def __workflow__(:definition), do: workflow_definition()

      @doc false
      def __workflow__(:contract), do: unquote(Macro.escape(@contract))

      @doc false
      def __workflow__(:steps), do: workflow_definition().steps

      @doc false
      def __workflow__(key)
          when key in [
                 :entry_step,
                 :entry_steps,
                 :definition_version,
                 :initial_step,
                 :payload,
                 :retries,
                 :transitions,
                 :triggers
               ] do
        Map.fetch!(workflow_definition(), key)
      end
    end
  end

  defp build_definition!(env) do
    steps = spark_steps(env.module)

    definition = %{
      definition_version: Info.definition_version(env.module),
      triggers: spark_triggers(env.module),
      steps: steps,
      transitions: spark_transitions(env.module),
      retries: Validation.derive_retries(steps)
    }

    Validation.validate!(definition, env)

    triggers = Validation.normalize_triggers!(definition)

    definition
    |> Map.put(:triggers, triggers)
    |> Map.put(:payload, Validation.workflow_payload!(triggers))
    |> Map.put(:entry_steps, Validation.entry_steps!(definition, env))
    |> Map.put(:initial_step, Validation.initial_step!(definition, env))
    |> Map.put(:entry_step, Validation.entry_step!(definition, env))
  end

  defp spark_entities(module) do
    Spark.Dsl.Extension.get_entities(module, [:workflow])
  end

  defp spark_steps(module) do
    module
    |> Info.steps()
    |> Enum.map(fn %StepSpec{} = step ->
      step
      |> Map.from_struct()
      |> Map.take([:name, :module, :opts, :metadata])
      |> maybe_drop_interop_metadata()
      |> Map.reject(fn {_key, value} -> value in [nil, %{}] end)
    end)
  end

  defp maybe_drop_interop_metadata(%{metadata: %{contract: :squidie_step}} = step), do: step
  defp maybe_drop_interop_metadata(step), do: Map.delete(step, :metadata)

  defp spark_triggers(module) do
    module
    |> spark_entities()
    |> Enum.filter(&match?(%TriggerSpec{}, &1))
    |> Enum.map(&trigger_definition/1)
  end

  defp trigger_definition(%TriggerSpec{} = trigger) do
    %{
      name: trigger.name,
      definitions: Enum.map(trigger.definitions, &trigger_definition_entry/1),
      payload: trigger_payload(trigger.payload)
    }
  end

  defp trigger_definition_entry(%TriggerDefinitionSpec{} = definition) do
    definition
    |> Map.from_struct()
    |> Map.take([:type, :config])
  end

  defp trigger_payload(payload_blocks) do
    payload_blocks
    |> Enum.flat_map(fn %PayloadSpec{fields: fields} -> fields end)
    |> Enum.map(&payload_field/1)
  end

  defp payload_field(%PayloadFieldSpec{} = field) do
    field
    |> Map.from_struct()
    |> Map.take([:name, :type, :opts])
  end

  defp spark_transitions(module) do
    module
    |> spark_entities()
    |> Enum.filter(&match?(%TransitionSpec{}, &1))
    |> Enum.map(&transition_definition/1)
  end

  defp transition_definition(%TransitionSpec{} = transition) do
    transition
    |> Map.from_struct()
    |> Map.take([:from, :on, :to, :recovery, :condition])
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp resolve_step_metadata(%{module: module} = step) do
    case Squidie.Step.metadata(module) do
      %{} = metadata -> Map.put(step, :metadata, metadata)
      nil -> maybe_drop_interop_metadata(step)
    end
  end
end
