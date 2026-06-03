defmodule Squidie.Workflow.SparkExtension do
  @moduledoc """
  Spark extension that defines the Squidie workflow DSL.

  The extension owns trigger, payload, step, and transition declarations. It
  stores native `Squidie.Step` metadata when available and marks built-in or
  raw Jido actions as explicit interop contracts so the compiled spec remains
  inspectable without changing the runtime execution model.
  """

  @step_schema [
    name: [
      type: :atom,
      required: true,
      doc: "The workflow step name."
    ],
    module: [
      type: :atom,
      required: true,
      doc: "The native Squidie step, raw Jido action, or built-in step kind."
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "Step runtime options such as input, output, retry, recovery, and dependencies."
    ]
  ]

  @step %Spark.Dsl.Entity{
    name: :step,
    target: Squidie.Workflow.StepSpec,
    args: [:name, :module, {:optional, :opts, []}],
    schema: @step_schema,
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @approval_step %Spark.Dsl.Entity{
    name: :approval_step,
    target: Squidie.Workflow.StepSpec,
    args: [:name, {:optional, :opts, []}],
    schema: Keyword.delete(@step_schema, :module),
    auto_set_fields: [module: :approval],
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @manual_review_step %Spark.Dsl.Entity{
    name: :manual_review_step,
    target: Squidie.Workflow.StepSpec,
    args: [:name, {:optional, :opts, []}],
    schema: Keyword.delete(@step_schema, :module),
    auto_set_fields: [module: :approval],
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @trigger_definition_schema [
    expression: [
      type: :string,
      required: true,
      doc: "Cron expression for scheduled workflow activation."
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "Trigger options such as timezone and idempotency."
    ]
  ]

  @manual %Spark.Dsl.Entity{
    name: :manual,
    target: Squidie.Workflow.TriggerDefinitionSpec,
    schema: [],
    auto_set_fields: [type: :manual, config: %{}]
  }

  @cron %Spark.Dsl.Entity{
    name: :cron,
    target: Squidie.Workflow.TriggerDefinitionSpec,
    args: [:expression, {:optional, :opts, []}],
    schema: @trigger_definition_schema,
    auto_set_fields: [type: :cron],
    transform: {__MODULE__, :put_cron_config, []}
  }

  @payload_field_schema [
    name: [
      type: :atom,
      required: true,
      doc: "Payload field name."
    ],
    type: [
      type: :atom,
      required: true,
      doc: "Payload field type."
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "Payload field options such as defaults."
    ]
  ]

  @payload_field %Spark.Dsl.Entity{
    name: :field,
    target: Squidie.Workflow.PayloadFieldSpec,
    args: [:name, :type, {:optional, :opts, []}],
    schema: @payload_field_schema
  }

  @invalid_payload_field %Spark.Dsl.Entity{
    name: :field,
    target: Squidie.Workflow.PayloadFieldSpec,
    args: [:name, :type, {:optional, :opts, []}],
    schema: @payload_field_schema,
    transform: {__MODULE__, :reject_payload_field_outside_payload, []}
  }

  @payload %Spark.Dsl.Entity{
    name: :payload,
    target: Squidie.Workflow.PayloadSpec,
    entities: [fields: [@payload_field]]
  }

  @trigger_schema [
    name: [
      type: :atom,
      required: true,
      doc: "Workflow trigger name."
    ]
  ]

  @trigger %Spark.Dsl.Entity{
    name: :trigger,
    target: Squidie.Workflow.TriggerSpec,
    args: [:name],
    schema: @trigger_schema,
    entities: [
      definitions: [@manual, @cron],
      payload: [@payload],
      invalid_fields: [@invalid_payload_field]
    ]
  }

  @transition_schema [
    from: [
      type: :atom,
      required: true,
      doc: "Source step name."
    ],
    opts: [
      type: :keyword_list,
      required: true,
      doc: "Transition options including :on, :to, :recovery, and :condition."
    ]
  ]

  @transition %Spark.Dsl.Entity{
    name: :transition,
    target: Squidie.Workflow.TransitionSpec,
    args: [:from, :opts],
    schema: @transition_schema,
    transform: {__MODULE__, :put_transition_options, []}
  }

  @workflow_schema [
    version: [
      type: :string,
      doc: "Human-readable workflow definition version for operator diagnostics."
    ]
  ]

  @workflow %Spark.Dsl.Section{
    name: :workflow,
    schema: @workflow_schema,
    entities: [
      @trigger,
      @step,
      @approval_step,
      @manual_review_step,
      @transition,
      @invalid_payload_field
    ],
    describe: "Declares Squidie workflow triggers, steps, and transitions."
  }

  use Spark.Dsl.Extension, sections: [@workflow]

  @doc false
  @spec put_step_metadata(Squidie.Workflow.StepSpec.t()) ::
          {:ok, Squidie.Workflow.StepSpec.t()}
  def put_step_metadata(%Squidie.Workflow.StepSpec{} = step) do
    metadata =
      case Squidie.Step.metadata(step.module) do
        %{} = native_metadata -> native_metadata
        nil -> interop_metadata(step.module)
      end

    {:ok, %{step | metadata: metadata}}
  end

  @doc false
  @spec put_cron_config(Squidie.Workflow.TriggerDefinitionSpec.t()) ::
          {:ok, Squidie.Workflow.TriggerDefinitionSpec.t()}
  def put_cron_config(%Squidie.Workflow.TriggerDefinitionSpec{} = definition) do
    config = %{
      expression: definition.expression,
      timezone: Keyword.get(definition.opts, :timezone)
    }

    normalized_config =
      if Keyword.has_key?(definition.opts, :idempotency) do
        Map.put(config, :idempotency, Keyword.get(definition.opts, :idempotency))
      else
        config
      end

    {:ok, %{definition | config: normalized_config}}
  end

  @doc false
  @spec reject_payload_field_outside_payload(Squidie.Workflow.PayloadFieldSpec.t()) ::
          {:error, String.t()}
  def reject_payload_field_outside_payload(%Squidie.Workflow.PayloadFieldSpec{}) do
    {:error, "field/3 must be declared inside a trigger payload block"}
  end

  @doc false
  @spec put_transition_options(Squidie.Workflow.TransitionSpec.t()) ::
          {:ok, Squidie.Workflow.TransitionSpec.t()} | {:error, String.t()}
  def put_transition_options(%Squidie.Workflow.TransitionSpec{} = transition) do
    with {:ok, on} <- fetch_transition_option(transition.opts, :on),
         {:ok, to} <- fetch_transition_option(transition.opts, :to) do
      transition =
        %{transition | on: on, to: to}
        |> maybe_put_transition_recovery()
        |> maybe_put_transition_condition()

      {:ok, transition}
    end
  end

  defp interop_metadata(module) when module in [:wait, :log, :pause, :approval] do
    %{contract: :built_in, kind: module}
  end

  defp interop_metadata(module) when is_atom(module) do
    %{contract: :jido_action, module: module}
  end

  defp fetch_transition_option(opts, option) do
    case Keyword.fetch(opts, option) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, "transition must specify #{inspect(option)} option"}
    end
  end

  defp maybe_put_transition_recovery(%Squidie.Workflow.TransitionSpec{opts: opts} = transition) do
    case Keyword.fetch(opts, :recovery) do
      {:ok, recovery} -> %{transition | recovery: recovery}
      :error -> transition
    end
  end

  defp maybe_put_transition_condition(%Squidie.Workflow.TransitionSpec{opts: opts} = transition) do
    case Keyword.fetch(opts, :condition) do
      {:ok, condition} ->
        %{
          transition
          | condition: Squidie.Workflow.TransitionCondition.normalize!(condition)
        }

      :error ->
        transition
    end
  end
end
