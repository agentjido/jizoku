defmodule Jizoku.Workflow.Info do
  @moduledoc """
  Read helpers for compiled Jizoku workflow Spark metadata.

  The runtime still exposes `workflow_definition/0` for durable execution, while
  this module gives tests, tooling, and planner adapters direct access to the
  normalized workflow specification produced by the workflow DSL.
  """

  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.Spec
  alias Jizoku.Workflow.StepSpec
  alias Spark.Dsl.Extension

  @doc """
  Returns the normalized, serializable workflow spec.
  """
  @spec spec(module()) :: Spec.t()
  def spec(workflow) when is_atom(workflow) do
    case fetch_spec(workflow) do
      {:ok, spec} -> spec
      {:error, reason} -> raise ArgumentError, "invalid Jizoku workflow: #{inspect(reason)}"
    end
  end

  @doc """
  Fetches the normalized workflow spec.
  """
  @spec fetch_spec(module()) :: {:ok, Spec.t()} | {:error, Definition.load_error()}
  def fetch_spec(workflow) when is_atom(workflow) do
    with {:ok, definition} <- Definition.load(workflow) do
      {:ok, Spec.from_definition(workflow, definition)}
    end
  end

  @doc """
  Returns the optional workflow definition version.
  """
  @spec definition_version(module()) :: String.t() | nil
  def definition_version(workflow) when is_atom(workflow) do
    Extension.get_opt(workflow, [:workflow], :version)
  end

  @doc """
  Returns normalized workflow triggers.
  """
  @spec triggers(module()) :: [Definition.trigger()]
  def triggers(workflow) when is_atom(workflow), do: spec(workflow).triggers

  @doc """
  Returns the merged workflow payload contract.
  """
  @spec payload(module()) :: [Definition.payload_field()]
  def payload(workflow) when is_atom(workflow), do: spec(workflow).payload

  @doc """
  Returns normalized workflow transitions.
  """
  @spec transitions(module()) :: [Definition.transition()]
  def transitions(workflow) when is_atom(workflow), do: spec(workflow).transitions

  @doc """
  Returns normalized retry policies.
  """
  @spec retries(module()) :: [Definition.retry()]
  def retries(workflow) when is_atom(workflow), do: spec(workflow).retries

  @doc """
  Returns Spark step entities with runtime-resolved native step metadata.
  """
  @spec steps(module()) :: [Jizoku.Workflow.StepSpec.t()]
  def steps(workflow) when is_atom(workflow) do
    workflow
    |> Extension.get_entities([:workflow])
    |> Enum.filter(&match?(%StepSpec{}, &1))
    |> Enum.map(&resolve_step_metadata/1)
  end

  defp resolve_step_metadata(%StepSpec{module: module} = step) do
    case Jizoku.Step.metadata(module) do
      %{} = metadata -> %{step | metadata: metadata}
      nil -> step
    end
  end
end
