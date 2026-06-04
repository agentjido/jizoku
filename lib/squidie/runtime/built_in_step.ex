# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.BuiltInStep do
  @moduledoc """
  Executes declarative built-in workflow steps.

  Built-in steps let workflows express simple runtime primitives without
  requiring host applications to define dedicated Jido actions for them.
  """

  require Logger

  @type built_in_step_error ::
          {:unknown_built_in_step, Squidie.Workflow.Definition.built_in_step_kind()}
  @type execution_result :: {:ok, map(), keyword()} | {:error, built_in_step_error()}

  @doc false
  @spec execute_wait(keyword()) :: {:ok, map(), keyword()}
  def execute_wait(opts) do
    duration = Keyword.fetch!(opts, :duration)
    {:ok, %{}, [schedule_in: ceil(duration / 1_000)]}
  end

  @doc false
  @spec execute_log(keyword()) :: {:ok, map(), keyword()}
  def execute_log(opts) do
    level = Keyword.get(opts, :level, :info)
    message = Keyword.fetch!(opts, :message)

    Logger.log(level, message)

    {:ok, %{}, []}
  end
end
