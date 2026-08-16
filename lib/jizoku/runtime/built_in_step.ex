# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.BuiltInStep do
  @moduledoc """
  Executes declarative built-in workflow steps.

  Built-in steps let workflows express simple runtime primitives without
  requiring host applications to define dedicated Jido actions for them.
  """

  require Logger

  alias Jizoku.Workflow.EventWait

  @type built_in_step_error ::
          {:unknown_built_in_step, Jizoku.Workflow.Definition.built_in_step_kind()}
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

  @doc false
  @spec execute_await_event(map(), keyword()) :: {:ok, map(), keyword()} | {:error, map()}
  def execute_await_event(input, opts) when is_map(input) and is_list(opts) do
    event = Keyword.fetch!(opts, :event)

    with {:ok, correlation} <- resolve_correlation(input, Keyword.fetch!(opts, :correlation)),
         true <- EventWait.valid_correlation_value?(correlation),
         {:ok, timeout} <- EventWait.timeout_from_opts(opts) do
      wait = maybe_put(%{event: event, correlation: correlation}, :timeout, timeout)

      {:ok, %{}, [pause: true, await_event: wait]}
    else
      _invalid ->
        {:error,
         non_retryable_error(
           "event_correlation_unavailable",
           "event wait correlation could not be resolved from step input"
         )}
    end
  end

  defp resolve_correlation(_input, correlation) when is_binary(correlation) do
    {:ok, correlation}
  end

  defp resolve_correlation(input, path) when is_list(path) and path != [] do
    Enum.reduce_while(path, {:ok, input}, fn segment, {:ok, current} ->
      case fetch_path_segment(current, segment) do
        {:ok, value} -> {:cont, {:ok, value}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp resolve_correlation(_input, _correlation) do
    :error
  end

  defp fetch_path_segment(current, segment) when is_map(current) do
    Map.fetch(current, segment)
  end

  defp fetch_path_segment(_current, _segment) do
    :error
  end

  defp non_retryable_error(code, message) do
    %{}
    |> Map.put(:code, code)
    |> Map.put(:message, message)
    |> Map.put(:retryable?, false)
  end

  defp maybe_put(map, _key, nil) do
    map
  end

  defp maybe_put(map, key, value) do
    Map.put(map, key, value)
  end
end
