defmodule Squidie.Runtime.RetryPolicy do
  @moduledoc """
  Resolves workflow retry configuration into concrete runtime decisions.

  This module turns declarative workflow retry definitions into explicit retry
  outcomes that runtime step execution can consume without needing to re-interpret the
  workflow contract on every failure.
  """

  @type delay_ms :: non_neg_integer()
  @type resolution ::
          {:retry, pos_integer(), delay_ms()} | {:exhausted, pos_integer()} | :no_retry

  @doc """
  Resolves the retry outcome after a failed attempt for the given workflow step.
  """
  @spec resolve(module(), atom(), pos_integer()) :: resolution
  def resolve(workflow, step, attempt_number)
      when is_integer(attempt_number) and attempt_number > 0 do
    case max_attempts(workflow, step) do
      {:ok, max_attempts} when attempt_number < max_attempts ->
        {:retry, attempt_number + 1, backoff_delay(workflow, step, attempt_number)}

      {:ok, max_attempts} ->
        {:exhausted, max_attempts}

      :no_retry ->
        :no_retry
    end
  end

  @doc """
  Returns the configured backoff delay in milliseconds for the next retry
  attempt after the given failed attempt number.
  """
  @spec backoff_delay(module(), atom(), pos_integer()) :: delay_ms()
  def backoff_delay(workflow, step, attempt_number)
      when is_atom(step) and is_integer(attempt_number) and attempt_number > 0 do
    case backoff(workflow, step) do
      {:ok, [type: :exponential, min: min_delay, max: max_delay]} ->
        multiplier = Integer.pow(2, attempt_number - 1)
        delay = min_delay * multiplier
        min(delay, max_delay)

      :no_backoff ->
        0
    end
  end

  @doc """
  Returns the configured maximum attempt count for a workflow step.
  """
  @spec max_attempts(module(), atom()) :: {:ok, pos_integer()} | :no_retry
  def max_attempts(workflow, step) when is_atom(step) do
    workflow
    |> retries()
    |> Enum.find_value(:no_retry, fn
      %{step: ^step, opts: opts} -> {:ok, Keyword.fetch!(opts, :max_attempts)}
      _retry -> false
    end)
  end

  @doc """
  Returns the configured retry backoff policy for a workflow step.
  """
  @spec backoff(module(), atom()) :: {:ok, keyword()} | :no_backoff
  def backoff(workflow, step) when is_atom(step) do
    workflow
    |> retries()
    |> Enum.find_value(:no_backoff, fn
      %{step: ^step, opts: opts} ->
        case Keyword.get(opts, :backoff) do
          nil -> {:ok, [type: :none]}
          backoff when is_list(backoff) -> {:ok, backoff}
          _other -> false
        end

      _retry ->
        false
    end)
    |> normalize_backoff()
  end

  defp retries(workflow) do
    if function_exported?(workflow, :__workflow__, 1) do
      workflow.__workflow__(:retries)
    else
      []
    end
  end

  defp normalize_backoff({:ok, [type: :none]}), do: :no_backoff
  defp normalize_backoff({:ok, backoff}), do: {:ok, backoff}
  defp normalize_backoff(:no_backoff), do: :no_backoff
end
