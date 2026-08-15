defmodule Jizoku.Step.ErrorPayload do
  @moduledoc false

  @doc """
  Builds a validation error payload for step adapters.
  """
  @spec validation_failed(String.t(), map()) :: map()
  def validation_failed(message, errors) when is_binary(message) and is_map(errors) do
    Map.new(message: message, validation_errors: errors, retryable?: false)
  end

  @doc """
  Builds a stable nonretryable error payload for internal step adapters.
  """
  @spec terminal(String.t(), String.t()) :: map()
  def terminal(code, message) when is_binary(code) and is_binary(message) do
    Map.new(code: code, message: message, retryable?: false)
  end
end
