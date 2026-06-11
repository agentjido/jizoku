defmodule Squidie.Step.ErrorPayload do
  @moduledoc false

  @doc """
  Builds a validation error payload for step adapters.
  """
  @spec validation_failed(String.t(), map()) :: map()
  def validation_failed(message, errors) when is_binary(message) and is_map(errors) do
    Map.new(message: message, validation_errors: errors, retryable?: false)
  end
end
