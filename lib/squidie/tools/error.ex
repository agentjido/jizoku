defmodule Squidie.Tools.Error do
  @moduledoc """
  Normalized tool failure shape.
  """

  @enforce_keys [:adapter, :kind, :message]
  defstruct [:adapter, :kind, :message, details: %{}, retryable?: false]

  @type kind ::
          :adapter_contract
          | :http
          | :invalid_context
          | :invalid_request
          | :timeout
          | :transport

  @type t :: %__MODULE__{
          adapter: module(),
          kind: kind(),
          message: String.t(),
          details: map(),
          retryable?: boolean()
        }

  @doc """
  Builds a normalized tool error.
  """
  @spec new(keyword()) :: t()
  def new(attrs) when is_list(attrs) do
    attrs
    |> Enum.into(%{})
    |> then(&struct!(__MODULE__, &1))
  end

  @doc """
  Converts a tool error into a plain map suitable for step error payloads.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = error) do
    %{
      adapter: inspect(error.adapter),
      kind: error.kind,
      message: error.message,
      details: error.details,
      retryable?: error.retryable?
    }
  end
end
