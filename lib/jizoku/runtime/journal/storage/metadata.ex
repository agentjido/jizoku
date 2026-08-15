# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Journal.Storage.Metadata do
  @moduledoc false

  @doc false
  @spec normalize(term()) :: {:ok, map()} | {:error, {:unsupported_term, term()}}
  def normalize(%{} = metadata) when map_size(metadata) == 0 do
    {:ok, metadata}
  end

  def normalize(%{} = metadata) do
    with {:ok, encoded} <- Jason.encode(metadata),
         {:ok, normalized} <- Jason.decode(encoded) do
      {:ok, normalized}
    else
      {:error, error} -> {:error, {:unsupported_term, unsupported_value(error, metadata)}}
    end
  end

  def normalize(metadata) do
    {:error, {:unsupported_term, metadata}}
  end

  defp unsupported_value(%Protocol.UndefinedError{value: value}, _metadata) do
    value
  end

  defp unsupported_value(_error, metadata) do
    metadata
  end
end
