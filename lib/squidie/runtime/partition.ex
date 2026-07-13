defmodule Squidie.Runtime.Partition do
  @moduledoc """
  Normalizes the optional logical namespace used by Squidie's durable runtime.

  A `nil` partition preserves the legacy journal namespace. Explicit partitions
  are opaque, storage-safe identifiers; authorization remains the host
  application's responsibility.
  """

  alias Squidie.Runtime.Journal.Options

  @type t :: String.t() | nil

  @doc "Validates and normalizes an optional durable runtime partition."
  @spec normalize(term()) :: {:ok, t()} | {:error, {:invalid_option, {:partition, :invalid}}}
  def normalize(partition), do: Options.partition(partition)

  @doc "Builds a collision-safe encoded identity from validated string components."
  @spec identity([String.t()]) :: String.t()
  def identity(parts) when is_list(parts) do
    parts
    |> Enum.map(fn part -> [Integer.to_string(byte_size(part)), ":", part] end)
    |> IO.iodata_to_binary()
    |> Base.url_encode64(padding: false)
  end
end
