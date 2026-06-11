defmodule Squidie.MapField do
  @moduledoc false

  @doc """
  Fetches an atom-keyed field with string-key fallback.
  """
  @spec get(term(), atom(), term()) :: term()
  def get(map, key, default \\ nil)

  def get(map, key, default) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key), default)
    end
  end

  def get(_map, _key, default), do: default
end
