defmodule Squidie.Workflow.RegistryHelpers do
  @moduledoc false

  @doc """
  Normalizes map or keyword-list registries into key-entry pairs.
  """
  @spec registry_pairs(term(), (term() -> map())) :: {:ok, list()} | {:error, map()}
  def registry_pairs(registry, _invalid_error) when is_map(registry),
    do: {:ok, Enum.to_list(registry)}

  def registry_pairs(registry, invalid_error) when is_list(registry) do
    if Keyword.keyword?(registry) do
      {:ok, registry}
    else
      {:error, invalid_error.(registry)}
    end
  end

  def registry_pairs(registry, invalid_error), do: {:error, invalid_error.(registry)}

  @doc """
  Fetches a registry entry from a map or atom-keyed keyword registry.
  """
  @spec fetch_registry_entry(term(), atom() | String.t()) :: {:ok, term()} | :error
  def fetch_registry_entry(registry, key) when is_map(registry), do: Map.fetch(registry, key)

  def fetch_registry_entry(registry, key) when is_list(registry) do
    if Keyword.keyword?(registry) and is_atom(key) do
      Keyword.fetch(registry, key)
    else
      :error
    end
  end

  def fetch_registry_entry(_registry, _key), do: :error

  @doc """
  Normalizes a value into a JSON-safe representation or returns the failing path.
  """
  @spec json_value(term(), [term()]) :: {:ok, term()} | {:error, [term()]}
  def json_value(nil, _path), do: {:ok, nil}
  def json_value(value, _path) when is_boolean(value), do: {:ok, value}
  def json_value(value, _path) when is_integer(value), do: {:ok, value}
  def json_value(value, _path) when is_float(value), do: {:ok, value}
  def json_value(value, _path) when is_binary(value), do: {:ok, value}
  def json_value(value, _path) when is_atom(value), do: {:ok, Atom.to_string(value)}

  def json_value(value, path) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> json_value(path)
  end

  def json_value([], _path), do: {:ok, []}

  def json_value(value, path) when is_list(value) do
    if Keyword.keyword?(value), do: json_map(value, path), else: json_list(value, path)
  end

  def json_value(value, path) when is_map(value), do: json_map(Map.to_list(value), path)
  def json_value(_value, path), do: {:error, path}

  defp json_map(pairs, path) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn {key, item}, {:ok, acc} ->
      with {:ok, key} <- json_key(key, path),
           {:ok, item} <- json_value(item, [key | path]) do
        {:cont, {:ok, Map.put(acc, key, item)}}
      else
        {:error, path} -> {:halt, {:error, path}}
      end
    end)
  end

  defp json_key(key, _path) when is_atom(key), do: {:ok, Atom.to_string(key)}
  defp json_key(key, _path) when is_binary(key), do: {:ok, key}
  defp json_key(key, _path) when is_integer(key), do: {:ok, Integer.to_string(key)}
  defp json_key(_key, path), do: {:error, path}

  defp json_list(list, path) do
    list
    |> Stream.with_index()
    |> Enum.reduce_while({:ok, []}, fn {item, index}, {:ok, acc} ->
      case json_value(item, [index | path]) do
        {:ok, item} -> {:cont, {:ok, [item | acc]}}
        {:error, path} -> {:halt, {:error, path}}
      end
    end)
    |> case do
      {:ok, items} -> {:ok, Enum.reverse(items)}
      {:error, path} -> {:error, path}
    end
  end
end
