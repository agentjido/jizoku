# credo:disable-for-next-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Workflow.InputMapping do
  @moduledoc false

  @type selection_mapping :: [atom()]
  @type path_mapping :: keyword([atom()])
  @type t :: selection_mapping() | path_mapping()

  @doc false
  @spec valid?(term()) :: boolean()
  def valid?(mapping) when is_list(mapping) and mapping != [] do
    selection_mapping?(mapping) or path_mapping?(mapping)
  end

  def valid?(_mapping), do: false

  @doc false
  @spec apply(map(), t() | nil) ::
          {:ok, map()} | {:error, {:missing_input_path, map()} | {:invalid_input_mapping, term()}}
  def apply(input, nil) when is_map(input), do: {:ok, input}

  def apply(input, mapping) when is_map(input) and is_list(mapping) do
    cond do
      selection_mapping?(mapping) ->
        {:ok, Map.take(input, mapping)}

      path_mapping?(mapping) ->
        apply_path_mapping(input, mapping)

      true ->
        {:error, {:invalid_input_mapping, mapping}}
    end
  end

  defp selection_mapping?(mapping), do: Enum.all?(mapping, &is_atom/1)

  defp path_mapping?(mapping) do
    Keyword.keyword?(mapping) and unique_targets?(mapping) and
      Enum.all?(mapping, fn {target, path} ->
        is_atom(target) and is_list(path) and path != [] and Enum.all?(path, &is_atom/1)
      end)
  end

  defp unique_targets?(mapping) do
    targets = Keyword.keys(mapping)
    Enum.uniq(targets) == targets
  end

  defp apply_path_mapping(input, mapping) do
    Enum.reduce_while(mapping, {:ok, %{}}, fn {target, path}, {:ok, acc} ->
      case fetch_path(input, path, target) do
        {:ok, value} -> {:cont, {:ok, Map.put(acc, target, value)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp fetch_path(input, path, target) do
    do_fetch_path(input, path, target, path, [])
  end

  defp do_fetch_path(value, [], _target, _path, _consumed), do: {:ok, value}

  defp do_fetch_path(%{} = current, [segment | remaining], target, path, consumed) do
    case Map.fetch(current, segment) do
      {:ok, value} -> do_fetch_path(value, remaining, target, path, [segment | consumed])
      :error -> missing_path(target, path, Enum.reverse([segment | consumed]))
    end
  end

  defp do_fetch_path(_current, [segment | _remaining], target, path, consumed),
    do: missing_path(target, path, Enum.reverse([segment | consumed]))

  defp missing_path(target, path, missing_at) do
    {:error,
     {:missing_input_path,
      %{
        target: target,
        path: path,
        missing_at: missing_at
      }}}
  end
end
