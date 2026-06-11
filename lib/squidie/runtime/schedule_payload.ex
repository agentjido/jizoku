defmodule Squidie.Runtime.SchedulePayload do
  @moduledoc false

  @doc """
  Extracts and validates a cron schedule intended-window payload.
  """
  @spec intended_window(map()) ::
          {:ok, map() | nil} | {:error, {:invalid_schedule_intended_window, term()}}
  def intended_window(payload) when is_map(payload) do
    case value(payload, "intended_window") do
      %{} = window -> normalize_intended_window(window)
      nil -> {:ok, nil}
      invalid -> {:error, {:invalid_schedule_intended_window, invalid}}
    end
  end

  @doc """
  Fetches a schedule payload value by string key with atom-key fallback.
  """
  @spec value(map(), String.t()) :: term()
  def value(payload, key) when is_map(payload) and is_binary(key) do
    value_with_fallback(payload, key, String.to_existing_atom(key))
  rescue
    _exception in [ArgumentError] ->
      Map.get(payload, key)
  end

  defp normalize_intended_window(window) do
    with {:ok, start_at} <- window_value(window, :start_at),
         {:ok, end_at} <- window_value(window, :end_at) do
      %{}
      |> maybe_put(:start_at, start_at)
      |> maybe_put(:end_at, end_at)
      |> case do
        empty when map_size(empty) == 0 -> {:ok, nil}
        intended_window -> {:ok, intended_window}
      end
    end
  end

  defp window_value(window, key) when is_atom(key) do
    case value_with_fallback(window, Atom.to_string(key), key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      invalid -> {:error, {:invalid_schedule_intended_window, %{key => invalid}}}
    end
  end

  defp value_with_fallback(map, preferred_key, fallback_key) do
    case Map.fetch(map, preferred_key) do
      {:ok, value} -> value
      :error -> Map.get(map, fallback_key)
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
