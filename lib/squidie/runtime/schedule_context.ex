defmodule Squidie.Runtime.ScheduleContext do
  @moduledoc false

  @doc """
  Extracts a schedule context map from atom- or string-keyed context payloads.
  """
  @spec get(map()) :: map()
  def get(context) when is_map(context) do
    case Map.fetch(context, :schedule) do
      {:ok, schedule} -> schedule
      :error -> Map.get(context, "schedule", %{})
    end
  end

  @doc """
  Fetches a schedule context value by atom key with string fallback.
  """
  @spec value(term(), atom()) :: term()
  def value(schedule, key) when is_map(schedule) and is_atom(key) do
    Squidie.MapField.get(schedule, key)
  end

  def value(_schedule, _key), do: nil
end
