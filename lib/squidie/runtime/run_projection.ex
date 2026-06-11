defmodule Squidie.Runtime.RunProjection do
  @moduledoc false

  alias Squidie.Runtime.DispatchProtocol.Entry

  @doc """
  Builds the stored run-summary payload for run index projections.
  """
  @spec summary(String.t(), String.t(), String.t(), DateTime.t()) :: map()
  def summary(run_id, workflow, queue, %DateTime{} = indexed_at) do
    Map.new(run_id: run_id, workflow: workflow, queue: queue, indexed_at: indexed_at)
  end

  @doc """
  Returns run summaries in stable indexed-at order.
  """
  @spec sorted_runs(map()) :: [map()]
  def sorted_runs(runs) when is_map(runs) do
    runs
    |> Map.values()
    |> Enum.sort_by(fn %{run_id: run_id, indexed_at: indexed_at} ->
      {DateTime.to_unix(indexed_at, :microsecond), run_id}
    end)
  end

  @doc """
  Returns the projected run ids in stable indexed-at order.
  """
  @spec run_ids(map()) :: [String.t()]
  def run_ids(runs) when is_map(runs) do
    runs
    |> sorted_runs()
    |> Enum.map(& &1.run_id)
  end

  @doc """
  Returns anomalies in chronological replay order.
  """
  @spec anomalies([map()]) :: [map()]
  def anomalies(anomalies) when is_list(anomalies), do: Enum.reverse(anomalies)

  @doc """
  Builds a projection anomaly from a journal entry and reason.
  """
  @spec anomaly(Entry.t(), atom()) :: map()
  def anomaly(%Entry{} = entry, reason) do
    data = entry_data(entry)

    %{reason: reason, entry_type: entry.type}
    |> maybe_put(:run_id, data)
    |> maybe_put(:workflow, data)
    |> maybe_put(:queue, data)
  end

  defp entry_data(%Entry{data: data}) when is_map(data), do: data
  defp entry_data(%Entry{}), do: %{}

  defp maybe_put(anomaly, key, data) do
    case Map.get(data, key) do
      value when is_binary(value) -> Map.put(anomaly, key, value)
      _value -> anomaly
    end
  end
end
