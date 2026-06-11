# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.RunCatalogProjection do
  @moduledoc """
  Rebuildable projection over the global journal run catalog.

  Catalog entries are lookup facts, not execution state. They let host-facing
  tools discover all known journal-backed runs without scanning adapter-specific
  storage internals.
  """

  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.RunProjection

  @type anomaly :: %{
          required(:reason) => atom(),
          required(:entry_type) => atom(),
          optional(:run_id) => String.t(),
          optional(:workflow) => String.t(),
          optional(:queue) => String.t()
        }

  @type run_summary :: %{
          required(:run_id) => String.t(),
          required(:workflow) => String.t(),
          required(:queue) => String.t(),
          required(:indexed_at) => DateTime.t()
        }

  @type t :: %__MODULE__{
          runs: %{optional(String.t()) => run_summary()},
          anomalies: [anomaly()]
        }

  defstruct runs: %{}, anomalies: []

  @doc false
  @spec new() :: t()
  def new, do: %__MODULE__{}

  @doc false
  @spec rebuild([Entry.t()]) :: t()
  def rebuild(entries) when is_list(entries) do
    replay(new(), entries)
  end

  @doc false
  @spec replay(t(), [Entry.t()]) :: t()
  def replay(%__MODULE__{} = projection, entries) when is_list(entries) do
    Enum.reduce(entries, projection, &apply_entry/2)
  end

  @doc false
  @spec runs(t()) :: [run_summary()]
  def runs(%__MODULE__{runs: runs}) do
    RunProjection.sorted_runs(runs)
  end

  @doc false
  @spec run_ids(t()) :: [String.t()]
  def run_ids(%__MODULE__{} = projection) do
    RunProjection.run_ids(projection.runs)
  end

  @doc false
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: RunProjection.anomalies(anomalies)

  defp apply_entry(%Entry{type: :run_cataloged, data: data} = entry, %__MODULE__{} = projection) do
    if valid_catalog_data?(data) do
      catalog_run(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(%Entry{}, %__MODULE__{} = projection), do: projection

  defp valid_catalog_data?(data) when is_map(data) do
    is_binary(Map.get(data, :run_id)) and is_binary(Map.get(data, :workflow)) and
      is_binary(Map.get(data, :queue))
  end

  defp valid_catalog_data?(_data), do: false

  defp catalog_run(%__MODULE__{runs: runs} = projection, entry, data) do
    summary = RunProjection.summary(data.run_id, data.workflow, data.queue, entry.occurred_at)

    case Map.fetch(runs, data.run_id) do
      {:ok, ^summary} ->
        projection

      {:ok, _existing_summary} ->
        add_anomaly(projection, entry, :conflicting_run_catalog)

      :error ->
        %__MODULE__{projection | runs: Map.put(runs, data.run_id, summary)}
    end
  end

  defp add_anomaly(%__MODULE__{} = projection, %Entry{} = entry, reason) do
    %__MODULE__{
      projection
      | anomalies: [RunProjection.anomaly(entry, reason) | projection.anomalies]
    }
  end
end
