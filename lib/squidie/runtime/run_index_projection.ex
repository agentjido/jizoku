defmodule Squidie.Runtime.RunIndexProjection do
  @moduledoc """
  Rebuildable projection over a workflow's run-index journal.

  Run-index entries are lookup facts, not execution state. They let the
  Jido-native runtime rebuild "which runs exist for this workflow?" from the
  journal boundary without scanning storage adapter internals.

  Duplicate entries for the same run are idempotent when they carry the same
  workflow and timestamp. Conflicting or malformed persisted entries are kept as
  anomalies so callers can surface index drift without losing the valid portion
  of the read model.
  """

  alias Squidie.Runtime.DispatchProtocol.Entry

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
          required(:indexed_at) => DateTime.t(),
          required(:queue) => String.t()
        }

  @type t :: %__MODULE__{
          workflow: String.t() | nil,
          runs: %{optional(String.t()) => run_summary()},
          anomalies: [anomaly()]
        }

  defstruct workflow: nil,
            runs: %{},
            anomalies: []

  @doc """
  Returns a new empty run-index projection.
  """
  @spec new(String.t() | nil) :: t()
  def new(workflow \\ nil), do: %__MODULE__{workflow: workflow}

  @doc """
  Rebuilds a run-index projection from durable journal entries.
  """
  @spec rebuild([Entry.t()]) :: t()
  def rebuild(entries) when is_list(entries) do
    replay(new(), entries)
  end

  @doc """
  Replays additional run-index entries into an existing projection.
  """
  @spec replay(t(), [Entry.t()]) :: t()
  def replay(%__MODULE__{} = projection, entries) when is_list(entries) do
    Enum.reduce(entries, projection, &apply_entry/2)
  end

  @doc """
  Returns the workflow this index projection describes.
  """
  @spec workflow(t()) :: String.t() | nil
  def workflow(%__MODULE__{workflow: workflow}), do: workflow

  @doc """
  Returns indexed run summaries ordered by index timestamp and run id.
  """
  @spec runs(t()) :: [run_summary()]
  def runs(%__MODULE__{runs: runs}) do
    runs
    |> Map.values()
    |> Enum.sort_by(fn %{run_id: run_id, indexed_at: indexed_at} ->
      {DateTime.to_unix(indexed_at, :microsecond), run_id}
    end)
  end

  @doc """
  Returns indexed run ids in the same deterministic order as `runs/1`.
  """
  @spec run_ids(t()) :: [String.t()]
  def run_ids(%__MODULE__{} = projection) do
    projection
    |> runs()
    |> Enum.map(& &1.run_id)
  end

  @doc """
  Returns malformed or conflicting index facts discovered during replay.
  """
  @spec anomalies(t()) :: [anomaly()]
  def anomalies(%__MODULE__{anomalies: anomalies}), do: Enum.reverse(anomalies)

  defp apply_entry(%Entry{type: :run_indexed, data: data} = entry, %__MODULE__{} = projection) do
    if valid_index_data?(data) do
      index_run(projection, entry, data)
    else
      add_anomaly(projection, entry, :malformed_entry)
    end
  end

  defp apply_entry(%Entry{}, %__MODULE__{} = projection), do: projection

  defp valid_index_data?(data) when is_map(data) do
    is_binary(Map.get(data, :run_id)) and is_binary(Map.get(data, :workflow)) and
      is_binary(Map.get(data, :queue))
  end

  defp valid_index_data?(_data), do: false

  defp index_run(%__MODULE__{workflow: nil} = projection, entry, data) do
    index_run(%__MODULE__{projection | workflow: data.workflow}, entry, data)
  end

  defp index_run(%__MODULE__{workflow: workflow} = projection, entry, data)
       when data.workflow != workflow do
    add_anomaly(projection, entry, :conflicting_workflow)
  end

  defp index_run(%__MODULE__{runs: runs} = projection, entry, data) do
    summary =
      %{
        run_id: data.run_id,
        workflow: data.workflow,
        indexed_at: entry.occurred_at,
        queue: data.queue
      }

    case Map.fetch(runs, data.run_id) do
      {:ok, ^summary} ->
        projection

      {:ok, _existing_summary} ->
        add_anomaly(projection, entry, :conflicting_run_index)

      :error ->
        %__MODULE__{projection | runs: Map.put(runs, data.run_id, summary)}
    end
  end

  defp add_anomaly(%__MODULE__{} = projection, %Entry{} = entry, reason) do
    data = entry_data(entry)

    anomaly =
      %{
        reason: reason,
        entry_type: entry.type
      }
      |> maybe_put_run_id(data)
      |> maybe_put_workflow(data)
      |> maybe_put_queue(data)

    %__MODULE__{projection | anomalies: [anomaly | projection.anomalies]}
  end

  defp entry_data(%Entry{data: data}) when is_map(data), do: data
  defp entry_data(%Entry{}), do: %{}

  defp maybe_put_run_id(anomaly, %{run_id: run_id}) when is_binary(run_id) do
    Map.put(anomaly, :run_id, run_id)
  end

  defp maybe_put_run_id(anomaly, _data), do: anomaly

  defp maybe_put_workflow(anomaly, %{workflow: workflow}) when is_binary(workflow) do
    Map.put(anomaly, :workflow, workflow)
  end

  defp maybe_put_workflow(anomaly, _data), do: anomaly

  defp maybe_put_queue(map, %{queue: queue}) when is_binary(queue) do
    Map.put(map, :queue, queue)
  end

  defp maybe_put_queue(map, _data), do: map
end
