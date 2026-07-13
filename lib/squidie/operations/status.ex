defmodule Squidie.Operations.Status do
  @moduledoc """
  Builds a read-only operational summary from Squidie's durable projections.

  The report is JSON-safe and deliberately excludes workflow payloads, attempt
  inputs and results, claim tokens, owner metadata, and command history.
  """

  alias Squidie.Operations.Collector
  alias Squidie.Runtime.DispatchProtocol

  @schema_version 1

  @type report :: map()

  @spec report(keyword()) :: {:ok, report()} | {:error, term()}
  @doc "Collects durable projections and returns a versioned, JSON-safe operational status report."
  def report(overrides \\ []) do
    with {:ok, %Collector{} = collector} <- Collector.collect(overrides) do
      {:ok, from_collector(collector)}
    end
  end

  @doc "Builds a versioned, JSON-safe status report from a collected operational snapshot."
  @spec from_collector(Collector.t()) :: report()
  def from_collector(%Collector{} = collector) do
    %{
      schema_version: @schema_version,
      generated_at: DateTime.to_iso8601(collector.now),
      partition: collector.config.partition,
      totals: %{
        runs: length(collector.runs),
        queues: map_size(collector.queues),
        anomalies: total_anomalies(collector)
      },
      run_counts: run_counts(collector.runs, collector.config.partition),
      queues: queue_summaries(collector)
    }
  end

  defp run_counts(runs, partition) do
    runs
    |> Enum.frequencies_by(
      &{Map.get(&1, :partition, partition), &1.workflow, &1.queue, &1.status}
    )
    |> Enum.map(fn {{partition, workflow, queue, status}, count} ->
      %{partition: partition, workflow: workflow, queue: queue, status: status, count: count}
    end)
    |> Enum.sort_by(&{&1.partition, &1.workflow, &1.queue, Atom.to_string(&1.status)})
  end

  defp queue_summaries(%Collector{} = collector) do
    collector.queues
    |> Enum.map(fn {_name, queue} -> queue_summary(collector, queue) end)
    |> Enum.sort_by(& &1.queue)
  end

  defp queue_summary(%Collector{} = collector, queue) do
    visible = DispatchProtocol.Projection.visible_attempts(queue.projection, collector.now)
    scheduled = scheduled_attempts(queue, collector.now)
    claimed = active_claims(queue)
    queue_runs = Enum.filter(collector.runs, &(&1.queue == queue.queue))

    %{
      partition: Map.get(queue, :partition, collector.config.partition),
      queue: queue.queue,
      visible_attempt_depth: length(visible),
      scheduled_attempt_depth: length(scheduled),
      next_visible_at: next_visible_at(scheduled),
      claimed_attempt_depth: length(claimed),
      oldest_claim_age_seconds: oldest_claim_age_seconds(claimed, collector.now),
      pending_dispatch_count:
        Enum.sum_by(queue_runs, &length(Collector.pending_dispatches(&1, queue))),
      pending_result_count:
        Enum.sum_by(queue_runs, &length(Collector.pending_results(&1, queue))),
      manual_intervention_count:
        Enum.count(queue_runs, &(not &1.terminal? and not is_nil(&1.manual_state))),
      anomaly_count:
        length(DispatchProtocol.Projection.anomalies(queue.projection)) +
          Enum.sum_by(queue_runs, &length(&1.anomalies))
    }
  end

  defp scheduled_attempts(queue, now) do
    Enum.filter(queue.attempts, fn attempt ->
      attempt.status in [:available, :retry_scheduled] and
        DateTime.compare(attempt.visible_at, now) == :gt and
        not MapSet.member?(queue.projection.terminal_runs, attempt.run_id)
    end)
  end

  defp active_claims(queue) do
    Enum.filter(queue.attempts, fn attempt ->
      attempt.status == :claimed and
        not MapSet.member?(queue.projection.terminal_runs, attempt.run_id)
    end)
  end

  defp oldest_claim_age_seconds([], _now), do: nil

  defp oldest_claim_age_seconds(claimed, now) do
    claimed
    |> Enum.map(& &1.claimed_at)
    |> Enum.reject(&is_nil/1)
    |> Enum.min(DateTime, fn -> nil end)
    |> case do
      nil -> nil
      claimed_at -> max(DateTime.diff(now, claimed_at, :second), 0)
    end
  end

  defp total_anomalies(%Collector{} = collector) do
    length(collector.catalog_anomalies) +
      Enum.sum_by(collector.runs, &length(&1.anomalies)) +
      Enum.sum_by(collector.queues, fn {_name, queue} ->
        length(DispatchProtocol.Projection.anomalies(queue.projection))
      end)
  end

  defp iso8601(nil), do: nil
  defp iso8601(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)

  defp next_visible_at(scheduled) do
    scheduled
    |> Enum.map(& &1.visible_at)
    |> Enum.min(DateTime, fn -> nil end)
    |> iso8601()
  end
end
