defmodule Squidie.Telemetry do
  @moduledoc """
  Stable telemetry events and reporter-neutral metric definitions for Squidie.

  Squidie emits telemetry but does not install reporters, exporters, dashboards,
  loggers, or alerting. Hosts remain responsible for those integrations.
  """

  alias Telemetry.Metrics

  @span_prefixes [
    [:squidie, :runtime, :command, :apply],
    [:squidie, :runtime, :executor, :execute_next],
    [:squidie, :runtime, :step, :execute]
  ]

  @point_events [
    [:squidie, :runtime, :command, :received],
    [:squidie, :runtime, :run, :started],
    [:squidie, :runtime, :run, :terminal],
    [:squidie, :runtime, :runnable, :planned],
    [:squidie, :runtime, :runnable, :applied],
    [:squidie, :runtime, :attempt, :scheduled],
    [:squidie, :runtime, :attempt, :retry_scheduled],
    [:squidie, :runtime, :attempt, :claimed],
    [:squidie, :runtime, :attempt, :heartbeat],
    [:squidie, :runtime, :attempt, :completed],
    [:squidie, :runtime, :attempt, :failed],
    [:squidie, :runtime, :manual, :paused],
    [:squidie, :runtime, :manual, :resolved],
    [:squidie, :runtime, :child, :started],
    [:squidie, :runtime, :dynamic_work, :recorded]
  ]

  @span_events for prefix <- @span_prefixes,
                   suffix <- [:start, :stop, :exception],
                   do: Enum.concat(prefix, [suffix])

  @doc "Returns every public Squidie telemetry event name."
  @spec events() :: [[atom(), ...]]
  def events, do: @span_events ++ @point_events

  @doc "Returns the recommended bounded-cardinality metric definitions."
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics, do: build_metrics(false)

  @doc "Returns the recommended metrics with partition included as an explicit opt-in tag."
  @spec partition_metrics() :: [Telemetry.Metrics.t()]
  def partition_metrics, do: build_metrics(true)

  @doc "Returns whether an event prefix is a public Squidie span boundary."
  @spec span_prefix?(term()) :: boolean()
  def span_prefix?(prefix), do: prefix in @span_prefixes

  @doc "Returns whether an event name is a public Squidie lifecycle point event."
  @spec point_event?(term()) :: boolean()
  def point_event?(event), do: event in @point_events

  defp build_metrics(include_partition?) do
    [
      duration_metric(
        "squidie.runtime.command.apply.duration",
        [:squidie, :runtime, :command, :apply, :stop],
        tags([:command_type, :outcome], include_partition?)
      ),
      duration_metric(
        "squidie.runtime.executor.execute_next.duration",
        [:squidie, :runtime, :executor, :execute_next, :stop],
        tags([:queue, :outcome], include_partition?)
      ),
      duration_metric(
        "squidie.runtime.step.execute.duration",
        [:squidie, :runtime, :step, :execute, :stop],
        tags([:workflow, :step, :outcome], include_partition?)
      ),
      exception_metric(
        "squidie.runtime.command.apply.exception.count",
        [:squidie, :runtime, :command, :apply, :exception],
        tags([:command_type], include_partition?)
      ),
      exception_metric(
        "squidie.runtime.executor.execute_next.exception.count",
        [:squidie, :runtime, :executor, :execute_next, :exception],
        tags([:queue], include_partition?)
      ),
      exception_metric(
        "squidie.runtime.step.execute.exception.count",
        [:squidie, :runtime, :step, :execute, :exception],
        tags([:workflow, :step], include_partition?)
      ),
      point_metric(
        "squidie.runtime.command.received.count",
        [:squidie, :runtime, :command, :received],
        tags([:command_type], include_partition?)
      ),
      point_metric(
        "squidie.runtime.run.started.count",
        [:squidie, :runtime, :run, :started],
        tags([:workflow], include_partition?)
      ),
      point_metric(
        "squidie.runtime.run.terminal.count",
        [:squidie, :runtime, :run, :terminal],
        tags([:workflow, :status], include_partition?)
      ),
      point_metric(
        "squidie.runtime.runnable.planned.count",
        [:squidie, :runtime, :runnable, :planned],
        tags([:workflow, :step], include_partition?)
      ),
      point_metric(
        "squidie.runtime.runnable.applied.count",
        [:squidie, :runtime, :runnable, :applied],
        tags([:workflow, :step, :outcome], include_partition?)
      ),
      point_metric(
        "squidie.runtime.attempt.scheduled.count",
        [:squidie, :runtime, :attempt, :scheduled],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "squidie.runtime.attempt.retry_scheduled.count",
        [:squidie, :runtime, :attempt, :retry_scheduled],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "squidie.runtime.attempt.completed.count",
        [:squidie, :runtime, :attempt, :completed],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "squidie.runtime.attempt.failed.count",
        [:squidie, :runtime, :attempt, :failed],
        tags([:queue, :workflow, :step], include_partition?)
      )
    ]
  end

  defp duration_metric(name, event, tags) do
    Metrics.distribution(name,
      event_name: event,
      measurement: :duration,
      unit: {:native, :millisecond},
      tags: tags
    )
  end

  defp exception_metric(name, event, tags) do
    Metrics.counter(name, event_name: event, measurement: :duration, tags: tags)
  end

  defp point_metric(name, event, tags) do
    Metrics.counter(name, event_name: event, measurement: :count, tags: tags)
  end

  defp tags(tags, false), do: tags
  defp tags(tags, true), do: Enum.concat(tags, [:partition])
end
