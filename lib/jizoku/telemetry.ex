defmodule Jizoku.Telemetry do
  @moduledoc """
  Stable telemetry events and reporter-neutral metric definitions for Jizoku.

  Jizoku emits telemetry but does not install reporters, exporters, dashboards,
  loggers, or alerting. Hosts remain responsible for those integrations.
  """

  alias Telemetry.Metrics

  @span_prefixes [
    [:jizoku, :runtime, :command, :apply],
    [:jizoku, :runtime, :executor, :execute_next],
    [:jizoku, :runtime, :step, :execute],
    [:jizoku, :runtime, :jido_signal, :deliver]
  ]

  @point_events [
    [:jizoku, :runtime, :command, :received],
    [:jizoku, :runtime, :run, :started],
    [:jizoku, :runtime, :run, :terminal],
    [:jizoku, :runtime, :runnable, :planned],
    [:jizoku, :runtime, :runnable, :applied],
    [:jizoku, :runtime, :attempt, :scheduled],
    [:jizoku, :runtime, :attempt, :retry_scheduled],
    [:jizoku, :runtime, :attempt, :claimed],
    [:jizoku, :runtime, :attempt, :heartbeat],
    [:jizoku, :runtime, :attempt, :completed],
    [:jizoku, :runtime, :attempt, :failed],
    [:jizoku, :runtime, :manual, :paused],
    [:jizoku, :runtime, :manual, :resolved],
    [:jizoku, :runtime, :child, :started],
    [:jizoku, :runtime, :dynamic_work, :recorded],
    [:jizoku, :runtime, :jido_signal, :enqueued],
    [:jizoku, :runtime, :jido_signal, :delivered]
  ]

  @span_events for prefix <- @span_prefixes,
                   suffix <- [:start, :stop, :exception],
                   do: Enum.concat(prefix, [suffix])

  @doc "Returns every public Jizoku telemetry event name."
  @spec events() :: [[atom(), ...]]
  def events, do: @span_events ++ @point_events

  @doc "Returns the recommended bounded-cardinality metric definitions."
  @spec metrics() :: [Telemetry.Metrics.t()]
  def metrics, do: build_metrics(false)

  @doc "Returns the recommended metrics with partition included as an explicit opt-in tag."
  @spec partition_metrics() :: [Telemetry.Metrics.t()]
  def partition_metrics, do: build_metrics(true)

  @doc "Returns whether an event prefix is a public Jizoku span boundary."
  @spec span_prefix?(term()) :: boolean()
  def span_prefix?(prefix), do: prefix in @span_prefixes

  @doc "Returns whether an event name is a public Jizoku lifecycle point event."
  @spec point_event?(term()) :: boolean()
  def point_event?(event), do: event in @point_events

  defp build_metrics(include_partition?) do
    [
      duration_metric(
        "jizoku.runtime.command.apply.duration",
        [:jizoku, :runtime, :command, :apply, :stop],
        tags([:command_type, :outcome], include_partition?)
      ),
      duration_metric(
        "jizoku.runtime.executor.execute_next.duration",
        [:jizoku, :runtime, :executor, :execute_next, :stop],
        tags([:queue, :outcome], include_partition?)
      ),
      duration_metric(
        "jizoku.runtime.step.execute.duration",
        [:jizoku, :runtime, :step, :execute, :stop],
        tags([:workflow, :step, :outcome], include_partition?)
      ),
      exception_metric(
        "jizoku.runtime.command.apply.exception.count",
        [:jizoku, :runtime, :command, :apply, :exception],
        tags([:command_type], include_partition?)
      ),
      exception_metric(
        "jizoku.runtime.executor.execute_next.exception.count",
        [:jizoku, :runtime, :executor, :execute_next, :exception],
        tags([:queue], include_partition?)
      ),
      exception_metric(
        "jizoku.runtime.step.execute.exception.count",
        [:jizoku, :runtime, :step, :execute, :exception],
        tags([:workflow, :step], include_partition?)
      ),
      exception_metric(
        "jizoku.runtime.jido_signal.deliver.exception.count",
        [:jizoku, :runtime, :jido_signal, :deliver, :exception],
        tags([:route], include_partition?)
      ),
      duration_metric(
        "jizoku.runtime.jido_signal.deliver.duration",
        [:jizoku, :runtime, :jido_signal, :deliver, :stop],
        tags([:route, :outcome], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.command.received.count",
        [:jizoku, :runtime, :command, :received],
        tags([:command_type], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.run.started.count",
        [:jizoku, :runtime, :run, :started],
        tags([:workflow], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.run.terminal.count",
        [:jizoku, :runtime, :run, :terminal],
        tags([:workflow, :status], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.runnable.planned.count",
        [:jizoku, :runtime, :runnable, :planned],
        tags([:workflow, :step], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.runnable.applied.count",
        [:jizoku, :runtime, :runnable, :applied],
        tags([:workflow, :step, :outcome], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.attempt.scheduled.count",
        [:jizoku, :runtime, :attempt, :scheduled],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.attempt.retry_scheduled.count",
        [:jizoku, :runtime, :attempt, :retry_scheduled],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.attempt.completed.count",
        [:jizoku, :runtime, :attempt, :completed],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.attempt.failed.count",
        [:jizoku, :runtime, :attempt, :failed],
        tags([:queue, :workflow, :step], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.jido_signal.enqueued.count",
        [:jizoku, :runtime, :jido_signal, :enqueued],
        tags([:route], include_partition?)
      ),
      point_metric(
        "jizoku.runtime.jido_signal.delivered.count",
        [:jizoku, :runtime, :jido_signal, :delivered],
        tags([:route], include_partition?)
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
