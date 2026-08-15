defmodule Jizoku.TelemetryTest do
  use ExUnit.Case, async: false

  alias Jizoku.Telemetry
  alias Jizoku.Telemetry.Emitter
  alias Jizoku.Test.TelemetryCapture

  @command_prefix [:jizoku, :runtime, :command, :apply]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7",
    parent_span_id: "b7ad6b7169203331",
    causation_id: "signal-123",
    tracestate: "must-not-appear"
  }

  test "publishes the stable event catalog" do
    assert Telemetry.events() == [
             [:jizoku, :runtime, :command, :apply, :start],
             [:jizoku, :runtime, :command, :apply, :stop],
             [:jizoku, :runtime, :command, :apply, :exception],
             [:jizoku, :runtime, :executor, :execute_next, :start],
             [:jizoku, :runtime, :executor, :execute_next, :stop],
             [:jizoku, :runtime, :executor, :execute_next, :exception],
             [:jizoku, :runtime, :step, :execute, :start],
             [:jizoku, :runtime, :step, :execute, :stop],
             [:jizoku, :runtime, :step, :execute, :exception],
             [:jizoku, :runtime, :jido_signal, :deliver, :start],
             [:jizoku, :runtime, :jido_signal, :deliver, :stop],
             [:jizoku, :runtime, :jido_signal, :deliver, :exception],
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
  end

  test "returns standard low-cardinality metrics with partition opt-in" do
    metrics = Telemetry.metrics()
    partition_metrics = Telemetry.partition_metrics()

    assert [_metric | _metrics] = metrics
    assert Enum.all?(metrics, &is_struct(&1))
    assert Enum.all?(metrics, fn metric -> :partition not in metric.tags end)
    assert Enum.all?(metrics, fn metric -> :run_id not in metric.tags end)
    assert Enum.all?(metrics, fn metric -> :trace_id not in metric.tags end)

    assert Enum.map(partition_metrics, & &1.name) == Enum.map(metrics, & &1.name)
    assert Enum.all?(partition_metrics, fn metric -> :partition in metric.tags end)
  end

  test "point events expose only allowlisted bounded metadata" do
    event = [:jizoku, :runtime, :run, :started]
    TelemetryCapture.attach([event])

    assert :ok =
             Emitter.point(event, %{
               workflow: "Elixir.CheckoutWorkflow",
               run_id: "run-123",
               partition: "tenant_acme",
               trace: @trace,
               payload: %{password: "secret-sentinel"},
               result: "secret-sentinel",
               error: "secret-sentinel",
               metadata: %{token: "secret-sentinel"},
               owner_id: "secret-sentinel",
               claim_token: "secret-sentinel",
               idempotency_key: "secret-sentinel"
             })

    assert_receive {:telemetry_event, ^event, %{count: 1, system_time: system_time}, metadata}
    assert is_integer(system_time)

    assert metadata == %{
             workflow: "Elixir.CheckoutWorkflow",
             run_id: "run-123",
             partition: "tenant_acme",
             trace_id: @trace.trace_id,
             span_id: @trace.span_id,
             parent_span_id: @trace.parent_span_id,
             causation_id: @trace.causation_id
           }

    refute inspect(metadata) =~ "secret-sentinel"
    refute Map.has_key?(metadata, :tracestate)
  end

  test "returned errors close normal spans with an error outcome" do
    events = Enum.map([:start, :stop, :exception], &Enum.concat(@command_prefix, [&1]))
    TelemetryCapture.attach(events)

    assert {:error, :not_found} =
             Emitter.span(@command_prefix, %{command_type: :cancel_run, trace: @trace}, fn ->
               {:error, :not_found}
             end)

    assert_receive {:telemetry_event, start_event,
                    %{monotonic_time: start_time, system_time: system_time}, start_metadata}

    assert start_event == Enum.concat(@command_prefix, [:start])
    assert is_integer(start_time)
    assert is_integer(system_time)
    assert start_metadata.outcome == :unknown

    assert_receive {:telemetry_event, stop_event,
                    %{duration: duration, monotonic_time: stop_time}, stop_metadata}

    assert stop_event == Enum.concat(@command_prefix, [:stop])
    assert duration >= 0
    assert stop_time >= start_time
    assert stop_metadata.outcome == :error
    refute_receive {:telemetry_event, _, _, _}
  end

  test "raise, throw, and exit emit sanitized exceptions and preserve semantics" do
    exception_event = Enum.concat(@command_prefix, [:exception])
    TelemetryCapture.attach([exception_event])

    assert_raise RuntimeError, "secret-sentinel", fn ->
      Emitter.span(@command_prefix, %{command_type: :cancel_run}, fn ->
        raise "secret-sentinel"
      end)
    end

    assert_receive {:telemetry_event, ^exception_event, %{duration: duration}, metadata}
    assert duration >= 0
    assert metadata == %{command_type: :cancel_run, outcome: :exception}
    refute inspect(metadata) =~ "secret-sentinel"

    assert catch_throw(Emitter.span(@command_prefix, %{}, fn -> throw(:expected_throw) end)) ==
             :expected_throw

    assert catch_exit(Emitter.span(@command_prefix, %{}, fn -> exit(:expected_exit) end)) ==
             :expected_exit
  end

  test "telemetry handler failures do not change runtime results" do
    event = Enum.concat(@command_prefix, [:start])
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, _measurements, _metadata, _config ->
          raise "handler failure"
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:ok, :done} = Emitter.span(@command_prefix, %{}, fn -> {:ok, :done} end)
  end
end
