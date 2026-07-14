defmodule Squidie.Runtime.Journal.SignalInterpreterTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.Journal.Cancellation
  alias Squidie.Runtime.Journal.Commands
  alias Squidie.Runtime.Journal.ManualControl
  alias Squidie.Runtime.Journal.SignalInterpreter
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Trace
  alias Squidie.Test.TelemetryCapture

  @run_id "3c82d86d-31a6-4d57-9e41-4f5c95125be6"

  test "old journal command modules delegate to the canonical commands namespace" do
    assert {:ok, %Signal{} = cancel_signal} = Signal.cancel_run(@run_id)

    assert SignalInterpreter.apply(cancel_signal, :bad_opts) ==
             Commands.SignalInterpreter.apply(cancel_signal, :bad_opts)

    assert Cancellation.apply_signal(%{}, []) ==
             Commands.Cancellation.apply_signal(%{}, [])

    assert ManualControl.apply_signal(%{}, []) ==
             Commands.ManualControl.apply_signal(%{}, [])
  end

  test "validates malformed supported runtime command signals" do
    for type <- [:start_run, :start_cron, :replay_run] do
      signal = %Signal{
        type: type,
        payload: %{},
        metadata: %{},
        occurred_at: DateTime.utc_now()
      }

      assert {:error, {:invalid_signal, ^type}} = SignalInterpreter.apply(signal, [])
    end
  end

  test "rejects malformed start command signals without raising" do
    for signal <- [
          %Signal{
            type: :start_run,
            payload: nil,
            metadata: %{},
            occurred_at: DateTime.utc_now()
          },
          %Signal{
            type: :start_run,
            payload: %{workflow: :bad_workflow, trigger: "manual", input: %{}},
            metadata: %{},
            occurred_at: DateTime.utc_now()
          },
          %Signal{
            type: :start_cron,
            payload: %{workflow: :bad_workflow, trigger: nil, input: %{}},
            metadata: %{},
            occurred_at: DateTime.utc_now()
          }
        ] do
      signal_type = signal.type

      assert {:error, {:invalid_signal, ^signal_type}} = SignalInterpreter.apply(signal, [])
    end
  end

  test "rejects unsupported runtime command signals" do
    signal = %Signal{
      type: :unknown_command,
      payload: %{},
      metadata: %{},
      occurred_at: DateTime.utc_now()
    }

    assert {:error, {:unsupported_signal, :unknown_command}} =
             SignalInterpreter.apply(signal, [])
  end

  test "rejects malformed interpreter inputs" do
    assert {:ok, %Signal{} = signal} = Signal.cancel_run(@run_id)

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             SignalInterpreter.apply(signal, :bad_opts)

    assert {:error, :invalid_signal} = SignalInterpreter.apply(%{}, [])
  end

  test "rejects a signal and runtime partition mismatch before storage access" do
    assert {:ok, %Signal{} = signal} =
             Signal.cancel_run(@run_id, partition: "tenant_acme")

    assert {:error, {:partition_mismatch, :signal}} =
             SignalInterpreter.apply(signal, partition: "tenant_globex")
  end

  test "emits symmetric command spans with durable correlation and error outcomes" do
    prefix = [:squidie, :runtime, :command, :apply]
    events = Enum.map([:start, :stop, :exception], &Enum.concat(prefix, [&1]))
    TelemetryCapture.attach(events)

    assert {:ok, trace} = Trace.new_root(causation_id: "command-signal-1")

    assert {:ok, %Signal{} = signal} =
             Signal.cancel_run(@run_id,
               id: "command-signal-1",
               trace: trace,
               partition: "tenant_acme"
             )

    assert {:error, {:partition_mismatch, :signal}} =
             SignalInterpreter.apply(signal, partition: "tenant_globex")

    assert_receive {:telemetry_event, start_event,
                    %{monotonic_time: started_at, system_time: system_time}, start_metadata}

    assert start_event == Enum.concat(prefix, [:start])
    assert is_integer(started_at)
    assert is_integer(system_time)

    assert start_metadata == %{
             command_type: :cancel_run,
             signal_id: "command-signal-1",
             partition: "tenant_acme",
             run_id: @run_id,
             trace_id: trace.trace_id,
             span_id: trace.span_id,
             causation_id: trace.causation_id,
             outcome: :unknown
           }

    assert_receive {:telemetry_event, stop_event,
                    %{duration: duration, monotonic_time: stopped_at}, stop_metadata}

    assert stop_event == Enum.concat(prefix, [:stop])
    assert duration >= 0
    assert stopped_at >= started_at
    assert stop_metadata == %{start_metadata | outcome: :error}
    refute_receive {:telemetry_event, _, _, _}
  end

  test "journal control modules reject unsupported or malformed direct signals" do
    assert {:ok, %Signal{} = replay_signal} = Signal.replay_run(@run_id)
    assert {:ok, %Signal{} = cancel_signal} = Signal.cancel_run(@run_id)

    assert {:error, {:unsupported_signal, :replay_run}} =
             Cancellation.apply_signal(replay_signal, [])

    assert {:error, :invalid_signal} = Cancellation.apply_signal(%{}, [])

    assert {:error, {:unsupported_signal, :cancel_run}} =
             ManualControl.apply_signal(cancel_signal, [])

    assert {:error, :invalid_signal} = ManualControl.apply_signal(%{}, [])
  end

  test "manual control rejects malformed signals for supported command types" do
    for type <- [:resume_run, :approve_run, :reject_run] do
      signal = %Signal{
        type: type,
        payload: %{},
        metadata: %{},
        occurred_at: DateTime.utc_now()
      }

      assert {:error, {:invalid_signal, ^type}} = ManualControl.apply_signal(signal, [])
      assert {:error, {:invalid_signal, ^type}} = SignalInterpreter.apply(signal, [])
    end
  end

  test "manual control validates opts before supported signal fallback" do
    for build_signal <- [
          fn -> Signal.resume_run(@run_id, %{}) end,
          fn -> Signal.approve_run(@run_id, %{}) end,
          fn -> Signal.reject_run(@run_id, %{}) end
        ] do
      assert {:ok, %Signal{} = signal} = build_signal.()

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               ManualControl.apply_signal(signal, :bad_opts)

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               SignalInterpreter.apply(signal, :bad_opts)
    end
  end
end
