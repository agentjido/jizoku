defmodule Squidie.Runtime.Journal.SignalInterpreterTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.Journal.Cancellation
  alias Squidie.Runtime.Journal.ManualControl
  alias Squidie.Runtime.Journal.SignalInterpreter
  alias Squidie.Runtime.Signal

  @run_id "3c82d86d-31a6-4d57-9e41-4f5c95125be6"

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
