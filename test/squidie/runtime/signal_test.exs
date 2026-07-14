defmodule Squidie.Runtime.SignalTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.ScheduleIdentity
  alias Squidie.Runtime.Signal

  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  @occurred_at ~U[2026-05-26 12:00:00Z]
  @run_id "2b81e1da-04d8-4f0e-99fa-9dbd0ff7ec5d"
  @workflow __MODULE__.CheckoutWorkflow

  test "builds a start run command signal" do
    assert {:ok,
            %Signal{
              type: :start_run,
              payload: %{
                workflow: "Elixir.Squidie.Runtime.SignalTest.CheckoutWorkflow",
                trigger: "manual",
                input: %{"order_id" => "ord_123"}
              },
              metadata: %{request_id: "req_123"},
              occurred_at: @occurred_at,
              idempotency_key: "start:ord_123"
            }} =
             Signal.start_run(@workflow, :manual, %{"order_id" => "ord_123"},
               metadata: %{request_id: "req_123"},
               occurred_at: @occurred_at,
               idempotency_key: "start:ord_123"
             )
  end

  test "validates and preserves an explicit partition" do
    assert {:ok, %Signal{partition: "tenant_acme"}} =
             Signal.cancel_run(@run_id,
               partition: "tenant_acme",
               occurred_at: @occurred_at
             )

    assert {:ok, %Signal{partition: nil}} =
             Signal.cancel_run(@run_id, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:partition, :invalid}}} =
             Signal.cancel_run(@run_id,
               partition: "contains secret/value",
               occurred_at: @occurred_at
             )
  end

  test "builds a start run command signal from serialized names" do
    assert {:ok,
            %Signal{
              type: :start_run,
              payload: %{
                workflow: "Elixir.ExternalWorkflow",
                trigger: "manual",
                input: %{}
              },
              occurred_at: @occurred_at
            }} =
             Signal.start_run("Elixir.ExternalWorkflow", "manual", %{}, occurred_at: @occurred_at)
  end

  test "builds a cron start command signal with scheduler identity" do
    input = %{
      "signal_id" => "checkout:nightly:2026-05-26T12",
      "intended_window" => %{
        "start_at" => "2026-05-26T12:00:00Z",
        "end_at" => "2026-05-26T13:00:00Z"
      }
    }

    assert {:ok,
            %Signal{
              type: :start_cron,
              payload: %{
                workflow: "Elixir.Squidie.Runtime.SignalTest.CheckoutWorkflow",
                trigger: "nightly",
                input: ^input
              },
              occurred_at: @occurred_at,
              idempotency_key: "checkout:nightly:2026-05-26T12"
            }} = Signal.start_cron(@workflow, :nightly, input, occurred_at: @occurred_at)
  end

  test "builds a cron start command signal with caller idempotency" do
    input = %{"signal_id" => "scheduler-signal"}

    assert {:ok,
            %Signal{
              type: :start_cron,
              idempotency_key: "caller-signal"
            }} =
             Signal.start_cron(@workflow, :nightly, input,
               occurred_at: @occurred_at,
               idempotency_key: "caller-signal"
             )
  end

  test "builds a cron start command signal deriving idempotency from intended window" do
    input = %{
      "intended_window" => %{
        "start_at" => "2026-05-26T12:00:00Z",
        "end_at" => "2026-05-26T13:00:00Z"
      }
    }

    {:ok, expected_signal_id} =
      ScheduleIdentity.signal_id(
        "Elixir.Squidie.Runtime.SignalTest.CheckoutWorkflow",
        "nightly",
        input
      )

    assert {:ok,
            %Signal{
              type: :start_cron,
              payload: %{
                workflow: "Elixir.Squidie.Runtime.SignalTest.CheckoutWorkflow",
                trigger: "nightly",
                input: ^input
              },
              idempotency_key: ^expected_signal_id
            }} = Signal.start_cron(@workflow, :nightly, input, occurred_at: @occurred_at)
  end

  test "builds a cron start command signal without scheduler identity" do
    assert {:ok,
            %Signal{
              type: :start_cron,
              idempotency_key: nil
            }} = Signal.start_cron(@workflow, :nightly, %{}, occurred_at: @occurred_at)
  end

  test "builds an approve run command signal" do
    assert {:ok,
            %Signal{
              type: :approve_run,
              payload: %{run_id: @run_id, attributes: %{"approved_by" => "ops"}},
              occurred_at: @occurred_at
            }} =
             Signal.approve_run(@run_id, %{"approved_by" => "ops"}, occurred_at: @occurred_at)
  end

  test "builds a reject run command signal" do
    assert {:ok,
            %Signal{
              type: :reject_run,
              payload: %{run_id: @run_id, attributes: %{"reason" => "missing inventory"}},
              occurred_at: @occurred_at
            }} =
             Signal.reject_run(@run_id, %{"reason" => "missing inventory"},
               occurred_at: @occurred_at
             )
  end

  test "builds a resume run command signal" do
    assert {:ok,
            %Signal{
              type: :resume_run,
              payload: %{run_id: @run_id, attributes: %{"resumed_by" => "operator"}},
              occurred_at: @occurred_at
            }} =
             Signal.resume_run(@run_id, %{"resumed_by" => "operator"}, occurred_at: @occurred_at)
  end

  test "builds a cancel run command signal" do
    assert {:ok,
            %Signal{
              type: :cancel_run,
              payload: %{run_id: @run_id},
              metadata: %{source: :dashboard},
              occurred_at: @occurred_at
            }} =
             Signal.cancel_run(@run_id,
               metadata: %{source: :dashboard},
               occurred_at: @occurred_at
             )
  end

  test "builds a replay run command signal" do
    assert {:ok,
            %Signal{
              type: :replay_run,
              payload: %{run_id: @run_id, allow_irreversible: true},
              occurred_at: @occurred_at
            }} = Signal.replay_run(@run_id, allow_irreversible: true, occurred_at: @occurred_at)
  end

  test "defaults envelope metadata and timestamp" do
    assert {:ok,
            %Signal{
              type: :cancel_run,
              metadata: %{},
              occurred_at: %DateTime{},
              id: id,
              trace: nil,
              idempotency_key: nil
            }} = Signal.cancel_run(@run_id)

    assert is_binary(id)
    assert {:ok, _id} = Ecto.UUID.cast(id)
  end

  test "normalizes explicit signal identity and trace" do
    assert {:ok,
            %Signal{
              id: "external-command-123",
              trace: @trace,
              partition: "tenant_acme"
            }} =
             Signal.cancel_run(@run_id,
               id: "external-command-123",
               trace: %{
                 "trace_id" => @trace.trace_id,
                 "span_id" => @trace.span_id
               },
               partition: "tenant_acme",
               occurred_at: @occurred_at
             )
  end

  test "rejects malformed signal identity and trace without echoing values" do
    assert {:error, {:invalid_signal, {:id, :expected_non_empty_string}}} =
             Signal.cancel_run(@run_id, id: "", occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:id, :too_long}}} =
             Signal.cancel_run(@run_id,
               id: String.duplicate("x", 256),
               occurred_at: @occurred_at
             )

    assert {:error, {:invalid_signal, {:trace, {:trace_id, :invalid}}}} =
             Signal.cancel_run(@run_id,
               trace: %{trace_id: "sensitive-malformed-value", span_id: @trace.span_id},
               occurred_at: @occurred_at
             )
  end

  test "rejects invalid signal payload data" do
    assert {:error, {:invalid_signal, {:payload, :expected_map}}} =
             Signal.start_run(@workflow, :manual, [], occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:attributes, :expected_map}}} =
             Signal.approve_run(@run_id, [], occurred_at: @occurred_at)
  end

  test "rejects invalid durable identity data" do
    assert {:error, {:invalid_signal, {:run_id, :invalid}}} =
             Signal.cancel_run("not-a-run-id", occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:run_id, :invalid}}} =
             Signal.cancel_run(123, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:workflow, :invalid}}} =
             Signal.start_run(nil, :manual, %{}, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:workflow, :invalid}}} =
             Signal.start_run(true, :manual, %{}, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:trigger, :required}}} =
             Signal.start_cron(@workflow, nil, %{}, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:trigger, :invalid}}} =
             Signal.start_run(@workflow, "", %{}, occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:trigger, :invalid}}} =
             Signal.start_run(@workflow, 123, %{}, occurred_at: @occurred_at)
  end

  test "rejects invalid envelope data" do
    assert {:error, {:invalid_signal, {:options, :expected_keyword}}} =
             Signal.cancel_run(@run_id, [:not_keyword])

    assert {:error, {:invalid_signal, {:options, :expected_keyword}}} =
             Signal.replay_run(@run_id, [:not_keyword])

    assert {:error, {:invalid_signal, {:options, :expected_keyword}}} =
             Signal.replay_run(@run_id, %{})

    assert {:error, {:invalid_signal, {:unknown, :unsupported}}} =
             Signal.cancel_run(@run_id, unknown: true)

    assert {:error, {:invalid_signal, {:metadata, :expected_map}}} =
             Signal.cancel_run(@run_id, metadata: [], occurred_at: @occurred_at)

    assert {:error, {:invalid_signal, {:occurred_at, :expected_datetime}}} =
             Signal.cancel_run(@run_id, occurred_at: "now")

    assert {:error, {:invalid_signal, {:idempotency_key, :expected_non_empty_string}}} =
             Signal.cancel_run(@run_id, idempotency_key: "", occurred_at: @occurred_at)
  end

  test "rejects unsupported replay options" do
    assert {:error, {:invalid_signal, {:allow_irreversible, :expected_boolean}}} =
             Signal.replay_run(@run_id, allow_irreversible: :yes, occurred_at: @occurred_at)
  end

  test "rejects invalid cron scheduler identity" do
    assert {:error,
            {:invalid_signal,
             {:schedule_identity, {:invalid_schedule_intended_window, %{start_at: 123}}}}} =
             Signal.start_cron(
               @workflow,
               :nightly,
               %{
                 "intended_window" => %{
                   "start_at" => 123,
                   "end_at" => "2026-05-26T13:00:00Z"
                 }
               },
               occurred_at: @occurred_at
             )
  end
end
