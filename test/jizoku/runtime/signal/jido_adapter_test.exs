defmodule Jizoku.Runtime.Signal.JidoAdapterTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Signal.JidoAdapter

  @occurred_at ~U[2026-05-26 12:00:00Z]
  @run_id "2b81e1da-04d8-4f0e-99fa-9dbd0ff7ec5d"
  @workflow __MODULE__.CheckoutWorkflow
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7",
    parent_span_id: "b7ad6b7169203331",
    causation_id: "signal-123",
    tracestate: "vendor=value"
  }

  test "converts a Jizoku command signal to a Jido signal envelope" do
    assert {:ok, signal} =
             Signal.start_run(@workflow, :manual, %{"order_id" => "ord_123"},
               metadata: %{request_id: "req_123"},
               occurred_at: @occurred_at,
               idempotency_key: "start:ord_123"
             )

    assert {:ok,
            %Jido.Signal{
              type: "jizoku.runtime.command.start_run",
              source: "/jizoku/runtime/commands",
              subject: "Elixir.Jizoku.Runtime.Signal.JidoAdapterTest.CheckoutWorkflow",
              time: "2026-05-26T12:00:00Z",
              datacontenttype: "application/vnd.jizoku.runtime-signal+json",
              data: %{
                "type" => "start_run",
                "payload" => %{
                  "workflow" => "Elixir.Jizoku.Runtime.Signal.JidoAdapterTest.CheckoutWorkflow",
                  "trigger" => "manual",
                  "input" => %{"order_id" => "ord_123"}
                },
                "metadata" => %{request_id: "req_123"},
                "occurred_at" => "2026-05-26T12:00:00Z",
                "idempotency_key" => "start:ord_123"
              }
            }} = JidoAdapter.to_jido(signal)
  end

  test "round trips supported command signals through Jido envelopes" do
    signals = [
      Signal.start_run(@workflow, :manual, %{}, occurred_at: @occurred_at),
      Signal.start_cron(@workflow, :nightly, %{"signal_id" => "scheduler-1"},
        occurred_at: @occurred_at
      ),
      Signal.approve_run(@run_id, %{actor: "ops"}, occurred_at: @occurred_at),
      Signal.reject_run(@run_id, %{comment: "nope"}, occurred_at: @occurred_at),
      Signal.resume_run(@run_id, %{actor: "ops"}, occurred_at: @occurred_at),
      Signal.cancel_run(@run_id, occurred_at: @occurred_at),
      Signal.replay_run(@run_id, allow_irreversible: true, occurred_at: @occurred_at),
      Signal.signal_run(@run_id, "payment.completed", %{status: "settled"},
        correlation: "pay_123",
        idempotency_key: "provider-event-123",
        occurred_at: @occurred_at
      )
    ]

    for {:ok, signal} <- signals do
      assert {:ok, jido_signal} = JidoAdapter.to_jido(signal)
      assert {:ok, ^signal} = JidoAdapter.from_jido(jido_signal)
    end
  end

  test "round trips partition as a top-level transport field" do
    assert {:ok, signal} =
             Signal.cancel_run(@run_id,
               partition: "tenant_acme",
               occurred_at: @occurred_at
             )

    assert {:ok, %Jido.Signal{data: %{"partition" => "tenant_acme"}} = jido_signal} =
             JidoAdapter.to_jido(signal)

    assert {:ok, ^signal} = JidoAdapter.from_jido(jido_signal)
  end

  test "preserves the exact outer Jido id and correlation extension with partition" do
    assert {:ok, signal} =
             Signal.cancel_run(@run_id,
               id: "external-command-123",
               trace: @trace,
               partition: "tenant_acme",
               occurred_at: @occurred_at
             )

    assert {:ok,
            %Jido.Signal{
              id: "external-command-123",
              extensions: %{"correlation" => @trace},
              data: %{"partition" => "tenant_acme"}
            } = jido_signal} = JidoAdapter.to_jido(signal)

    assert {:ok, ^signal} = JidoAdapter.from_jido(jido_signal)
  end

  test "preserves an external command source as audit provenance" do
    data = %{
      "payload" => %{"run_id" => @run_id}
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", data,
               id: "host-command-123",
               source: "/my_app/orders",
               time: DateTime.to_iso8601(@occurred_at)
             )

    assert {:ok,
            %Signal{
              source: "/my_app/orders",
              metadata: %{},
              occurred_at: @occurred_at
            } = runtime_signal} =
             JidoAdapter.from_jido(jido_signal)

    assert {:ok, %Jido.Signal{source: "/my_app/orders"}} =
             JidoAdapter.to_jido(runtime_signal)
  end

  test "accepts legacy Jido signals without correlation and rejects malformed extensions safely" do
    data = %{
      "type" => "cancel_run",
      "payload" => %{"run_id" => @run_id},
      "metadata" => %{},
      "occurred_at" => "2026-05-26T12:00:00Z"
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", data,
               id: "legacy-command-123",
               source: "/jizoku/runtime/commands",
               subject: @run_id
             )

    assert {:ok, %Signal{id: "legacy-command-123", trace: nil}} =
             JidoAdapter.from_jido(jido_signal)

    malformed = %{
      jido_signal
      | extensions: %{
          "correlation" => %{
            "trace_id" => "sensitive-malformed-value",
            "span_id" => @trace.span_id
          }
        }
    }

    assert {:error, {:invalid_signal_adapter, {:trace, {:trace_id, :invalid}}}} =
             JidoAdapter.from_jido(malformed)

    refute inspect(JidoAdapter.from_jido(malformed)) =~ "sensitive-malformed-value"

    invalid_id = %{jido_signal | id: <<255>>}

    assert {:error, {:invalid_signal_adapter, {:id, :invalid}}} =
             JidoAdapter.from_jido(invalid_id)
  end

  test "converts serialized Jido signal data back to a Jizoku signal" do
    idempotency_key = "cancel:#{@run_id}"

    data = %{
      "type" => "cancel_run",
      "payload" => %{"run_id" => @run_id},
      "metadata" => %{"request_id" => "req_123"},
      "occurred_at" => "2026-05-26T12:00:00Z",
      "idempotency_key" => idempotency_key
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", data,
               source: "/jizoku/runtime/commands",
               subject: @run_id,
               time: "2026-05-26T12:00:00Z",
               datacontenttype: "application/vnd.jizoku.runtime-signal+json"
             )

    assert {:ok,
            %Signal{
              type: :cancel_run,
              payload: %{run_id: @run_id},
              metadata: %{"request_id" => "req_123"},
              occurred_at: @occurred_at,
              idempotency_key: ^idempotency_key
            }} = JidoAdapter.from_jido(jido_signal)
  end

  test "converts atom-shaped Jido signal data back to a Jizoku signal" do
    data = %{
      type: :replay_run,
      payload: %{run_id: @run_id, allow_irreversible: true},
      metadata: %{},
      occurred_at: @occurred_at
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.replay_run", data,
               source: "/jizoku/runtime/commands",
               subject: @run_id
             )

    assert {:ok,
            %Signal{
              type: :replay_run,
              payload: %{run_id: @run_id, allow_irreversible: true},
              metadata: %{},
              occurred_at: @occurred_at,
              idempotency_key: nil
            }} = JidoAdapter.from_jido(jido_signal)
  end

  test "rejects inbound Jido command signals whose subject does not match command identity" do
    data = %{
      "type" => "cancel_run",
      "payload" => %{"run_id" => @run_id},
      "metadata" => %{},
      "occurred_at" => "2026-05-26T12:00:00Z"
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", data,
               source: "/jizoku/runtime/commands",
               subject: "6c0de7fd-82a9-46c8-a9e9-40317458b6da"
             )

    assert {:error, {:invalid_signal_adapter, {:subject, :mismatch}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects inbound start command signals whose subject does not match workflow" do
    data = %{
      "type" => "start_run",
      "payload" => %{
        "workflow" => "Elixir.Jizoku.Runtime.Signal.JidoAdapterTest.CheckoutWorkflow",
        "trigger" => "manual",
        "input" => %{}
      },
      "metadata" => %{},
      "occurred_at" => "2026-05-26T12:00:00Z"
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.start_run", data,
               source: "/jizoku/runtime/commands",
               subject: "Elixir.OtherWorkflow"
             )

    assert {:error, {:invalid_signal_adapter, {:subject, :mismatch}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects inbound cron command signals without a trigger" do
    data = %{
      "type" => "start_cron",
      "payload" => %{
        "workflow" => "Elixir.Jizoku.Runtime.Signal.JidoAdapterTest.CheckoutWorkflow",
        "trigger" => nil,
        "input" => %{}
      },
      "metadata" => %{},
      "occurred_at" => "2026-05-26T12:00:00Z"
    }

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.start_cron", data,
               source: "/jizoku/runtime/commands",
               subject: "Elixir.Jizoku.Runtime.Signal.JidoAdapterTest.CheckoutWorkflow"
             )

    assert {:error, {:invalid_signal_adapter, {:trigger, :expected_non_empty_string}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects inbound run command signals with malformed run ids" do
    invalid_cases = [
      {"jizoku.runtime.command.approve_run", "approve_run",
       %{"run_id" => "not-a-uuid", "attributes" => %{}}},
      {"jizoku.runtime.command.reject_run", "reject_run",
       %{"run_id" => "not-a-uuid", "attributes" => %{}}},
      {"jizoku.runtime.command.resume_run", "resume_run",
       %{"run_id" => "not-a-uuid", "attributes" => %{}}},
      {"jizoku.runtime.command.cancel_run", "cancel_run", %{"run_id" => "not-a-uuid"}},
      {"jizoku.runtime.command.replay_run", "replay_run",
       %{"run_id" => "not-a-uuid", "allow_irreversible" => false}},
      {"jizoku.runtime.command.signal_run", "signal_run",
       %{
         "run_id" => "not-a-uuid",
         "event" => "payment.completed",
         "correlation" => "pay_123",
         "event_payload" => %{}
       }}
    ]

    for {jido_type, command_type, payload} <- invalid_cases do
      data = %{
        "type" => command_type,
        "payload" => payload,
        "metadata" => %{},
        "occurred_at" => "2026-05-26T12:00:00Z"
      }

      assert {:ok, jido_signal} =
               Jido.Signal.new(jido_type, data,
                 source: "/jizoku/runtime/commands",
                 subject: "not-a-uuid"
               )

      assert {:error, {:invalid_signal_adapter, {:run_id, :invalid}}} =
               JidoAdapter.from_jido(jido_signal)
    end
  end

  test "rejects Jido signals outside the Jizoku command taxonomy" do
    assert {:ok, jido_signal} =
             Jido.Signal.new("other.command", %{}, source: "/other", subject: "other")

    assert {:error, {:invalid_signal_adapter, {:type, :unsupported}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects unsupported adapter inputs and command types" do
    assert {:error, {:invalid_signal_adapter, {:signal, :expected_jizoku_signal}}} =
             JidoAdapter.to_jido(%{})

    assert {:error, {:invalid_signal_adapter, {:signal, :expected_jido_signal}}} =
             JidoAdapter.from_jido(%{})

    invalid_signal = %Signal{
      type: :unsupported,
      payload: %{run_id: @run_id},
      metadata: %{},
      occurred_at: @occurred_at
    }

    assert {:error, {:invalid_signal_adapter, {:type, :unsupported}}} =
             JidoAdapter.to_jido(invalid_signal)

    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.unsupported", %{"type" => "unsupported"},
               source: "/jizoku/runtime/commands",
               subject: @run_id
             )

    assert {:error, {:invalid_signal_adapter, {:type, :unsupported}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects malformed Jizoku signal structs without raising" do
    signal = %Signal{
      type: :start_run,
      payload: %{workflow: "Elixir.BadWorkflow"},
      metadata: %{},
      occurred_at: @occurred_at
    }

    assert {:error, {:invalid_signal_adapter, {:trigger, :missing}}} =
             JidoAdapter.to_jido(signal)
  end

  test "rejects malformed Jizoku signal structs without subject identity" do
    signal = %Signal{
      type: :cancel_run,
      payload: %{},
      metadata: %{},
      occurred_at: @occurred_at
    }

    assert {:error, {:invalid_signal_adapter, {:payload, :missing_subject_identity}}} =
             JidoAdapter.to_jido(signal)
  end

  test "rejects malformed Jizoku Jido signal payloads" do
    assert {:ok, jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", %{},
               source: "/jizoku/runtime/commands",
               subject: @run_id
             )

    assert {:error, {:invalid_signal_adapter, {:data, :missing_signal_payload}}} =
             JidoAdapter.from_jido(jido_signal)
  end

  test "rejects mismatched and malformed Jizoku Jido signal data" do
    invalid_cases = [
      {
        "jizoku.runtime.command.cancel_run",
        %{
          "type" => "cancel_run",
          "payload" => %{"run_id" => @run_id},
          "metadata" => [],
          "occurred_at" => "2026-05-26T12:00:00Z"
        },
        {:metadata, :expected_map}
      },
      {
        "jizoku.runtime.command.cancel_run",
        %{
          "type" => "cancel_run",
          "payload" => %{"run_id" => @run_id},
          "metadata" => %{},
          "occurred_at" => "not-a-date"
        },
        {:occurred_at, :expected_datetime}
      },
      {
        "jizoku.runtime.command.cancel_run",
        %{
          "type" => "cancel_run",
          "payload" => %{"run_id" => @run_id},
          "metadata" => %{},
          "occurred_at" => "2026-05-26T12:00:00Z",
          "idempotency_key" => ""
        },
        {:idempotency_key, :expected_non_empty_string}
      },
      {
        "jizoku.runtime.command.replay_run",
        %{
          "type" => "replay_run",
          "payload" => %{"run_id" => @run_id, "allow_irreversible" => "yes"},
          "metadata" => %{},
          "occurred_at" => "2026-05-26T12:00:00Z"
        },
        {:allow_irreversible, :expected_boolean}
      },
      {
        "jizoku.runtime.command.cancel_run",
        %{
          "type" => :reject_run,
          "payload" => %{"run_id" => @run_id, "attributes" => %{}},
          "metadata" => %{},
          "occurred_at" => "2026-05-26T12:00:00Z"
        },
        {:type, {:mismatch, :reject_run}}
      },
      {
        "jizoku.runtime.command.cancel_run",
        %{
          "type" => "not_a_command",
          "payload" => %{"run_id" => @run_id},
          "metadata" => %{},
          "occurred_at" => "2026-05-26T12:00:00Z"
        },
        {:type, {:mismatch, "not_a_command"}}
      }
    ]

    for {jido_type, data, reason} <- invalid_cases do
      assert {:ok, jido_signal} =
               Jido.Signal.new(jido_type, data,
                 source: "/jizoku/runtime/commands",
                 subject: @run_id
               )

      assert {:error, {:invalid_signal_adapter, ^reason}} = JidoAdapter.from_jido(jido_signal)
    end
  end
end
