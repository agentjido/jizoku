defmodule Squidie.Runtime.Jido.OutboxTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Jido.Outbox

  @run_id "11111111-1111-5111-8111-111111111111"
  @runnable_key "#{@run_id}:emit:1"
  @now ~U[2026-08-12 18:00:00.000000Z]

  test "prepares a stable storage-safe signal intent and round-trips the envelope" do
    signal = signal("signal-1", %{"order_id" => "ord-1"})

    assert {:ok, first} = Outbox.prepare(signal, @run_id, @runnable_key)
    assert {:ok, second} = Outbox.prepare(signal, @run_id, @runnable_key)
    assert first == second
    assert first.route == "default"
    assert first.signal_id == "signal-1"
    assert is_binary(first.outbox_id)
    assert is_binary(first.signal_fingerprint)

    assert {:ok, decoded} = Outbox.decode_signal(first.signal)
    assert decoded.id == signal.id
    assert decoded.source == signal.source
    assert decoded.type == signal.type
    assert decoded.subject == signal.subject
    assert decoded.time == signal.time
    assert decoded.datacontenttype == signal.datacontenttype
    assert decoded.dataschema == signal.dataschema
    assert decoded.data == signal.data
    assert decoded.extensions == signal.extensions
  end

  test "rejects embedded dispatch state and unsafe signal payloads" do
    embedded = %{signal("signal-1", %{}) | jido_dispatch: {:noop, []}}

    assert {:error, {:invalid_jido_emit, :embedded_dispatch}} =
             Outbox.prepare(embedded, @run_id, @runnable_key)

    unsafe = signal("signal-2", %{"pid" => self()})

    assert {:error, {:invalid_jido_emit, :signal}} =
             Outbox.prepare(unsafe, @run_id, @runnable_key)
  end

  test "projects enqueue and acknowledgement idempotently" do
    assert {:ok, intent} =
             Outbox.prepare(signal("signal-1", %{"order_id" => "ord-1"}), @run_id, @runnable_key)

    assert {:ok, %Entry{} = enqueue} = Outbox.enqueue_entry(intent, @now)
    assert {:ok, acknowledge} = Outbox.acknowledge_entry(intent, DateTime.add(@now, 1, :second))

    pending = Outbox.apply_entry(Outbox.new_projection(), enqueue)
    assert [item] = Outbox.pending(pending)
    assert item["outbox_id"] == intent.outbox_id
    assert item["status"] == "pending"
    assert Outbox.anomalies(pending) == []

    duplicate = Outbox.apply_entry(pending, enqueue)
    assert duplicate == pending

    later = DateTime.add(@now, 5, :second)

    later_duplicate = %Entry{
      enqueue
      | occurred_at: later,
        data: Map.put(enqueue.data, :occurred_at, later)
    }

    assert Outbox.apply_entry(pending, later_duplicate) == pending

    delivered = Outbox.apply_entry(duplicate, acknowledge)
    assert Outbox.pending(delivered) == []
    assert [delivered_item] = Outbox.items(delivered)
    assert delivered_item["status"] == "delivered"
    assert delivered_item["delivered_at"] == DateTime.add(@now, 1, :second)
    assert Outbox.apply_entry(delivered, acknowledge) == delivered

    assert Outbox.apply_entry(delivered, later_duplicate) == delivered

    assert {:ok, later_acknowledge} =
             Outbox.acknowledge_entry(intent, DateTime.add(@now, 10, :second))

    assert Outbox.apply_entry(delivered, later_acknowledge) == delivered
  end

  test "keeps the first enqueue and reports conflicts and invalid acknowledgements" do
    assert {:ok, intent} =
             Outbox.prepare(signal("signal-1", %{"order_id" => "ord-1"}), @run_id, @runnable_key)

    assert {:ok, %Entry{} = enqueue} = Outbox.enqueue_entry(intent, @now)
    projection = Outbox.apply_entry(Outbox.new_projection(), enqueue)

    assert {:ok, conflicting_intent} =
             Outbox.prepare(
               signal("signal-1", %{"order_id" => "changed"}),
               @run_id,
               @runnable_key
             )

    assert conflicting_intent.outbox_id == intent.outbox_id
    assert {:ok, %Entry{} = conflicting} = Outbox.enqueue_entry(conflicting_intent, @now)
    after_conflict = Outbox.apply_entry(projection, conflicting)

    assert Outbox.items(after_conflict) == Outbox.items(projection)

    invalid_ack =
      %Entry{
        enqueue
        | type: :jido_signal_delivery_acknowledged,
          data: %{
            "outbox_id" => intent.outbox_id,
            "signal_fingerprint" => "wrong",
            run_id: @run_id,
            signal_id: "signal-1",
            occurred_at: @now
          }
      }

    after_ack = Outbox.apply_entry(after_conflict, invalid_ack)

    assert Enum.map(Outbox.anomalies(after_ack), & &1["reason"]) == [
             "conflicting_enqueue",
             "invalid_acknowledgement"
           ]

    assert [pending] = Outbox.pending(after_ack)
    assert pending["signal"]["data"] == %{"order_id" => "ord-1"}
  end

  test "rejects forged identities and fingerprints before exposing pending work" do
    assert {:ok, intent} =
             Outbox.prepare(signal("signal-1", %{"order_id" => "ord-1"}), @run_id, @runnable_key)

    assert {:ok, %Entry{} = enqueue} = Outbox.enqueue_entry(intent, @now)

    for data <- [
          Map.put(enqueue.data, "outbox_id", "forged"),
          Map.put(enqueue.data, "signal_fingerprint", "forged")
        ] do
      projection = Outbox.apply_entry(Outbox.new_projection(), %Entry{enqueue | data: data})
      assert Outbox.pending(projection) == []
      assert [%{"reason" => "malformed_entry"}] = Outbox.anomalies(projection)
    end
  end

  test "rejects orphan and mismatched acknowledgement identities" do
    assert {:ok, intent} =
             Outbox.prepare(signal("signal-1", %{"order_id" => "ord-1"}), @run_id, @runnable_key)

    assert {:ok, %Entry{} = enqueue} = Outbox.enqueue_entry(intent, @now)

    assert {:ok, %Entry{} = acknowledge} =
             Outbox.acknowledge_entry(intent, DateTime.add(@now, 1, :second))

    projection = Outbox.apply_entry(Outbox.new_projection(), enqueue)

    invalid_entries = [
      %Entry{
        acknowledge
        | data: Map.put(acknowledge.data, "outbox_id", "orphan")
      },
      %Entry{acknowledge | data: Map.put(acknowledge.data, :run_id, "another-run")},
      %Entry{acknowledge | data: Map.put(acknowledge.data, :signal_id, "another-signal")}
    ]

    after_invalid = Enum.reduce(invalid_entries, projection, &Outbox.apply_entry(&2, &1))
    assert [_pending] = Outbox.pending(after_invalid)

    assert Enum.map(Outbox.anomalies(after_invalid), & &1["reason"]) == [
             "invalid_acknowledgement",
             "invalid_acknowledgement",
             "invalid_acknowledgement"
           ]
  end

  test "rejects structurally valid checkpoints whose signal identity was changed" do
    assert {:ok, intent} =
             Outbox.prepare(signal("signal-1", %{"order_id" => "ord-1"}), @run_id, @runnable_key)

    assert {:ok, %Entry{} = enqueue} = Outbox.enqueue_entry(intent, @now)
    projection = Outbox.apply_entry(Outbox.new_projection(), enqueue)
    assert Outbox.valid_projection?(projection)

    tampered =
      put_in(
        projection,
        ["items", intent.outbox_id, "signal", "data", "order_id"],
        "changed"
      )

    refute Outbox.valid_projection?(tampered)

    assert {:ok, acknowledgement} =
             Outbox.acknowledge_entry(intent, DateTime.add(@now, 1, :second))

    delivered = Outbox.apply_entry(projection, acknowledgement)

    impossible_delivery =
      put_in(
        delivered,
        ["items", intent.outbox_id, "delivered_at"],
        DateTime.add(@now, -1, :second)
      )

    refute Outbox.valid_projection?(impossible_delivery)
  end

  test "redacts malformed acknowledgement identifiers from anomalies" do
    invalid_ids = [
      %{"secret" => "ack-secret"},
      String.duplicate("x", 256),
      <<255>>
    ]

    for invalid_id <- invalid_ids do
      malformed = %Entry{
        type: :jido_signal_delivery_acknowledged,
        thread: {:run, @run_id},
        data: %{
          "outbox_id" => invalid_id,
          "signal_fingerprint" => "wrong",
          run_id: @run_id,
          signal_id: "signal-1",
          occurred_at: @now
        },
        occurred_at: @now
      }

      projection = Outbox.apply_entry(Outbox.new_projection(), malformed)
      assert [%{"reason" => "invalid_acknowledgement"} = anomaly] = Outbox.anomalies(projection)
      refute Map.has_key?(anomaly, "outbox_id")
      refute inspect(Outbox.projection_anomalies(projection)) =~ "ack-secret"
    end
  end

  test "rejects forged checkpoint anomaly fields" do
    projection = Outbox.new_projection()

    forged =
      Map.put(projection, "anomalies", [
        %{
          "reason" => "invalid_acknowledgement",
          "entry_type" => "jido_signal_delivery_acknowledged",
          "outbox_id" => "customer-secret"
        }
      ])

    refute Outbox.valid_projection?(forged)
  end

  test "malformed facts are fail-closed projection anomalies" do
    malformed = %Entry{
      type: :jido_signal_enqueued,
      thread: {:run, @run_id},
      data: %{run_id: @run_id, signal_id: "signal-1"},
      occurred_at: @now
    }

    projection = Outbox.apply_entry(Outbox.new_projection(), malformed)
    assert Outbox.pending(projection) == []
    assert [%{"reason" => "malformed_entry"}] = Outbox.anomalies(projection)
  end

  defp signal(id, data) do
    {:ok, signal} =
      Jido.Signal.new("sample.order.ready", data,
        id: id,
        source: "/minimal_host/orders",
        subject: "order-1",
        time: DateTime.to_iso8601(@now),
        datacontenttype: "application/vnd.sample.order+json",
        dataschema: "https://example.test/schemas/order-ready"
      )

    %{signal | extensions: %{"opaque" => %{"tenant" => "tenant-1"}}}
  end
end
