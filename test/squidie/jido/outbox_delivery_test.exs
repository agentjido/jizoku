defmodule Squidie.Jido.OutboxDeliveryTest do
  use ExUnit.Case, async: false

  alias Jido.Agent.Directive
  alias Squidie.Runtime.Jido.Outbox
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Test
  alias Squidie.Test.TelemetryCapture

  defmodule EmitAction do
    use Jido.Action,
      name: "outbox_delivery_emit",
      description: "Emits a signal for durable delivery tests",
      schema: []

    @impl Jido.Action
    def run(_input, context) do
      {:ok, signal} =
        Jido.Signal.new("sample.order.accepted", %{"secret" => "payload-secret"},
          id: "delivery-signal-1",
          source: "/delivery-tests",
          subject: context.run_id
        )

      {:ok, %{accepted: true}, [%Directive.Emit{signal: signal}]}
    end
  end

  defmodule Workflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :emit, EmitAction
      transition :emit, on: :ok, to: :complete
    end
  end

  defmodule FailingAdapter do
    @behaviour Jido.Signal.Dispatch.Adapter

    @impl Jido.Signal.Dispatch.Adapter
    def validate_opts(opts) do
      if is_pid(Keyword.get(opts, :target)), do: {:ok, opts}, else: {:error, :invalid_target}
    end

    @impl Jido.Signal.Dispatch.Adapter
    def deliver(signal, opts) do
      send(Keyword.fetch!(opts, :target), {:failed_delivery, signal.id})
      {:error, %{secret: "adapter-secret"}}
    end
  end

  defmodule KillAdapter do
    @behaviour Jido.Signal.Dispatch.Adapter

    @impl Jido.Signal.Dispatch.Adapter
    def validate_opts(opts) do
      if is_pid(Keyword.get(opts, :target)), do: {:ok, opts}, else: {:error, :invalid_target}
    end

    @impl Jido.Signal.Dispatch.Adapter
    def deliver(signal, opts) do
      send(Keyword.fetch!(opts, :target), {:sent_before_crash, signal.id})
      Process.exit(self(), :kill)
    end
  end

  defmodule AckTimeoutStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      delegate(:get_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      delegate(:put_checkpoint, [key, data], opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      delegate(:delete_checkpoint, [key], opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      delegate(:load_thread, [thread_id], opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      key = {__MODULE__, Keyword.fetch!(opts, :fault_ref)}

      if Enum.map(entries, & &1.kind) == [:jido_signal_delivery_acknowledged] and
           is_nil(Process.get(key)) do
        Process.put(key, true)
        {:ok, _thread} = delegate(:append_thread, [thread_id, entries], opts)
        {:error, :injected_ack_timeout}
      else
        delegate(:append_thread, [thread_id, entries], opts)
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      delegate(:delete_thread, [thread_id], opts)
    end

    defp delegate(callback, args, opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      apply(
        adapter,
        callback,
        Enum.concat(args, [delegate_opts ++ Keyword.take(opts, [:expected_rev])])
      )
    end
  end

  @now ~U[2026-08-12 20:00:00.000000Z]

  setup do
    previous_emit = Application.fetch_env(:squidie, :jido_emit_effects)
    Application.put_env(:squidie, :jido_emit_effects, :enabled)

    on_exit(fn ->
      case previous_emit do
        {:ok, value} -> Application.put_env(:squidie, :jido_emit_effects, value)
        :error -> Application.delete_env(:squidie, :jido_emit_effects)
      end
    end)

    :ok
  end

  test "execute delivery acknowledges a terminal emit without exposing its payload" do
    TelemetryCapture.attach([
      [:squidie, :runtime, :jido_signal, :enqueued],
      [:squidie, :runtime, :jido_signal, :deliver, :start],
      [:squidie, :runtime, :jido_signal, :deliver, :stop],
      [:squidie, :runtime, :jido_signal, :delivered]
    ])

    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: Workflow,
               now: @now,
               queue: "priority",
               partition: "tenant_delivery",
               jido_dispatch_routes: routes(self())
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, completed} = Test.drain(runtime, run)

    assert_receive {:signal, %Jido.Signal{id: "delivery-signal-1", data: data}}
    assert data == %{"secret" => "payload-secret"}
    refute_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}
    refute inspect(completed) =~ "payload-secret"

    assert completed.jido_signals.pending_count == 0
    assert completed.jido_signals.delivered_count == 1
    assert [public_item] = completed.jido_signals.items
    assert public_item.signal_id == "delivery-signal-1"
    assert public_item.signal_type == "sample.order.accepted"
    assert public_item.route == "default"
    assert public_item.status == :delivered
    assert completed.queue == "priority"
    assert completed.partition == "tenant_delivery"
    refute Map.has_key?(public_item, :data)
    refute Map.has_key?(public_item, :source)

    assert {:ok, timeline} = Test.timeline(runtime, completed)

    assert Enum.map(
             Enum.filter(
               timeline.events,
               &(&1.type in [:jido_signal_enqueued, :jido_signal_delivered])
             ),
             & &1.type
           ) == [:jido_signal_enqueued, :jido_signal_delivered]

    assert Enum.map(
             Enum.filter(
               timeline.events,
               &(&1.type in [
                   :runnable_applied,
                   :jido_signal_enqueued,
                   :run_terminal,
                   :jido_signal_delivered
                 ])
             ),
             & &1.type
           ) == [
             :runnable_applied,
             :jido_signal_enqueued,
             :run_terminal,
             :jido_signal_delivered
           ]

    assert {:ok, explanation} = Test.explain(runtime, completed)
    assert explanation.details.jido_signal_delivery.pending_count == 0
    assert explanation.details.jido_signal_delivery.delivered_count == 1
    refute :deliver_jido_signals in explanation.next_actions
    refute inspect(timeline) =~ "payload-secret"
    refute inspect(explanation.details.jido_signal_delivery) =~ "payload-secret"

    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :enqueued], _, metadata}
    assert metadata.signal_id == "delivery-signal-1"
    assert metadata.route == "default"
    refute inspect(metadata) =~ "payload-secret"

    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :deliver, :start], _, _}
    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :deliver, :stop], _, _}
    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :delivered], _, _}

    assert [item] = outbox_items(runtime, run.run_id)
    assert item["status"] == "delivered"
    assert item["delivered_at"] == @now
  end

  test "dispatch failure stays pending and returns only structural diagnostics" do
    TelemetryCapture.attach([
      [:squidie, :runtime, :jido_signal, :deliver, :start],
      [:squidie, :runtime, :jido_signal, :deliver, :stop]
    ])

    assert {:ok, runtime} = Test.start_runtime(workflow: Workflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, _completed} = Test.drain(runtime, run)

    assert {:error, {:jido_signal_delivery_failed, failure}} =
             Squidie.deliver_jido_signals(run.run_id,
               journal_storage: runtime.storage,
               now: @now,
               jido_dispatch_routes: %{"default" => {FailingAdapter, target: self()}}
             )

    assert failure.reason == :dispatch_failed
    assert failure.signal_id == "delivery-signal-1"
    refute inspect(failure) =~ "adapter-secret"
    refute inspect(failure) =~ "payload-secret"
    assert_receive {:failed_delivery, "delivery-signal-1"}
    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :deliver, :start], _, _}

    assert_receive {:telemetry_event, [:squidie, :runtime, :jido_signal, :deliver, :stop], _,
                    metadata}

    assert metadata.outcome == :error
    refute Map.has_key?(metadata, :reason)
    refute inspect(metadata) =~ "adapter-secret"
    refute inspect(metadata) =~ "payload-secret"

    assert [%{"status" => "pending"}] = outbox_items(runtime, run.run_id)

    assert {:ok, explanation} = Test.explain(runtime, run)
    assert explanation.details.jido_signal_delivery.pending_count == 1
    assert :deliver_jido_signals in explanation.next_actions
    refute inspect(explanation.details.jido_signal_delivery) =~ "payload-secret"
  end

  test "ack conflicts retry without redelivery and terminal reconciliation needs no runnable" do
    assert {:ok, runtime} = Test.start_runtime(workflow: Workflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, _completed} = Test.drain(runtime, run)
    assert :ok = Test.inject_append_conflict(runtime, :run)

    assert {:ok, completed} =
             Squidie.execute_next(
               runtime: :journal,
               journal_storage: runtime.storage,
               now: @now,
               owner_id: "delivery-worker",
               jido_dispatch_routes: routes(self())
             )

    assert completed.terminal_status == :completed
    assert_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}
    refute_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}
    assert [%{"status" => "delivered"}] = outbox_items(runtime, run.run_id)
  end

  test "a send-before-ack crash redelivers the stable signal id" do
    assert {:ok, runtime} = Test.start_runtime(workflow: Workflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, _completed} = Test.drain(runtime, run)

    parent = self()

    {delivery_pid, monitor_ref} =
      spawn_monitor(fn ->
        Squidie.deliver_jido_signals(run.run_id,
          journal_storage: runtime.storage,
          now: @now,
          jido_dispatch_routes: %{"default" => {KillAdapter, target: parent}}
        )
      end)

    assert_receive {:sent_before_crash, "delivery-signal-1"}
    assert_receive {:DOWN, ^monitor_ref, :process, ^delivery_pid, :killed}
    assert [%{"status" => "pending"}] = outbox_items(runtime, run.run_id)

    assert {:ok, _snapshot} =
             Squidie.deliver_jido_signals(run.run_id,
               journal_storage: runtime.storage,
               now: @now,
               jido_dispatch_routes: routes(self())
             )

    assert_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}
    assert [%{"status" => "delivered"}] = outbox_items(runtime, run.run_id)
  end

  test "a committed-unknown acknowledgement converges without redelivery" do
    assert {:ok, runtime} = Test.start_runtime(workflow: Workflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, _completed} = Test.drain(runtime, run)

    fault_storage =
      {AckTimeoutStorage, delegate: runtime.storage, fault_ref: make_ref()}

    assert {:ok, snapshot} =
             Squidie.deliver_jido_signals(run.run_id,
               journal_storage: fault_storage,
               now: @now,
               jido_dispatch_routes: routes(self())
             )

    assert snapshot.jido_signals.pending_count == 0
    assert snapshot.jido_signals.delivered_count == 1
    assert_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}
    refute_receive {:signal, %Jido.Signal{id: "delivery-signal-1"}}

    assert {:ok, entries} =
             Squidie.Runtime.Journal.load_entries(runtime.storage, {:run, run.run_id})

    assert Enum.count(entries, &(&1.type == :jido_signal_delivery_acknowledged)) == 1
  end

  test "invalid or missing routes fail before journal mutation and redact adapter state" do
    assert {:ok, runtime} = Test.start_runtime(workflow: Workflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, _completed} = Test.drain(runtime, run)
    before_state = persistence_state(runtime)

    assert {:error, {:invalid_option, {:jido_dispatch_routes, :invalid}}} =
             Squidie.execute_next(
               runtime: :journal,
               journal_storage: runtime.storage,
               owner_id: "invalid-route-worker",
               now: @now,
               jido_dispatch_routes: %{"default" => {:pid, target: :not_a_pid}}
             )

    assert persistence_state(runtime) == before_state

    assert {:error, {:jido_signal_delivery_failed, failure}} =
             Squidie.deliver_jido_signals(run.run_id,
               journal_storage: runtime.storage,
               now: @now,
               jido_dispatch_routes: %{"another" => routes(self())["default"]}
             )

    assert failure.reason == :route_not_configured
    refute inspect(failure) =~ "payload-secret"
    assert persistence_state(runtime) == before_state
  end

  defp routes(target) do
    %{"default" => {:pid, target: target, delivery_mode: :async}}
  end

  defp outbox_items(runtime, run_id) do
    assert {:ok, storage} =
             Squidie.Runtime.Journal.Storage.scope(runtime.storage, runtime.partition)

    assert {:ok, agent} = WorkflowAgent.rebuild(storage, run_id)

    agent.state.projection
    |> Projection.jido_outbox()
    |> Outbox.items()
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
