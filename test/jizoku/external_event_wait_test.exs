defmodule Jizoku.ExternalEventWaitTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.Runtime.Journal

  @storage {ETS, table: :jizoku_external_event_wait_test}
  @queue "external-event-wait"
  @started_at ~U[2026-08-16 15:00:00Z]

  defmodule RecordEvent do
    use Jizoku.Step,
      name: "record_external_event",
      input_schema: [event: [type: :map, required: true]]

    @impl Jizoku.Step
    def run(%{event: event}, _context) do
      {:ok, %{received_status: event.status}}
    end
  end

  defmodule PaymentWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :payment_id, :string
        end
      end

      step :await_payment, :await_event,
        event: "payment.completed",
        correlation: [:payment_id],
        output: :event

      step :record_event, RecordEvent, input: [:event]

      transition :await_payment, on: :ok, to: :record_event
      transition :record_event, on: :ok, to: :complete
    end
  end

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "suspends durably and resumes once for a matching external event" do
    run_id = Ecto.UUID.generate()

    assert {:ok, %{run_id: ^run_id}} =
             Jizoku.start(
               PaymentWorkflow,
               :manual,
               %{payment_id: "pay_123"},
               runtime_options(run_id, @started_at)
             )

    assert {:ok, waiting} = Jizoku.execute_next(worker_options(@started_at))
    assert waiting.status == :paused

    assert %{
             kind: "event_wait",
             metadata: %{
               event: "payment.completed",
               correlation: "pay_123",
               wait_id: wait_id
             }
           } = waiting.manual_state

    assert is_binary(wait_id)

    assert :ok = delete_run_checkpoint(run_id)

    delivered_at = DateTime.add(@started_at, 1, :second)
    payload = %{payment_id: "pay_123", status: "settled"}

    assert {:error, {:event_wait_mismatch, mismatch}} =
             Jizoku.signal_run(
               run_id,
               "payment.completed",
               payload,
               Keyword.merge(control_options(delivered_at),
                 correlation: "pay_other",
                 idempotency_key: "provider-event-mismatch"
               )
             )

    assert mismatch.expected_correlation == "pay_123"
    assert mismatch.received_correlation == "pay_other"

    assert {:ok, resumed} =
             Jizoku.signal_run(
               run_id,
               "payment.completed",
               payload,
               control_options(delivered_at)
             )

    assert resumed.status == :running
    assert resumed.manual_state == nil
    assert [%{step: "record_event", status: :available}] = resumed.visible_attempts

    assert {:ok, duplicate} =
             Jizoku.signal_run(
               run_id,
               "payment.completed",
               payload,
               control_options(delivered_at)
             )

    assert duplicate.thread_revisions == resumed.thread_revisions

    assert {:error, {:idempotency_conflict, "provider-event-456"}} =
             Jizoku.signal_run(
               run_id,
               "payment.completed",
               %{payload | status: "conflicting"},
               control_options(delivered_at)
             )

    assert {:ok, completed} =
             Jizoku.execute_next(worker_options(DateTime.add(delivered_at, 1, :second)))

    assert completed.status == :completed
    assert completed.context.received_status == "settled"

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    assert Enum.count(entries, &(&1.type == :external_event_wait_opened)) == 1
    assert Enum.count(entries, &(&1.type == :external_event_received)) == 1
    assert Enum.count(entries, &(&1.type == :external_event_wait_resolved)) == 1
  end

  test "cancellation fences later external event delivery" do
    run_id = Ecto.UUID.generate()

    assert {:ok, _started} =
             Jizoku.start(
               PaymentWorkflow,
               :manual,
               %{payment_id: "pay_cancelled"},
               runtime_options(run_id, @started_at)
             )

    assert {:ok, %{status: :paused}} = Jizoku.execute_next(worker_options(@started_at))

    assert {:ok, %{status: :cancelled}} =
             Jizoku.cancel(
               run_id,
               journal_storage: @storage,
               queue: @queue,
               now: DateTime.add(@started_at, 1, :second)
             )

    assert {:error, :terminal_run} =
             Jizoku.signal_run(
               run_id,
               "payment.completed",
               %{status: "settled"},
               Keyword.merge(control_options(DateTime.add(@started_at, 2, :second)),
                 correlation: "pay_cancelled",
                 idempotency_key: "provider-event-after-cancel"
               )
             )

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    refute Enum.any?(entries, &(&1.type == :external_event_received))
  end

  test "concurrent deliveries select one durable continuation" do
    run_id = Ecto.UUID.generate()

    assert {:ok, _started} =
             Jizoku.start(
               PaymentWorkflow,
               :manual,
               %{payment_id: "pay_race"},
               runtime_options(run_id, @started_at)
             )

    assert {:ok, %{status: :paused}} = Jizoku.execute_next(worker_options(@started_at))

    tasks =
      for delivery <- ["first", "second"] do
        Task.async(fn ->
          receive do
            :deliver ->
              Jizoku.signal_run(
                run_id,
                "payment.completed",
                %{status: delivery},
                Keyword.merge(control_options(DateTime.add(@started_at, 1, :second)),
                  correlation: "pay_race",
                  idempotency_key: "provider-event-#{delivery}"
                )
              )
          end
        end)
      end

    Enum.each(tasks, &send(&1.pid, :deliver))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &match?({:ok, %{status: :running}}, &1)) == 1
    assert Enum.count(results, &match?({:error, {:event_wait_not_found, _details}}, &1)) == 1

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    assert Enum.count(entries, &(&1.type == :external_event_received)) == 1
    assert Enum.count(entries, &(&1.type == :external_event_wait_resolved)) == 1

    assert {:ok, %{status: :completed}} =
             Jizoku.execute_next(worker_options(DateTime.add(@started_at, 2, :second)))
  end

  defp runtime_options(run_id, now) do
    [
      run_id: run_id,
      journal_storage: @storage,
      queue: @queue,
      now: now
    ]
  end

  defp worker_options(now) do
    [
      journal_storage: @storage,
      queue: @queue,
      owner_id: "event-wait-worker",
      now: now
    ]
  end

  defp control_options(now) do
    [
      journal_storage: @storage,
      queue: @queue,
      correlation: "pay_123",
      idempotency_key: "provider-event-456",
      now: now
    ]
  end

  defp delete_run_checkpoint(run_id) do
    {adapter, opts} = @storage

    adapter.delete_checkpoint(
      {"jizoku", :checkpoint, Journal.thread_id({:run, run_id})},
      opts
    )
  end

  defp cleanup_storage do
    case :ets.whereis(:jizoku_external_event_wait_test) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end
  end
end
