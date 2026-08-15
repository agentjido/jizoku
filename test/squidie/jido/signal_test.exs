defmodule Squidie.Jido.SignalTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Test.Storage
  alias Squidie.Workflow.Definition

  defmodule RecordOrder do
    use Squidie.Step, name: :record_order

    @impl Squidie.Step
    def run(%{order_id: order_id}, _context) do
      {:ok, %{order: %{id: order_id, status: "recorded"}}}
    end
  end

  defmodule OrderWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :order_id, :string
        end
      end

      step :record_order, RecordOrder
      transition :record_order, on: :ok, to: :complete
    end
  end

  defmodule ManualWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :pause, :pause
      approval_step :review, output: :approval
      transition :pause, on: :ok, to: :review
      transition :review, on: :ok, to: :complete
      transition :review, on: :error, to: :complete
    end
  end

  @now ~U[2026-08-11 12:00:00.000000Z]
  @queue "jido-commands"
  @partition "tenant_acme"
  @source "/my_app/orders"
  @checkpoint_version_key "squidie.workflow_projection.checkpoint_version"
  @command_history_count_key "squidie.workflow_projection.command_history_count"
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7",
    parent_span_id: "b7ad6b7169203331",
    causation_id: "upstream-command-123",
    tracestate: "vendor=value"
  }

  setup do
    assert {:ok, server} = Storage.start_link(self(), @now)
    on_exit(fn -> Storage.stop(server) end)

    {:ok, server: server, storage: {Storage, server: server}}
  end

  test "applies a raw Jido start command without an adapter call", %{
    server: server,
    storage: storage
  } do
    assert {:ok, jido_signal} = raw_start_signal()

    assert {:ok, started} = Squidie.apply_signal(jido_signal, runtime_opts(storage))
    assert started.workflow == Definition.serialize_workflow(OrderWorkflow)
    assert started.partition == @partition
    assert started.queue == @queue
    assert started.input == %{order_id: "ord_123"}

    assert [receipt] = started.command_history
    assert receipt.source == @source
    assert receipt.signal_id == "jido-start-123"
    assert receipt.signal_type == "start_run"
    assert receipt.metadata == %{"request_id" => "req_123"}
    assert receipt.occurred_at == @now
    assert receipt.trace == @trace

    started_state = persistence_state(server)
    assert {:ok, ^started} = Squidie.apply_signal(jido_signal, runtime_opts(storage))
    assert persistence_state(server) == started_state

    conflicting = put_in(jido_signal.data["payload"]["input"]["order_id"], "ord_conflict")
    assert {:error, :conflict} = Squidie.apply_signal(conflicting, runtime_opts(storage))
    assert persistence_state(server) == started_state

    assert {:ok, completed} =
             Squidie.execute_next(runtime_opts(storage, partition: @partition))

    assert completed.status == :completed
    assert completed.context.order == %{id: "ord_123", status: "recorded"}
  end

  test "rejects full-revision checkpoints that could drop source provenance", %{
    storage: storage
  } do
    assert {:ok, jido_signal} = raw_start_signal()
    assert {:ok, started} = Squidie.apply_signal(jido_signal, runtime_opts(storage))

    assert {:ok, scoped_storage} =
             Squidie.Runtime.Journal.Storage.scope(storage, @partition)

    assert {:ok, agent} = WorkflowAgent.rebuild(scoped_storage, started.run_id)
    assert {:ok, thread} = Journal.load_thread(scoped_storage, {:run, started.run_id})

    source_less_projection =
      Map.update!(agent.state.projection, :command_history, fn history ->
        Enum.map(history, &Map.delete(&1, :source))
      end)

    incompatible_projections = [
      source_less_projection
      |> Map.delete(@checkpoint_version_key)
      |> Map.delete(@command_history_count_key),
      Map.put(source_less_projection, @command_history_count_key, 0)
    ]

    assert Map.get(agent.state.projection, @checkpoint_version_key) == 2
    refute Map.has_key?(agent.state.projection, :checkpoint_version)
    refute Map.has_key?(agent.state.projection, :command_history_count)

    for projection <- incompatible_projections do
      assert :ok =
               Journal.put_checkpoint(
                 scoped_storage,
                 {:run, started.run_id},
                 projection,
                 thread.rev,
                 now: @now
               )

      assert {:ok, rebuilt} = WorkflowAgent.rebuild(scoped_storage, started.run_id)

      assert [%{source: @source}] =
               Squidie.Runtime.WorkflowAgent.Projection.command_history(rebuilt.state.projection)
    end
  end

  test "applies raw Jido control commands idempotently without journal writes on duplicate", %{
    server: server,
    storage: storage
  } do
    assert {:ok, start_signal} = raw_start_signal()
    assert {:ok, started} = Squidie.apply_signal(start_signal, runtime_opts(storage))

    assert {:ok, cancel_signal} = raw_cancel_signal(started.run_id)
    assert {:ok, cancelled} = Squidie.apply_signal(cancel_signal, runtime_opts(storage))
    assert cancelled.status == :cancelled

    assert %{
             source: @source,
             metadata: %{},
             occurred_at: @now
           } = Enum.at(cancelled.command_history, 0)

    state_after_cancel = persistence_state(server)

    assert {:ok, duplicate} = Squidie.apply_signal(cancel_signal, runtime_opts(storage))
    assert duplicate == cancelled
    assert persistence_state(server) == state_after_cancel
  end

  test "rejects malformed or unsupported raw Jido signals without writes", %{
    server: server,
    storage: storage
  } do
    assert {:ok, malformed} =
             Jido.Signal.new("squidie.runtime.command.cancel_run", %{},
               id: "malformed-command",
               source: @source,
               subject: Ecto.UUID.generate()
             )

    before_state = persistence_state(server)

    assert {:error, {:invalid_signal_adapter, {:data, :missing_signal_payload}}} =
             Squidie.apply_signal(malformed, runtime_opts(storage))

    assert persistence_state(server) == before_state

    assert {:ok, unsupported} =
             Jido.Signal.new("orders.created", %{"order_id" => "secret-order"},
               id: "domain-signal",
               source: "/my_app/orders",
               subject: "secret-order"
             )

    assert {:error, {:invalid_signal_adapter, {:type, :unsupported}}} =
             Squidie.apply_signal(unsupported, runtime_opts(storage))

    assert persistence_state(server) == before_state
    refute inspect(Squidie.apply_signal(unsupported, runtime_opts(storage))) =~ "secret-order"

    invalid_source = %{malformed | source: ""}

    assert {:error, {:invalid_signal_adapter, {:source, :invalid}}} =
             Squidie.apply_signal(invalid_source, runtime_opts(storage))

    assert persistence_state(server) == before_state

    assert {:ok, mismatched_time} = raw_start_signal()
    mismatched_time = %{mismatched_time | time: "2026-08-11T13:00:00.000000Z"}

    assert {:error, {:invalid_signal_adapter, {:time, :mismatch}}} =
             Squidie.apply_signal(mismatched_time, runtime_opts(storage))

    assert persistence_state(server) == before_state

    assert {:ok, ambiguous} = raw_cancel_signal(Ecto.UUID.generate())
    ambiguous = %{ambiguous | data: Map.put(ambiguous.data, :payload, %{})}

    assert {:error, {:invalid_signal_adapter, {:payload, :ambiguous}}} =
             Squidie.apply_signal(ambiguous, runtime_opts(storage))

    assert persistence_state(server) == before_state

    assert {:ok, conflicting} = raw_start_signal()

    conflicting =
      put_in(
        conflicting.data["payload"]["input"],
        %{"order_id" => "ord_string", order_id: "ord_atom"}
      )

    assert {:error, {:invalid_signal, {:input, :conflicting_keys}}} =
             Squidie.apply_signal(conflicting, runtime_opts(storage))

    assert persistence_state(server) == before_state
  end

  test "validates routing before converting a raw Jido signal", %{
    server: server,
    storage: storage
  } do
    assert {:ok, jido_signal} = raw_start_signal()
    malformed = %{jido_signal | data: %{}}
    before_state = persistence_state(server)

    assert {:error, {:invalid_option, {:runtime, :invalid}}} =
             Squidie.apply_signal(malformed, runtime_opts(storage, runtime: :unsupported))

    assert persistence_state(server) == before_state
  end

  test "normalizes raw Jido resume and approval attributes", %{storage: storage} do
    assert {:ok, start_signal} =
             Signal.start_run(ManualWorkflow, :manual, %{},
               partition: @partition,
               occurred_at: @now
             )

    assert {:ok, _started} = Squidie.apply_signal(start_signal, runtime_opts(storage))
    assert {:ok, paused} = Squidie.execute_next(runtime_opts(storage, partition: @partition))
    assert paused.manual_state.kind == "pause"

    attrs = %{"actor" => "ops", "comment" => "continue", "metadata" => %{"team" => "risk"}}
    assert {:ok, resume_signal} = raw_manual_signal(:resume_run, paused.run_id, attrs)
    assert {:ok, resumed} = Squidie.apply_signal(resume_signal, runtime_opts(storage))
    assert resumed.manual_state == nil

    assert {:ok, review} = Squidie.execute_next(runtime_opts(storage, partition: @partition))
    assert review.manual_state.kind == "approval"

    assert {:ok, approve_signal} = raw_manual_signal(:approve_run, review.run_id, attrs)
    assert {:ok, approved} = Squidie.apply_signal(approve_signal, runtime_opts(storage))
    assert approved.status == :completed

    assert [approval, resume | _start] = Enum.reverse(approved.command_history)
    assert approval.payload.attributes == %{actor: "ops", comment: "continue"}
    assert approval.metadata == %{"team" => "risk"}
    assert resume.payload.attributes == %{actor: "ops", comment: "continue"}
    assert resume.metadata == %{"team" => "risk"}
  end

  test "keeps native Squidie signals compatible", %{storage: storage} do
    assert {:ok, signal} =
             Signal.start_run(OrderWorkflow, :manual, %{order_id: "ord_native"},
               partition: @partition,
               occurred_at: @now
             )

    assert {:ok, started} = Squidie.apply_signal(signal, runtime_opts(storage))
    assert started.input == %{order_id: "ord_native"}
    assert started.partition == @partition
  end

  defp raw_start_signal do
    workflow = Definition.serialize_workflow(OrderWorkflow)

    data = %{
      "type" => "start_run",
      "payload" => %{
        "workflow" => workflow,
        "trigger" => "manual",
        "input" => %{"order_id" => "ord_123"}
      },
      "metadata" => %{"request_id" => "req_123"},
      "occurred_at" => DateTime.to_iso8601(@now),
      "idempotency_key" => "start:ord_123",
      "partition" => @partition
    }

    with {:ok, signal} <-
           Jido.Signal.new("squidie.runtime.command.start_run", data,
             id: "jido-start-123",
             source: @source,
             subject: workflow,
             time: DateTime.to_iso8601(@now),
             datacontenttype: "application/vnd.squidie.runtime-signal+json"
           ) do
      {:ok, %{signal | extensions: %{"correlation" => @trace}}}
    end
  end

  defp raw_cancel_signal(run_id) do
    data = %{
      "payload" => %{"run_id" => run_id},
      "idempotency_key" => "cancel:#{run_id}",
      "partition" => @partition
    }

    Jido.Signal.new("squidie.runtime.command.cancel_run", data,
      id: "jido-cancel-123",
      source: @source,
      time: DateTime.to_iso8601(@now),
      datacontenttype: "application/vnd.squidie.runtime-signal+json"
    )
  end

  defp raw_manual_signal(type, run_id, attributes) do
    type_name = Atom.to_string(type)

    Jido.Signal.new(
      "squidie.runtime.command.#{type_name}",
      %{
        "payload" => %{"run_id" => run_id, "attributes" => attributes},
        "idempotency_key" => "#{type_name}:#{run_id}",
        "partition" => @partition
      },
      id: "jido-#{type_name}-123",
      source: @source,
      time: DateTime.to_iso8601(@now)
    )
  end

  defp runtime_opts(storage, overrides \\ []) do
    Keyword.merge(
      [runtime: :journal, journal_storage: storage, queue: @queue, now: @now],
      overrides
    )
  end

  defp persistence_state(server) do
    server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
