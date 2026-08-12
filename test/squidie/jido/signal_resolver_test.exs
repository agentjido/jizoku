defmodule Squidie.Jido.SignalResolverTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Signal.JidoAdapter
  alias Squidie.Runtime.Signal.JidoResolver
  alias Squidie.Test.Storage
  alias Squidie.Workflow.Definition

  defmodule IngressTestStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts), do: Storage.get_checkpoint(key, delegate_opts(opts))

    @impl Jido.Storage
    def put_checkpoint(key, data, opts),
      do: Storage.put_checkpoint(key, data, delegate_opts(opts))

    @impl Jido.Storage
    def delete_checkpoint(key, opts), do: Storage.delete_checkpoint(key, delegate_opts(opts))

    @impl Jido.Storage
    def load_thread(thread_id, opts), do: Storage.load_thread(thread_id, delegate_opts(opts))

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      wait_at_barrier(thread_id, opts)
      result = Storage.append_thread(thread_id, entries, delegate_opts(opts))

      if match?({:ok, _thread}, result) and String.contains?(thread_id, ":jido_signal:") and
           consume_fault(opts) do
        {:error, :timeout}
      else
        result
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts), do: Storage.delete_thread(thread_id, delegate_opts(opts))

    defp wait_at_barrier(thread_id, opts) do
      if String.contains?(thread_id, ":jido_signal:") and
           is_pid(Keyword.get(opts, :barrier_owner)) do
        send(Keyword.fetch!(opts, :barrier_owner), {:jido_append_ready, self()})

        receive do
          :release_jido_append -> :ok
        end
      end
    end

    defp consume_fault(opts) do
      case Keyword.get(opts, :fault_agent) do
        fault_agent when is_pid(fault_agent) ->
          Agent.get_and_update(fault_agent, fn
            true -> {true, false}
            false -> {false, false}
          end)

        nil ->
          false
      end
    end

    defp delegate_opts(opts) do
      Keyword.drop(opts, [:barrier_owner, :fault_agent])
    end
  end

  defmodule RecordOrder do
    use Squidie.Step, name: :record_resolved_order

    @impl Squidie.Step
    def run(%{order_id: order_id}, _context) do
      {:ok, %{resolved_order_id: order_id}}
    end
  end

  defmodule OrderWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :domain_signal do
        manual()

        payload do
          field :order_id, :string
        end
      end

      step :record_order, RecordOrder
      transition :record_order, on: :ok, to: :complete
    end
  end

  defmodule Resolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{type: "orders.created", data: %{"order_id" => order_id}}) do
      {:ok,
       {:start_run, Squidie.Jido.SignalResolverTest.OrderWorkflow, :domain_signal,
        %{order_id: order_id}}}
    end

    def resolve(%Jido.Signal{type: "orders.cancelled", data: %{"run_id" => run_id}}) do
      {:ok, {:cancel_run, run_id}}
    end

    def resolve(%Jido.Signal{}), do: {:error, :unrouted}
  end

  defmodule StringWorkflowResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}) do
      {:ok, {:start_run, "Elixir.Untrusted.Workflow", :manual, %{}}}
    end
  end

  defmodule RaisingResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}) do
      raise "resolver secret must not escape"
    end
  end

  defmodule ThrowingResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}), do: throw(:resolver_secret)
  end

  defmodule ExitingResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}), do: exit(:resolver_secret)
  end

  defmodule InvalidResultResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}), do: {:ok, {:unsupported, :command}}
  end

  defmodule MalformedResultResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}), do: :invalid_result
  end

  defmodule NonAtomRejectionResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{}), do: {:error, %{secret: "resolver secret"}}
  end

  defmodule AlternateResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{type: "orders.created"}) do
      {:ok,
       {:start_run, Squidie.Jido.SignalResolverTest.OrderWorkflow, :domain_signal,
        %{order_id: "alternate"}}}
    end
  end

  defmodule CommandResolver do
    @behaviour Squidie.Jido.SignalResolver

    @impl Squidie.Jido.SignalResolver
    def resolve(%Jido.Signal{type: "run.resume", data: %{"run_id" => run_id}}) do
      {:ok, {:resume_run, run_id, %{actor: "ops"}}}
    end

    def resolve(%Jido.Signal{type: "run.approve", data: %{"run_id" => run_id}}) do
      {:ok, {:approve_run, run_id, %{actor: "ops"}}}
    end

    def resolve(%Jido.Signal{type: "run.reject", data: %{"run_id" => run_id}}) do
      {:ok, {:reject_run, run_id, %{actor: "ops"}}}
    end

    def resolve(%Jido.Signal{type: "run.replay", data: %{"run_id" => run_id}}) do
      {:ok, {:replay_run, run_id, true}}
    end
  end

  @now ~U[2026-08-12 12:00:00.000000Z]
  @queue "jido-domain-signals"
  @partition "tenant_resolver"
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7",
    parent_span_id: nil,
    causation_id: "domain-request-123",
    tracestate: nil
  }

  setup do
    assert {:ok, server} = Storage.start_link(self(), @now)
    on_exit(fn -> Storage.stop(server) end)

    {:ok, server: server, storage: {Storage, server: server}}
  end

  test "routes a validated domain signal into an idempotent durable start", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_123")

    assert {:ok, started} =
             Squidie.apply_signal(signal, runtime_opts(storage, Resolver))

    assert started.workflow == Definition.serialize_workflow(OrderWorkflow)
    assert started.input == %{order_id: "ord_123"}
    assert started.queue == @queue
    assert started.partition == @partition

    assert [receipt] = started.command_history
    assert receipt.signal_type == "start_run"
    assert receipt.signal_id == "domain-order-123"
    assert receipt.idempotency_key == jido_identity("/my_app/orders", "domain-order-123")
    assert receipt.source == "/my_app/orders"
    assert receipt.occurred_at == @now
    assert receipt.trace == Map.drop(@trace, [:parent_span_id, :tracestate])

    assert receipt.metadata == %{
             "jido" => %{
               "subject" => "orders/ord_123",
               "type" => "orders.created"
             }
           }

    persisted = persistence_state(server)

    assert {:ok, ^started} =
             Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))

    assert persistence_state(server) == persisted

    conflicting = %{signal | data: %{"order_id" => "ord_changed"}}

    assert {:error, {:conflicting_jido_signal, :envelope}} =
             Squidie.apply_signal(conflicting, runtime_opts(storage, Resolver))

    assert persistence_state(server) == persisted

    assert {:ok, completed} = Squidie.execute_next(execution_opts(storage))
    assert completed.status == :completed
    assert completed.context.resolved_order_id == "ord_123"
  end

  test "routes a domain control command without letting it escape runtime scope", %{
    server: server,
    storage: storage
  } do
    assert {:ok, run} =
             Squidie.start(
               OrderWorkflow,
               :domain_signal,
               %{order_id: "ord_cancel"},
               runtime_opts(storage)
             )

    assert {:ok, signal} =
             Jido.Signal.new("orders.cancelled", %{"run_id" => run.run_id},
               id: "domain-cancel-123",
               source: "/my_app/orders",
               subject: "orders/ord_cancel",
               time: DateTime.to_iso8601(@now)
             )

    assert {:ok, cancelled} =
             Squidie.apply_signal(signal, runtime_opts(storage, Resolver))

    assert cancelled.status == :cancelled
    assert cancelled.partition == @partition
    assert cancelled.queue == @queue

    assert [receipt | _start] = Enum.reverse(cancelled.command_history)
    assert receipt.signal_id == "domain-cancel-123"
    assert receipt.idempotency_key == jido_identity("/my_app/orders", "domain-cancel-123")
    assert receipt.source == "/my_app/orders"
    assert receipt.metadata["jido"]["type"] == "orders.cancelled"

    persisted = persistence_state(server)

    assert {:ok, ^cancelled} =
             Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))

    assert persistence_state(server) == persisted
  end

  test "treats source and id together as the partition-scoped event identity", %{
    server: server,
    storage: storage
  } do
    assert {:ok, first_signal} = order_signal("ord_first")
    second_signal = %{first_signal | source: "/partner/orders"}

    assert {:ok, first} = Squidie.apply_signal(first_signal, runtime_opts(storage, Resolver))
    assert {:ok, second} = Squidie.apply_signal(second_signal, runtime_opts(storage, Resolver))

    refute first.run_id == second.run_id

    first_receipt = hd(first.command_history)
    second_receipt = hd(second.command_history)

    assert Map.fetch!(first_receipt, :idempotency_key) ==
             jido_identity("/my_app/orders", "domain-order-123")

    assert Map.fetch!(second_receipt, :idempotency_key) ==
             jido_identity("/partner/orders", "domain-order-123")

    jido_threads =
      server
      |> :sys.get_state()
      |> Map.fetch!(:threads)
      |> Map.keys()
      |> Enum.filter(&String.contains?(&1, ":jido_signal:"))

    assert [_first_thread, _second_thread] = jido_threads
  end

  test "repairs a committed ingress decision after an unknown append result", %{
    server: server
  } do
    assert {:ok, fault_agent} = Agent.start_link(fn -> true end)

    storage =
      {IngressTestStorage, server: server, fault_agent: fault_agent, partition: @partition}

    assert {:ok, signal} = order_signal("ord_unknown")

    assert {:error, :timeout} =
             Squidie.apply_signal(signal, runtime_opts(storage, Resolver))

    state_after_unknown = persistence_state(server)
    assert map_size(state_after_unknown.threads) == 1

    assert {:ok, started} =
             Squidie.apply_signal(
               signal,
               runtime_opts(storage, RaisingResolver, queue: "changed-queue")
             )

    assert started.input == %{order_id: "ord_unknown"}
    assert started.queue == @queue
    assert map_size(persistence_state(server).threads) > 1

    assert :not_found =
             Storage.load_thread(
               Journal.thread_id({:dispatch, "changed-queue"}, @partition),
               server: server
             )
  end

  test "serializes concurrent resolver decisions through one durable ingress winner", %{
    server: server
  } do
    storage = {IngressTestStorage, server: server, barrier_owner: self()}
    assert {:ok, signal} = order_signal("ord_race")

    first =
      Task.async(fn ->
        Squidie.apply_signal(signal, runtime_opts(storage, Resolver))
      end)

    second =
      Task.async(fn ->
        Squidie.apply_signal(signal, runtime_opts(storage, AlternateResolver))
      end)

    assert_barrier_callers([first.pid, second.pid])
    send(first.pid, :release_jido_append)
    assert {:ok, winner} = Task.await(first)
    send(second.pid, :release_jido_append)
    assert {:ok, duplicate} = Task.await(second)

    assert duplicate == winner
    assert winner.input == %{order_id: "ord_race"}
    assert one_thread_of_kind?(server, ":jido_signal:")
    assert one_thread_of_kind?(server, ":run:")
  end

  test "rejects the losing envelope in a concurrent identity conflict", %{server: server} do
    storage = {IngressTestStorage, server: server, barrier_owner: self()}
    assert {:ok, signal} = order_signal("ord_race_winner")
    changed = %{signal | data: %{"order_id" => "ord_race_loser"}}

    first =
      Task.async(fn ->
        Squidie.apply_signal(signal, runtime_opts(storage, Resolver))
      end)

    second =
      Task.async(fn ->
        Squidie.apply_signal(changed, runtime_opts(storage, Resolver))
      end)

    assert_barrier_callers([first.pid, second.pid])
    send(first.pid, :release_jido_append)
    assert {:ok, winner} = Task.await(first)
    send(second.pid, :release_jido_append)

    assert {:error, {:conflicting_jido_signal, :envelope}} = Task.await(second)
    assert winner.input == %{order_id: "ord_race_winner"}
    assert one_thread_of_kind?(server, ":jido_signal:")
    assert one_thread_of_kind?(server, ":run:")
  end

  test "isolates the same event identity across runtime partitions", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_partitioned")

    assert {:ok, first} =
             Squidie.apply_signal(signal, runtime_opts(storage, Resolver, partition: "tenant_a"))

    assert {:ok, second} =
             Squidie.apply_signal(signal, runtime_opts(storage, Resolver, partition: "tenant_b"))

    assert first.run_id == second.run_id
    assert first.partition == "tenant_a"
    assert second.partition == "tenant_b"

    state = :sys.get_state(server)
    threads = Map.fetch!(state, :threads)
    identity = event_key("/my_app/orders", "domain-order-123")

    assert Map.has_key?(threads, Journal.thread_id({:jido_signal, identity}, "tenant_a"))
    assert Map.has_key?(threads, Journal.thread_id({:jido_signal, identity}, "tenant_b"))
  end

  test "normalizes every bounded run-control resolver command" do
    run_id = Ecto.UUID.generate()

    for {type, expected_type, expected_payload} <- [
          {"run.resume", :resume_run, %{run_id: run_id, attributes: %{actor: "ops"}}},
          {"run.approve", :approve_run, %{run_id: run_id, attributes: %{actor: "ops"}}},
          {"run.reject", :reject_run, %{run_id: run_id, attributes: %{actor: "ops"}}},
          {"run.replay", :replay_run, %{run_id: run_id, allow_irreversible: true}}
        ] do
      assert {:ok, signal} =
               Jido.Signal.new(type, %{"run_id" => run_id},
                 id: "#{type}-1",
                 source: "/my_app/control",
                 time: DateTime.to_iso8601(@now)
               )

      assert {:ok, runtime_signal} = JidoResolver.resolve(CommandResolver, signal)
      assert runtime_signal.type == expected_type
      assert runtime_signal.payload == expected_payload
      assert runtime_signal.source == "/my_app/control"
      assert runtime_signal.idempotency_key == jido_identity("/my_app/control", "#{type}-1")
    end
  end

  test "fails closed for missing, invalid, rejected, and unsafe resolver results", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_invalid")
    initial = persistence_state(server)

    assert {:error, {:invalid_signal_adapter, {:type, :unsupported}}} =
             Squidie.apply_signal(signal, runtime_opts(storage))

    assert {:error, {:invalid_option, {:jido_signal_resolver, :invalid}}} =
             Squidie.apply_signal(signal, runtime_opts(storage, NotAResolver))

    assert {:error, {:invalid_jido_signal_command, :unsupported}} =
             Squidie.apply_signal(signal, runtime_opts(storage, StringWorkflowResolver))

    rejected = %{signal | type: "orders.unknown"}

    assert {:error, {:jido_signal_rejected, :unrouted}} =
             Squidie.apply_signal(rejected, runtime_opts(storage, Resolver))

    assert {:error, {:jido_signal_resolver_failed, :exception}} =
             Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))

    refute inspect(Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))) =~
             "resolver secret"

    assert persistence_state(server) == initial
  end

  test "validates the Jido envelope before invoking the host resolver", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_invalid_envelope")
    initial = persistence_state(server)

    invalid_signals = [
      %{signal | specversion: "0.3"},
      %{signal | id: ""},
      %{signal | source: ""},
      %{signal | type: ""},
      %{signal | subject: ""},
      %{signal | time: nil},
      %{signal | data: %{unsafe: self()}},
      %{signal | extensions: %{"correlation" => self()}}
    ]

    for invalid <- invalid_signals do
      assert {:error, {:invalid_signal_adapter, _reason}} =
               Squidie.apply_signal(invalid, runtime_opts(storage, RaisingResolver))
    end

    assert persistence_state(server) == initial
  end

  test "isolates resolver throw, exit, and malformed result failures", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_isolation")
    initial = persistence_state(server)

    assert {:error, {:jido_signal_resolver_failed, :throw}} =
             Squidie.apply_signal(signal, runtime_opts(storage, ThrowingResolver))

    assert {:error, {:jido_signal_resolver_failed, :exit}} =
             Squidie.apply_signal(signal, runtime_opts(storage, ExitingResolver))

    assert {:error, {:invalid_jido_signal_command, :unsupported}} =
             Squidie.apply_signal(signal, runtime_opts(storage, InvalidResultResolver))

    assert {:error, {:invalid_jido_signal_command, :invalid_resolver_result}} =
             Squidie.apply_signal(signal, runtime_opts(storage, MalformedResultResolver))

    assert {:error, {:invalid_jido_signal_command, :invalid_resolver_result}} =
             Squidie.apply_signal(signal, runtime_opts(storage, NonAtomRejectionResolver))

    refute inspect(Squidie.apply_signal(signal, runtime_opts(storage, ThrowingResolver))) =~
             "resolver_secret"

    assert persistence_state(server) == initial
  end

  test "rejects a malformed persisted ingress decision without target writes", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_malformed_persisted")
    assert {:ok, envelope} = JidoAdapter.domain_envelope(signal)
    assert {:ok, scoped_storage} = Options.storage_from_opts(runtime_opts(storage))

    assert {:ok, entry} =
             DispatchProtocol.new_entry(:jido_signal_resolved, %{
               event_key: envelope.event_key,
               signal_id: envelope.id,
               source: envelope.source,
               envelope_fingerprint: envelope.envelope_fingerprint,
               resolved_signal: %{
                 id: "different-id",
                 source: envelope.source,
                 type: :start_run,
                 payload: %{
                   workflow: Definition.serialize_workflow(OrderWorkflow),
                   trigger: "domain_signal",
                   input: %{order_id: "wrong"}
                 },
                 occurred_at: envelope.occurred_at,
                 partition: nil,
                 trace: envelope.trace,
                 metadata: %{"jido" => %{"type" => envelope.type}},
                 idempotency_key: envelope.identity_key
               },
               queue: @queue,
               occurred_at: envelope.occurred_at
             })

    assert {:ok, _thread} = Journal.append_entries(scoped_storage, [entry], expected_rev: 0)
    before = persistence_state(server)

    assert {:error, {:invalid_jido_signal_ingress, :malformed}} =
             Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))

    assert persistence_state(server) == before
    assert one_thread_of_kind?(server, ":jido_signal:")
    refute one_thread_of_kind?(server, ":run:")
  end

  test "does not let a resolver intercept a malformed reserved command", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} =
             Jido.Signal.new("squidie.runtime.command.cancel_run", %{},
               source: "/my_app/orders",
               time: DateTime.to_iso8601(@now)
             )

    initial = persistence_state(server)

    assert {:error, {:invalid_signal_adapter, {:data, :missing_signal_payload}}} =
             Squidie.apply_signal(signal, runtime_opts(storage, RaisingResolver))

    assert persistence_state(server) == initial
  end

  test "validates routing before conversion or resolver invocation", %{
    server: server,
    storage: storage
  } do
    assert {:ok, signal} = order_signal("ord_routing")
    initial = persistence_state(server)

    assert {:error, {:invalid_option, {:runtime, :invalid}}} =
             Squidie.apply_signal(
               signal,
               runtime_opts(storage, RaisingResolver, runtime: :invalid)
             )

    assert persistence_state(server) == initial
  end

  defp order_signal(order_id) do
    with {:ok, signal} <-
           Jido.Signal.new("orders.created", %{"order_id" => order_id},
             id: "domain-order-123",
             source: "/my_app/orders",
             subject: "orders/#{order_id}",
             time: DateTime.to_iso8601(@now)
           ) do
      {:ok, %{signal | extensions: %{"correlation" => @trace}}}
    end
  end

  defp runtime_opts(storage, resolver_or_overrides \\ [])

  defp runtime_opts(storage, resolver) when is_atom(resolver) do
    runtime_opts(storage, resolver, [])
  end

  defp runtime_opts(storage, overrides) when is_list(overrides) do
    Keyword.merge(
      [
        runtime: :journal,
        journal_storage: storage,
        queue: @queue,
        partition: @partition,
        now: @now
      ],
      overrides
    )
  end

  defp runtime_opts(storage, resolver, overrides) do
    storage
    |> runtime_opts(overrides)
    |> Keyword.put(:jido_signal_resolver, resolver)
  end

  defp execution_opts(storage) do
    runtime_opts(storage, owner_id: "jido-resolver-worker")
  end

  defp jido_identity(source, id) do
    "jido:" <> event_key(source, id)
  end

  defp event_key(source, id) do
    digest =
      {source, id}
      |> :erlang.term_to_binary([:deterministic])
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.url_encode64(padding: false)

    digest
  end

  defp assert_barrier_callers(expected_pids) do
    received =
      for _index <- expected_pids do
        assert_receive {:jido_append_ready, pid}
        pid
      end

    assert MapSet.new(received) == MapSet.new(expected_pids)
  end

  defp one_thread_of_kind?(server, marker) do
    count =
      server
      |> :sys.get_state()
      |> Map.fetch!(:threads)
      |> Map.keys()
      |> Enum.count(&String.contains?(&1, marker))

    count == 1
  end

  defp persistence_state(server) do
    server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
