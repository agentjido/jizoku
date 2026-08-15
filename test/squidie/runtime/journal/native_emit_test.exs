defmodule Squidie.Runtime.Journal.NativeEmitTest do
  use ExUnit.Case, async: false

  alias Jido.Agent.Directive
  alias Jido.Storage.ETS
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.AgentRecovery
  alias Squidie.Runtime.Jido.Outbox
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Executor
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  defmodule FaultStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts), do: delegate(:get_checkpoint, [key], opts)

    @impl Jido.Storage
    def put_checkpoint(key, data, opts), do: delegate(:put_checkpoint, [key, data], opts)

    @impl Jido.Storage
    def delete_checkpoint(key, opts), do: delegate(:delete_checkpoint, [key], opts)

    @impl Jido.Storage
    def load_thread(thread_id, opts), do: delegate(:load_thread, [thread_id], opts)

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      kinds = Enum.map(entries, & &1.kind)
      mode = Keyword.fetch!(opts, :fault_mode)
      key = {__MODULE__, Keyword.fetch!(opts, :fault_ref)}

      case fault_action(mode, kinds, key) do
        {:fail_before, reason} ->
          Process.put(key, true)
          {:error, reason}

        {:commit_then_fail, reason} ->
          Process.put(key, true)
          {:ok, _thread} = delegate(:append_thread, [thread_id, entries], opts)
          {:error, reason}

        :corrupt_completion ->
          Process.put(key, true)

          corrupted_entries =
            Enum.map(entries, fn entry ->
              payload =
                put_in(
                  entry.payload,
                  [:data, :result, "__squidie_jido_result__", "fingerprint"],
                  "corrupted"
                )

              %{entry | payload: payload}
            end)

          {:ok, _thread} = delegate(:append_thread, [thread_id, corrupted_entries], opts)
          {:error, :conflict}

        :delegate ->
          delegate(:append_thread, [thread_id, entries], opts)
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts), do: delegate(:delete_thread, [thread_id], opts)

    defp fault_action(mode, kinds, key) do
      if Process.get(key) do
        :delegate
      else
        pending_fault_action(mode, kinds)
      end
    end

    defp pending_fault_action(:completion_timeout, [:attempt_completed]) do
      {:commit_then_fail, :injected_completion_timeout}
    end

    defp pending_fault_action(:completion_failure, [:attempt_completed]) do
      {:fail_before, :injected_completion_failure}
    end

    defp pending_fault_action(:corrupt_completion, [:attempt_completed]) do
      :corrupt_completion
    end

    defp pending_fault_action(:run_timeout, [
           :runnable_applied,
           :jido_signal_enqueued,
           :run_terminal
         ]) do
      {:commit_then_fail, :injected_run_timeout}
    end

    defp pending_fault_action(:run_failure, [
           :runnable_applied,
           :jido_signal_enqueued,
           :run_terminal
         ]) do
      {:fail_before, :injected_run_failure}
    end

    defp pending_fault_action(_mode, _kinds), do: :delegate

    defp delegate(callback, args, opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      apply(
        adapter,
        callback,
        Enum.concat(args, [delegate_opts ++ Keyword.take(opts, [:expected_rev])])
      )
    end
  end

  defmodule EmitAction do
    use Jido.Action,
      name: "native_emit_source",
      description: "Emits a durable Jido signal",
      schema: []

    @impl Jido.Action
    def run(_input, context) do
      invocation_key = {__MODULE__, context.run_id}
      :persistent_term.put(invocation_key, :persistent_term.get(invocation_key, 0) + 1)

      {:ok, signal} =
        Jido.Signal.new("sample.order.accepted", %{"order_id" => "order-1"},
          id: "signal-order-1",
          source: "/minimal_host/orders",
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

  defmodule RecoverEmit do
    use Squidie.Step, name: :native_emit_recovery

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{emit_recovered: true}}
    end
  end

  defmodule RecoveryWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :emit, EmitAction
      step :recover_emit, RecoverEmit
      transition :emit, on: :ok, to: :complete
      transition :emit, on: :error, to: :recover_emit
      transition :recover_emit, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :squidie_native_emit_test}
  @run_id "33333333-3333-5333-8333-333333333333"
  @now ~U[2026-08-12 18:30:00.000000Z]

  setup do
    previous = Application.fetch_env(:squidie, :jido_effects)
    previous_emit = Application.fetch_env(:squidie, :jido_emit_effects)
    Application.put_env(:squidie, :jido_effects, :enabled)
    Application.put_env(:squidie, :jido_emit_effects, :enabled)
    cleanup_storage()
    :persistent_term.erase({EmitAction, @run_id})

    on_exit(fn ->
      restore_activation(previous)
      restore_emit_activation(previous_emit)
      :persistent_term.erase({EmitAction, @run_id})
      cleanup_storage()
    end)

    :ok
  end

  test "repairs an unknown completion without rerunning the action" do
    assert {:ok, _started} = start_run()

    assert {:error, :injected_completion_timeout} =
             execute(fault_storage(:completion_timeout), "claim-1")

    assert invocation_count() == 1
    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :jido_signal_enqueued))

    assert {:ok, completed} = execute(@storage, "claim-repair")
    assert completed.terminal_status == :completed
    assert completed.context.accepted == true
    assert invocation_count() == 1
    assert_emit_batch_once()
  end

  test "unknown and fail-before run appends converge through the durable completion" do
    assert {:ok, _started} = start_run()
    assert {:error, :injected_run_timeout} = execute(fault_storage(:run_timeout), "claim-1")
    assert invocation_count() == 1
    assert_emit_batch_once()

    assert {:ok, :none} = execute(@storage, "claim-repair")
    assert {:ok, completed} = Inspection.snapshot(@storage, @run_id, now: @now)
    assert completed.terminal_status == :completed
    assert invocation_count() == 1
    assert_emit_batch_once()

    cleanup_storage()
    :persistent_term.erase({EmitAction, @run_id})
    assert {:ok, _started} = start_run()

    assert {:error, :injected_run_failure} =
             execute(fault_storage(:run_failure), "claim-failure")

    assert invocation_count() == 1
    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :jido_signal_enqueued))

    assert {:ok, repaired} = execute(@storage, "claim-repair-after-failure")
    assert repaired.terminal_status == :completed
    assert invocation_count() == 1
    assert_emit_batch_once()
  end

  test "a fail-before completion reruns after lease expiry and enqueues once" do
    assert {:ok, _started} = start_run()

    assert {:error, :injected_completion_failure} =
             execute(fault_storage(:completion_failure), "claim-failure")

    assert invocation_count() == 1
    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    refute Enum.any?(dispatch_entries, &(&1.type == :attempt_completed))

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :jido_signal_enqueued))

    retry_at = DateTime.add(@now, 301, :second)

    assert {:ok, completed} =
             execute(@storage, "claim-after-expiry", retry_at, DateTime.add(retry_at, 1, :second))

    assert completed.terminal_status == :completed
    assert invocation_count() == 2
    assert_emit_batch_once()
  end

  test "a malformed durable emit resolves through a sanitized error transition" do
    assert {:ok, _started} = start_run(RecoveryWorkflow)

    assert {:error, :conflicting_completion} =
             execute(fault_storage(:corrupt_completion), "claim-corrupt")

    assert invocation_count() == 1
    assert {:ok, run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries_before, &(&1.type == :jido_signal_enqueued))
    refute Enum.any?(run_entries_before, &(&1.type == :runnable_applied))

    assert {:ok, recovered} = AgentRecovery.recover(@storage, @run_id)
    assert recovered.applied_attempts == []
    assert {:ok, ^run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})

    assert {:ok, after_resolution} =
             execute(@storage, "claim-resolve", DateTime.add(@now, 1, :second))

    assert after_resolution.terminal? == false
    assert invocation_count() == 1
    refute inspect(after_resolution) =~ "corrupted"
    refute Map.has_key?(after_resolution.context, :accepted)

    assert {:ok, run_entries_after} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries_after, &(&1.type == :jido_signal_enqueued))

    assert {:ok, completed} =
             execute(@storage, "claim-recovery", DateTime.add(@now, 2, :second))

    assert completed.terminal_status == :completed
    assert completed.context.emit_recovered == true
    refute Map.has_key?(completed.context, :accepted)
    assert invocation_count() == 1

    assert %{transition: %{"on" => "error", "to" => "recover_emit"}} =
             Enum.find(completed.attempts, &(&1.step == "emit"))
  end

  defp start_run(workflow \\ Workflow) do
    Starter.start_run(workflow, :manual, %{},
      journal_storage: @storage,
      run_id: @run_id,
      now: @now
    )
  end

  defp execute(storage, claim_id) do
    execute(storage, claim_id, @now, DateTime.add(@now, 1, :second))
  end

  defp execute(storage, claim_id, %DateTime{} = now) do
    execute(storage, claim_id, now, DateTime.add(now, 1, :second))
  end

  defp execute(storage, claim_id, %DateTime{} = now, %DateTime{} = finished_at) do
    Executor.execute_next(
      runtime: :journal,
      journal_storage: storage,
      now: now,
      finished_at: finished_at,
      owner_id: "native-emit-worker",
      claim_id: claim_id,
      claim_token: claim_id <> "-token"
    )
  end

  defp fault_storage(mode) do
    {FaultStorage, delegate: @storage, fault_mode: mode, fault_ref: make_ref()}
  end

  defp invocation_count do
    :persistent_term.get({EmitAction, @run_id}, 0)
  end

  defp assert_emit_batch_once do
    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    types = Enum.map(entries, & &1.type)
    applied_index = Enum.find_index(types, &(&1 == :runnable_applied))

    assert Enum.slice(types, applied_index, 3) == [
             :runnable_applied,
             :jido_signal_enqueued,
             :run_terminal
           ]

    assert Enum.count(types, &(&1 == :jido_signal_enqueued)) == 1

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert [pending] = Outbox.pending(Projection.jido_outbox(agent.state.projection))
    assert pending["signal_id"] == "signal-order-1"
  end

  defp restore_activation({:ok, value}) do
    Application.put_env(:squidie, :jido_effects, value)
  end

  defp restore_activation(:error) do
    Application.delete_env(:squidie, :jido_effects)
  end

  defp restore_emit_activation({:ok, value}) do
    Application.put_env(:squidie, :jido_emit_effects, value)
  end

  defp restore_emit_activation(:error) do
    Application.delete_env(:squidie, :jido_emit_effects)
  end

  defp cleanup_storage do
    tables = [
      :squidie_native_emit_test_checkpoints,
      :squidie_native_emit_test_threads,
      :squidie_native_emit_test_thread_meta
    ]

    Enum.each(tables, fn table ->
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end)
  rescue
    ArgumentError -> :ok
  end
end
