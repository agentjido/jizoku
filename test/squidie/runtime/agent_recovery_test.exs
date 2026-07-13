defmodule Squidie.Runtime.AgentRecoveryTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.AgentRecovery
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.WorkflowAgent

  @storage {Jido.Storage.ETS, table: :squidie_agent_recovery_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @charge_key "run_123:charge_card:1"
  @refund_key "run_123:refund_card:1"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @claimed_at ~U[2026-05-15 00:00:20Z]
  @completed_at ~U[2026-05-15 00:00:40Z]
  @lease_until ~U[2026-05-15 00:01:00Z]

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "recovers missing dispatches before applying completed results after restart" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [charge_runnable(), refund_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, charge_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, charge_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, charge_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [
               charge_scheduled,
               charge_claimed,
               charge_completed
             ])

    assert {:ok,
            %{
              workflow_agent: workflow_agent,
              dispatch_agent: dispatch_agent,
              scheduled_runnables: [%{runnable_key: @refund_key}],
              applied_attempts: [%{runnable_key: @charge_key}]
            }} = AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert workflow_agent.state.thread_rev == 3
    assert dispatch_agent.state.thread_rev == 4
    assert WorkflowAgent.applied_runnable_keys(workflow_agent) == MapSet.new([@charge_key])

    assert [%{runnable_key: @refund_key, status: :available}] =
             DispatchAgent.visible_attempts(dispatch_agent, @completed_at)

    assert {:ok, [_run_started, _runnables_planned, applied_entry]} =
             Journal.load_entries(@storage, {:run, @run_id})

    assert applied_entry.type == :runnable_applied
    assert applied_entry.data.runnable_key == @charge_key
    assert applied_entry.data.result == %{"status" => "captured"}

    assert {:ok, [_scheduled, _claimed, _completed, recovered_scheduled]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert recovered_scheduled.type == :attempt_scheduled
    assert recovered_scheduled.data.runnable_key == @refund_key
  end

  test "leaves deferred completions for executor recovery instead of applying them generically" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [charge_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, charge_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, charge_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, charge_completed} =
             DispatchProtocol.new_entry(
               :attempt_completed,
               completed_attrs(
                 result: %{},
                 execution_opts: [
                   defer: %{reason: %{code: "gateway_pending"}},
                   schedule_in: 30
                 ]
               )
             )

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [
               charge_scheduled,
               charge_claimed,
               charge_completed
             ])

    assert {:ok,
            %{
              workflow_agent: workflow_agent,
              scheduled_runnables: [],
              applied_attempts: []
            }} = AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert workflow_agent.state.thread_rev == 2
    assert WorkflowAgent.applied_runnable_keys(workflow_agent) == MapSet.new()

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :runnable_applied))
  end

  test "treats repeated recovery as idempotent after durable entries were restored" do
    seed_recoverable_journal()

    assert {:ok,
            %{
              scheduled_runnables: [%{runnable_key: @refund_key}],
              applied_attempts: [%{runnable_key: @charge_key}]
            }} = AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert {:ok,
            %{
              workflow_agent: workflow_agent,
              dispatch_agent: dispatch_agent,
              scheduled_runnables: [],
              applied_attempts: []
            }} = AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert workflow_agent.state.thread_rev == 3
    assert dispatch_agent.state.thread_rev == 4

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(run_entries, &(&1.type == :runnable_applied)) == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
  end

  test "scopes direct restart recovery to the requested partition" do
    assert {:ok, partitioned_storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_acme")

    seed_recoverable_journal(partitioned_storage)

    assert {:ok,
            %{
              workflow_agent: %{state: %{partition: "tenant_acme"}},
              dispatch_agent: %{state: %{partition: "tenant_acme"}},
              scheduled_runnables: [%{runnable_key: @refund_key}],
              applied_attempts: [%{runnable_key: @charge_key}]
            }} =
             AgentRecovery.recover(@storage, @run_id, "default",
               partition: "tenant_acme",
               now: @completed_at
             )

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @run_id})
    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  defp seed_recoverable_journal(storage \\ @storage) do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [charge_runnable(), refund_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, charge_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, charge_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, charge_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    assert {:ok, _run_thread} = Journal.append_entries(storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(storage, [
               charge_scheduled,
               charge_claimed,
               charge_completed
             ])

    :ok
  end

  defp charge_runnable do
    Map.delete(scheduled_attrs(), :occurred_at)
  end

  defp refund_runnable do
    Map.delete(
      scheduled_attrs(
        runnable_key: @refund_key,
        idempotency_key: "#{@run_id}:refund_card:payment_456",
        step: "refund_card",
        input: %{"payment_id" => "pay_456"}
      ),
      :occurred_at
    )
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @charge_key,
        idempotency_key: "#{@run_id}:charge_card:payment_123",
        attempt_number: 1,
        queue: "default",
        step: "charge_card",
        input: %{"payment_id" => "pay_123"},
        visible_at: @visible_at,
        occurred_at: @visible_at
      },
      Map.new(attrs)
    )
  end

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @charge_key,
        claim_id: "claim_1",
        claim_token_hash: "token_hash_1",
        owner_id: "worker_1",
        queue: "default",
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp completed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @charge_key,
        claim_id: "claim_1",
        claim_token_hash: "token_hash_1",
        queue: "default",
        result: %{"status" => "captured"},
        occurred_at: @completed_at
      },
      Map.new(attrs)
    )
  end

  defp table_name(:checkpoints), do: :squidie_agent_recovery_test_checkpoints
  defp table_name(:threads), do: :squidie_agent_recovery_test_threads
  defp table_name(:thread_meta), do: :squidie_agent_recovery_test_thread_meta

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      table = table_name(suffix)

      delete_table(table)
    end
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
