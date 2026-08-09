defmodule Squidie.Runtime.AgentRecoveryTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.AgentRecovery
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Executor
  alias Squidie.Runtime.WorkflowAgent

  @storage {Jido.Storage.ETS, table: :squidie_agent_recovery_test}
  @run_id "run_123"
  @other_run_id "run_789"
  @workflow "BillingWorkflow"
  @charge_key "run_123:charge_card:1"
  @other_key "run_789:charge_card:1"
  @refund_key "run_123:refund_card:1"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @claimed_at ~U[2026-05-15 00:00:20Z]
  @completed_at ~U[2026-05-15 00:00:40Z]
  @lease_until ~U[2026-05-15 00:01:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

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

  test "preserves durable runnable trace through checkpoint-loss recovery" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               trace: @trace,
               occurred_at: @started_at
             })

    traced_runnables =
      Enum.map([charge_runnable(), refund_runnable()], &Map.put(&1, :trace, @trace))

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: traced_runnables,
               occurred_at: @visible_at
             })

    assert {:ok, charge_scheduled} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(trace: @trace)
             )

    assert {:ok, charge_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs(trace: @trace))

    assert {:ok, charge_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs(trace: @trace))

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [charge_scheduled, charge_claimed, charge_completed])

    assert {:ok, %{applied_attempts: [applied_attempt], dispatch_agent: dispatch_agent}} =
             AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert applied_attempt.trace == @trace
    assert dispatch_agent.state.projection.attempts[@refund_key].trace == @trace
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

  test "workflow recovery selectors hide fenced dispatches and results without writing" do
    seed_recoverable_journal()
    append_continuation_fence()

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    before_recovery = journal_state()

    assert WorkflowAgent.pending_dispatches(workflow_agent, dispatch_agent) == []
    assert WorkflowAgent.pending_results(workflow_agent, dispatch_agent) == []

    assert {:ok, %{runnables: []}} =
             WorkflowAgent.schedule_pending_dispatches(
               @storage,
               workflow_agent,
               dispatch_agent,
               now: @completed_at
             )

    assert {:ok, %{attempts: []}} =
             WorkflowAgent.apply_pending_results(
               @storage,
               workflow_agent,
               dispatch_agent,
               now: @completed_at
             )

    assert journal_state() == before_recovery
  end

  test "agent recovery stops at a fence before scheduling or applying" do
    seed_recoverable_journal()
    append_continuation_fence()
    before_recovery = journal_state()

    assert {:error, {:continuation_repair_required, @run_id}} =
             AgentRecovery.recover(@storage, @run_id, "default", now: @completed_at)

    assert journal_state() == before_recovery
  end

  test "executor stops at a fence before every ordinary recovery branch" do
    for scenario <- [:planned, :completed, :failed, :deferred] do
      cleanup_storage()
      seed_executor_scenario(scenario)
      append_continuation_fence()
      before_recovery = journal_state()

      assert {:error, {:continuation_repair_required, @run_id}} =
               Executor.execute_next(
                 runtime: :journal,
                 journal_storage: @storage,
                 queue: "default",
                 owner_id: "recovery-worker",
                 now: @completed_at
               )

      assert journal_state() == before_recovery
    end
  end

  test "executor recovers an unrelated run in the same queue before reporting a fence" do
    append_continuation_fence()
    seed_planned_journal(@other_run_id, @other_key)

    assert {:ok, %{run_id: @other_run_id}} =
             Executor.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               owner_id: "recovery-worker",
               now: @completed_at
             )

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    assert Enum.map(dispatch_entries, &{&1.type, &1.data.run_id}) == [
             {:run_continuation_fenced, @run_id},
             {:run_queued, @other_run_id},
             {:attempt_scheduled, @other_run_id}
           ]
  end

  test "targeted recovery ignores another run's fence in the same queue" do
    append_continuation_fence()
    seed_planned_journal(@other_run_id, @other_key)

    assert {:ok,
            %{
              scheduled_runnables: [%{run_id: @other_run_id, runnable_key: @other_key}],
              applied_attempts: []
            }} =
             AgentRecovery.recover(@storage, @other_run_id, "default", now: @completed_at)

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @run_id})
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

  defp seed_executor_scenario(:planned) do
    seed_planned_journal(@run_id, @charge_key)
  end

  defp seed_executor_scenario(result_type) do
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

    result_entry =
      case result_type do
        :completed ->
          entry!(:attempt_completed, completed_attrs())

        :failed ->
          entry!(:attempt_failed, failed_attrs())

        :deferred ->
          entry!(
            :attempt_completed,
            completed_attrs(
              result: %{},
              execution_opts: [defer: %{reason: %{code: "gateway_pending"}}, schedule_in: 30]
            )
          )
      end

    assert {:ok, _thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs()),
               entry!(:attempt_claimed, claimed_attrs()),
               result_entry
             ])
  end

  defp seed_planned_journal(run_id, runnable_key) do
    runnable =
      charge_runnable()
      |> Map.put(:run_id, run_id)
      |> Map.put(:runnable_key, runnable_key)
      |> Map.put(:idempotency_key, "#{run_id}:charge_card:payment_123")

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:run_started, %{
                 run_id: run_id,
                 workflow: @workflow,
                 occurred_at: @started_at
               }),
               entry!(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [runnable],
                 occurred_at: @visible_at
               })
             ])

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:run_queued, %{
                 run_id: run_id,
                 queue: "default",
                 occurred_at: @visible_at
               })
             ])
  end

  defp append_continuation_fence do
    fence =
      entry!(:run_continuation_fenced, %{
        run_id: @run_id,
        successor_run_id: "run_456",
        continuation_key: "page-2",
        workflow: @workflow,
        trigger: "continue",
        input: %{"cursor" => "page-2"},
        definition: :current,
        definition_version: nil,
        definition_fingerprint: "definition-fingerprint-v1",
        queue: "default",
        trace: @trace,
        occurred_at: @completed_at
      })

    assert {:ok, _thread} = Journal.append_entries(@storage, [fence])
  end

  defp journal_state do
    %{
      run: Journal.load_entries(@storage, {:run, @run_id}),
      dispatch: Journal.load_entries(@storage, {:dispatch, "default"})
    }
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

  defp failed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @charge_key,
        claim_id: "claim_1",
        claim_token_hash: "token_hash_1",
        queue: "default",
        error: %{"code" => "gateway_timeout"},
        occurred_at: @completed_at
      },
      Map.new(attrs)
    )
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
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
