defmodule Squidie.Runtime.WorkflowAgentTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.CommandReceipt
  alias Squidie.Runtime.Journal.DispatchScheduler
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Test.TelemetryCapture

  @storage {Jido.Storage.ETS, table: :squidie_workflow_agent_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
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

  test "rebuilds a keyed workflow agent from durable run entries" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert agent.id == "squidie.workflow.run_123"
    assert agent.state.run_id == @run_id
    assert agent.state.workflow == @workflow
    assert agent.state.thread_rev == 2
    assert %Projection{} = agent.state.projection
    assert WorkflowAgent.status(agent) == :running
    assert WorkflowAgent.applied_runnable_keys(agent) == MapSet.new()
  end

  test "preserves signal identity and trace in command and run projections" do
    assert {:ok, receipt} =
             CommandReceipt.new(
               :start_run,
               %{
                 run_id: @run_id,
                 signal_id: "signal-123",
                 trace: @trace,
                 payload: %{workflow: @workflow},
                 metadata: %{}
               },
               @started_at
             )

    assert {:ok, started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               trace: @trace,
               occurred_at: @started_at
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [receipt, started])
    TelemetryCapture.attach(Squidie.Telemetry.events())

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert Projection.trace(agent.state.projection) == @trace

    assert [command] = Projection.command_history(agent.state.projection)
    assert command.signal_id == "signal-123"
    assert command.trace == @trace
    refute_receive {:telemetry_event, _, _, _}
  end

  test "partitioned workflow agents have isolated collision-safe identities" do
    assert WorkflowAgent.agent_id(@run_id, nil) == "squidie.workflow.run_123"

    acme_id = WorkflowAgent.agent_id(@run_id, "tenant.acme")
    globex_id = WorkflowAgent.agent_id(@run_id, "tenant_globex")

    refute acme_id == globex_id
    refute acme_id == WorkflowAgent.agent_id("acme.run_123", "tenant")
  end

  test "rejects workflow writes and checkpoints through a mismatched partition" do
    assert {:ok, acme_storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_acme")

    assert {:ok, globex_storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_globex")

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} =
             Journal.append_entries(acme_storage, [run_started, runnables_planned])

    assert {:ok, agent} = WorkflowAgent.rebuild(acme_storage, @run_id)

    assert {:error, {:partition_mismatch, :workflow_agent}} =
             WorkflowAgent.put_checkpoint(globex_storage, agent, updated_at: @completed_at)

    assert {:error, {:partition_mismatch, :workflow_agent}} =
             WorkflowAgent.apply_result(globex_storage, agent, completed_attempt(),
               now: @completed_at
             )

    assert {:error, :not_found} = Journal.load_entries(globex_storage, {:run, @run_id})
    assert {:error, :not_found} = Journal.fetch_checkpoint(globex_storage, {:run, @run_id})
  end

  test "projects runtime command receipts from durable run entries" do
    projection =
      Projection.rebuild([
        workflow_entry(:run_signal_received, %{
          run_id: @run_id,
          signal_type: "start_run",
          payload: %{
            workflow: @workflow,
            trigger: "manual",
            input: %{"account_id" => "acct_123"}
          },
          metadata: %{request_id: "req_123"},
          occurred_at: @started_at
        }),
        workflow_entry(:run_started, %{
          run_id: @run_id,
          workflow: @workflow
        })
      ])

    assert Projection.command_history(projection) == [
             %{
               signal_type: "start_run",
               payload: %{
                 workflow: @workflow,
                 trigger: "manual",
                 input: %{"account_id" => "acct_123"}
               },
               metadata: %{request_id: "req_123"},
               occurred_at: @started_at
             }
           ]
  end

  test "rebuilds a receipt-only run thread without applying workflow state" do
    assert {:ok, run_signal_received} =
             DispatchProtocol.new_entry(:run_signal_received, %{
               run_id: @run_id,
               signal_type: "cancel_run",
               payload: %{run_id: @run_id},
               metadata: %{request_id: "req_123"},
               occurred_at: @started_at
             })

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [run_signal_received])
    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert agent.state.workflow == nil
    assert WorkflowAgent.status(agent) == :new

    assert Projection.command_history(agent.state.projection) == [
             %{
               signal_type: "cancel_run",
               payload: %{run_id: @run_id},
               metadata: %{request_id: "req_123"},
               occurred_at: @started_at
             }
           ]
  end

  test "rebuilds parent and child run lineage from durable run entries" do
    child_run_id = "child_run_123"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, child_started} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{
                 runnable_key: @runnable_key,
                 step: "charge_card",
                 attempt: 1
               },
               metadata: %{subscription_id: "sub_123"},
               started_at: @completed_at,
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, child_started])

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert [
             %{
               child_run_id: ^child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{
                 runnable_key: @runnable_key,
                 step: "charge_card",
                 attempt: 1
               },
               metadata: %{subscription_id: "sub_123"},
               started_at: @completed_at
             }
           ] = Projection.child_runs(agent.state.projection)
  end

  test "deduplicates matching child run lineage during reconstruction" do
    child_run_id = "child_run_123"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, child_started} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{runnable_key: @runnable_key, step: "charge_card", attempt: 1},
               occurred_at: @visible_at
             })

    assert {:ok, retry_child_started} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{runnable_key: "#{@runnable_key}:retry", step: "charge_card", attempt: 2},
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [run_started, child_started, retry_child_started])

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert [%{child_run_id: ^child_run_id}] = Projection.child_runs(agent.state.projection)
    assert Projection.anomalies(agent.state.projection) == []
  end

  test "records an anomaly for conflicting child run lineage during reconstruction" do
    child_run_id = "child_run_123"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, child_started} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{runnable_key: @runnable_key, step: "charge_card", attempt: 1},
               occurred_at: @visible_at
             })

    assert {:ok, conflicting_child_started} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: child_run_id,
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{runnable_key: @runnable_key, step: "charge_card", attempt: 1},
               metadata: %{subscription_id: "conflicting"},
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [
               run_started,
               child_started,
               conflicting_child_started
             ])

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert [%{child_run_id: ^child_run_id, metadata: %{}}] =
             Projection.child_runs(agent.state.projection)

    assert [
             %{
               reason: :conflicting_child_run,
               entry_type: :child_run_started,
               run_id: @run_id,
               child_run_id: ^child_run_id
             }
           ] = Projection.anomalies(agent.state.projection)
  end

  test "uses a current checkpoint instead of replaying the full run thread" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [run_started])

    checkpoint_projection = %Projection{
      run_id: @run_id,
      workflow: @workflow,
      status: :running,
      planned_runnables: %{@runnable_key => %{runnable_key: @runnable_key}}
    }

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, checkpoint_projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert agent.state.projection == checkpoint_projection
    assert WorkflowAgent.planned_runnable_keys(agent) == [@runnable_key]
  end

  test "upgrades older checkpoints without child run projection fields" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [run_started])

    checkpoint_projection =
      Map.delete(
        %Projection{
          run_id: @run_id,
          workflow: @workflow,
          status: :running
        },
        :child_runs
      )

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, checkpoint_projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert Projection.child_runs(agent.state.projection) == []
  end

  test "upgrades older top-level workflow checkpoints without trace" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [run_started])

    checkpoint_projection =
      Map.delete(
        %Projection{run_id: @run_id, workflow: @workflow, status: :running},
        :trace
      )

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:run, @run_id},
               checkpoint_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert Projection.trace(agent.state.projection) == nil
  end

  test "rebuilds failed checkpoints missing terminal_error from durable history" do
    error = %{
      code: "gateway_timeout",
      message: "gateway timeout",
      retryable?: false,
      secret: "should stay internal in projection"
    }

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :failed,
               error: error,
               occurred_at: @visible_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [run_started, run_terminal])

    checkpoint_projection =
      Map.delete(
        %Projection{
          run_id: @run_id,
          workflow: @workflow,
          status: :failed,
          terminal_status: :failed
        },
        :terminal_error
      )

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, checkpoint_projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert Projection.terminal_error(agent.state.projection) == error
  end

  test "persists a checkpoint from the rebuilt workflow agent state" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])
    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert :ok = WorkflowAgent.put_checkpoint(@storage, agent, updated_at: @completed_at)

    assert {:ok,
            %{
              thread: {:run, @run_id},
              thread_rev: 2,
              projection: %Projection{} = checkpoint_projection,
              updated_at: @completed_at
            }} = Journal.fetch_checkpoint(@storage, {:run, @run_id})

    assert Projection.planned_runnable_keys(checkpoint_projection) == [@runnable_key]
  end

  test "replays entries newer than a stale workflow checkpoint" do
    checkpoint_runnable_key = "run_123:refund_card:1"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: checkpoint_runnable_key,
               occurred_at: @completed_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [run_started])

    checkpoint_projection = %Projection{
      run_id: @run_id,
      workflow: @workflow,
      status: :running,
      planned_runnables: %{checkpoint_runnable_key => %{runnable_key: checkpoint_runnable_key}}
    }

    assert :ok =
             Journal.put_checkpoint(@storage, {:run, @run_id}, checkpoint_projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [applied], expected_rev: 1)

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert agent.state.thread_rev == 2
    assert WorkflowAgent.status(agent) == :idle
    assert WorkflowAgent.applied_runnable_keys(agent) == MapSet.new([checkpoint_runnable_key])
  end

  test "keeps completed dispatch results pending until the workflow applies them" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, dispatch_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, dispatch_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, dispatch_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [
               dispatch_scheduled,
               dispatch_claimed,
               dispatch_completed
             ])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert [%{runnable_key: @runnable_key, status: :completed}] =
             WorkflowAgent.pending_results(workflow_agent, dispatch_agent)

    assert {:ok, applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               occurred_at: @completed_at
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [applied], expected_rev: 2)
    assert {:ok, applied_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert WorkflowAgent.pending_results(applied_workflow_agent, dispatch_agent) == []
  end

  test "applies a completed dispatch result through a durable run-thread entry" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, dispatch_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, dispatch_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, dispatch_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [
               dispatch_scheduled,
               dispatch_claimed,
               dispatch_completed
             ])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert [completed_attempt] = WorkflowAgent.pending_results(workflow_agent, dispatch_agent)

    assert {:ok, %{agent: applied_agent, attempt: ^completed_attempt}} =
             WorkflowAgent.apply_result(@storage, workflow_agent, completed_attempt,
               now: @completed_at
             )

    assert applied_agent.state.thread_rev == 3
    assert WorkflowAgent.applied_runnable_keys(applied_agent) == MapSet.new([@runnable_key])
    assert WorkflowAgent.pending_results(applied_agent, dispatch_agent) == []

    assert {:ok, [_run_started, _runnables_planned, applied_entry]} =
             Journal.load_entries(@storage, {:run, @run_id})

    assert applied_entry.type == :runnable_applied
    assert applied_entry.data.runnable_key == @runnable_key
    assert applied_entry.data.result == %{"status" => "captured"}
  end

  test "applies completed dispatch execution options to the run-thread entry" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    completed_attempt = completed_attempt(execution_opts: [schedule_in: 2])

    assert {:ok, %{agent: applied_agent}} =
             WorkflowAgent.apply_result(@storage, workflow_agent, completed_attempt,
               now: @completed_at
             )

    assert {:ok, [_run_started, _runnables_planned, applied_entry]} =
             Journal.load_entries(@storage, {:run, @run_id})

    assert applied_entry.data.execution_opts == [schedule_in: 2]

    assert Projection.applied_execution_opts(applied_agent.state.projection, @runnable_key) == [
             schedule_in: 2
           ]
  end

  test "does not flatten deferred dispatch completions through generic result recovery" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, dispatch_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, dispatch_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, dispatch_completed} =
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

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [
               dispatch_scheduled,
               dispatch_claimed,
               dispatch_completed
             ])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")
    assert [completed_attempt] = WorkflowAgent.pending_results(workflow_agent, dispatch_agent)

    assert {:error, :deferred_completion_requires_executor} =
             WorkflowAgent.apply_result(@storage, workflow_agent, completed_attempt,
               now: @completed_at
             )

    assert {:ok, %{agent: recovered_agent, attempts: []}} =
             WorkflowAgent.apply_pending_results(@storage, workflow_agent, dispatch_agent,
               now: @completed_at
             )

    assert recovered_agent.state.thread_rev == 2
    assert WorkflowAgent.applied_runnable_keys(recovered_agent) == MapSet.new()
    assert [^completed_attempt] = WorkflowAgent.pending_results(recovered_agent, dispatch_agent)
  end

  test "recovers completed dispatch results after a restart loses the live wakeup" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, dispatch_scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, dispatch_claimed} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, dispatch_completed} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [
               dispatch_scheduled,
               dispatch_claimed,
               dispatch_completed
             ])

    assert {:ok, restarted_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, restarted_dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: recovered_agent, attempts: [%{runnable_key: @runnable_key}]}} =
             WorkflowAgent.apply_pending_results(
               @storage,
               restarted_workflow_agent,
               restarted_dispatch_agent,
               now: @completed_at
             )

    assert recovered_agent.state.thread_rev == 3
    assert WorkflowAgent.applied_runnable_keys(recovered_agent) == MapSet.new([@runnable_key])

    assert {:ok, [_run_started, _runnables_planned, applied_entry]} =
             Journal.load_entries(@storage, {:run, @run_id})

    assert applied_entry.type == :runnable_applied
    assert applied_entry.data.result == %{"status" => "captured"}

    assert {:ok, rebuilt_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert WorkflowAgent.pending_results(rebuilt_agent, restarted_dispatch_agent) == []
  end

  test "recovers planned runnables after a restart loses the dispatch append" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [planned_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, restarted_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, empty_dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert [%{runnable_key: @runnable_key}] =
             WorkflowAgent.pending_dispatches(restarted_workflow_agent, empty_dispatch_agent)

    assert {:ok,
            %{
              agent: recovered_dispatch_agent,
              runnables: [%{runnable_key: @runnable_key}]
            }} =
             WorkflowAgent.schedule_pending_dispatches(
               @storage,
               restarted_workflow_agent,
               empty_dispatch_agent,
               now: @visible_at
             )

    assert recovered_dispatch_agent.state.thread_rev == 1

    assert [%{runnable_key: @runnable_key, status: :available}] =
             DispatchAgent.visible_attempts(recovered_dispatch_agent, @visible_at)

    assert recovered_dispatch_agent.state.projection.attempts[@runnable_key].workflow == @workflow

    assert {:ok, [scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})

    assert scheduled_entry.type == :attempt_scheduled
    assert scheduled_entry.data.runnable_key == @runnable_key
    assert scheduled_entry.data.workflow == @workflow
    assert scheduled_entry.data.idempotency_key == @idempotency_key
    assert scheduled_entry.data.input == %{"payment_id" => "pay_123"}

    assert {:ok, rebuilt_dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert WorkflowAgent.pending_dispatches(restarted_workflow_agent, rebuilt_dispatch_agent) ==
             []
  end

  test "does not recover planned runnables that were already applied" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [planned_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, runnable_applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               result: %{"status" => "captured"},
               occurred_at: @completed_at
             })

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [run_started, runnables_planned, runnable_applied])

    assert {:ok, restarted_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, empty_dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert WorkflowAgent.pending_dispatches(restarted_workflow_agent, empty_dispatch_agent) == []

    assert {:ok, %{agent: ^empty_dispatch_agent, runnables: []}} =
             WorkflowAgent.schedule_pending_dispatches(
               @storage,
               restarted_workflow_agent,
               empty_dispatch_agent,
               now: @visible_at
             )

    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "does not recover an unscheduled runnable after a run continued" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :continued,
               occurred_at: @visible_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [planned_runnable()],
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [run_started, runnables_planned, run_terminal])

    assert {:ok, continued_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, empty_dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: ^empty_dispatch_agent, runnables: []}} =
             DispatchScheduler.schedule_pending_dispatches(
               @storage,
               continued_workflow_agent,
               empty_dispatch_agent,
               @completed_at,
               25
             )

    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "projects manual pause and resolution facts from the run thread" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, paused} =
             DispatchProtocol.new_entry(:manual_step_paused, %{
               run_id: @run_id,
               step: :wait_for_review,
               kind: :approval,
               metadata: %{output_key: "approval"},
               occurred_at: @visible_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [run_started, paused])

    assert {:ok, paused_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert WorkflowAgent.status(paused_agent) == :paused

    assert Projection.manual_state(paused_agent.state.projection) == %{
             step: "wait_for_review",
             kind: "approval",
             paused_at: @visible_at,
             metadata: %{output_key: "approval"}
           }

    assert {:ok, resolved} =
             DispatchProtocol.new_entry(:manual_step_resolved, %{
               run_id: @run_id,
               step: :wait_for_review,
               action: :approved,
               result: %{actor: "ops_123"},
               occurred_at: @completed_at
             })

    assert {:ok, %{rev: 3}} = Journal.append_entries(@storage, [resolved], expected_rev: 2)

    assert {:ok, resolved_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert WorkflowAgent.status(resolved_agent) == :started
    assert Projection.manual_state(resolved_agent.state.projection) == nil
  end

  test "reports conflicting manual lifecycle facts as workflow anomalies" do
    projection =
      Projection.rebuild([
        workflow_entry(:run_started, %{
          run_id: @run_id,
          workflow: @workflow
        }),
        workflow_entry(:manual_step_paused, %{
          run_id: @run_id,
          step: "wait_for_review",
          kind: "approval",
          metadata: %{}
        }),
        workflow_entry(:manual_step_paused, %{
          run_id: @run_id,
          step: "wait_for_approval",
          kind: "pause",
          metadata: %{}
        }),
        workflow_entry(:manual_step_resolved, %{
          run_id: @run_id,
          step: "wait_for_approval",
          action: "resumed",
          result: %{}
        })
      ])

    assert Projection.status(projection) == :paused

    assert Projection.manual_state(projection) == %{
             step: "wait_for_review",
             kind: "approval",
             paused_at: @started_at,
             metadata: %{}
           }

    assert [
             %{
               entry_type: :manual_step_paused,
               reason: :active_manual_step,
               run_id: @run_id,
               step: "wait_for_approval"
             },
             %{
               entry_type: :manual_step_resolved,
               reason: :stale_manual_resolution,
               run_id: @run_id,
               step: "wait_for_approval"
             }
           ] = Projection.anomalies(projection)
  end

  test "terminal run facts clear current manual state and reject later manual pause facts" do
    projection =
      Projection.rebuild([
        workflow_entry(:run_started, %{
          run_id: @run_id,
          workflow: @workflow
        }),
        workflow_entry(:manual_step_paused, %{
          run_id: @run_id,
          step: "wait_for_review",
          kind: "approval",
          metadata: %{}
        }),
        workflow_entry(:run_terminal, %{
          run_id: @run_id,
          status: :cancelled
        }),
        workflow_entry(:manual_step_paused, %{
          run_id: @run_id,
          step: "wait_for_approval",
          kind: "pause",
          metadata: %{}
        })
      ])

    assert Projection.status(projection) == :cancelled
    assert Projection.manual_state(projection) == nil

    assert [
             %{
               entry_type: :manual_step_paused,
               reason: :terminal_run,
               run_id: @run_id,
               step: "wait_for_approval"
             }
           ] = Projection.anomalies(projection)
  end

  test "treats applying the same completed dispatch result as idempotent" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               result: %{"status" => "captured"},
               occurred_at: @completed_at
             })

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [run_started, runnables_planned, applied])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    completed_attempt = completed_attempt()

    assert {:ok, %{agent: ^workflow_agent, attempt: ^completed_attempt}} =
             WorkflowAgent.apply_result(@storage, workflow_agent, completed_attempt,
               now: @completed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :runnable_applied)) == 1
  end

  test "rejects duplicate result application when the persisted result differs" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               result: %{"status" => "captured"},
               occurred_at: @completed_at
             })

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [run_started, runnables_planned, applied])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    conflicting_attempt = completed_attempt(result: %{"status" => "declined"})

    assert {:error, {:conflicting_result, @runnable_key}} =
             WorkflowAgent.apply_result(@storage, workflow_agent, conflicting_attempt,
               now: @completed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :runnable_applied)) == 1
  end

  test "returns conflict when applying a result from a stale workflow projection" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, other_runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: "run_123:refund_card:1", step: "refund_card"}],
               occurred_at: @completed_at
             })

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])
    assert {:ok, stale_workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [other_runnables_planned], expected_rev: 2)

    completed_attempt = completed_attempt()

    assert {:error, :conflict} =
             WorkflowAgent.apply_result(@storage, stale_workflow_agent, completed_attempt,
               now: @completed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries, &(&1.type == :runnable_applied))
  end

  test "rejects non-pending dispatch results before writing" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])
    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    wrong_run_attempt = completed_attempt(run_id: "run_456")
    unplanned_attempt = completed_attempt(runnable_key: "run_123:stale_step:1")
    claimed_attempt = %{completed_attempt() | status: :claimed}
    missing_result_attempt = completed_attempt(result: nil)

    assert {:error, :wrong_run} =
             WorkflowAgent.apply_result(@storage, workflow_agent, wrong_run_attempt,
               now: @completed_at
             )

    assert {:error, :unknown_runnable_intent} =
             WorkflowAgent.apply_result(@storage, workflow_agent, unplanned_attempt,
               now: @completed_at
             )

    assert {:error, :result_not_completed} =
             WorkflowAgent.apply_result(@storage, workflow_agent, claimed_attempt,
               now: @completed_at
             )

    assert {:error, :missing_result} =
             WorkflowAgent.apply_result(@storage, workflow_agent, missing_result_attempt,
               now: @completed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries, &(&1.type == :runnable_applied))
  end

  test "rejects dispatch result application after the workflow run became terminal" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :cancelled,
               occurred_at: @completed_at
             })

    assert {:ok, _run_thread} =
             Journal.append_entries(@storage, [run_started, runnables_planned, run_terminal])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert {:error, :terminal_run} =
             WorkflowAgent.apply_result(@storage, workflow_agent, completed_attempt(),
               now: @completed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(entries, &(&1.type == :runnable_applied))
  end

  test "ignores completed dispatch results for unplanned runnable keys in the same run" do
    stale_runnable_key = "run_123:stale_step:1"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, stale_scheduled} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(runnable_key: stale_runnable_key)
             )

    assert {:ok, stale_claimed} =
             DispatchProtocol.new_entry(
               :attempt_claimed,
               claimed_attrs(runnable_key: stale_runnable_key)
             )

    assert {:ok, stale_completed} =
             DispatchProtocol.new_entry(
               :attempt_completed,
               completed_attrs(runnable_key: stale_runnable_key)
             )

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [stale_scheduled, stale_claimed, stale_completed])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert WorkflowAgent.pending_results(workflow_agent, dispatch_agent) == []
  end

  test "ignores completed dispatch results from other runs on the same queue" do
    other_run_id = "run_456"
    other_runnable_key = "run_456:charge_card:1"

    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, other_scheduled} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(
                 run_id: other_run_id,
                 runnable_key: other_runnable_key,
                 idempotency_key: "#{other_run_id}:charge_card:payment_789"
               )
             )

    assert {:ok, other_claimed} =
             DispatchProtocol.new_entry(
               :attempt_claimed,
               claimed_attrs(run_id: other_run_id, runnable_key: other_runnable_key)
             )

    assert {:ok, other_completed} =
             DispatchProtocol.new_entry(
               :attempt_completed,
               completed_attrs(run_id: other_run_id, runnable_key: other_runnable_key)
             )

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [run_started, runnables_planned])

    assert {:ok, _dispatch_thread} =
             Journal.append_entries(@storage, [other_scheduled, other_claimed, other_completed])

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert WorkflowAgent.pending_results(workflow_agent, dispatch_agent) == []
  end

  test "projects applied runnable execution metadata" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [%{runnable_key: @runnable_key, step: "charge_card"}],
               occurred_at: @visible_at
             })

    assert {:ok, runnable_applied} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               result: %{},
               execution_opts: [schedule_in: 2],
               occurred_at: @completed_at
             })

    projection = Projection.rebuild([run_started, runnables_planned, runnable_applied])

    assert Projection.applied_execution_opts(projection, @runnable_key) == [schedule_in: 2]
    assert Projection.applied_at(projection, @runnable_key) == @completed_at
  end

  test "ignores runnables without runnable keys when rebuilding projection" do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: @run_id,
               runnables: [
                 %{step: "charge_card"},
                 %{key: "actual-key", step: "refund_card"},
                 %{"runnable_key" => "", step: "skip_me"}
               ],
               occurred_at: @visible_at
             })

    projection = Projection.rebuild([run_started, runnables_planned])

    assert Projection.planned_runnable_keys(projection) == ["actual-key"]
  end

  test "records anomalies instead of raising for malformed persisted workflow entries" do
    malformed_entries = [
      workflow_entry(:run_started, %{}),
      workflow_entry(:runnables_planned, %{run_id: @run_id, runnables: :not_a_list}),
      workflow_entry(:runnable_applied, %{run_id: @run_id}),
      workflow_entry(:run_terminal, %{run_id: @run_id})
    ]

    projection = Projection.rebuild(malformed_entries)

    assert Projection.status(projection) == :new
    assert Projection.planned_runnable_keys(projection) == []
    assert Projection.applied_runnable_keys(projection) == MapSet.new()

    assert [
             %{entry_type: :run_started, reason: :malformed_entry},
             %{entry_type: :runnables_planned, reason: :malformed_entry, run_id: @run_id},
             %{entry_type: :runnable_applied, reason: :malformed_entry, run_id: @run_id},
             %{entry_type: :run_terminal, reason: :malformed_entry, run_id: @run_id}
           ] = Projection.anomalies(projection)
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        idempotency_key: @idempotency_key,
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

  defp planned_runnable(attrs \\ %{}) do
    Map.delete(scheduled_attrs(attrs), :occurred_at)
  end

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
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
        runnable_key: @runnable_key,
        claim_id: "claim_1",
        claim_token_hash: "token_hash_1",
        queue: "default",
        result: %{"status" => "captured"},
        occurred_at: @completed_at
      },
      Map.new(attrs)
    )
  end

  defp completed_attempt(attrs \\ %{}) do
    struct!(
      Squidie.Runtime.DispatchProtocol.ActionAttempt,
      Map.merge(
        %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          idempotency_key: @idempotency_key,
          attempt_number: 1,
          step: "charge_card",
          input: %{"payment_id" => "pay_123"},
          visible_at: @visible_at,
          status: :completed,
          claim_id: "claim_1",
          claim_token_hash: "token_hash_1",
          owner_id: "worker_1",
          lease_until: @lease_until,
          result: %{"status" => "captured"}
        },
        Map.new(attrs)
      )
    )
  end

  defp workflow_entry(type, data) do
    %Entry{
      type: type,
      thread: {:run, @run_id},
      data: data,
      occurred_at: @started_at
    }
  end

  defp table_name(:checkpoints), do: :squidie_workflow_agent_test_checkpoints
  defp table_name(:threads), do: :squidie_workflow_agent_test_threads
  defp table_name(:thread_meta), do: :squidie_workflow_agent_test_thread_meta

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
