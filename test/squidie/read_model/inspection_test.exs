defmodule Squidie.ReadModel.InspectionTest do
  use ExUnit.Case, async: false

  alias Squidie.ReadModel.Inspection
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Jido.ResultEnvelope
  alias Squidie.Runtime.Journal

  @storage {Jido.Storage.ETS, table: :squidie_read_model_inspection_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @queue "default"
  @runnable_key "run_123:charge_card:1"
  @second_runnable_key "run_123:send_receipt:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @second_idempotency_key "run_123:send_receipt:payment_456"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @later_visible_at ~U[2026-05-15 00:00:30Z]
  @claimed_at ~U[2026-05-15 00:00:20Z]
  @completed_at ~U[2026-05-15 00:00:40Z]
  @lease_until ~U[2026-05-15 00:01:00Z]
  @expired_at ~U[2026-05-15 00:01:01Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "builds a snapshot from run and dispatch projections" do
    append_run_entries([run_signal_received(), run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.run_id == @run_id
    assert snapshot.workflow == @workflow
    assert snapshot.queue == @queue
    assert snapshot.status == :running
    assert snapshot.reason == :attempt_visible
    assert snapshot.thread_revisions == %{run: 3, dispatch: 1}

    assert snapshot.command_history == [
             %{
               signal_type: "start_run",
               payload: %{
                 workflow: @workflow,
                 trigger: "manual",
                 input: %{"payment_id" => "pay_123"}
               },
               metadata: %{request_id: "req_123"},
               occurred_at: @started_at
             }
           ]

    assert snapshot.planned_runnable_keys == [@runnable_key]
    assert snapshot.applied_runnable_keys == []
    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.visible_attempts
    assert snapshot.pending_dispatches == []
    assert snapshot.pending_results == []
    assert snapshot.expired_claims == []
    assert snapshot.terminal? == false
  end

  test "exposes only public completion results for encoded and ordinary attempts" do
    envelope =
      ResultEnvelope.wrap_run_instruction(
        %{accepted: true},
        %{"dynamic_work" => %{dynamic_key: "one"}, "runnables" => [%{step: "one"}]}
      )

    append_run_entries([run_started(), runnables_planned()])

    append_dispatch_entries([
      attempt_scheduled(),
      attempt_claimed(),
      attempt_completed(
        result: envelope,
        completion_encoding: ResultEnvelope.completion_encoding()
      )
    ])

    assert {:ok, snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert [%{result: %{accepted: true}}] = snapshot.attempts
    refute inspect(snapshot) =~ "__squidie_jido_result__"

    cleanup_storage()
    append_run_entries([run_started(), runnables_planned()])

    malformed =
      put_in(
        envelope,
        ["__squidie_jido_result__", "fingerprint"],
        "malformed"
      )

    append_dispatch_entries([
      attempt_scheduled(),
      attempt_claimed(),
      attempt_completed(
        result: malformed,
        completion_encoding: ResultEnvelope.completion_encoding()
      )
    ])

    assert {:ok, snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert [malformed_attempt] = snapshot.attempts
    refute Map.has_key?(malformed_attempt, :result)
    refute inspect(snapshot) =~ "__squidie_jido_result__"

    cleanup_storage()
    append_run_entries([run_started(), runnables_planned()])
    ordinary = %{"__squidie_jido_result__" => %{application: "ordinary"}}

    append_dispatch_entries([
      attempt_scheduled(),
      attempt_claimed(),
      attempt_completed(result: ordinary)
    ])

    assert {:ok, snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert [%{result: ^ordinary}] = snapshot.attempts
  end

  test "builds chronological timeline events from snapshot facts" do
    append_run_entries([
      run_signal_received(),
      run_started(),
      runnables_planned(),
      runnable_applied(),
      run_terminal(:completed)
    ])

    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert {:ok, timeline} = Inspection.timeline(snapshot)

    assert timeline.run_id == @run_id
    assert timeline.workflow == @workflow
    assert timeline.queue == @queue
    assert timeline.status == :completed

    assert Enum.map(timeline.events, & &1.type) == [
             :command_received,
             :run_started,
             :attempt_scheduled,
             :attempt_claimed,
             :attempt_completed,
             :runnable_applied,
             :run_terminal
           ]

    assert [
             %{
               type: :command_received,
               occurred_at: @started_at,
               summary: "start_run command received"
             },
             %{type: :run_started, occurred_at: @started_at, summary: "run started"},
             %{
               type: :attempt_scheduled,
               occurred_at: @started_at,
               step_id: "charge_card",
               status: :available,
               summary: "charge_card attempt scheduled"
             },
             %{
               type: :attempt_claimed,
               occurred_at: @claimed_at,
               step_id: "charge_card",
               status: :claimed,
               summary: "charge_card attempt claimed"
             },
             %{
               type: :attempt_completed,
               occurred_at: @completed_at,
               step_id: "charge_card",
               status: :completed,
               summary: "charge_card attempt completed"
             },
             %{
               type: :runnable_applied,
               occurred_at: @completed_at,
               step_id: "charge_card",
               status: :applied,
               summary: "charge_card result applied"
             },
             %{
               type: :run_terminal,
               occurred_at: @completed_at,
               status: :completed,
               summary: "run completed"
             }
           ] = timeline.events

    refute inspect(timeline.events) =~ "pay_123"
    refute inspect(timeline.events) =~ "captured"
  end

  test "builds failed timeline events" do
    append_run_entries([
      run_started(),
      runnables_planned(),
      run_terminal(:failed, error: %{code: "gateway_error"})
    ])

    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_failed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert {:ok, timeline} = Inspection.timeline(snapshot)

    assert %{
             type: :attempt_failed,
             occurred_at: @completed_at,
             step_id: "charge_card",
             status: :failed,
             summary: "charge_card attempt failed"
           } = Enum.find(timeline.events, &(&1.type == :attempt_failed))

    assert %{
             type: :run_terminal,
             occurred_at: @completed_at,
             status: :failed,
             summary: "run failed"
           } = Enum.find(timeline.events, &(&1.type == :run_terminal))
  end

  test "builds manual timeline events without dropping false details" do
    append_run_entries([
      run_started(),
      runnables_planned(),
      manual_step_paused(reason: false)
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert {:ok, timeline} = Inspection.timeline(snapshot)

    assert %{
             type: :manual_step_paused,
             occurred_at: @completed_at,
             step_id: "wait_for_review",
             status: :paused,
             details: %{kind: "approval", reason: false}
           } = Enum.find(timeline.events, &(&1.type == :manual_step_paused))
  end

  test "shows scheduled attempts before they become visible" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.status == :running
    assert snapshot.reason == :attempt_scheduled_for_later
    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []

    assert [
             %{
               runnable_key: @runnable_key,
               status: :available,
               visible_at: @visible_at
             }
           ] = snapshot.scheduled_attempts
  end

  test "surfaces sanitized terminal errors for failed runs" do
    append_run_entries([
      run_started(),
      runnables_planned(),
      run_terminal(:failed,
        error: %{
          code: "gateway_timeout",
          message: "gateway timeout",
          retryable?: false,
          type: "Elixir.RuntimeError",
          path: ["draft", "drafts"],
          target: "drafts",
          missing_at: ["draft", "drafts"],
          secret: "token=super-secret-token"
        }
      )
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

    assert snapshot.terminal_error == %{
             code: "gateway_timeout",
             message: "gateway timeout",
             retryable?: false,
             type: "Elixir.RuntimeError",
             path: ["draft", "drafts"],
             target: "drafts",
             missing_at: ["draft", "drafts"]
           }
  end

  test "surfaces sanitized terminal errors when persisted fields use string keys" do
    append_run_entries([
      run_started(),
      runnables_planned(),
      run_terminal(:failed,
        error: %{
          "code" => "gateway_timeout",
          "message" => "gateway timeout",
          "retryable?" => false,
          "type" => "Elixir.RuntimeError",
          "path" => ["draft", "drafts"],
          "target" => "drafts",
          "missing_at" => ["draft", "drafts"],
          "secret" => "token=super-secret-token"
        }
      )
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

    assert snapshot.terminal_error == %{
             code: "gateway_timeout",
             message: "gateway timeout",
             retryable?: false,
             type: "Elixir.RuntimeError",
             path: ["draft", "drafts"],
             target: "drafts",
             missing_at: ["draft", "drafts"]
           }
  end

  test "reports the earliest visible time across scheduled attempts" do
    append_run_entries([
      run_started(),
      runnables_planned([
        planned_runnable(),
        planned_runnable(
          idempotency_key: @second_idempotency_key,
          runnable_key: @second_runnable_key,
          step: "send_receipt",
          visible_at: @later_visible_at
        )
      ])
    ])

    append_dispatch_entries([
      attempt_scheduled(),
      attempt_scheduled(
        idempotency_key: @second_idempotency_key,
        runnable_key: @second_runnable_key,
        step: "send_receipt",
        visible_at: @later_visible_at
      )
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.reason == :attempt_scheduled_for_later
    assert snapshot.next_visible_at == @visible_at

    assert Enum.map(snapshot.scheduled_attempts, & &1.runnable_key) == [
             @runnable_key,
             @second_runnable_key
           ]
  end

  test "shows planned runnables that have not been durably scheduled yet" do
    append_run_entries([run_started(), runnables_planned()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.status == :running
    assert snapshot.reason == :planned_dispatch_pending_schedule
    assert snapshot.thread_revisions == %{run: 2, dispatch: 0}

    assert [
             %{
               runnable_key: @runnable_key,
               step: "charge_card",
               input: %{"payment_id" => "pay_123"}
             }
           ] = snapshot.pending_dispatches

    assert snapshot.visible_attempts == []
    assert snapshot.attempts == []
  end

  test "shows completed dispatch results that are not applied to the run thread yet" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.status == :running
    assert snapshot.reason == :completed_result_pending_apply

    assert [
             %{
               runnable_key: @runnable_key,
               status: :completed,
               result: %{"status" => "captured"},
               applied?: false
             }
           ] = snapshot.pending_results

    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []
  end

  test "evaluates active attempt deadlines from persisted runnable metadata" do
    deadline =
      deadline(
        started_at: @visible_at,
        within: 20_000,
        due_soon: 15_000,
        escalate_after: 5_000
      )

    append_run_entries([
      run_started(),
      runnables_planned([planned_runnable(deadline: deadline)])
    ])

    append_dispatch_entries([attempt_scheduled(deadline: deadline), attempt_claimed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.reason == :attempt_claimed

    assert %{
             status: :escalated,
             step: "charge_card",
             escalation: %{outcome: :diagnostic}
           } = snapshot.deadline

    assert DateTime.compare(snapshot.deadline.due_at, ~U[2026-05-15 00:00:30Z]) == :eq
    assert DateTime.compare(snapshot.deadline.escalated_at, ~U[2026-05-15 00:00:35Z]) == :eq

    assert [
             %{
               runnable_key: @runnable_key,
               deadline: %{status: :escalated, escalation: %{outcome: :diagnostic}}
             }
           ] = snapshot.attempts
  end

  test "does not promote completed historical deadlines as the run active deadline" do
    completed_deadline =
      deadline(
        started_at: @visible_at,
        within: 1_000,
        due_soon: 500,
        escalate_after: 500
      )

    active_deadline =
      deadline(
        started_at: @completed_at,
        within: 60_000,
        due_soon: 10_000,
        escalate_after: 10_000
      )

    active_key = "#{@run_id}:capture_receipt:1"

    append_run_entries([
      run_started(),
      runnables_planned([
        planned_runnable(deadline: completed_deadline),
        planned_runnable(
          runnable_key: active_key,
          idempotency_key: active_key,
          step: "capture_receipt",
          deadline: active_deadline
        )
      ]),
      runnable_applied()
    ])

    append_dispatch_entries([
      attempt_scheduled(deadline: completed_deadline),
      attempt_claimed(),
      attempt_completed(),
      attempt_scheduled(
        runnable_key: active_key,
        idempotency_key: active_key,
        step: "capture_receipt",
        visible_at: @completed_at,
        deadline: active_deadline
      )
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert %{step: "capture_receipt", status: :on_time} = snapshot.deadline
  end

  test "evaluates manual step deadlines from persisted manual metadata" do
    deadline =
      deadline(
        started_at: @completed_at,
        within: 5_000,
        due_soon: 2_000,
        escalation: :operator_action
      )

    append_run_entries([
      run_started(),
      runnables_planned(),
      manual_step_paused(deadline: deadline)
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id,
               queue: @queue,
               now: DateTime.add(@completed_at, 6, :second)
             )

    assert snapshot.reason == :manual_intervention_required

    assert %{
             deadline: %{
               status: :overdue,
               step: "wait_for_review",
               escalation: %{outcome: :operator_action}
             }
           } = snapshot.manual_state

    assert %{status: :overdue, step: "wait_for_review"} = snapshot.deadline
  end

  test "derives idle reason from applied run-thread facts" do
    append_run_entries([run_started(), runnables_planned(), runnable_applied()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.status == :idle
    assert snapshot.reason == :idle
    assert snapshot.applied_runnable_keys == [@runnable_key]
    assert snapshot.pending_results == []

    assert [%{runnable_key: @runnable_key, status: :completed, applied?: true}] =
             snapshot.attempts
  end

  test "exposes child runs reconstructed from the parent run thread" do
    append_run_entries([run_started(), child_run_started()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert [
             %{
               child_run_id: "child_run_123",
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{
                 runnable_key: @runnable_key,
                 step: "charge_card",
                 attempt: 1
               },
               metadata: %{subscription_id: "sub_123"}
             }
           ] = snapshot.child_runs
  end

  test "exposes parent run context on child snapshots" do
    append_run_entries([
      run_started(%{
        parent: %{
          run_id: "parent_run_123",
          runnable_key: "parent_run_123:fanout:1",
          step: "fanout",
          attempt: 1,
          child_key: "digest_subscription_1",
          metadata: %{subscription_id: "sub_123"}
        }
      })
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.parent_run == %{
             run_id: "parent_run_123",
             runnable_key: "parent_run_123:fanout:1",
             step: "fanout",
             attempt: 1,
             child_key: "digest_subscription_1",
             metadata: %{subscription_id: "sub_123"}
           }
  end

  test "merges applied result context in durable application order" do
    approval_gate_key = "#{@run_id}:wait_for_approval:1"
    record_approval_key = "#{@run_id}:record_approval:1"
    approval_recorded_at = DateTime.add(@completed_at, 1, :second)

    append_run_entries([
      run_started(),
      runnables_planned([
        planned_runnable(
          runnable_key: approval_gate_key,
          idempotency_key: approval_gate_key,
          step: "wait_for_approval"
        ),
        planned_runnable(
          runnable_key: record_approval_key,
          idempotency_key: record_approval_key,
          step: "record_approval"
        )
      ]),
      runnable_applied(
        runnable_key: approval_gate_key,
        result: %{approval: %{}},
        occurred_at: @completed_at
      ),
      runnable_applied(
        runnable_key: record_approval_key,
        result: %{approval: %{status: "approved", actor: "ops_123"}},
        occurred_at: approval_recorded_at
      ),
      run_terminal(:completed, occurred_at: approval_recorded_at)
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: approval_recorded_at)

    assert snapshot.context.approval == %{status: "approved", actor: "ops_123"}
  end

  test "shows manual pause state without suggesting dispatch recovery" do
    append_run_entries([run_started(), runnables_planned(), manual_step_paused()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.status == :paused
    assert snapshot.reason == :manual_intervention_required

    assert snapshot.manual_state == %{
             step: "wait_for_review",
             kind: "approval",
             paused_at: @completed_at,
             metadata: %{output_key: "approval"}
           }

    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.visible_attempts
  end

  test "uses terminal run facts to suppress dispatch redelivery views" do
    append_run_entries([run_started(), runnables_planned(), run_terminal(:completed)])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

    assert snapshot.status == :completed
    assert snapshot.reason == :terminal
    assert snapshot.terminal? == true
    assert snapshot.terminal_status == :completed
    assert snapshot.scheduled_attempts == []
    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []
    assert [%{runnable_key: @runnable_key, status: :claimed}] = snapshot.attempts
  end

  test "uses terminal run facts to suppress scheduled attempt views" do
    deadline = deadline(started_at: @visible_at, within: 20_000)

    append_run_entries([
      run_started(),
      runnables_planned([planned_runnable(deadline: deadline)]),
      run_terminal(:cancelled)
    ])

    append_dispatch_entries([attempt_scheduled(deadline: deadline)])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.status == :cancelled
    assert snapshot.reason == :terminal
    assert snapshot.manual_state == nil
    assert snapshot.deadline == nil
    assert snapshot.scheduled_attempts == []
    assert snapshot.visible_attempts == []
    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.attempts
  end

  test "keeps failed and cancelled terminal statuses visible in snapshots" do
    for status <- [:failed, :cancelled] do
      cleanup_storage()
      append_run_entries([run_started(), runnables_planned(), run_terminal(status)])
      append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

      assert {:ok, %Snapshot{} = snapshot} =
               Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

      assert snapshot.status == status
      assert snapshot.reason == :terminal
      assert snapshot.terminal? == true
      assert snapshot.terminal_status == status
      assert snapshot.visible_attempts == []
      assert snapshot.expired_claims == []
    end
  end

  test "returns not found when the run thread is missing" do
    assert {:error, :not_found} =
             Inspection.snapshot(@storage, "missing_run",
               queue: @queue,
               now: @visible_at
             )
  end

  test "returns invalid option errors for invalid option values" do
    assert {:error, {:invalid_option, {:now, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: :soon)

    assert {:error, {:invalid_option, {:queue, :invalid}}} =
             Inspection.snapshot(@storage, @run_id,
               queue: %{name: @queue},
               now: @visible_at
             )
  end

  test "returns invalid option errors for malformed or unsupported options" do
    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, %{queue: @queue})

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, [:bad])

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             Inspection.snapshot(@storage, @run_id, unknown: true)
  end

  test "redacts malformed option values from public errors" do
    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id, %{claim_token: "super-secret-token"})

    assert reason == {:invalid_option, {:opts, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id, [
               {:claim_token, "super-secret-token"},
               :bad
             ])

    assert reason == {:invalid_option, {:opts, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id,
               queue: %{claim_token: "super-secret-token"},
               now: @visible_at
             )

    assert reason == {:invalid_option, {:queue, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id,
               queue: @queue,
               now: %{claim_token: "super-secret-token"}
             )

    assert reason == {:invalid_option, {:now, :invalid}}
    refute inspect(reason) =~ "super-secret-token"
  end

  defp append_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp append_dispatch_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp run_started(context \\ %{}) do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      context: context,
      occurred_at: @started_at
    })
  end

  defp run_signal_received do
    entry!(:run_signal_received, %{
      run_id: @run_id,
      signal_type: :start_run,
      payload: %{
        workflow: @workflow,
        trigger: "manual",
        input: %{"payment_id" => "pay_123"}
      },
      metadata: %{request_id: "req_123"},
      occurred_at: @started_at
    })
  end

  defp child_run_started(metadata \\ %{subscription_id: "sub_123"}) do
    entry!(:child_run_started, %{
      run_id: @run_id,
      child_run_id: "child_run_123",
      child_workflow: @workflow,
      child_trigger: "manual",
      child_key: "digest_subscription_1",
      origin: %{
        runnable_key: @runnable_key,
        step: "charge_card",
        attempt: 1
      },
      metadata: metadata,
      occurred_at: @visible_at
    })
  end

  defp runnables_planned(runnables \\ [planned_runnable()]) do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: runnables,
      occurred_at: @visible_at
    })
  end

  defp runnable_applied(overrides \\ []) do
    entry!(:runnable_applied, %{
      run_id: @run_id,
      runnable_key: Keyword.get(overrides, :runnable_key, @runnable_key),
      result: Keyword.get(overrides, :result, %{"status" => "captured"}),
      occurred_at: Keyword.get(overrides, :occurred_at, @completed_at)
    })
  end

  defp run_terminal(status, overrides \\ []) do
    entry!(
      :run_terminal,
      maybe_put_error(
        %{
          run_id: @run_id,
          status: status,
          occurred_at: Keyword.get(overrides, :occurred_at, @completed_at)
        },
        Keyword.get(overrides, :error)
      )
    )
  end

  defp maybe_put_error(attrs, nil), do: attrs
  defp maybe_put_error(attrs, error), do: Map.put(attrs, :error, error)

  defp manual_step_paused(overrides \\ []) do
    entry!(:manual_step_paused, %{
      run_id: @run_id,
      step: :wait_for_review,
      kind: :approval,
      reason: Keyword.get(overrides, :reason),
      metadata: %{output_key: "approval"},
      deadline: Keyword.get(overrides, :deadline),
      occurred_at: @completed_at
    })
  end

  defp attempt_scheduled(overrides \\ []) do
    entry!(:attempt_scheduled, scheduled_attrs(overrides))
  end

  defp attempt_claimed do
    entry!(:attempt_claimed, %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim_1",
      claim_token_hash: "token_hash_1",
      owner_id: "worker_1",
      queue: @queue,
      lease_until: @lease_until,
      occurred_at: @claimed_at
    })
  end

  defp attempt_completed(overrides \\ []) do
    base_attrs = %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim_1",
      claim_token_hash: "token_hash_1",
      queue: @queue,
      result: %{"status" => "captured"},
      occurred_at: @completed_at
    }

    merged_attrs = Map.merge(base_attrs, Map.new(overrides))

    attrs =
      case Keyword.get(overrides, :completion_encoding) do
        nil -> merged_attrs
        encoding -> Map.put(merged_attrs, "completion_encoding", encoding)
      end

    entry!(:attempt_completed, Map.delete(attrs, :completion_encoding))
  end

  defp attempt_failed do
    entry!(:attempt_failed, %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim_1",
      claim_token_hash: "token_hash_1",
      queue: @queue,
      error: %{code: "gateway_error", message: "gateway failed"},
      occurred_at: @completed_at
    })
  end

  defp planned_runnable(overrides \\ []) do
    overrides
    |> scheduled_attrs()
    |> Map.delete(:occurred_at)
  end

  defp scheduled_attrs(overrides) do
    base = %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      idempotency_key: @idempotency_key,
      attempt_number: 1,
      queue: @queue,
      step: "charge_card",
      input: %{"payment_id" => "pay_123"},
      visible_at: @visible_at,
      occurred_at: @started_at
    }

    Map.merge(base, Map.new(overrides))
  end

  defp deadline(opts) do
    started_at = Keyword.fetch!(opts, :started_at)
    within = Keyword.fetch!(opts, :within)
    due_at = DateTime.add(started_at, within, :millisecond)
    due_soon = Keyword.get(opts, :due_soon)
    escalate_after = Keyword.get(opts, :escalate_after)

    %{
      policy: %{
        within: within,
        due_soon: due_soon,
        escalate_after: escalate_after,
        escalation: Keyword.get(opts, :escalation, :diagnostic)
      },
      started_at: started_at,
      due_at: due_at,
      due_soon_at: if(is_integer(due_soon), do: DateTime.add(due_at, -due_soon, :millisecond)),
      escalated_at:
        if(is_integer(escalate_after), do: DateTime.add(due_at, escalate_after, :millisecond))
    }
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp table_name(:checkpoints), do: :squidie_read_model_inspection_test_checkpoints
  defp table_name(:threads), do: :squidie_read_model_inspection_test_threads
  defp table_name(:thread_meta), do: :squidie_read_model_inspection_test_thread_meta

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(table_name(suffix))
    end
  end

  defp delete_table_if_present(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
