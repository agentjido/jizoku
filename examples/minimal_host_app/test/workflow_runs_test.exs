defmodule MinimalHostApp.WorkflowRunsTest do
  use MinimalHostApp.DataCase

  alias MinimalHostApp.CronPlugin
  alias MinimalHostApp.RuntimeSignals
  alias MinimalHostApp.Smoke
  alias MinimalHostApp.Steps
  alias MinimalHostApp.Workers.JizokuWorker
  alias MinimalHostApp.WorkflowRuns
  alias MinimalHostApp.Workflows.DailyDigest
  alias MinimalHostApp.Workflows.DependencyRecovery
  alias MinimalHostApp.Workflows.JidoDirectiveBoundary
  alias MinimalHostApp.Workflows.JidoEmitWorkflow
  alias MinimalHostApp.Workflows.JidoErrorRecovery
  alias MinimalHostApp.Workflows.JidoInstructionWorkflow
  alias MinimalHostApp.Workflows.JidoRunInstructionWorkflow
  alias MinimalHostApp.Workflows.ManualApproval
  alias MinimalHostApp.Workflows.PaymentRecovery
  alias MinimalHostApp.Workflows.RetryVerification
  alias Oban.Job
  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.ReadModel.Listing.Summary
  alias Jizoku.ReadModel.Listing.Page
  alias Jizoku.Runtime.Signal

  test "public test runtime drains the host dependency workflow in memory" do
    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: DependencyRecovery,
               now: ~U[2026-08-10 12:00:00Z]
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} =
             Jizoku.Test.start(runtime, %{
               account_id: "account-test-kit",
               invoice_id: "invoice-test-kit",
               attempt_id: "attempt-test-kit"
             })

    execute_task =
      Task.async(fn ->
        Jizoku.Test.execute_until(runtime, run, fn snapshot ->
          Map.has_key?(snapshot.context, :account) and
            Map.has_key?(snapshot.context, :invoice)
        end)
      end)

    assert {:reached, intermediate} = Task.await(execute_task)
    assert intermediate.context.account.id == "account-test-kit"
    assert intermediate.context.invoice.id == "invoice-test-kit"
    refute Map.has_key?(intermediate.context, :notification)

    assert {:ok, restarted_runtime} = Jizoku.Test.restart_runtime(runtime)
    on_exit(fn -> Jizoku.Test.stop_runtime(restarted_runtime) end)
    assert restarted_runtime.id != runtime.id
    assert {:error, :runtime_stopped} = Jizoku.Test.inspect(runtime, run)

    assert :ok = Jizoku.Test.delete_checkpoints(restarted_runtime)
    assert {:ok, rebuilt} = Jizoku.Test.inspect(restarted_runtime, run)
    assert rebuilt == intermediate

    assert :ok = Jizoku.Test.inject_append_conflict(restarted_runtime, :dispatch)
    assert {:error, :conflict} = Jizoku.Test.drain(restarted_runtime, run)
    assert {:completed, snapshot} = Jizoku.Test.drain(restarted_runtime, run)
    assert snapshot.context.account.id == "account-test-kit"
    assert snapshot.context.invoice.id == "invoice-test-kit"
    assert snapshot.context.notification.channel == "email"
    assert {:ok, ^snapshot} = Jizoku.Test.check_invariants(restarted_runtime, run)

    assert {:ok,
            %{
              schema_version: 1,
              workflow: "Elixir.MinimalHostApp.Workflows.DependencyRecovery",
              queue: "default",
              partition: nil,
              status: :completed,
              terminal_status: :completed,
              events: golden_events
            } = golden} = Jizoku.Test.golden_history(restarted_runtime, run)

    assert Enum.map(golden_events, &{&1.type, Map.get(&1, :step), Map.get(&1, :runnable)}) == [
             {:command_received, nil, nil},
             {:run_started, nil, nil},
             {:attempt_claimed, "load_account", "runnable-1"},
             {:attempt_claimed, "load_invoice", "runnable-2"},
             {:attempt_claimed, "prepare_notification", "runnable-3"},
             {:attempt_completed, "load_account", "runnable-1"},
             {:attempt_completed, "load_invoice", "runnable-2"},
             {:attempt_completed, "prepare_notification", "runnable-3"},
             {:runnable_applied, "load_account", "runnable-1"},
             {:runnable_applied, "load_invoice", "runnable-2"},
             {:runnable_applied, "prepare_notification", "runnable-3"},
             {:attempt_scheduled, "load_account", "runnable-1"},
             {:attempt_scheduled, "load_invoice", "runnable-2"},
             {:attempt_scheduled, "prepare_notification", "runnable-3"},
             {:run_terminal, nil, nil}
           ]

    refute Kernel.inspect(golden) =~ "account-test-kit"
    refute Kernel.inspect(golden) =~ "invoice-test-kit"
  end

  test "raw Jido action directives fail before their output is applied" do
    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: JidoDirectiveBoundary,
               now: ~U[2026-08-10 12:00:00Z]
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Jizoku.Test.start(runtime, %{})
    assert {:failed, failed} = Jizoku.Test.drain(runtime, run)
    assert failed.applied_runnable_keys == []

    assert failed.terminal_error == %{
             code: "unsupported_jido_directive",
             message: "Jido action directives are not supported",
             retryable?: false
           }

    refute inspect(failed) =~ "sample-secret"
  end

  test "raw Jido Emit directives deliver and expose redacted durable diagnostics" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: JidoEmitWorkflow,
               now: now,
               jido_dispatch_routes: jido_routes(self())
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Jizoku.Test.start(runtime, %{order_id: "order-test-runtime"})
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)

    assert_receive {:signal,
                    %Jido.Signal{
                      type: "minimal_host.order.prepared",
                      id: signal_id,
                      data: %{"order_id" => "order-test-runtime"}
                    }}

    assert signal_id == "#{run.run_id}:order-prepared"
    refute_receive {:signal, %Jido.Signal{id: ^signal_id}}
    assert completed.context.jido_signal_prepared == true
    assert completed.jido_signals.pending_count == 0
    assert completed.jido_signals.delivered_count == 1

    assert {:ok, timeline} = Jizoku.Test.timeline(runtime, completed)

    assert Enum.map(
             Enum.filter(
               timeline.events,
               &(&1.type in [:jido_signal_enqueued, :run_terminal, :jido_signal_delivered])
             ),
             & &1.type
           ) == [:jido_signal_enqueued, :run_terminal, :jido_signal_delivered]

    refute inspect(completed.jido_signals) =~ "order-test-runtime"
    refute inspect(timeline) =~ "order-test-runtime"
  end

  test "raw Jido Emit directives cross the Ecto journal and acknowledge delivery" do
    queue = "jido-emit-#{System.unique_integer([:positive])}"

    assert {:ok, started} =
             Jizoku.start(
               JidoEmitWorkflow,
               :manual,
               %{order_id: "order-ecto"},
               queue: queue
             )

    assert {:ok, completed} =
             Jizoku.execute_next(
               queue: queue,
               owner_id: "minimal-host-jido-emit",
               jido_dispatch_routes: jido_routes(self())
             )

    assert completed.run_id == started.run_id
    assert completed.status == :completed
    assert completed.jido_signals.pending_count == 0
    assert completed.jido_signals.delivered_count == 1

    assert_receive {:signal,
                    %Jido.Signal{
                      id: signal_id,
                      data: %{"order_id" => "order-ecto"}
                    }}

    assert signal_id == "#{started.run_id}:order-prepared"
    refute_receive {:signal, %Jido.Signal{id: ^signal_id}}

    assert {:ok, explanation} = Jizoku.explain_run(started.run_id, queue: queue)
    assert explanation.details.jido_signal_delivery.delivered_count == 1
    refute :deliver_jido_signals in explanation.next_actions
    refute inspect(explanation.details.jido_signal_delivery) =~ "order-ecto"
  end

  test "raw Jido error directives use durable workflow error transitions" do
    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: JidoErrorRecovery,
               now: ~U[2026-08-10 12:00:00Z]
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Jizoku.Test.start(runtime, %{})
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.jido_error_recovered == true
    refute Map.has_key?(completed.context, :must_not_be_applied)

    assert completed.attempts
           |> Enum.map(&{&1.step, &1.status})
           |> MapSet.new() ==
             MapSet.new([
               {"reject", :failed},
               {"recover", :completed}
             ])

    refute inspect(completed) =~ "sample-secret"
  end

  test "raw Jido instructions schedule allowlisted durable work" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: JidoInstructionWorkflow,
               action_registry: %{"sample.enrich" => JidoInstructionWorkflow.Enrich},
               now: now
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)
    assert {:ok, run} = Jizoku.Test.start(runtime, %{})

    assert {:reached, prepared} =
             Jizoku.Test.execute_until(
               runtime,
               run,
               &(&1.applied_runnable_keys != []),
               max_steps: 1
             )

    assert %{runnable_key: runnable_key, attempt_number: attempt} =
             Enum.find(prepared.planned_runnables, &(&1.step == "prepare"))

    origin = %{runnable_key: runnable_key, step: "prepare", attempt: attempt}

    instruction =
      Jido.Instruction.new!(
        id: "sample-instruction-1",
        action: JidoInstructionWorkflow.Enrich,
        params: %{order_id: "order-123"},
        context: %{request_id: "request-456"}
      )

    opts = [
      runtime: :journal,
      read_model: :read_model,
      journal_storage: runtime.storage,
      queue: runtime.queue,
      partition: runtime.partition,
      now: now,
      origin: origin,
      action_registry: %{"sample.enrich" => JidoInstructionWorkflow.Enrich}
    ]

    assert {:ok, scheduled} = WorkflowRuns.schedule_dynamic_work(run.run_id, instruction, opts)
    assert {:ok, ^scheduled} = WorkflowRuns.schedule_dynamic_work(run.run_id, instruction, opts)

    assert {:blocked, enriched} = Jizoku.Test.drain(runtime, run)

    assert enriched.context.instruction_order == %{
             id: "order-123",
             request_id: "request-456"
           }

    assert {:ok, _advanced} = Jizoku.Test.advance_time(runtime, 60, :second)
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.instruction_workflow_finished
  end

  test "raw Jido RunInstruction directives durably execute follow-up work" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: JidoRunInstructionWorkflow,
               action_registry: %{"sample.enrich" => JidoInstructionWorkflow.Enrich},
               now: now
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)
    assert {:ok, run} = Jizoku.Test.start(runtime, %{})
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.prepared == true
    assert completed.context.instruction_order.id == "order-from-directive"

    assert completed.context.instruction_order.request_id ==
             "sample-directive-request"

    assert [dynamic] = completed.dynamic_work
    assert dynamic.dynamic_key == "jido-instruction:sample-followup"
    assert [node] = dynamic.nodes

    assert node.metadata["jido_instruction"] == %{
             "context" => %{request_id: "sample-directive-request"},
             "id" => "sample-followup"
           }
  end

  test "raw Jido RunInstruction directives survive the Ecto journal boundary" do
    queue = "jido-run-instruction-#{System.unique_integer([:positive])}"
    registry = %{"sample.enrich" => JidoInstructionWorkflow.Enrich}

    assert {:ok, started} =
             Jizoku.start(JidoRunInstructionWorkflow, :manual, %{}, queue: queue)

    assert {:ok, after_source} =
             Jizoku.execute_next(
               queue: queue,
               owner_id: "minimal-host-jido-directive-source",
               action_registry: registry
             )

    assert after_source.run_id == started.run_id
    refute after_source.terminal?
    assert after_source.context.prepared == true
    refute Map.has_key?(after_source.context, :instruction_order)

    assert {:ok, completed} =
             Jizoku.execute_next(
               queue: queue,
               owner_id: "minimal-host-jido-directive-followup",
               action_registry: registry
             )

    assert completed.run_id == started.run_id
    assert completed.status == :completed

    assert completed.context.instruction_order == %{
             id: "order-from-directive",
             request_id: "sample-directive-request"
           }
  end

  test "public test runtime advances a host retry without sleeping" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: RetryVerification,
               now: now
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} =
             Jizoku.Test.start(runtime, %{attempt_id: "attempt-test-kit-virtual-time"})

    on_exit(fn ->
      :persistent_term.erase({MinimalHostApp.Steps.FailOnce, run.run_id})
    end)

    assert {:blocked, retrying} = Jizoku.Test.drain(runtime, run)
    assert retrying.next_visible_at == DateTime.add(now, 1_000, :millisecond)

    assert {:ok, explanation} = Jizoku.Test.explain(runtime, run)
    assert explanation.reason == :attempt_scheduled_for_later
    assert explanation.next_actions == [:wait_until_attempt_visible]
    assert explanation.details.next_visible_at == retrying.next_visible_at

    assert {:ok, timeline} = Jizoku.Test.timeline(runtime, run)
    assert Enum.any?(timeline.events, &(&1.type == :attempt_failed))

    assertion =
      assert_raise ExUnit.AssertionError, fn ->
        Jizoku.Test.assert_status(runtime, run, :completed, diagnostics: :timeline)
      end

    assert assertion.message =~ "timeline (schema v1)"
    assert assertion.message =~ "attempt_failed"

    assert {:ok, _now} = Jizoku.Test.advance_time(runtime, 1, :second)
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)

    assert completed.context.retry_probe == %{
             attempt_id: "attempt-test-kit-virtual-time",
             status: "ok"
           }
  end

  test "public test runtime executes a runtime-authored spec with deterministic stubs" do
    spec = %{
      workflow: MinimalHostApp.RuntimeAuthoredStubTest,
      definition_version: "minimal-host-test-stub-v1",
      triggers: [%{name: :manual, type: :manual, config: %{}, payload: []}],
      payload: [],
      steps: [
        %{name: :prepare, action: "test.prepare", opts: [output: :prepared]},
        %{
          name: :deliver,
          action: "test.deliver",
          opts: [input: [:prepared], output: :delivery]
        }
      ],
      transitions: [
        %{from: :prepare, on: :ok, to: :deliver},
        %{from: :deliver, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:prepare],
      initial_step: :prepare,
      entry_step: :prepare
    }

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: spec,
               action_stubs: %{
                 "test.prepare" => [{:ok, %{message_id: "message-123"}}],
                 "test.deliver" => [{:ok, %{status: "delivered"}}]
               }
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Jizoku.Test.start(runtime, %{})
    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.prepared == %{message_id: "message-123"}
    assert completed.context.delivery == %{status: "delivered"}

    assert {:ok, calls} = Jizoku.Test.stub_calls(runtime, "test.deliver")
    assert [%{input: %{prepared: %{message_id: "message-123"}}}] = calls
  end

  test "public test runtime starts a host cron trigger at frozen time" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: DailyDigest,
               now: now
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} =
             Jizoku.Test.start_cron(
               runtime,
               :daily_digest,
               %{channel: "ops", digest_date: "2026-08-10"},
               idempotency_key: "daily-digest-test-kit"
             )

    assert run.started_at == now
    assert run.context.schedule.received_at == DateTime.to_iso8601(now)
    assert run.context.schedule.signal_id == "daily-digest-test-kit"

    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.digest_delivery.channel == "ops"
    assert completed.context.digest_delivery.digest_date == "2026-08-10"
  end

  test "public test runtime approves a host workflow without a worker" do
    now = ~U[2026-08-10 12:00:00Z]

    assert {:ok, runtime} =
             Jizoku.Test.start_runtime(
               workflow: ManualApproval,
               now: now
             )

    on_exit(fn -> Jizoku.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Jizoku.Test.start(runtime, %{account_id: "acct-test-kit"})
    assert {:blocked, paused} = Jizoku.Test.drain(runtime, run)
    assert paused.status == :paused
    assert paused.manual_state.step == "wait_for_approval"

    assert {:ok, _now} = Jizoku.Test.advance_time(runtime, 30, :second)

    assert {:ok, %{status: :running}} =
             Jizoku.Test.approve(
               runtime,
               run,
               %{actor: "ops-test", comment: "approved in test"},
               idempotency_key: "approval-test-kit"
             )

    assert {:completed, completed} = Jizoku.Test.drain(runtime, run)
    assert completed.context.approval.status == "approved"
    assert completed.context.approval.actor == "ops-test"
    assert completed.context.approval.comment == "approved in test"
  end

  defmodule InvalidRecurringIdempotentCronWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :daily_digest do
        cron "0 9 * * *", timezone: "Etc/UTC", idempotency: :return_existing_run

        payload do
          field :channel, :string, default: "ops"
          field :digest_date, :string, default: {:today, :iso8601}
        end
      end

      step :announce_digest, :log, message: "posting daily digest"
      step :record_digest_delivery, MinimalHostApp.Steps.RecordDigestDelivery

      transition :announce_digest, on: :ok, to: :record_digest_delivery
      transition :record_digest_delivery, on: :ok, to: :complete
    end
  end

  test "host app workflow examples expose Spark-backed workflow DSL metadata" do
    daily_digest_entities = Spark.Dsl.Extension.get_entities(DailyDigest, [:workflow])

    assert [
             %Jizoku.Workflow.TriggerSpec{
               name: :manual_digest,
               definitions: [%Jizoku.Workflow.TriggerDefinitionSpec{type: :manual}],
               payload: [%Jizoku.Workflow.PayloadSpec{fields: manual_fields}]
             },
             %Jizoku.Workflow.TriggerSpec{
               name: :daily_digest,
               definitions: [
                 %Jizoku.Workflow.TriggerDefinitionSpec{
                   type: :cron,
                   config: %{
                     expression: "@reboot",
                     timezone: "Etc/UTC",
                     idempotency: :return_existing_run
                   }
                 }
               ],
               payload: [%Jizoku.Workflow.PayloadSpec{fields: cron_fields}]
             }
           ] = Enum.filter(daily_digest_entities, &match?(%Jizoku.Workflow.TriggerSpec{}, &1))

    assert Enum.map(manual_fields, & &1.name) == [:channel, :digest_date]
    assert Enum.map(cron_fields, & &1.name) == [:channel, :digest_date]

    payment_recovery_entities = Spark.Dsl.Extension.get_entities(PaymentRecovery, [:workflow])

    assert Enum.any?(payment_recovery_entities, fn
             %Jizoku.Workflow.TransitionSpec{
               from: :check_gateway_status,
               on: :ok,
               to: :notify_customer,
               condition: %{path: [:gateway_check, :status_code], greater_than: 199}
             } ->
               true

             _other ->
               false
           end)

    assert Enum.any?(payment_recovery_entities, fn
             %Jizoku.Workflow.TransitionSpec{
               from: :check_gateway_status,
               on: :ok,
               to: :issue_gateway_credit,
               condition: condition
             } ->
               is_nil(condition)

             _other ->
               false
           end)

    assert Enum.any?(payment_recovery_entities, fn
             %Jizoku.Workflow.TransitionSpec{
               from: :check_gateway_status,
               on: :error,
               to: :issue_gateway_credit,
               recovery: :compensation
             } ->
               true

             _other ->
               false
           end)
  end

  test "host app examples validate runtime-authored specs through a safe action registry" do
    spec = %Jizoku.Workflow.Spec{
      workflow: MinimalHostApp.RuntimeAuthoredPaymentRecovery,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [
            %{name: :account_id, type: :string, opts: []},
            %{name: :invoice_id, type: :string, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :account_id, type: :string, opts: []},
        %{name: :invoice_id, type: :string, opts: []}
      ],
      steps: [
        %{name: :load_invoice, action: "payment.load_invoice", opts: []},
        %{name: :notify_customer, action: "payment.notify_customer", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :notify_customer},
        %{from: :notify_customer, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }

    registry = %{
      "payment.load_invoice" => Steps.LoadInvoice,
      "payment.notify_customer" => Steps.NotifyCustomer
    }

    assert :ok = Jizoku.Workflow.validate_spec(spec, action_registry: registry)

    assert {:ok, resolved} =
             Jizoku.Workflow.resolve_spec_actions(spec, action_registry: registry)

    assert Enum.map(resolved.steps, &{&1.name, &1.module, &1.metadata.action}) == [
             {:load_invoice, Steps.LoadInvoice, "payment.load_invoice"},
             {:notify_customer, Steps.NotifyCustomer, "payment.notify_customer"}
           ]
  end

  test "starts a runtime-authored workflow through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_runtime_digest(%{
               channel: "ops",
               digest_date: "2026-05-30"
             })

    assert run.workflow == "Elixir.MinimalHostApp.RuntimeAuthoredDigest"
    assert run.trigger == "manual_digest"
    assert run.definition_version == "minimal-host-runtime-digest-v1"
    assert [%{step: "record_digest_delivery", status: :available}] = run.visible_attempts

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert completed_run.status == :completed

    assert {:ok, graph} = Jizoku.inspect_run_graph(run.run_id)
    assert Enum.map(graph.nodes, & &1.id) == ["record_digest_delivery"]
  end

  test "continues a recurring cursor as a fresh linked run through the host boundary" do
    queue = "minimal-host-continuation-#{System.unique_integer([:positive])}"

    assert {:ok, predecessor} =
             WorkflowRuns.start_recurring_cursor(%{cursor: 0}, queue: queue)

    assert {:ok, successor} =
             Jizoku.execute_next(
               queue: queue,
               owner_id: "minimal-host-continuation-worker-1"
             )

    assert successor.run_id != predecessor.run_id
    assert successor.input == %{cursor: 1}
    assert successor.continuation.continued_from.run_id == predecessor.run_id

    assert {:ok, completed_successor} =
             Jizoku.execute_next(
               queue: queue,
               owner_id: "minimal-host-continuation-worker-2"
             )

    assert completed_successor.run_id == successor.run_id
    assert completed_successor.status == :completed

    assert {:ok, continued_predecessor} =
             WorkflowRuns.inspect_run(predecessor.run_id, queue: queue)

    assert continued_predecessor.status == :continued
    assert continued_predecessor.continuation.continued_to.run_id == successor.run_id

    assert {:ok, chain} =
             WorkflowRuns.inspect_continuation_chain(successor.run_id,
               direction: :backward,
               max_hops: 5
             )

    assert chain.truncated? == false
    assert Enum.map(chain.runs, & &1.run_id) == [successor.run_id, predecessor.run_id]
  end

  test "host app examples round-trip workflow specs through the editor JSON contract" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(PaymentRecovery)

    round_tripped =
      spec
      |> Jizoku.Workflow.EditorSpec.to_map()
      |> Jason.encode!()
      |> Jason.decode!()

    assert :ok = Jizoku.Workflow.EditorSpec.validate_map(round_tripped)

    assert {:ok, graph} = Jizoku.Workflow.EditorSpec.preview_graph(round_tripped)

    assert Enum.map(graph["nodes"], & &1["id"]) == [
             "load_invoice",
             "check_gateway_status",
             "issue_gateway_credit",
             "notify_customer"
           ]

    assert Enum.any?(graph["edges"], &(&1["recovery"] == "compensation"))

    assert Enum.any?(graph["edges"], fn edge ->
             edge["from"] == "check_gateway_status" and
               edge["to"] == "notify_customer" and
               edge["condition"] == %{
                 "path" => ["gateway_check", "status_code"],
                 "greater_than" => 199
               }
           end)

    assert Enum.any?(graph["edges"], fn edge ->
             edge["from"] == "check_gateway_status" and
               edge["to"] == "issue_gateway_credit" and
               edge["outcome"] == "ok" and
               edge["condition"] == nil
           end)
  end

  test "starts the example payment recovery workflow through the host boundary" do
    bypass = Bypass.open()

    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789",
      gateway_url: endpoint_url(bypass.port, "/gateway")
    }

    assert {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)

    assert run.workflow == "Elixir.MinimalHostApp.Workflows.PaymentRecovery"
    assert run.trigger == "payment_recovery"
    assert run.status == :running
    assert run.input == attrs
    assert [%{step: "load_invoice", status: :available}] = run.visible_attempts

    assert {:ok, advanced_run} =
             Jizoku.execute_next(owner_id: "minimal-host-app-sla-contract-test")

    assert advanced_run.run_id == run.run_id

    assert [
             %{
               step: "check_gateway_status",
               status: :available,
               deadline: %{status: :on_time, escalation: %{outcome: :diagnostic}}
             }
           ] = advanced_run.visible_attempts

    assert {:ok, [listed_run]} =
             Jizoku.list_runs([workflow: MinimalHostApp.Workflows.PaymentRecovery],
               now: DateTime.utc_now()
             )

    assert listed_run.run_id == run.run_id
    assert listed_run.deadline.status == :on_time
    assert listed_run.deadline.step == "check_gateway_status"

    assert {:ok, graph} = Jizoku.inspect_run_graph(run.run_id)
    graph_nodes = Map.new(Jizoku.Runs.GraphInspection.to_map(graph).nodes, &{&1.id, &1})

    assert graph_nodes["check_gateway_status"].deadline.status == :on_time
    assert graph_nodes["check_gateway_status"].deadline.escalation == %{outcome: :diagnostic}
  end

  test "pages approved search attributes through the host dashboard boundary" do
    account_id = "acct_dashboard_#{System.unique_integer([:positive])}"

    attrs = fn attempt_id ->
      %{
        account_id: account_id,
        invoice_id: "invoice_#{attempt_id}",
        attempt_id: attempt_id
      }
    end

    assert {:ok, first_run} =
             WorkflowRuns.start_indexed_dependency_recovery(attrs.("attempt_a"),
               queue: "minimal-host-dashboard"
             )

    assert {:ok, second_run} =
             WorkflowRuns.start_indexed_dependency_recovery(attrs.("attempt_b"),
               queue: "minimal-host-dashboard"
             )

    update_opts = [
      queue: "minimal-host-dashboard",
      idempotency_key: "dashboard:confirm-workflow-kind"
    ]

    assert {:ok, updated_run} =
             Jizoku.update_search_attributes(
               first_run.run_id,
               %{"workflow_kind" => "dependency_recovery"},
               update_opts
             )

    assert {:ok, duplicate_update} =
             Jizoku.update_search_attributes(
               first_run.run_id,
               %{"workflow_kind" => "dependency_recovery"},
               update_opts
             )

    assert duplicate_update.thread_revisions == updated_run.thread_revisions

    assert {:ok, %Page{items: [first_item], next_cursor: cursor}} =
             WorkflowRuns.page_dependency_recovery_runs(account_id, first: 1)

    assert first_item.search_attributes == %{}
    assert is_binary(cursor)

    assert {:ok, %Page{items: [second_item], next_cursor: nil}} =
             WorkflowRuns.page_dependency_recovery_runs(account_id, first: 1, after: cursor)

    assert second_item.search_attributes == %{}

    assert {:ok, %Page{items: overridden_items}} =
             WorkflowRuns.page_dependency_recovery_runs(account_id,
               first: 2,
               visibility_policy: :auditor
             )

    assert Enum.all?(overridden_items, &(&1.search_attributes == %{}))

    assert MapSet.new([first_item.run_id, second_item.run_id]) ==
             MapSet.new([first_run.run_id, second_run.run_id])

    assert {:ok, %Page{items: auditor_items}} =
             Jizoku.list_runs(
               [
                 workflow: DependencyRecovery,
                 attributes: %{"account_id" => account_id},
                 first: 2
               ],
               visibility_policy: :auditor
             )

    assert Enum.all?(auditor_items, fn item ->
             item.search_attributes == %{
               "account_id" => account_id,
               "workflow_kind" => "dependency_recovery"
             }
           end)
  end

  test "returns payload validation errors for missing indexed account identifiers" do
    assert {:error, {:invalid_payload, details}} =
             WorkflowRuns.start_indexed_dependency_recovery(%{
               invoice_id: "invoice_missing_account",
               attempt_id: "attempt_missing_account"
             })

    assert details.missing_fields == [:account_id]
  end

  test "archives and restores terminal runs through the host dashboard boundary" do
    unique = System.unique_integer([:positive])
    account_id = "acct_archive_#{unique}"
    queue = "minimal-host-archive-#{unique}"
    runtime_opts = [queue: queue]

    assert {:ok, started} =
             WorkflowRuns.start_indexed_dependency_recovery(
               %{
                 account_id: account_id,
                 invoice_id: "invoice_archive_#{unique}",
                 attempt_id: "attempt_archive_#{unique}"
               },
               runtime_opts
             )

    assert {:ok, _cancelled} = WorkflowRuns.cancel(started.run_id, runtime_opts)

    assert {:ok, archived} =
             WorkflowRuns.archive_for_retention_hold(started.run_id, runtime_opts)

    assert archived.archived?
    assert archived.archive_reason == "retention_hold"

    assert {:ok, %Page{items: [], next_cursor: nil}} =
             WorkflowRuns.page_dependency_recovery_runs(account_id, first: 10)

    assert {:ok, [listed]} =
             WorkflowRuns.list_archived_dependency_recovery_runs(account_id)

    assert listed.run_id == started.run_id
    assert listed.archived?
    assert listed.archive_reason == nil

    assert {:ok, restored} = WorkflowRuns.unarchive(started.run_id, runtime_opts)
    refute restored.archived?

    assert {:ok, %Page{items: [visible], next_cursor: nil}} =
             WorkflowRuns.page_dependency_recovery_runs(account_id, first: 10)

    assert visible.run_id == started.run_id
  end

  test "previews and applies retention through the host operations boundary" do
    unique = System.unique_integer([:positive])
    queue = "minimal-host-retention-#{unique}"
    runtime_opts = [queue: queue]

    assert {:ok, started} =
             WorkflowRuns.start_indexed_dependency_recovery(
               %{
                 account_id: "acct_retention_#{unique}",
                 invoice_id: "invoice_retention_#{unique}",
                 attempt_id: "attempt_retention_#{unique}"
               },
               runtime_opts
             )

    assert {:ok, _cancelled} = WorkflowRuns.cancel(started.run_id, runtime_opts)

    assert {:ok, _archived} =
             WorkflowRuns.archive_for_retention_hold(started.run_id, runtime_opts)

    now = DateTime.utc_now()

    assert {:ok, plan} =
             WorkflowRuns.preview_retention(
               [terminal_before: DateTime.add(now, 60, :second)],
               now: now
             )

    assert Enum.map(plan.eligible, & &1.run_id) == [started.run_id]

    assert {:ok, receipt} =
             WorkflowRuns.apply_retention(plan, plan.confirmation_token)

    assert receipt.run_ids == [started.run_id]
    assert receipt.run_entries_deleted > 0
    assert receipt.dispatch_entries_deleted > 0

    assert {:ok, duplicate_receipt} =
             WorkflowRuns.apply_retention(plan, plan.confirmation_token)

    assert duplicate_receipt.idempotent?
    assert %{duplicate_receipt | idempotent?: false} == receipt
    assert {:error, :not_found} = WorkflowRuns.inspect_run(started.run_id, runtime_opts)
  end

  test "inspects a started run through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_payment_recovery(%{
               account_id: "acct_123",
               invoice_id: "inv_456",
               attempt_id: "attempt_789",
               gateway_url: "http://127.0.0.1:4010/gateway"
             })

    assert {:ok, inspected_run} = WorkflowRuns.inspect_payment_recovery(run.run_id)
    assert %{inspected_run | definition_resolution: nil} == run
    assert inspected_run.definition_resolution.status == :resolved
  end

  test "surfaces payment recovery compensation through host inspection history" do
    {server_pid, port} =
      MinimalHostApp.RuntimeHarness.start_gateway_server(
        fn _attempt -> MinimalHostApp.RuntimeHarness.failure_gateway_response(503, "down") end,
        10
      )

    on_exit(fn -> MinimalHostApp.RuntimeHarness.stop_gateway_server(server_pid) end)

    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789",
      gateway_url: endpoint_url(port, "/gateway")
    }

    assert {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.compensation == %{
             account_id: "acct_123",
             invoice_id: "inv_456",
             status: "credit_issued"
           }

    assert %{
             idempotency_key: _idempotency_key,
             claim_id: _claim_id
           } =
             failed_attempt =
             Enum.find(history_run.attempts, fn attempt ->
               attempt.step == "check_gateway_status" and attempt.status == :failed
             end)

    refute Map.has_key?(failed_attempt, :claim_token)

    assert [
             {"load_invoice", :completed, true, 1},
             {"check_gateway_status", :failed, false, 1},
             {"check_gateway_status", :failed, false, 2},
             {"check_gateway_status", :failed, false, 3},
             {"check_gateway_status", :failed, false, 4},
             {"check_gateway_status", :failed, true, 5},
             {"issue_gateway_credit", :completed, true, 1}
           ] =
             Enum.map(history_run.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number})
  end

  test "executes a dependency-based workflow through the host boundary" do
    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789"
    }

    assert {:ok, run} = WorkflowRuns.start_dependency_recovery(attrs)
    assert run.definition_version == "2026-05-26.dependency-recovery"
    assert Enum.map(run.visible_attempts, & &1.step) == ["load_account", "load_invoice"]

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed
    assert completed_run.definition_version == "2026-05-26.dependency-recovery"
    assert history_run.definition_version == "2026-05-26.dependency-recovery"
    assert completed_run.context.account == %{id: "acct_123", tier: "standard"}

    assert completed_run.context.invoice == %{
             id: "inv_456",
             account_id: "acct_123",
             attempt_id: "attempt_789"
           }

    assert completed_run.context.notification == %{
             channel: "email",
             account_id: "acct_123",
             invoice_id: "inv_456",
             account_tier: "standard"
           }

    assert [
             _load_account,
             _load_invoice,
             %{step: "prepare_notification", input: prepare_notification_input}
           ] = history_run.attempts

    assert prepare_notification_input == %{
             account_id: "acct_123",
             invoice_id: "inv_456",
             account_tier: "standard"
           }
  end

  test "executes a nested workflow through the host boundary with durable parent and child retries" do
    child_queue = "minimal-host-app-nested-child-#{System.unique_integer([:positive])}"

    attrs = %{
      party_id: "party_123",
      guest_id: "guest_456",
      child_queue: child_queue,
      fail_after_child_start: true,
      fail_child_once: true
    }

    assert {:ok, run} = WorkflowRuns.start_nested_invite_delivery(attrs)
    assert [%{step: "start_nested_invite", status: :available}] = run.visible_attempts

    assert {:ok, retried_parent} =
             Jizoku.execute_next(owner_id: "minimal-host-app-nested-parent-test")

    assert retried_parent.status == :running

    assert [
             %{
               child_run_id: child_run_id,
               child_key: "invite_guest_456",
               child_trigger: "deliver_invite",
               metadata: %{guest_id: "guest_456"}
             }
           ] = retried_parent.child_runs

    assert {:ok, parent_graph} = Jizoku.inspect_run_graph(run.run_id)

    parent_graph_map = Jizoku.Runs.GraphInspection.to_map(parent_graph)

    assert [
             %{
               from: "start_nested_invite",
               to: ^child_run_id,
               type: :child_run,
               status: :linked,
               child_run_id: ^child_run_id,
               child_key: "invite_guest_456",
               child_trigger: "deliver_invite",
               metadata: %{guest_id: "guest_456"}
             }
           ] = parent_graph_map.child_links

    assert [%{step: "start_nested_invite", status: :retry_scheduled, attempt_number: 2}] =
             retried_parent.visible_attempts

    assert {:ok, child_before_parent_retry} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert child_before_parent_retry.status == :running

    assert [%{step: "deliver_invite", status: :available}] =
             child_before_parent_retry.visible_attempts

    Repo.delete_all("jizoku_journal_checkpoints")

    assert {:ok, reconstructed_retried_parent} = WorkflowRuns.inspect_run(run.run_id)

    assert {:ok, reconstructed_waiting_child} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert {:ok, reconstructed_parent_graph} = Jizoku.inspect_run_graph(run.run_id)

    reconstructed_parent_graph_map =
      Jizoku.Runs.GraphInspection.to_map(reconstructed_parent_graph)

    assert [%{from: "start_nested_invite", to: ^child_run_id, type: :child_run}] =
             reconstructed_parent_graph_map.child_links

    assert reconstructed_retried_parent.child_runs == retried_parent.child_runs
    assert reconstructed_waiting_child.parent_run == child_before_parent_retry.parent_run
    assert reconstructed_waiting_child.status == :running

    assert {:ok, completed_parent} =
             Jizoku.execute_next(owner_id: "minimal-host-app-nested-parent-test")

    assert completed_parent.status == :completed

    assert {:ok, child_still_running} = WorkflowRuns.inspect_run(child_run_id, queue: child_queue)
    assert child_still_running.status == :running

    assert {:ok, child_retrying} =
             Jizoku.execute_next(
               owner_id: "minimal-host-app-nested-child-test",
               queue: child_queue
             )

    assert child_retrying.status == :running

    assert [%{step: "deliver_invite", status: :retry_scheduled, attempt_number: 2}] =
             child_retrying.visible_attempts

    Repo.delete_all("jizoku_journal_checkpoints")

    assert {:ok, reconstructed_retrying_child} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert reconstructed_retrying_child.visible_attempts == child_retrying.visible_attempts
    assert reconstructed_retrying_child.parent_run == child_before_parent_retry.parent_run

    assert {:ok, completed_child} =
             Jizoku.execute_next(
               owner_id: "minimal-host-app-nested-child-test",
               queue: child_queue
             )

    assert completed_child.status == :completed

    assert completed_parent.context.invite_child == %{
             run_id: child_run_id,
             child_key: "invite_guest_456",
             queue: child_queue,
             reused_after_retry?: true
           }

    assert {:ok, parent_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert {:ok, child_history} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue, include_history: true)

    assert [
             {"start_nested_invite", :failed, false, 1},
             {"start_nested_invite", :completed, true, 2}
           ] =
             Enum.map(
               parent_history.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             )

    assert [
             {"deliver_invite", :failed, false, 1},
             {"deliver_invite", :completed, true, 2}
           ] =
             Enum.map(
               child_history.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             )

    assert [%{runnable_key: parent_runnable_key} | _remaining_parent_attempts] =
             parent_history.attempts

    assert child_history.parent_run == %{
             run_id: run.run_id,
             runnable_key: parent_runnable_key,
             step: "start_nested_invite",
             attempt: 1,
             child_key: "invite_guest_456",
             metadata: %{guest_id: "guest_456"}
           }

    Repo.delete_all("jizoku_journal_checkpoints")

    assert {:ok, reconstructed_parent} = WorkflowRuns.inspect_run(run.run_id)
    assert {:ok, reconstructed_child} = WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert reconstructed_parent.child_runs == parent_history.child_runs
    assert reconstructed_child.parent_run == child_history.parent_run

    assert {:ok, replayed_parent} = WorkflowRuns.replay(run.run_id)
    assert replayed_parent.replayed_from_run_id == run.run_id
    assert replayed_parent.child_runs == []

    assert {:ok, replayed_after_first_attempt} =
             Jizoku.execute_next(owner_id: "minimal-host-app-nested-replay-test")

    assert [%{child_run_id: replayed_child_run_id}] = replayed_after_first_attempt.child_runs
    refute replayed_child_run_id == child_run_id

    assert {:ok, replayed_completed_parent} =
             Jizoku.execute_next(owner_id: "minimal-host-app-nested-replay-test")

    assert replayed_completed_parent.status == :completed
    assert replayed_completed_parent.context.invite_child.run_id == replayed_child_run_id
    assert replayed_completed_parent.context.invite_child.reused_after_retry? == true

    assert {:ok, replayed_retrying_child} =
             Jizoku.execute_next(
               owner_id: "minimal-host-app-nested-replay-child-test",
               queue: child_queue
             )

    assert replayed_retrying_child.status == :running

    assert {:ok, replayed_completed_child} =
             Jizoku.execute_next(
               owner_id: "minimal-host-app-nested-replay-child-test",
               queue: child_queue
             )

    assert replayed_completed_child.status == :completed

    assert {:ok, replayed_child_history} =
             WorkflowRuns.inspect_run(replayed_child_run_id,
               queue: child_queue,
               include_history: true
             )

    assert [
             {"deliver_invite", :failed, false, 1},
             {"deliver_invite", :completed, true, 2}
           ] =
             Enum.map(
               replayed_child_history.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             )

    assert replayed_child_history.parent_run.run_id == replayed_parent.run_id
    assert replayed_child_history.parent_run.child_key == "invite_guest_456"
  end

  test "executes a dependency workflow through the supervised journal run loop" do
    attrs = %{
      account_id: "acct_supervised_run",
      invoice_id: "inv_supervised_run",
      attempt_id: "attempt_supervised_run"
    }

    assert {:ok, run} = WorkflowRuns.start_dependency_recovery(attrs)

    journal_run_name = :"minimal_host_app_journal_run_#{System.unique_integer([:positive])}"

    start_supervised!(
      {MinimalHostApp.JournalRun,
       name: journal_run_name,
       owner_id: "minimal-host-app-supervised-test",
       heartbeat_interval_ms: 100,
       idle_interval_ms: 10,
       error_interval_ms: 10}
    )

    assert {:ok, completed_run} = await_terminal_without_harness(run.run_id)
    assert completed_run.status == :completed
    assert completed_run.context.notification.account_id == "acct_supervised_run"
  end

  test "executes a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-#{System.unique_integer([:positive])}"

    with_jizoku_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        assert {:ok, config} = Jizoku.config()
        assert config.runtime == :journal
        assert config.read_model == :read_model
        assert config.journal_storage.adapter == Jizoku.Runtime.Journal.Storage.Ecto
        assert config.journal_storage.opts == [repo: Repo]

        attrs = %{
          account_id: "acct_default_journal",
          invoice_id: "inv_default_journal",
          attempt_id: "attempt_default_journal"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert started_run.queue == queue
        assert started_run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
        assert started_run.status == :running

        assert {:ok, %Snapshot{} = completed_run} =
                 drain_default_journal_run(started_run.run_id, queue, 10)

        assert completed_run.status == :completed
        assert completed_run.applied_runnable_keys == completed_run.planned_runnable_keys

        assert {:ok, listed_runs} = WorkflowRuns.list_runs(now: DateTime.utc_now())

        listed_run = Enum.find(listed_runs, &(&1.run_id == started_run.run_id))
        assert %Summary{} = listed_run

        assert listed_run.run_id == started_run.run_id
        assert listed_run.queue == queue
        assert listed_run.status == :completed
        refute Map.has_key?(Map.from_struct(listed_run), :attempts)
        refute Map.has_key?(Map.from_struct(listed_run), :input)
        refute Map.has_key?(Map.from_struct(listed_run), :result)

        row = %{
          id: listed_run.run_id,
          workflow: listed_run.workflow,
          queue: listed_run.queue,
          status: listed_run.status,
          inserted_at: listed_run.indexed_at
        }

        assert row.id == started_run.run_id
        assert row.queue == queue

        assert {:ok, %Snapshot{} = inspected_run} =
                 WorkflowRuns.inspect_run(row.id,
                   queue: row.queue,
                   now: DateTime.utc_now(),
                   include_history: true
                 )

        assert inspected_run.run_id == row.id
        assert inspected_run.queue == row.queue

        assert Enum.map(completed_run.attempts, &{&1.step, &1.status, &1.applied?}) == [
                 {"load_account", :completed, true},
                 {"load_invoice", :completed, true},
                 {"prepare_notification", :completed, true}
               ]
      end
    )
  end

  test "cancels a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-cancel-#{System.unique_integer([:positive])}"

    with_jizoku_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        attrs = %{
          account_id: "acct_default_journal_cancel",
          invoice_id: "inv_default_journal_cancel",
          attempt_id: "attempt_default_journal_cancel"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert started_run.queue == queue
        assert started_run.status == :running
        assert [_attempt | _] = started_run.visible_attempts

        assert {:ok, %Snapshot{} = cancelled_run} = WorkflowRuns.cancel(started_run.run_id)

        assert cancelled_run.run_id == started_run.run_id
        assert cancelled_run.queue == queue
        assert cancelled_run.status == :cancelled
        assert cancelled_run.terminal?
        assert cancelled_run.terminal_status == :cancelled
        assert cancelled_run.visible_attempts == []

        assert [
                 %{signal_type: "start_run"},
                 %{
                   signal_type: "cancel_run",
                   metadata: %{source: "minimal_host_app.workflow_runs"}
                 }
               ] = cancelled_run.command_history

        assert {:ok, %Snapshot{} = inspected_run} = WorkflowRuns.inspect_run(started_run.run_id)
        assert inspected_run.status == :cancelled
        assert inspected_run.queue == queue

        assert {:ok, :none} =
                 Jizoku.execute_next(owner_id: "minimal-host-app-default-journal-cancel-test")
      end
    )
  end

  test "replays a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-replay-#{System.unique_integer([:positive])}"

    with_jizoku_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        attrs = %{
          account_id: "acct_default_journal_replay",
          invoice_id: "inv_default_journal_replay",
          attempt_id: "attempt_default_journal_replay"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert {:ok, %Snapshot{status: :completed} = completed_run} =
                 drain_default_journal_run(started_run.run_id, queue, 10)

        assert {:ok, %Snapshot{} = replayed_run} = WorkflowRuns.replay(completed_run.run_id)

        assert replayed_run.run_id != completed_run.run_id
        assert replayed_run.replayed_from_run_id == completed_run.run_id
        assert replayed_run.queue == queue
        assert replayed_run.input == attrs
        assert replayed_run.status == :running
        assert replayed_run.visible_attempts != []
      end
    )
  end

  test "commits local repo transaction groups through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_123",
               fail_after_reserve: false
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert completed_run.status == :completed
    assert completed_run.context.local_ledger == %{status: "committed", entries: 2}
    assert local_ledger_entries(run.run_id) == ["reserve", "capture"]
  end

  test "rolls back local repo transaction groups when the step fails" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_456",
               fail_after_reserve: true
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, failed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert failed_run.status == :failed

    assert [%{step: "post_local_ledger_entries", status: :failed, error: error}] =
             failed_run.attempts

    assert error.message == "step execution failed"
    assert error.retryable? == false

    assert local_ledger_entries(run.run_id) == []
  end

  test "approves a manual approval workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_approval_123"})

    assert {:ok, %Snapshot{status: :paused}} =
             Jizoku.execute_next(owner_id: "minimal-host-app-approval-test")

    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.manual_state.step == "wait_for_approval"
    assert paused_run.manual_state.deadline.status == :on_time
    assert paused_run.manual_state.deadline.escalation == %{outcome: :operator_action}

    assert {:ok, resumed_run} =
             WorkflowRuns.approve(
               run.run_id,
               %{actor: "ops_123", comment: "approved", metadata: %{ticket: "SUP-123"}}
             )

    assert resumed_run.status == :running
    assert [%{step: "record_approval", status: :available}] = resumed_run.visible_attempts

    assert {:ok, completed_run} =
             MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.approval.status == "approved"
    assert completed_run.context.approval.decision == "approved"
    assert completed_run.context.approval.actor == "ops_123"
    assert completed_run.context.approval.comment == "approved"

    assert completed_history.manual_state == nil

    assert [
             %{signal_type: "start_run"},
             %{
               signal_type: "approve_run",
               payload: %{
                 run_id: approved_run_id,
                 attributes: %{actor: "ops_123", comment: "approved"}
               },
               metadata: %{ticket: "SUP-123"},
               actor: "ops_123",
               comment: "approved"
             }
           ] = completed_history.command_history

    assert approved_run_id == run.run_id
  end

  test "resumes a manual pause workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_pause(%{account_id: "acct_pause_123"})

    assert {:ok, %Snapshot{status: :paused}} =
             Jizoku.execute_next(owner_id: "minimal-host-app-resume-test")

    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.manual_state.step == "wait_for_resume"

    assert {:ok, resumed_run} = WorkflowRuns.resume(run.run_id, %{actor: "ops_resume"})

    assert resumed_run.status == :running
    assert [%{step: "record_resume", status: :available}] = resumed_run.visible_attempts

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed
    assert completed_history.manual_state == nil

    assert [
             %{signal_type: "start_run"},
             %{
               signal_type: "resume_run",
               payload: %{run_id: resumed_run_id, attributes: %{actor: "ops_resume"}}
             }
           ] = completed_history.command_history

    assert resumed_run_id == run.run_id
  end

  test "runs the daily digest workflow through its manual trigger" do
    attrs = %{channel: "ops-manual", digest_date: "2026-05-10"}

    assert {:ok, run} = WorkflowRuns.start_manual_digest(attrs)

    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DailyDigest"
    assert run.trigger == "manual_digest"
    assert run.input == attrs

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert completed_run.status == :completed
    assert completed_run.context.digest_delivery.channel == "ops-manual"
    assert completed_run.context.digest_delivery.digest_date == "2026-05-10"

    assert {:ok, inspected_run} = WorkflowRuns.inspect_run(run.run_id)
    assert inspected_run.input == attrs
  end

  test "runs the daily digest workflow through its cron trigger" do
    signal_id = unique_reboot_signal_id()

    existing_run_ids =
      case WorkflowRuns.list_daily_digest_runs() do
        {:ok, runs} -> MapSet.new(runs, & &1.run_id)
        {:error, _reason} -> MapSet.new()
      end

    job = %Oban.Job{
      args: %{
        "kind" => "cron",
        "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
        "trigger" => "daily_digest",
        "signal_id" => signal_id
      }
    }

    assert :ok = MinimalHostApp.Workers.JizokuWorker.perform(job)

    assert {:ok, runs} = WorkflowRuns.list_daily_digest_runs()
    run = Enum.find(runs, fn run -> not MapSet.member?(existing_run_ids, run.run_id) end)

    assert %Summary{} = run

    assert {:ok, inspected_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert inspected_run.status == :completed
    assert inspected_run.trigger == "daily_digest"
    assert is_binary(inspected_run.input.digest_date)
    assert inspected_run.input.channel == "ops"
    assert inspected_run.context.schedule.idempotency == :return_existing_run
    assert inspected_run.context.schedule.idempotency_key == signal_id
  end

  test "the host worker forwards non-cron payload errors from the runtime" do
    assert {:error, {:invalid_jizoku_payload, %{"kind" => "step", "run_id" => _run_id}}} =
             JizokuWorker.perform(%Job{
               args: %{
                 "kind" => "step",
                 "run_id" => Ecto.UUID.generate(),
                 "step" => "charge_card"
               }
             })
  end

  test "the cron delivery adapter reports adapter metadata" do
    assert {:ok, metadata} =
             MinimalHostApp.JizokuDeliveryAdapter.enqueue_cron(
               %{},
               DailyDigest,
               :daily_digest,
               signal_id: "minimal-host-app:metadata-test"
             )

    assert metadata.adapter == MinimalHostApp.JizokuDeliveryAdapter
    assert metadata.queue == :jizoku
    assert metadata.worker == "MinimalHostApp.Workers.JizokuWorker"
  end

  test "generates a new reboot signal id for each cron plugin boot" do
    first_signal_id = plugin_reboot_signal_id()
    second_signal_id = plugin_reboot_signal_id()

    assert first_signal_id != second_signal_id
    assert String.starts_with?(first_signal_id, "minimal-host-app:reboot:")

    assert String.ends_with?(
             first_signal_id,
             ":Elixir.MinimalHostApp.Workflows.DailyDigest:daily_digest"
           )
  end

  test "skips duplicate daily digest cron activation in the host example" do
    signal_id = "minimal-host-app:test:daily_digest:duplicate"

    payload = %{
      "kind" => "cron",
      "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
      "trigger" => "daily_digest",
      "signal_id" => signal_id
    }

    assert :ok = JizokuWorker.perform(%Job{args: payload})
    assert :ok = JizokuWorker.perform(%Job{args: payload})
    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert {:ok, runs} = WorkflowRuns.list_daily_digest_runs()

    runs_with_signal =
      Enum.filter(runs, fn run ->
        with {:ok, inspected_run} <- WorkflowRuns.inspect_run(run.run_id) do
          inspected_run.context.schedule.idempotency_key == signal_id
        else
          {:error, _reason} -> false
        end
      end)

    assert [_run] = runs_with_signal
  end

  test "rejects idempotent recurring cron workflows without dynamic schedule identity" do
    assert {:error, reason} =
             CronPlugin.validate(workflows: [InvalidRecurringIdempotentCronWorkflow])

    assert reason =~ "must provide dynamic schedule identity"
  end

  test "rejects a manual approval workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_review_123"})

    assert {:ok, %Snapshot{status: :paused}} =
             Jizoku.execute_next(owner_id: "minimal-host-app-rejection-test")

    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.manual_state.step == "wait_for_approval"

    assert {:ok, resumed_run} =
             WorkflowRuns.reject(run.run_id, %{actor: "ops_456", comment: "rejected"})

    assert resumed_run.status == :running
    assert [%{step: "record_rejection", status: :available}] = resumed_run.visible_attempts

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed
    assert completed_run.context.approval.status == "rejected"
    assert completed_run.context.approval.decision == "rejected"
    assert completed_run.context.approval.actor == "ops_456"
    assert completed_run.context.approval.comment == "rejected"

    assert completed_history.manual_state == nil

    assert [
             %{signal_type: "start_run"},
             %{
               signal_type: "reject_run",
               payload: %{
                 run_id: rejected_run_id,
                 attributes: %{actor: "ops_456", comment: "rejected"}
               }
             }
           ] = completed_history.command_history

    assert rejected_run_id == run.run_id
  end

  test "applies inbound Jido command signals to real runs through Jizoku signals" do
    assert {:ok, run} = WorkflowRuns.start_cancellable_wait(%{account_id: "acct_jido_cancel"})

    assert [%{step: "wait_for_cancellation", status: :available}] = run.visible_attempts

    assert {:ok, signal} =
             Signal.cancel_run(run.run_id,
               metadata: %{source: "jido_router_test"},
               idempotency_key: "minimal-host-app:jido-cancel:#{run.run_id}"
             )

    assert {:ok, jido_signal} = RuntimeSignals.to_jido(signal)
    jido_signal = %{jido_signal | source: "/minimal_host_app/orders"}

    assert {:ok, cancelled_run} = RuntimeSignals.apply(jido_signal)

    assert cancelled_run.status == :cancelled
    assert cancelled_run.visible_attempts == []

    command_history_before = cancelled_run.command_history

    assert {:ok, duplicate_cancelled_run} = RuntimeSignals.apply(jido_signal)

    assert duplicate_cancelled_run.command_history == command_history_before

    assert [
             %{signal_type: "start_run"},
             %{
               signal_type: "cancel_run",
               source: "/minimal_host_app/orders",
               metadata: %{source: "jido_router_test"},
               idempotency_key: "minimal-host-app:jido-cancel:" <> _
             }
           ] = cancelled_run.command_history

    assert {:ok, invalid_jido_signal} =
             Jido.Signal.new("jizoku.runtime.command.cancel_run", %{},
               source: "/jizoku/runtime/commands",
               subject: run.run_id
             )

    assert {:error, {:invalid_signal_adapter, {:data, :missing_signal_payload}}} =
             RuntimeSignals.apply(invalid_jido_signal)
  end

  test "routes an allowlisted Jido domain signal into a real workflow" do
    occurred_at = ~U[2026-08-12 12:00:00.000000Z]

    assert {:ok, signal} =
             Jido.Signal.new(
               "minimal_host.dependency_recovery.requested",
               %{
                 "account_id" => "acct_jido_domain",
                 "attempt_id" => "attempt_jido_domain",
                 "invoice_id" => "inv_jido_domain"
               },
               id: "minimal-host-domain-signal-123",
               source: "/minimal_host_app/orders",
               subject: "accounts/acct_jido_domain",
               time: DateTime.to_iso8601(occurred_at)
             )

    assert {:ok, started} = RuntimeSignals.apply_domain(signal)
    assert started.input.account_id == "acct_jido_domain"

    assert [receipt] = started.command_history
    assert receipt.signal_id == "minimal-host-domain-signal-123"
    assert receipt.source == "/minimal_host_app/orders"
    assert receipt.occurred_at == occurred_at

    assert receipt.metadata == %{
             "jido" => %{
               "subject" => "accounts/acct_jido_domain",
               "type" => "minimal_host.dependency_recovery.requested"
             }
           }

    assert {:ok, completed} =
             MinimalHostApp.RuntimeHarness.await_terminal_run(started.run_id)

    assert completed.status == :completed
    assert completed.context.notification.account_id == "acct_jido_domain"
  end

  test "applies native Jizoku command signals through the host signal boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_cancellable_wait(%{account_id: "acct_native_signal_cancel"})

    assert {:ok, signal} =
             Signal.cancel_run(run.run_id,
               metadata: %{source: "native_signal_test"},
               idempotency_key: "minimal-host-app:native-cancel:#{run.run_id}"
             )

    assert {:ok, cancelled_run} = RuntimeSignals.apply(signal)

    assert cancelled_run.status == :cancelled
    assert cancelled_run.visible_attempts == []

    assert [
             %{signal_type: "start_run"},
             %{
               signal_type: "cancel_run",
               metadata: %{source: "native_signal_test"},
               idempotency_key: "minimal-host-app:native-cancel:" <> _
             }
           ] = cancelled_run.command_history
  end

  test "runs the documented smoke path" do
    assert %{
             payment_recovery: payment_recovery,
             dependency_recovery: dependency_recovery,
             manual_approval: manual_approval,
             payment_webhook: payment_webhook,
             manual_digest: manual_digest,
             local_ledger_checkout: local_ledger_checkout,
             local_ledger_rollback: local_ledger_rollback,
             nested_invite_delivery: nested_invite_delivery,
             nested_invite_child: nested_invite_child,
             journal_run: journal_run,
             recurring_cursor: recurring_cursor,
             journal_recovery: journal_recovery,
             journal_cancellation: journal_cancellation,
             journal_replay: journal_replay,
             journal_command_signals: journal_command_signals,
             journal_cron_digest: journal_cron_digest,
             command_signals: command_signals,
             jido_command_signals: jido_command_signals,
             action_registry: action_registry,
             editor_spec_graph: editor_spec_graph,
             editor_action_registry_graph: editor_action_registry_graph,
             editor_spec_diff: editor_spec_diff,
             daily_digest: daily_digest
           } =
             Smoke.run_all!()

    assert payment_recovery.status == :completed
    assert payment_recovery.context.notification.channel == "email"
    assert payment_recovery.context.gateway_check.status == "retry_required"
    assert payment_recovery.context.gateway_check.attempt.idempotency_key
    assert payment_recovery.context.gateway_check.attempt.claim_id
    refute Map.has_key?(payment_recovery.context.gateway_check.attempt, :claim_token)

    assert dependency_recovery.status == :completed
    assert dependency_recovery.context.notification.channel == "email"

    assert manual_approval.status == :completed
    assert manual_approval.context.approval.status == "approved"

    assert payment_webhook.delivered.status == :completed
    assert payment_webhook.delivered.context.settlement_status == "settled"
    assert [%{status: :resolved}] = payment_webhook.delivered.event_waits

    assert payment_webhook.timed_out.status == :completed
    assert payment_webhook.timed_out.context.timed_out_event == "payment.completed"
    assert [%{status: :timed_out}] = payment_webhook.timed_out.event_waits

    assert manual_digest.status == :completed
    assert manual_digest.trigger == "manual_digest"

    assert local_ledger_checkout.status == :completed
    assert local_ledger_checkout.context.local_ledger.entries == 2

    assert local_ledger_rollback.status == :failed

    assert [%{step: "post_local_ledger_entries", status: :failed}] =
             local_ledger_rollback.attempts

    assert nested_invite_delivery.status == :completed
    assert nested_invite_delivery.context.invite_child.reused_after_retry? == true

    assert nested_invite_child.status == :completed
    assert nested_invite_child.context.invite_delivery.status == "delivered"

    assert journal_run.status == :completed
    assert journal_run.applied_runnable_keys == journal_run.planned_runnable_keys

    assert recurring_cursor.predecessor.status == :continued
    assert recurring_cursor.successor.status == :completed
    assert recurring_cursor.chain.truncated? == false
    assert recurring_cursor.chain.hops == 1
    assert [%{signal_type: "start_run"}] = journal_run.command_history

    assert journal_recovery.status == :completed
    assert journal_recovery.applied_runnable_keys == journal_recovery.planned_runnable_keys

    assert journal_cancellation.status == :cancelled
    assert journal_cancellation.visible_attempts == []

    assert Enum.map(journal_cancellation.command_history, & &1.signal_type) == [
             "start_run",
             "cancel_run"
           ]

    assert journal_replay.status == :completed
    assert journal_replay.replayed_from_run_id
    assert journal_replay.context.notification.channel == "email"
    assert journal_replay.context.gateway_check.status == "retry_required"

    assert [%{signal_type: "replay_run", payload: %{run_id: replay_source_run_id}}] =
             journal_replay.command_history

    assert replay_source_run_id == journal_replay.replayed_from_run_id

    assert %{
             start: %Jizoku.ReadModel.Inspection.Snapshot{
               status: :completed,
               command_history: [%{signal_type: "start_run"}]
             },
             replay: %Jizoku.ReadModel.Inspection.Snapshot{
               status: :completed,
               command_history: [%{signal_type: "replay_run"}]
             }
           } = journal_command_signals

    assert journal_command_signals.replay.replayed_from_run_id ==
             journal_command_signals.start.run_id

    assert journal_cron_digest.status == :completed
    assert journal_cron_digest.trigger == "daily_digest"
    assert journal_cron_digest.context.schedule.signal_id
    assert [%{signal_type: "start_cron"}] = journal_cron_digest.command_history

    assert %{
             start_run: %Jizoku.Runtime.Signal{type: :start_run},
             start_cron: %Jizoku.Runtime.Signal{
               type: :start_cron,
               idempotency_key: "minimal-host-app:smoke:daily_digest:" <> _
             },
             approve_run: %Jizoku.Runtime.Signal{type: :approve_run},
             reject_run: %Jizoku.Runtime.Signal{type: :reject_run},
             resume_run: %Jizoku.Runtime.Signal{type: :resume_run},
             cancel_run: %Jizoku.Runtime.Signal{type: :cancel_run},
             replay_run: %Jizoku.Runtime.Signal{
               type: :replay_run,
               payload: %{allow_irreversible: true}
             }
           } = command_signals

    assert %{
             start_run: %Jido.Signal{
               type: "jizoku.runtime.command.start_run",
               source: "/jizoku/runtime/commands"
             },
             start_cron: %Jido.Signal{
               type: "jizoku.runtime.command.start_cron",
               source: "/jizoku/runtime/commands"
             },
             approve_run: %Jido.Signal{type: "jizoku.runtime.command.approve_run"},
             reject_run: %Jido.Signal{type: "jizoku.runtime.command.reject_run"},
             resume_run: %Jido.Signal{type: "jizoku.runtime.command.resume_run"},
             cancel_run: %Jido.Signal{type: "jizoku.runtime.command.cancel_run"},
             replay_run: %Jido.Signal{type: "jizoku.runtime.command.replay_run"}
           } = jido_command_signals

    assert Enum.all?(jido_command_signals, fn
             {_name,
              %Jido.Signal{
                source: "/jizoku/runtime/commands",
                datacontenttype: "application/vnd.jizoku.runtime-signal+json"
              }} ->
               true

             _other ->
               false
           end)

    assert Enum.map(action_registry.steps, &{&1.name, &1.metadata.action}) == [
             {:load_invoice, "payment.load_invoice"},
             {:notify_customer, "payment.notify_customer"}
           ]

    assert Enum.map(editor_spec_graph["nodes"], & &1["id"]) == [
             "load_invoice",
             "check_gateway_status",
             "issue_gateway_credit",
             "notify_customer"
           ]

    assert Enum.any?(editor_spec_graph["edges"], &(&1["recovery"] == "compensation"))

    assert Enum.map(editor_action_registry_graph["nodes"], &{&1["id"], &1["action"]}) == [
             {"load_invoice", "payment.load_invoice"},
             {"notify_customer", "payment.notify_customer"}
           ]

    assert editor_spec_diff["summary"]["nodes_added"] == 1
    assert editor_spec_diff["summary"]["edges_added"] == 2
    assert editor_spec_diff["summary"]["edges_removed"] == 1
    assert [%{"id" => "archive_invoice"}] = editor_spec_diff["nodes"]["added"]

    assert daily_digest.status == :completed
    assert daily_digest.trigger == "daily_digest"
  end

  test "smoke run clears stale journal rows before starting" do
    now = DateTime.utc_now(:microsecond)
    thread_id = "jizoku:dispatch:stale-smoke-#{System.unique_integer([:positive])}"
    unknown_atom_name = "jizoku_unknown_atom_#{System.unique_integer([:positive])}"

    Repo.insert_all("jizoku_journal_threads", [
      %{
        id: thread_id,
        rev: 1,
        metadata: %{},
        created_at_ms: 0,
        updated_at_ms: 0,
        inserted_at: now,
        updated_at: now
      }
    ])

    Repo.insert_all("jizoku_journal_entries", [
      %{
        id: Ecto.UUID.dump!(Ecto.UUID.generate()),
        thread_id: thread_id,
        seq: 0,
        entry: :erlang.term_to_binary({:jizoku_ecto_term_v1, {:atom, unknown_atom_name}}),
        inserted_at: now,
        updated_at: now
      }
    ])

    assert %Snapshot{} = run = Smoke.run!()
    assert run.status == :completed
  end

  test "runs the journal run smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_run!()

    assert run.status == :completed
    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
    assert run.applied_runnable_keys == run.planned_runnable_keys

    assert Enum.map(run.attempts, &{&1.step, &1.status, &1.applied?}) == [
             {"load_account", :completed, true},
             {"load_invoice", :completed, true},
             {"prepare_notification", :completed, true}
           ]

    assert Enum.find_value(run.attempts, fn
             %{step: "prepare_notification", result: %{notification: notification}} ->
               notification

             _attempt ->
               nil
           end) == %{
             account_id: "acct_journal_demo",
             account_tier: "standard",
             channel: "email",
             invoice_id: "inv_journal_demo"
           }
  end

  test "runs the dynamic work inspection smoke path" do
    assert %{
             dynamic_work: [%{dynamic_key: "dynamic_invoice_fanout"}],
             dynamic_work_overlays: [
               %{
                 dynamic_key: "dynamic_invoice_fanout",
                 status: :scheduled,
                 origin_node_id: "load_account",
                 added_node_ids: ["notify_invoice:inv_dynamic_demo"],
                 added_edge_ids: ["load_account:dynamic:notify_invoice:inv_dynamic_demo"],
                 node_count: 1,
                 edge_count: 1
               }
             ]
           } =
             Smoke.run_dynamic_work_inspection!()
  end

  test "recovers the journal run smoke path from persisted entries" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_recovery!()

    assert run.status == :completed
    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
    assert run.applied_runnable_keys == run.planned_runnable_keys
  end

  test "runs the journal cancellation smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_cancellation!()

    assert run.status == :cancelled
    assert run.terminal?
    assert run.visible_attempts == []
    assert Enum.map(run.command_history, & &1.signal_type) == ["start_run", "cancel_run"]
  end

  test "runs the journal replay smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_replay!()

    assert run.status == :completed
    assert run.replayed_from_run_id
    assert run.applied_runnable_keys == run.planned_runnable_keys

    assert [%{signal_type: "replay_run", payload: %{run_id: replay_source_run_id}}] =
             run.command_history

    assert replay_source_run_id == run.replayed_from_run_id
  end

  test "runs journal start and replay through command signals" do
    assert %{start: start, replay: replay} = Smoke.run_journal_command_signals!()

    assert start.status == :completed

    assert [
             %{
               signal_type: "start_run",
               source: "/minimal_host_app/dependency_recovery",
               metadata: %{
                 "jido" => %{
                   "subject" => "accounts/acct_journal_signal_demo",
                   "type" => "minimal_host.dependency_recovery.requested"
                 }
               }
             }
           ] = start.command_history

    assert replay.status == :completed
    assert replay.replayed_from_run_id == start.run_id

    assert [%{signal_type: "replay_run", metadata: %{source: "minimal_host_app_smoke"}}] =
             replay.command_history
  end

  test "runs the journal cron smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_cron_digest!()

    assert run.status == :completed
    assert run.trigger == "daily_digest"
    assert run.context.schedule.signal_id
    assert [%{signal_type: "start_cron"}] = run.command_history
  end

  test "runs the journal cron duplicate smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} =
             run = Smoke.run_journal_cron_duplicate_digest!()

    assert run.status == :completed
    assert run.trigger == "daily_digest"
    assert run.context.schedule.idempotency == :return_existing_run
  end

  test "runs the cancellation smoke path" do
    assert %Jizoku.ReadModel.Inspection.Snapshot{} = run = Smoke.run_cancellation!()
    assert run.status == :cancelled
  end

  defp endpoint_url(port, path) do
    "http://127.0.0.1:#{port}#{path}"
  end

  defp unique_reboot_signal_id do
    "minimal-host-app:test:daily_digest:#{System.unique_integer([:positive])}"
  end

  defp plugin_reboot_signal_id do
    assert {:ok, {_supervisor_flags, [child_spec]}} =
             CronPlugin.init(conf: oban_config(), workflows: [DailyDigest])

    %{start: {Oban.Plugins.Cron, :start_link, [opts]}} = child_spec
    [{"@reboot", JizokuWorker, entry_opts}] = Keyword.fetch!(opts, :crontab)
    payload = Keyword.fetch!(entry_opts, :args)
    Map.fetch!(payload, "signal_id")
  end

  defp oban_config do
    :minimal_host_app
    |> Application.fetch_env!(Oban)
    |> Keyword.put(:testing, :disabled)
    |> Keyword.put(:plugins, false)
    |> Keyword.put(:queues, false)
    |> Keyword.put(:peer, {Oban.Peers.Isolated, [leader?: true]})
    |> Oban.Config.new()
  end

  defp jido_routes(target) do
    %{"default" => {:pid, target: target, delivery_mode: :async}}
  end

  defp local_ledger_entries(run_id) do
    Repo.all(
      from(entry in "local_ledger_entries",
        where: entry.run_id == ^run_id,
        order_by: [asc: entry.id],
        select: entry.entry
      )
    )
  end

  defp with_jizoku_env(config, fun) when is_list(config) and is_function(fun, 0) do
    original_config = Application.get_all_env(:jizoku)

    try do
      :jizoku
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:jizoku, &1))

      Enum.each(config, fn {key, value} -> Application.put_env(:jizoku, key, value) end)

      fun.()
    after
      :jizoku
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:jizoku, &1))

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(:jizoku, key, value)
      end)
    end
  end

  defp await_terminal_without_harness(run_id, attempts \\ 50)
  defp await_terminal_without_harness(_run_id, 0), do: {:error, :timeout}

  defp await_terminal_without_harness(run_id, attempts_remaining)
       when attempts_remaining > 0 do
    case WorkflowRuns.inspect_run(run_id) do
      {:ok, %{status: status} = run} when status in [:completed, :failed, :cancelled] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(20)
        await_terminal_without_harness(run_id, attempts_remaining - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp drain_default_journal_run(_run_id, _queue, 0), do: {:error, :timeout}

  defp drain_default_journal_run(run_id, queue, attempts_remaining) when attempts_remaining > 0 do
    case Jizoku.inspect_run(run_id) do
      {:ok, %Snapshot{terminal?: true} = run} ->
        {:ok, run}

      {:ok, %Snapshot{}} ->
        case Jizoku.execute_next(
               owner_id: "minimal-host-app-default-journal-test",
               queue: queue
             ) do
          {:ok, %Snapshot{terminal?: true} = run} ->
            {:ok, run}

          {:ok, %Snapshot{}} ->
            drain_default_journal_run(run_id, queue, attempts_remaining - 1)

          {:ok, :none} ->
            Process.sleep(50)
            drain_default_journal_run(run_id, queue, attempts_remaining - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
