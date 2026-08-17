defmodule Jizoku.TestTest do
  use ExUnit.Case, async: false

  alias Jizoku.Test

  defmodule CompleteStep do
    use Jizoku.Step, name: :record_value

    @impl Jizoku.Step
    def run(input, _context) do
      {:ok, %{value: Map.fetch!(input, :value)}}
    end
  end

  defmodule CompleteWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :value, :integer
        end
      end

      step :record_value, CompleteStep
      transition :record_value, on: :ok, to: :complete
    end
  end

  defmodule CountingStep do
    use Jizoku.Step, name: :count_once

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{run_id: run_id}) do
      test_pid = :persistent_term.get({__MODULE__, run_id})
      send(test_pid, {:counting_step_ran, run_id})
      {:ok, %{counted: true}}
    end
  end

  defmodule CountingWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :count_once, CountingStep
      transition :count_once, on: :ok, to: :complete
    end
  end

  defmodule DeferredStep do
    use Jizoku.Step, name: :deferred

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{runnable_key: runnable_key}) do
      if String.ends_with?(runnable_key, ":deferred") do
        {:ok, %{ready: true}}
      else
        {:defer, %{reason: "not_ready"}, schedule_in: 60}
      end
    end
  end

  defmodule DeferredWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :deferred, DeferredStep,
        deadline: [within: 60_000, due_soon: 20_000, escalation: :diagnostic]

      transition :deferred, on: :ok, to: :complete
    end
  end

  defmodule RetryStep do
    use Jizoku.Step, name: :retry_once

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{attempt: 1}) do
      {:retry, %{message: "retry later", code: "retry_later"}}
    end

    def run(_input, %Jizoku.Step.Context{}) do
      {:ok, %{retried: true}}
    end
  end

  defmodule RetryWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :retry_once, RetryStep,
        retry: [max_attempts: 2, backoff: [type: :exponential, min: 60_000, max: 60_000]]

      transition :retry_once, on: :ok, to: :complete
    end
  end

  defmodule WaitWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :value, :integer
        end
      end

      step :wait, :wait, duration: 60_000
      step :record_wait, WaitWorkflow.RecordWait
      transition :wait, on: :ok, to: :record_wait
      transition :record_wait, on: :ok, to: :complete
    end
  end

  defmodule WaitWorkflow.RecordWait do
    use Jizoku.Step, name: :record_wait

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{waited: true}}
    end
  end

  defmodule ManualResultStep do
    use Jizoku.Step, name: :manual_result

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{step: step}) do
      {:ok, %{manual_path: Atom.to_string(step)}}
    end
  end

  defmodule PauseControlWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :pause, :pause
      step :after_resume, ManualResultStep
      transition :pause, on: :ok, to: :after_resume
      transition :after_resume, on: :ok, to: :complete
    end
  end

  defmodule ApprovalControlWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      approval_step :review, output: :approval
      step :approved, ManualResultStep
      step :rejected, ManualResultStep
      transition :review, on: :ok, to: :approved
      transition :review, on: :error, to: :rejected
      transition :approved, on: :ok, to: :complete
      transition :rejected, on: :ok, to: :complete
    end
  end

  defmodule BarrierStep do
    use Jizoku.Step, name: :barrier

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{run_id: run_id}) do
      test_pid = :persistent_term.get({__MODULE__, run_id})
      send(test_pid, {:barrier_entered, self()})

      receive do
        :release_barrier -> {:ok, %{released: true}}
      end
    end
  end

  defmodule BarrierWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :barrier, BarrierStep
      transition :barrier, on: :ok, to: :complete
    end
  end

  defmodule TwoStepWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :value, :integer
        end
      end

      step :first, TwoStepWorkflow.FirstStep
      step :second, TwoStepWorkflow.SecondStep
      transition :first, on: :ok, to: :second
      transition :second, on: :ok, to: :complete
    end
  end

  defmodule TwoStepWorkflow.FirstStep do
    use Jizoku.Step, name: :first

    @impl Jizoku.Step
    def run(%{value: value}, _context) do
      {:ok, %{first: value}}
    end
  end

  defmodule TwoStepWorkflow.SecondStep do
    use Jizoku.Step, name: :second

    @impl Jizoku.Step
    def run(%{value: value}, _context) do
      {:ok, %{second: value}}
    end
  end

  defmodule FailingStep do
    use Jizoku.Step, name: :fail

    @impl Jizoku.Step
    def run(_input, _context) do
      {:error, %{code: "expected_test_failure", access_token: "secret-test-token"}}
    end
  end

  defmodule FailingWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :fail, FailingStep
      transition :fail, on: :ok, to: :complete
    end
  end

  defmodule CronWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      trigger :scheduled do
        cron "@hourly", timezone: "Etc/UTC", idempotency: :return_existing_run

        payload do
          field :signal_id, :string
          field :intended_window, :map, required: false
          field :value, :integer
        end
      end

      step :wait, :wait, duration: 60_000
      step :record_value, CompleteStep
      transition :wait, on: :ok, to: :record_value
      transition :record_value, on: :ok, to: :complete
    end
  end

  @now ~U[2026-08-09 12:00:00Z]

  test "isolates runtime storage and cleans it up with the runtime process" do
    assert {:ok, left} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    assert {:ok, right} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(right) end)

    assert left.storage != right.storage
    assert {:ok, left_run} = Test.start(left, %{value: 1})
    assert {:error, :not_found} = Test.inspect(right, left_run.run_id)

    assert :ok = Test.stop_runtime(left)
    assert {:error, :runtime_stopped} = Test.inspect(left, left_run.run_id)
  end

  test "starts and drains a workflow through the normal journal runtime" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 42})
    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.run_id == run.run_id
    assert snapshot.context == %{value: 42}
  end

  test "starts a cron trigger at frozen time and advances through its wait" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CronWorkflow,
               queue: "cron-priority",
               partition: "tenant_acme",
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    input = %{
      signal_id: "cron-window-1",
      intended_window: %{
        start_at: "2026-08-09T12:00:00Z",
        end_at: "2026-08-09T13:00:00Z"
      },
      value: 42
    }

    assert {:ok, run} =
             Test.start_cron(runtime, :scheduled, input, metadata: %{source: "frozen-test"})

    assert run.trigger == "scheduled"
    assert run.queue == "cron-priority"
    assert run.partition == "tenant_acme"
    assert run.started_at == @now
    assert run.context.schedule.received_at == DateTime.to_iso8601(@now)
    assert run.context.schedule.signal_id == "cron-window-1"
    assert run.context.schedule.intended_window.start_at == "2026-08-09T12:00:00Z"

    assert [
             %{
               signal_type: "start_cron",
               idempotency_key: "cron-window-1",
               metadata: %{source: "frozen-test"},
               occurred_at: @now
             }
           ] = run.command_history

    assert {:blocked, blocked} = Test.drain(runtime, run)
    assert [%{step: "record_value", visible_at: visible_at}] = blocked.scheduled_attempts
    assert visible_at == DateTime.add(@now, 60, :second)

    assert {:ok, ^visible_at} = Test.advance_time(runtime, 60, :second)
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.value == 42
  end

  test "rejects invalid cron starts without reserving or writing the root" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CronWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    before_state = runtime_full_state(runtime)

    task =
      Task.async(fn ->
        Test.start_cron(runtime, :scheduled, %{signal_id: "foreign", value: 1})
      end)

    assert {:error, :runtime_owner_required} = Task.await(task)
    assert runtime_full_state(runtime) == before_state

    assert {:error, {:invalid_schedule_trigger_type, :manual}} =
             Test.start_cron(runtime, :manual, %{value: 1})

    assert {:error, {:invalid_signal, {:metadata, :expected_map}}} =
             Test.start_cron(runtime, :scheduled, %{signal_id: "invalid", value: 1},
               metadata: :invalid
             )

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Test.start_cron(runtime, :scheduled, %{}, [:bad])

    assert runtime_full_state(runtime) == before_state

    input = %{signal_id: "valid", value: 1}
    assert {:ok, run} = Test.start_cron(runtime, :scheduled, input)
    started_state = runtime_full_state(runtime)

    assert {:ok, duplicate} = Test.start_cron(runtime, :scheduled, input)
    assert duplicate.run_id == run.run_id
    assert runtime_full_state(runtime) == started_state

    assert {:error, :conflict} =
             Test.start_cron(runtime, :scheduled, %{signal_id: "valid", value: 2})

    assert runtime_full_state(runtime) == started_state

    assert {:error, :runtime_already_started} =
             Test.start_cron(runtime, :scheduled, %{signal_id: "second", value: 2})

    assert runtime_full_state(runtime) == started_state
  end

  test "executes until a durable snapshot matches and resumes from there" do
    assert {:ok, runtime} = Test.start_runtime(workflow: TwoStepWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 7})

    assert {:reached, snapshot} =
             Test.execute_until(runtime, run, fn snapshot ->
               Map.has_key?(snapshot.context, :first)
             end)

    assert snapshot.status == :running
    assert snapshot.context.first == 7
    refute Map.has_key?(snapshot.context, :second)

    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.context.second == 7
  end

  test "returns an initially matching snapshot without executing work" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 42})

    assert {:reached, snapshot} =
             Test.execute_until(runtime, run, fn snapshot ->
               snapshot.status == :running
             end)

    assert %{snapshot | definition_resolution: nil} == run
    assert snapshot.definition_resolution.status == :resolved

    assert {:completed, _snapshot} =
             Test.execute_until(runtime, run, fn _snapshot -> false end)
  end

  test "preserves blocked and bounded results when the predicate is not reached" do
    assert {:ok, blocked_runtime} = Test.start_runtime(workflow: WaitWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(blocked_runtime) end)

    assert {:ok, blocked_run} = Test.start(blocked_runtime, %{value: 3})

    assert {:blocked, %{status: :running}} =
             Test.execute_until(blocked_runtime, blocked_run, fn _snapshot -> false end)

    assert {:ok, reached_runtime} =
             Test.start_runtime(workflow: TwoStepWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(reached_runtime) end)
    assert {:ok, reached_run} = Test.start(reached_runtime, %{value: 7})

    assert {:reached, %{context: %{first: 7}}} =
             Test.execute_until(
               reached_runtime,
               reached_run,
               fn snapshot -> Map.has_key?(snapshot.context, :first) end,
               max_steps: 1
             )

    assert {:ok, bounded_runtime} =
             Test.start_runtime(workflow: TwoStepWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(bounded_runtime) end)
    assert {:ok, bounded_run} = Test.start(bounded_runtime, %{value: 7})

    assert {:error, {:execution_limit_reached, %{limit: 1}}} =
             Test.execute_until(
               bounded_runtime,
               bounded_run,
               fn snapshot -> Map.has_key?(snapshot.context, :second) end,
               max_steps: 1
             )
  end

  test "validates predicates and releases the clock lease when one raises" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 42})

    assert {:error, {:invalid_option, {:predicate, :invalid}}} =
             Test.execute_until(runtime, run, :not_a_function)

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Test.execute_until(runtime, run, fn _snapshot -> false end, [:bad])

    assert_raise RuntimeError, "predicate failed", fn ->
      Test.execute_until(runtime, run, fn _snapshot ->
        raise "predicate failed"
      end)
    end

    assert {:ok, advanced} = Test.advance_time(runtime, 1, :second)
    assert advanced == DateTime.add(@now, 1, :second)
  end

  test "classifies the final snapshot after a concurrent drain completes the run" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 42})
    parent = self()

    execute_task =
      Task.async(fn ->
        Test.execute_until(runtime, run, fn snapshot ->
          if snapshot.status == :running do
            send(parent, {:predicate_paused, self()})

            receive do
              :release_predicate -> false
            end
          else
            send(parent, {:predicate_final, snapshot.status})
            snapshot.status == :completed
          end
        end)
      end)

    assert_receive {:predicate_paused, execute_pid}
    assert {:completed, completed} = Test.drain(runtime, run)
    send(execute_pid, :release_predicate)

    assert {:reached, %{run_id: run_id, status: :completed}} = Task.await(execute_task)
    assert run_id == completed.run_id
    assert_receive {:predicate_final, :completed}
  end

  test "resumes a paused workflow at the runtime clock" do
    assert {:ok, runtime} = Test.start_runtime(workflow: PauseControlWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(runtime, run)
    assert {:ok, resumed_at} = Test.advance_time(runtime, 5, :second)

    attrs = %{actor: "ops-1", comment: "resume", metadata: %{reason: "verified"}}

    assert {:ok, resumed} =
             Test.resume(runtime, run, attrs,
               idempotency_key: "resume-1",
               metadata: %{source: "test"}
             )

    assert resumed.status == :running
    assert resumed.manual_state == nil

    assert Enum.any?(resumed.command_history, fn
             %{
               signal_type: "resume_run",
               occurred_at: ^resumed_at,
               idempotency_key: "resume-1",
               metadata: %{source: "test"},
               actor: "ops-1",
               comment: "resume",
               payload: %{
                 attributes: %{actor: "ops-1", comment: "resume"}
               }
             } ->
               true

             _command ->
               false
           end)

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.manual_path == "after_resume"
  end

  test "resumes with command options and default attributes" do
    assert {:ok, runtime} = Test.start_runtime(workflow: PauseControlWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(runtime, run)

    assert {:ok, %{status: :running}} =
             Test.resume(runtime, run,
               idempotency_key: "resume-options-only",
               metadata: %{source: "options"}
             )
  end

  test "approves once under the configured queue and partition" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: ApprovalControlWorkflow,
               queue: "priority",
               partition: "tenant-controls",
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(runtime, run)

    attrs = %{
      actor: "reviewer-1",
      comment: "approved",
      metadata: %{reason: "policy"}
    }

    assert {:ok, approved} =
             Test.approve(runtime, run, attrs, idempotency_key: "approval-1")

    state_after_approval = runtime_journal_state(runtime, run.run_id)

    assert {:ok, duplicate} =
             Test.approve(runtime, run, attrs, idempotency_key: "approval-1")

    assert duplicate == approved
    assert runtime_journal_state(runtime, run.run_id) == state_after_approval

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.queue == "priority"
    assert completed.partition == "tenant-controls"
    assert completed.context.manual_path == "approved"

    assert %{
             data: %{
               action: "approved",
               metadata: %{
                 "actor" => "reviewer-1",
                 "comment" => "approved",
                 "metadata" => %{reason: "policy"}
               }
             }
           } =
             Enum.find(
               runtime_run_entries(runtime, run.run_id),
               &(&1.type == :manual_step_resolved)
             )

    {adapter, opts} = runtime.storage

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id(
                 {:dispatch, "default"},
                 "tenant-controls"
               ),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, "priority"}),
               opts
             )
  end

  test "rejects an approval through its rejection path" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ApprovalControlWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(runtime, run)

    assert {:ok, %{status: :running}} =
             Test.reject(runtime, run, %{actor: "reviewer-2"}, idempotency_key: "rejection-1")

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.manual_path == "rejected"
  end

  test "cancels the runtime root idempotently" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 42})
    assert {:ok, cancelled} = Test.cancel(runtime, run, idempotency_key: "cancel-1")
    assert cancelled.status == :cancelled

    state_after_cancel = runtime_journal_state(runtime, run.run_id)

    assert {:ok, duplicate} = Test.cancel(runtime, run, idempotency_key: "cancel-1")
    assert duplicate == cancelled
    assert runtime_journal_state(runtime, run.run_id) == state_after_cancel
    assert {:cancelled, _snapshot} = Test.drain(runtime, run)
  end

  test "rejects invalid control targets, attributes, and options before writes" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ApprovalControlWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(runtime, run)
    before_state = runtime_persistence_state(runtime)

    assert {:error, :run_outside_runtime} =
             Test.approve(runtime, Ecto.UUID.generate(), %{actor: "reviewer"})

    assert {:error, {:invalid_attributes, :expected_map}} =
             Test.approve(runtime, run, :invalid)

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Test.cancel(runtime, run, [:bad])

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             Test.reject(runtime, run, %{}, unknown: true)

    assert {:error, {:invalid_review, %{actor: :required}}} =
             Test.approve(runtime, run, %{}, idempotency_key: "approval-invalid")

    assert {:error, {:invalid_option, {:metadata, :invalid}}} =
             Test.reject(runtime, run, %{actor: "reviewer"}, metadata: :invalid)

    assert {:error, {:invalid_option, {:idempotency_key, :invalid}}} =
             Test.cancel(runtime, run, idempotency_key: "")

    assert runtime_persistence_state(runtime) == before_state
  end

  test "keeps bounded execution scoped to the runtime's single root run" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 1})
    assert {:error, :runtime_already_started} = Test.start(runtime, %{value: 2})
    assert {:error, :run_outside_runtime} = Test.drain(runtime, Ecto.UUID.generate())
    assert {:completed, %{run_id: run_id}} = Test.drain(runtime, run)
    assert run_id == run.run_id
  end

  test "releases a failed or abandoned root-run reservation" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    parent = self()

    assert {:error, {:invalid_payload, _details}} = Test.start(runtime, %{})

    live_holder =
      spawn(fn ->
        :ok = Jizoku.Test.Storage.reserve_start(runtime.storage_server)
        send(parent, {:start_reserved, self()})

        receive do
          :release_start -> Jizoku.Test.Storage.release_start(runtime.storage_server)
        end
      end)

    assert_receive {:start_reserved, ^live_holder}
    assert {:error, :runtime_already_started} = Test.start(runtime, %{value: 1})
    live_holder_ref = Process.monitor(live_holder)
    send(live_holder, :release_start)
    assert_receive {:DOWN, ^live_holder_ref, :process, ^live_holder, :normal}

    caller =
      spawn(fn ->
        :ok = Jizoku.Test.Storage.reserve_start(runtime.storage_server)
      end)

    caller_ref = Process.monitor(caller)
    assert_receive {:DOWN, ^caller_ref, :process, ^caller, :normal}

    unauthorized_caller =
      spawn(fn ->
        send(parent, {:unauthorized_start, Test.start(runtime, %{value: 2})})
      end)

    unauthorized_ref = Process.monitor(unauthorized_caller)
    assert_receive {:unauthorized_start, {:error, :runtime_owner_required}}
    assert_receive {:DOWN, ^unauthorized_ref, :process, ^unauthorized_caller, unauthorized_reason}
    assert unauthorized_reason in [:normal, :noproc]

    assert {:ok, run} = Test.start(runtime, %{value: 1})
    assert {:completed, %{run_id: run_id}} = Test.drain(runtime, run)
    assert run_id == run.run_id
  end

  test "keeps committed and unknown start outcomes fail closed" do
    assert {:ok, committed_runtime} =
             Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(committed_runtime) end)

    assert :ok =
             Jizoku.Test.Storage.put_append_fault(
               committed_runtime.storage_server,
               "jizoku:run_index:",
               {:error, :index_failed}
             )

    assert {:error, {:journal_start_committed, committed_run_id, _reason}} =
             Test.start(committed_runtime, %{value: 1})

    assert {:ok, %{run_id: ^committed_run_id}} = Test.inspect(committed_runtime, committed_run_id)

    assert {:error, :runtime_already_started} =
             Test.start(committed_runtime, %{value: 2})

    assert {:ok, unknown_runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(unknown_runtime) end)

    assert :ok =
             Jizoku.Test.Storage.put_append_fault(
               unknown_runtime.storage_server,
               "jizoku:run_index:",
               {:raise, RuntimeError.exception("unknown start outcome")}
             )

    assert_raise RuntimeError, "unknown start outcome", fn ->
      Test.start(unknown_runtime, %{value: 1})
    end

    assert {:error, :runtime_already_started} = Test.start(unknown_runtime, %{value: 2})
  end

  test "in-memory storage preserves checkpoint and expected-revision contracts" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    {adapter, opts} = runtime.storage
    entry = Jido.Thread.Entry.new(id: "entry-1", kind: :note, payload: %{value: 1})

    assert :not_found = adapter.get_checkpoint(:projection, opts)
    assert :ok = adapter.put_checkpoint(:projection, %{rev: 1}, opts)
    assert {:ok, %{rev: 1}} = adapter.get_checkpoint(:projection, opts)
    assert :ok = adapter.delete_checkpoint(:projection, opts)
    assert :not_found = adapter.get_checkpoint(:projection, opts)

    assert {:ok, %{rev: 1}} =
             adapter.append_thread("thread-1", [entry], Keyword.put(opts, :expected_rev, 0))

    assert {:error, :conflict} =
             adapter.append_thread("thread-1", [entry], Keyword.put(opts, :expected_rev, 0))

    assert {:ok, thread} = adapter.load_thread("thread-1", opts)
    assert Enum.map(thread.entries, & &1.seq) == [0]

    assert {:error, {:unsupported_term, unsafe}} =
             adapter.put_checkpoint(:unsafe, %{pid: self()}, opts)

    assert unsafe == self()
    assert :not_found = adapter.get_checkpoint(:unsafe, opts)

    unsafe_entry =
      Jido.Thread.Entry.new(id: "entry-unsafe", kind: :note, payload: %{pid: self()})

    assert {:error, {:unsupported_term, unsafe}} =
             adapter.append_thread("thread-unsafe", [unsafe_entry], opts)

    assert unsafe == self()
    assert :not_found = adapter.load_thread("thread-unsafe", opts)
  end

  test "injects one exact dispatch append conflict before action execution" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CountingWorkflow,
               queue: "priority",
               partition: "tenant-conflict",
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({CountingStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({CountingStep, run.run_id}) end)
    before_threads = runtime_persistence_state(runtime).threads

    assert :ok = Test.inject_append_conflict(runtime, :dispatch)

    assert {:error, :append_conflict_already_armed} =
             Test.inject_append_conflict(runtime, :run)

    assert {:error, :conflict} = Test.drain(runtime, run)
    assert runtime_persistence_state(runtime).threads == before_threads
    refute_receive {:counting_step_ran, _run_id}

    dispatch_thread_id =
      Jizoku.Runtime.Journal.thread_id({:dispatch, runtime.queue}, runtime.partition)

    assert runtime_fault_state(runtime).append_conflict_counts == %{dispatch_thread_id => 1}

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.counted
    assert_receive {:counting_step_ran, run_id}
    assert run_id == run.run_id
    refute_receive {:counting_step_ran, _run_id}

    {adapter, opts} = runtime.storage

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, "default"}, runtime.partition),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, runtime.queue}),
               opts
             )
  end

  test "retries a run append conflict without rerunning the completed action" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CountingWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({CountingStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({CountingStep, run.run_id}) end)

    assert :ok = Test.inject_append_conflict(runtime, :run)
    assert {:completed, completed} = Test.drain(runtime, run)
    assert_receive {:counting_step_ran, run_id}
    assert run_id == run.run_id
    assert completed.context.counted
    refute_receive {:counting_step_ran, _run_id}
    assert [_attempt] = completed.attempts

    run_thread_id =
      Jizoku.Runtime.Journal.thread_id({:run, run.run_id}, runtime.partition)

    assert runtime_fault_state(runtime) == %{
             next_append_conflict: nil,
             append_conflict_counts: %{run_thread_id => 1}
           }
  end

  test "rejects invalid append conflict injection without changing persistence" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    before_start_fault_state = runtime_fault_state(runtime)
    assert {:error, :run_not_started} = Test.inject_append_conflict(runtime, :run)
    assert runtime_fault_state(runtime) == before_start_fault_state
    assert {:ok, _run} = Test.start(runtime, %{value: 1})
    before_state = runtime_persistence_state(runtime)
    before_fault_state = runtime_fault_state(runtime)

    assert {:error, {:invalid_option, {:target, :invalid}}} =
             Test.inject_append_conflict(runtime, :unknown)

    task = Task.async(fn -> Test.inject_append_conflict(runtime, :dispatch) end)
    assert {:error, :runtime_owner_required} = Task.await(task)
    assert runtime_persistence_state(runtime) == before_state
    assert runtime_fault_state(runtime) == before_fault_state

    assert :ok = Test.stop_runtime(runtime)
    assert {:error, :runtime_stopped} = Test.inject_append_conflict(runtime, :dispatch)
  end

  test "does not arm an append conflict while execution is active" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}
    before_state = runtime_persistence_state(runtime)
    before_fault_state = runtime_fault_state(runtime)
    assert {:error, :runtime_busy} = Test.inject_append_conflict(runtime, :run)
    assert runtime_persistence_state(runtime) == before_state
    assert runtime_fault_state(runtime) == before_fault_state

    send(executor_pid, :release_barrier)
    assert {:completed, _snapshot} = Task.await(drain_task)
  end

  test "advances the runtime clock explicitly" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, @now} = Test.now(runtime)
    assert {:ok, advanced} = Test.advance_time(runtime, 1, :second)
    assert advanced == DateTime.add(@now, 1, :second)
    assert {:ok, ^advanced} = Test.now(runtime)

    task = Task.async(fn -> Test.advance_time(runtime, 1, :second) end)
    assert {:error, :runtime_owner_required} = Task.await(task)

    assert {:error, {:invalid_option, {:amount, :invalid}}} =
             Test.advance_time(runtime, -1, :second)

    assert {:error, {:invalid_option, {:unit, :invalid}}} =
             Test.advance_time(runtime, 1, :minute)
  end

  test "does not advance the clock while execution holds an older instant" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}

    assert {:error, :runtime_busy} = Test.advance_time(runtime, 1, :second)
    assert {:ok, @now} = Test.now(runtime)

    send(executor_pid, :release_barrier)
    assert {:completed, %{terminal_at: @now}} = Task.await(drain_task)

    assert {:ok, advanced} = Test.advance_time(runtime, 1, :second)
    assert advanced == DateTime.add(@now, 1, :second)
  end

  test "releases the execution clock lease when a helper exits" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}
    assert executor_pid == drain_task.pid
    assert nil == Task.shutdown(drain_task, :brutal_kill)

    assert {:ok, advanced} = Test.advance_time(runtime, 1, :second)
    assert advanced == DateTime.add(@now, 1, :second)
  end

  test "advances a built-in wait without sleeping" do
    assert {:ok, runtime} = Test.start_runtime(workflow: WaitWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 3})
    assert {:blocked, snapshot} = Test.drain(runtime, run)
    assert snapshot.next_visible_at == DateTime.add(@now, 60, :second)
    assert [%{step: "record_wait"}] = snapshot.scheduled_attempts

    assert {:ok, _now} = Test.advance_time(runtime, 60, :second)
    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.context.waited == true
  end

  test "advances deferred work and deadline classifications without sleeping" do
    assert {:ok, runtime} = Test.start_runtime(workflow: DeferredWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, snapshot} = Test.execute_until_blocked(runtime, run)
    assert snapshot.status == :running
    assert snapshot.started_at == @now
    assert snapshot.next_visible_at == DateTime.add(@now, 60, :second)
    assert snapshot.deadline.status == :on_time
    assert snapshot.deadline.due_at == DateTime.add(@now, 60_000, :millisecond)

    assert {:ok, _now} = Test.advance_time(runtime, 39, :second)
    assert {:blocked, %{deadline: %{status: :on_time}}} = Test.drain(runtime, run)

    assert {:ok, _now} = Test.advance_time(runtime, 1, :second)
    assert {:ok, %{deadline: %{status: :due_soon}}} = Test.inspect(runtime, run)

    assert {:ok, _now} = Test.advance_time(runtime, 20, :second)
    assert {:ok, %{deadline: %{status: :overdue}}} = Test.inspect(runtime, run)

    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.context.ready == true
  end

  test "advances retry backoff without sleeping" do
    assert {:ok, runtime} = Test.start_runtime(workflow: RetryWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, snapshot} = Test.drain(runtime, run)
    assert snapshot.next_visible_at == DateTime.add(@now, 60_000, :millisecond)

    assert [
             %{status: :failed, attempt_number: 1},
             %{status: :retry_scheduled, attempt_number: 2}
           ] = snapshot.attempts

    assert {:ok, _now} = Test.advance_time(runtime, 60, :second)
    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.context.retried == true
  end

  test "routes every helper through the configured queue and partition" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CompleteWorkflow,
               queue: "priority",
               partition: "tenant-test-kit",
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 9})
    assert {:completed, snapshot} = Test.drain(runtime, run)
    assert snapshot.queue == "priority"
    assert snapshot.partition == "tenant-test-kit"

    {adapter, opts} = runtime.storage

    assert {:ok, _thread} =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, "priority"}, "tenant-test-kit"),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, "default"}, "tenant-test-kit"),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Jizoku.Runtime.Journal.thread_id({:dispatch, "priority"}),
               opts
             )
  end

  test "fails with a deterministic diagnostic when the execution bound is reached" do
    assert {:ok, runtime} =
             Test.start_runtime(workflow: TwoStepWorkflow, now: @now, max_steps: 1)

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 7})

    assert {:error,
            {:execution_limit_reached,
             %{
               limit: 1,
               run_id: run_id,
               snapshot: %{status: :running, context: %{first: 7} = context}
             }}} = Test.drain(runtime, run)

    assert run_id == run.run_id
    refute Map.has_key?(context, :second)
    assert {:completed, %{context: %{second: 7}}} = Test.drain(runtime, run, max_steps: 1)
  end

  test "returns a failed terminal result without treating it as blocked" do
    assert {:ok, runtime} = Test.start_runtime(workflow: FailingWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:failed, %{run_id: run_id, terminal_status: :failed}} = Test.drain(runtime, run)
    assert run_id == run.run_id
  end

  test "projects a failed run timeline without mutating the journal" do
    assert {:ok, runtime} = Test.start_runtime(workflow: FailingWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:failed, failed} = Test.drain(runtime, run)
    before_state = runtime_persistence_state(runtime)

    assert {:ok, timeline} = Test.timeline(runtime, run)
    assert timeline.run_id == run.run_id
    assert timeline.status == :failed
    assert timeline.terminal_status == :failed

    assert Enum.map(timeline.events, & &1.type) == [
             :command_received,
             :run_started,
             :attempt_claimed,
             :attempt_failed,
             :attempt_scheduled,
             :run_terminal
           ]

    assert Enum.all?(timeline.events, &(&1.occurred_at == @now))

    assert {:ok, explanation} = Test.explain(runtime, failed)
    assert explanation.reason == :terminal
    assert explanation.details.terminal_status == :failed
    assert explanation.details.terminal_error.code == "expected_test_failure"
    assert explanation.details.terminal_error.message == "step execution failed"
    refute Map.has_key?(explanation.details.terminal_error, :access_token)
    refute Kernel.inspect(explanation) =~ "secret-test-token"
    assert runtime_persistence_state(runtime) == before_state
  end

  test "explains retry visibility using the runtime clock" do
    assert {:ok, runtime} = Test.start_runtime(workflow: RetryWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, retrying} = Test.drain(runtime, run)
    before_state = runtime_persistence_state(runtime)

    assert {:ok, explanation} = Test.explain(runtime, run)
    assert explanation.reason == :attempt_scheduled_for_later
    assert explanation.step == "retry_once"
    assert explanation.next_actions == [:wait_until_attempt_visible]
    assert explanation.details.next_visible_at == retrying.next_visible_at

    assert {:ok, timeline} = Test.timeline(runtime, run)
    assert Enum.any?(timeline.events, &(&1.type == :attempt_failed))
    assert runtime_persistence_state(runtime) == before_state
  end

  test "routes diagnostics through the isolated runtime" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CompleteWorkflow,
               queue: "priority",
               partition: "tenant-diagnostics",
               now: @now
             )

    assert {:ok, run} = Test.start(runtime, %{value: 1})
    assert {:ok, timeline} = Test.timeline(runtime, run)
    assert timeline.queue == "priority"
    assert timeline.partition == "tenant-diagnostics"

    assert {:ok, explanation} = Test.explain(runtime, run)
    assert explanation.queue == "priority"
    assert explanation.partition == "tenant-diagnostics"

    assert {:error, :not_found} = Test.timeline(runtime, Ecto.UUID.generate())
    assert :ok = Test.stop_runtime(runtime)
    assert {:error, :runtime_stopped} = Test.explain(runtime, run)
  end

  test "deletes checkpoints and rebuilds an active run from journal threads" do
    assert {:ok, runtime} = Test.start_runtime(workflow: TwoStepWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 7})

    assert {:reached, before_snapshot} =
             Test.execute_until(runtime, run, fn snapshot ->
               Map.has_key?(snapshot.context, :first)
             end)

    before_state = runtime_persistence_state(runtime)
    assert map_size(before_state.checkpoints) >= 2

    assert :ok = Test.delete_checkpoints(runtime)
    after_delete = runtime_persistence_state(runtime)
    assert after_delete.checkpoints == %{}
    assert after_delete.threads == before_state.threads

    assert {:ok, rebuilt} = Test.inspect(runtime, run)
    assert rebuilt == before_snapshot
    assert runtime_persistence_state(runtime) == after_delete

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context == %{first: 7, second: 7}
  end

  test "deletes checkpoints idempotently without changing a terminal journal" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 1})
    assert {:completed, completed} = Test.drain(runtime, run)
    before_threads = runtime_persistence_state(runtime).threads

    assert :ok = Test.delete_checkpoints(runtime)
    after_first_delete = runtime_persistence_state(runtime)
    assert after_first_delete.checkpoints == %{}
    assert :ok = Test.delete_checkpoints(runtime)
    assert runtime_persistence_state(runtime) == after_first_delete
    assert {:ok, rebuilt} = Test.inspect(runtime, run)
    assert rebuilt == completed
    assert runtime_persistence_state(runtime).threads == before_threads
  end

  test "fences checkpoint deletion by owner and active execution" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    before_unauthorized = runtime_persistence_state(runtime)
    task = Task.async(fn -> Test.delete_checkpoints(runtime) end)
    assert {:error, :runtime_owner_required} = Task.await(task)
    assert runtime_persistence_state(runtime) == before_unauthorized

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}
    before_state = runtime_persistence_state(runtime)
    assert {:error, :runtime_busy} = Test.delete_checkpoints(runtime)
    assert runtime_persistence_state(runtime) == before_state

    send(executor_pid, :release_barrier)
    assert {:completed, _snapshot} = Task.await(drain_task)
    assert :ok = Test.delete_checkpoints(runtime)
    assert runtime_persistence_state(runtime).checkpoints == %{}

    assert :ok = Test.stop_runtime(runtime)
    assert {:error, :runtime_stopped} = Test.delete_checkpoints(runtime)
  end

  test "deletes checkpoints after a drain holding a lease crashes" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}
    assert executor_pid == drain_task.pid
    before_state = runtime_persistence_state(runtime)
    assert nil == Task.shutdown(drain_task, :brutal_kill)

    assert :ok = Test.delete_checkpoints(runtime)
    after_delete = runtime_persistence_state(runtime)
    assert after_delete.checkpoints == %{}
    assert after_delete.threads == before_state.threads
  end

  test "restarts durable state and recovers an expired stale claim" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    crashed_drain = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, crashed_pid}
    assert crashed_pid == crashed_drain.pid
    assert nil == Task.shutdown(crashed_drain, :brutal_kill)

    assert {:ok, claimed} = Test.inspect(runtime, run)

    assert [%{step: "barrier", lease_until: lease_until}] =
             Enum.filter(claimed.attempts, &(&1.status == :claimed))

    assert lease_until == DateTime.add(@now, 300, :second)

    before_restart = runtime_persistence_state(runtime)
    assert map_size(before_restart.checkpoints) > 0
    assert {:ok, restarted} = Test.restart_runtime(runtime)
    on_exit(fn -> Test.stop_runtime(restarted) end)

    assert restarted.id != runtime.id
    assert restarted.storage_server != runtime.storage_server
    assert {:error, :runtime_stopped} = Test.inspect(runtime, run)
    assert runtime_persistence_state(restarted) == before_restart
    assert {:ok, ^claimed} = Test.inspect(restarted, run)

    assert {:ok, before_expiry} = Test.advance_time(restarted, 299, :second)
    assert before_expiry == DateTime.add(@now, 299, :second)
    before_blocked_drain = runtime_persistence_state(restarted)
    assert {:blocked, before_expiry_snapshot} = Test.drain(restarted, run)
    assert runtime_persistence_state(restarted) == before_blocked_drain

    assert [%{lease_until: ^lease_until} = before_expiry_attempt] =
             Enum.filter(before_expiry_snapshot.attempts, &(&1.status == :claimed))

    assert before_expiry_attempt ==
             Enum.find(claimed.attempts, &(&1.status == :claimed))

    refute_receive {:barrier_entered, _pid}

    assert {:ok, ^lease_until} = Test.advance_time(restarted, 1, :second)
    recovered_drain = Task.async(fn -> Test.drain(restarted, run) end)
    assert_receive {:barrier_entered, recovery_pid}
    send(recovery_pid, :release_barrier)

    assert {:completed, completed} = Task.await(recovered_drain)
    assert completed.context.released == true

    dispatch_entries = runtime_dispatch_entries(restarted)
    assert Enum.count(dispatch_entries, &(&1.type == :attempt_claimed)) == 2
    assert Enum.count(dispatch_entries, &(&1.type == :attempt_completed)) == 1
  end

  test "fences restart by owner and live execution" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BarrierWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    :persistent_term.put({BarrierStep, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({BarrierStep, run.run_id}) end)

    before_unauthorized = runtime_full_state(runtime)
    task = Task.async(fn -> Test.restart_runtime(runtime) end)
    assert {:error, :runtime_owner_required} = Task.await(task)
    assert runtime_full_state(runtime) == before_unauthorized

    drain_task = Task.async(fn -> Test.drain(runtime, run) end)
    assert_receive {:barrier_entered, executor_pid}
    before_busy = runtime_full_state(runtime)
    assert {:error, :runtime_busy} = Test.restart_runtime(runtime)
    assert runtime_full_state(runtime) == before_busy

    send(executor_pid, :release_barrier)
    assert {:completed, _snapshot} = Task.await(drain_task)
  end

  test "fences restart during start reservation and preserves armed append faults" do
    assert {:ok, reserved_runtime} =
             Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(reserved_runtime) end)

    assert :ok = Jizoku.Test.Storage.reserve_start(reserved_runtime.storage_server)
    before_reserved_restart = runtime_full_state(reserved_runtime)
    assert {:error, :runtime_busy} = Test.restart_runtime(reserved_runtime)
    assert runtime_full_state(reserved_runtime) == before_reserved_restart
    assert :ok = Jizoku.Test.Storage.release_start(reserved_runtime.storage_server)
    assert {:ok, restarted_reserved_runtime} = Test.restart_runtime(reserved_runtime)
    on_exit(fn -> Test.stop_runtime(restarted_reserved_runtime) end)

    assert {:ok, conflict_runtime} =
             Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(conflict_runtime) end)
    assert {:ok, run} = Test.start(conflict_runtime, %{value: 1})
    assert :ok = Test.inject_append_conflict(conflict_runtime, :dispatch)
    armed_conflict = runtime_fault_state(conflict_runtime)
    assert {:ok, restarted_conflict_runtime} = Test.restart_runtime(conflict_runtime)
    on_exit(fn -> Test.stop_runtime(restarted_conflict_runtime) end)
    assert runtime_fault_state(restarted_conflict_runtime) == armed_conflict
    assert {:error, :conflict} = Test.drain(restarted_conflict_runtime, run)
    assert {:completed, _snapshot} = Test.drain(restarted_conflict_runtime, run)

    assert {:ok, fault_runtime} =
             Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(fault_runtime) end)

    assert :ok =
             Jizoku.Test.Storage.put_append_fault(
               fault_runtime.storage_server,
               "jido:thread:run:",
               {:error, :persistent_fault}
             )

    armed_fault = :sys.get_state(fault_runtime.storage_server).append_fault
    assert {:ok, restarted_fault_runtime} = Test.restart_runtime(fault_runtime)
    on_exit(fn -> Test.stop_runtime(restarted_fault_runtime) end)
    assert :sys.get_state(restarted_fault_runtime.storage_server).append_fault == armed_fault
  end

  test "stops abandoned runtime storage when its owner exits" do
    parent = self()

    owner =
      spawn(fn ->
        {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
        send(parent, {:runtime, runtime})
      end)

    owner_ref = Process.monitor(owner)
    assert_receive {:runtime, runtime}
    storage_ref = Process.monitor(runtime.storage_server)
    assert_receive {:DOWN, ^owner_ref, :process, ^owner, :normal}
    assert_receive {:DOWN, ^storage_ref, :process, _, storage_reason}
    assert storage_reason in [:normal, :noproc]
    assert :ok = Test.stop_runtime(runtime)
    assert {:error, :runtime_stopped} = Test.inspect(runtime, Ecto.UUID.generate())
  end

  test "rejects invalid runtime and execution options without starting storage" do
    assert {:error, {:invalid_option, {:workflow, :invalid}}} =
             Test.start_runtime(workflow: :not_a_workflow)

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Test.start_runtime([:bad])

    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 1})

    assert {:error, {:invalid_option, {:max_steps, :invalid}}} =
             Test.drain(runtime, run, max_steps: 0)
  end

  defp runtime_journal_state(runtime, run_id) do
    {adapter, opts} = runtime.storage

    %{
      run:
        adapter.load_thread(
          Jizoku.Runtime.Journal.thread_id({:run, run_id}, runtime.partition),
          opts
        ),
      dispatch:
        adapter.load_thread(
          Jizoku.Runtime.Journal.thread_id({:dispatch, runtime.queue}, runtime.partition),
          opts
        )
    }
  end

  defp runtime_persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end

  defp runtime_full_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :root_run_id, :start_reservation, :threads])
  end

  defp runtime_fault_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:append_conflict_counts, :next_append_conflict])
  end

  defp runtime_run_entries(runtime, run_id) do
    {:ok, storage} =
      Jizoku.Runtime.Journal.Storage.scope(runtime.storage, runtime.partition)

    {:ok, entries} = Jizoku.Runtime.Journal.load_entries(storage, {:run, run_id})
    entries
  end

  defp runtime_dispatch_entries(runtime) do
    {:ok, storage} =
      Jizoku.Runtime.Journal.Storage.scope(runtime.storage, runtime.partition)

    {:ok, entries} =
      Jizoku.Runtime.Journal.load_entries(storage, {:dispatch, runtime.queue})

    entries
  end
end
