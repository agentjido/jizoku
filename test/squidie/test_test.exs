defmodule Squidie.TestTest do
  use ExUnit.Case, async: false

  alias Squidie.Test

  defmodule CompleteStep do
    use Squidie.Step, name: :record_value

    @impl Squidie.Step
    def run(input, _context) do
      {:ok, %{value: Map.fetch!(input, :value)}}
    end
  end

  defmodule CompleteWorkflow do
    use Squidie.Workflow

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

  defmodule DeferredStep do
    use Squidie.Step, name: :deferred

    @impl Squidie.Step
    def run(_input, %Squidie.Step.Context{runnable_key: runnable_key}) do
      if String.ends_with?(runnable_key, ":deferred") do
        {:ok, %{ready: true}}
      else
        {:defer, %{reason: "not_ready"}, schedule_in: 60}
      end
    end
  end

  defmodule DeferredWorkflow do
    use Squidie.Workflow

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
    use Squidie.Step, name: :retry_once

    @impl Squidie.Step
    def run(_input, %Squidie.Step.Context{attempt: 1}) do
      {:retry, %{message: "retry later", code: "retry_later"}}
    end

    def run(_input, %Squidie.Step.Context{}) do
      {:ok, %{retried: true}}
    end
  end

  defmodule RetryWorkflow do
    use Squidie.Workflow

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
    use Squidie.Workflow

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
    use Squidie.Step, name: :record_wait

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{waited: true}}
    end
  end

  defmodule BarrierStep do
    use Squidie.Step, name: :barrier

    @impl Squidie.Step
    def run(_input, %Squidie.Step.Context{run_id: run_id}) do
      test_pid = :persistent_term.get({__MODULE__, run_id})
      send(test_pid, {:barrier_entered, self()})

      receive do
        :release_barrier -> {:ok, %{released: true}}
      end
    end
  end

  defmodule BarrierWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :barrier, BarrierStep
      transition :barrier, on: :ok, to: :complete
    end
  end

  defmodule TwoStepWorkflow do
    use Squidie.Workflow

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
    use Squidie.Step, name: :first

    @impl Squidie.Step
    def run(%{value: value}, _context) do
      {:ok, %{first: value}}
    end
  end

  defmodule TwoStepWorkflow.SecondStep do
    use Squidie.Step, name: :second

    @impl Squidie.Step
    def run(%{value: value}, _context) do
      {:ok, %{second: value}}
    end
  end

  defmodule FailingStep do
    use Squidie.Step, name: :fail

    @impl Squidie.Step
    def run(_input, _context) do
      {:error, %{code: "expected_test_failure"}}
    end
  end

  defmodule FailingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :fail, FailingStep
      transition :fail, on: :ok, to: :complete
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

    assert {:error, _reason} = Test.start(runtime, %{})

    caller =
      spawn(fn ->
        :ok = Squidie.Test.Storage.reserve_start(runtime.storage_server)
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

    assert :ok =
             Squidie.Test.Storage.put_append_fault(
               committed_runtime.storage_server,
               "squidie:run_index:",
               {:error, :index_failed}
             )

    assert {:error, {:journal_start_committed, committed_run_id, _reason}} =
             Test.start(committed_runtime, %{value: 1})

    assert {:ok, %{run_id: ^committed_run_id}} = Test.inspect(committed_runtime, committed_run_id)

    assert {:error, :runtime_already_started} =
             Test.start(committed_runtime, %{value: 2})

    assert {:ok, unknown_runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)

    assert :ok =
             Squidie.Test.Storage.put_append_fault(
               unknown_runtime.storage_server,
               "squidie:run_index:",
               {:raise, RuntimeError.exception("unknown start outcome")}
             )

    assert_raise RuntimeError, "unknown start outcome", fn ->
      Test.start(unknown_runtime, %{value: 1})
    end

    assert {:error, :runtime_already_started} = Test.start(unknown_runtime, %{value: 2})

    on_exit(fn -> Test.stop_runtime(committed_runtime) end)
    on_exit(fn -> Test.stop_runtime(unknown_runtime) end)
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
               Squidie.Runtime.Journal.thread_id({:dispatch, "priority"}, "tenant-test-kit"),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Squidie.Runtime.Journal.thread_id({:dispatch, "default"}, "tenant-test-kit"),
               opts
             )

    assert :not_found =
             adapter.load_thread(
               Squidie.Runtime.Journal.thread_id({:dispatch, "priority"}),
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
end
