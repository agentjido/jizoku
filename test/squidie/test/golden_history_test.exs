defmodule Squidie.Test.GoldenHistoryTest do
  use ExUnit.Case, async: false

  alias Squidie.ReadModel.Timeline
  alias Squidie.ReadModel.Timeline.Event
  alias Squidie.Test
  alias Squidie.Test.GoldenHistory

  @now ~U[2026-08-11 12:00:00Z]

  defmodule CompleteStep do
    use Squidie.Step, name: :record_value

    @impl Squidie.Step
    def run(%{value: value}, _context) do
      {:ok, %{value: value}}
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

  defmodule RetryStep do
    use Squidie.Step, name: :retry_once

    @impl Squidie.Step
    def run(_input, %Squidie.Step.Context{attempt: 1}) do
      {:retry, %{code: "retry_later", message: "retry later"}}
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

  defmodule FailingStep do
    use Squidie.Step, name: :fail_safely

    @impl Squidie.Step
    def run(%{secret: secret}, _context) do
      {:error, %{code: "expected_failure", raw_error: secret}}
    end
  end

  defmodule FailingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :secret, :string
        end
      end

      step :fail_safely, FailingStep
      transition :fail_safely, on: :ok, to: :complete
    end
  end

  test "returns a literal versioned history without volatile identifiers or timestamps" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{value: 7})
    assert {:completed, _snapshot} = Test.drain(runtime, run)
    before_state = persistence_state(runtime)

    expected = %{
      schema_version: 1,
      workflow: "Elixir.Squidie.Test.GoldenHistoryTest.CompleteWorkflow",
      queue: "default",
      partition: nil,
      status: :completed,
      terminal_status: :completed,
      events: [
        %{
          type: :command_received,
          offset_us: 0,
          run: "run-1",
          details: %{signal_type: "start_run"}
        },
        %{type: :run_started, offset_us: 0, run: "run-1", status: :running},
        %{
          type: :attempt_claimed,
          offset_us: 0,
          run: "run-1",
          step: "record_value",
          runnable: "runnable-1",
          status: :claimed,
          details: %{attempt_number: 1}
        },
        %{
          type: :attempt_completed,
          offset_us: 0,
          run: "run-1",
          step: "record_value",
          runnable: "runnable-1",
          status: :completed,
          details: %{attempt_number: 1}
        },
        %{
          type: :runnable_applied,
          offset_us: 0,
          run: "run-1",
          step: "record_value",
          runnable: "runnable-1",
          status: :applied
        },
        %{
          type: :attempt_scheduled,
          offset_us: 0,
          run: "run-1",
          step: "record_value",
          runnable: "runnable-1",
          status: :available,
          details: %{attempt_number: 1, visible_offset_us: 0}
        },
        %{type: :run_terminal, offset_us: 0, run: "run-1", status: :completed}
      ]
    }

    assert {:ok, ^expected} = Test.golden_history(runtime, run)
    assert {:ok, ^expected} = Test.golden_history(runtime, run)
    assert persistence_state(runtime) == before_state

    assert {:ok, second_runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(second_runtime) end)

    assert {:ok, second_run} = Test.start(second_runtime, %{value: 7})
    assert second_run.run_id != run.run_id
    assert {:completed, _snapshot} = Test.drain(second_runtime, second_run)
    assert {:ok, ^expected} = Test.golden_history(second_runtime, second_run)
  end

  test "uses stable ordering and relative visibility offsets across virtual time" do
    assert {:ok, runtime} = Test.start_runtime(workflow: RetryWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:blocked, _snapshot} = Test.drain(runtime, run)
    assert {:ok, blocked_history} = Test.golden_history(runtime, run)

    scheduled = Enum.filter(blocked_history.events, &(&1.type == :attempt_scheduled))

    assert Enum.map(scheduled, & &1.details) == [
             %{attempt_number: 1, visible_offset_us: 0},
             %{attempt_number: 2, visible_offset_us: 60_000_000}
           ]

    assert Enum.map(blocked_history.events, & &1.type) == [
             :command_received,
             :run_started,
             :attempt_claimed,
             :attempt_failed,
             :attempt_scheduled,
             :attempt_scheduled
           ]

    assert {:ok, _now} = Test.advance_time(runtime, 60, :second)
    assert {:completed, _snapshot} = Test.drain(runtime, run)
    assert {:ok, completed_history} = Test.golden_history(runtime, run)

    assert completed_history.events
           |> Enum.filter(&(&1.offset_us == 60_000_000))
           |> Enum.map(& &1.type) == [
             :attempt_claimed,
             :attempt_completed,
             :runnable_applied,
             :run_terminal
           ]
  end

  test "omits application secrets, raw identifiers, and free-form event details" do
    secret = "golden-secret-sentinel"
    raw_run_id = "raw-root-#{secret}"
    raw_linked_id = "raw-linked-#{secret}"
    raw_runnable_key = "raw-runnable-#{secret}"

    timeline = %Timeline{
      run_id: raw_run_id,
      workflow: "Elixir.SafeWorkflow",
      queue: "default",
      status: :running,
      terminal?: false,
      terminal_status: nil,
      events: [
        %Event{
          type: :command_received,
          occurred_at: @now,
          run_id: raw_run_id,
          summary: secret,
          details: %{signal_type: secret, input: %{secret: secret}}
        },
        %Event{
          type: :run_continued_to,
          occurred_at: @now,
          run_id: raw_run_id,
          summary: secret,
          details: %{run_id: raw_linked_id, continuation_key: secret}
        },
        %Event{
          type: :manual_step_paused,
          occurred_at: @now,
          run_id: raw_run_id,
          step_id: "review",
          runnable_key: raw_runnable_key,
          summary: secret,
          details: %{kind: :approval, reason: secret, result: %{secret: secret}}
        }
      ]
    }

    golden = GoldenHistory.from_timeline(timeline)

    assert golden.events == [
             %{
               type: :command_received,
               offset_us: 0,
               run: "run-1",
               details: %{signal_type: :unknown}
             },
             %{
               type: :run_continued_to,
               offset_us: 0,
               run: "run-1",
               details: %{linked_run: "run-2"}
             },
             %{
               type: :manual_step_paused,
               offset_us: 0,
               run: "run-1",
               step: "review",
               runnable: "runnable-1",
               details: %{kind: :approval}
             }
           ]

    refute Kernel.inspect(golden) =~ secret

    assert {:ok, runtime} = Test.start_runtime(workflow: FailingWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{secret: secret})
    assert {:failed, _snapshot} = Test.drain(runtime, run)
    assert {:ok, failed_golden} = Test.golden_history(runtime, run)
    assert Enum.any?(failed_golden.events, &(&1.type == :attempt_failed))
    assert Enum.any?(failed_golden.events, &(&1.type == :run_terminal))
    refute Kernel.inspect(failed_golden) =~ secret
  end

  test "is unchanged after checkpoint deletion and preserves configured routing" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CompleteWorkflow,
               queue: "priority",
               partition: "tenant-golden",
               now: @now
             )

    assert {:ok, run} = Test.start(runtime, %{value: 1})
    assert {:completed, _snapshot} = Test.drain(runtime, run)
    assert {:ok, before_loss} = Test.golden_history(runtime, run)

    assert before_loss.queue == "priority"
    assert before_loss.partition == "tenant-golden"

    before_delete = persistence_state(runtime)
    assert map_size(before_delete.checkpoints) > 0

    assert :ok = Test.delete_checkpoints(runtime)
    after_delete = persistence_state(runtime)
    assert after_delete.checkpoints == %{}
    assert after_delete.threads == before_delete.threads

    assert {:ok, ^before_loss} = Test.golden_history(runtime, run)
    assert persistence_state(runtime) == after_delete
    assert {:error, :not_found} = Test.golden_history(runtime, Ecto.UUID.generate())

    assert :ok = Test.stop_runtime(runtime)
    assert {:error, :runtime_stopped} = Test.golden_history(runtime, run)
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
