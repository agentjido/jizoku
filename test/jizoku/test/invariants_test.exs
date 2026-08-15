defmodule Jizoku.Test.InvariantsTest do
  use ExUnit.Case, async: false

  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.Test
  alias Jizoku.Test.Invariants

  @now ~U[2026-08-11 12:00:00Z]

  defmodule CompleteStep do
    use Jizoku.Step, name: :complete

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{completed: true}}
    end
  end

  defmodule CompleteWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :finish, CompleteStep
      transition :finish, on: :ok, to: :complete
    end
  end

  defmodule RetryStep do
    use Jizoku.Step, name: :retry_once

    @impl Jizoku.Step
    def run(_input, %Jizoku.Step.Context{attempt: 1}) do
      {:retry, %{code: "retry_later", message: "retry later"}}
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
        retry: [max_attempts: 2, backoff: [type: :exponential, min: 1_000, max: 1_000]]

      transition :retry_once, on: :ok, to: :complete
    end
  end

  defmodule PauseWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :pause, :pause
      transition :pause, on: :ok, to: :complete
    end
  end

  test "accepts healthy running, recovery, manual, and terminal snapshots" do
    runnable = %{runnable_key: "run-1:step:1"}

    snapshots = [
      snapshot(planned_runnable_keys: ["run-1:step:1"], visible_attempts: [runnable]),
      snapshot(planned_runnable_keys: ["run-1:step:1"], pending_dispatches: [runnable]),
      snapshot(planned_runnable_keys: ["run-1:step:1"], pending_results: [runnable]),
      snapshot(
        reason: :attempt_scheduled_for_later,
        planned_runnable_keys: ["run-1:step:1"],
        scheduled_attempts: [runnable],
        next_visible_at: @now
      ),
      snapshot(
        reason: :attempt_claimed,
        planned_runnable_keys: ["run-1:step:1"],
        attempts: [Map.put(runnable, :status, :claimed)]
      ),
      snapshot(
        reason: :expired_claim,
        planned_runnable_keys: ["run-1:step:1"],
        expired_claims: [runnable]
      ),
      snapshot(
        status: :paused,
        reason: :manual_intervention_required,
        manual_state: %{step: "review"}
      ),
      snapshot(
        status: :completed,
        reason: :terminal,
        terminal?: true,
        terminal_status: :completed,
        terminal_at: @now,
        planned_runnable_keys: ["run-1:step:1"],
        applied_runnable_keys: ["run-1:step:1"],
        attempts: [runnable]
      )
    ]

    assert Enum.all?(snapshots, &(Invariants.check(&1) == :ok))
  end

  test "returns ordered redaction-safe violations for inconsistent snapshots" do
    secret = "do-not-report"

    invalid =
      snapshot(
        input: %{secret: secret},
        context: %{secret: secret},
        terminal_error: %{message: secret},
        status: %{secret: secret},
        reason: %{secret: secret},
        terminal?: true,
        terminal_status: %{secret: secret},
        terminal_at: nil,
        next_visible_at: @now,
        anomalies: [
          %{source: :workflow, reason: :malformed_entry, payload: secret},
          %{source: :dispatch, reason: :conflicting_terminal}
        ],
        planned_runnable_keys: ["known", "known"],
        applied_runnable_keys: ["applied-unknown", "applied-unknown"],
        pending_dispatches: [
          %{runnable_key: "known"},
          %{runnable_key: "known"},
          %{runnable_key: "applied-unknown"},
          %{missing: secret}
        ],
        pending_results: [%{"runnable_key" => "result-unknown", "result" => secret}]
      )

    assert {:error, {:invariant_violations, report}} = Invariants.check(invalid)
    assert report.version == 1
    assert report.run_id == invalid.run_id
    assert report.thread_revisions == invalid.thread_revisions

    assert Enum.map(report.violations, &{&1.code, &1.details}) == [
             {:projection_anomaly,
              %{
                count: 2,
                reasons: [:conflicting_terminal, :malformed_entry],
                sources: [:dispatch, :workflow]
              }},
             {:terminal_state_incoherent,
              %{
                active_fields: [:next_visible_at, :pending_dispatches, :pending_results],
                reason: :malformed,
                status: :malformed,
                terminal?: true,
                terminal_at?: false,
                terminal_status: :malformed
              }},
             {:duplicate_runnable_key,
              %{collection: :planned_runnable_keys, runnable_keys: ["known"]}},
             {:duplicate_runnable_key,
              %{collection: :applied_runnable_keys, runnable_keys: ["applied-unknown"]}},
             {:duplicate_runnable_key,
              %{collection: :pending_dispatches, runnable_keys: ["known"]}},
             {:malformed_runnable_key, %{collection: :pending_dispatches, count: 1}},
             {:unknown_runnable,
              %{collection: :applied_runnable_keys, runnable_keys: ["applied-unknown"]}},
             {:unknown_runnable,
              %{collection: :pending_dispatches, runnable_keys: ["applied-unknown"]}},
             {:unknown_runnable,
              %{collection: :pending_results, runnable_keys: ["result-unknown"]}},
             {:pending_and_applied,
              %{collection: :pending_dispatches, runnable_keys: ["applied-unknown"]}}
           ]

    refute inspect(report) =~ secret

    malformed_collections = [
      planned_runnable_keys: nil,
      applied_runnable_keys: [nil],
      pending_dispatches: nil,
      pending_results: [:invalid]
    ]

    Enum.each(malformed_collections, fn {collection, value} ->
      assert {:error, {:invariant_violations, malformed_report}} =
               Invariants.check(snapshot([{collection, value}]))

      assert [%{code: :malformed_runnable_key, details: %{collection: ^collection}}] =
               malformed_report.violations
    end)
  end

  test "reports pending-result conflicts and cross-view overlap independently" do
    assert {:error, {:invariant_violations, applied_report}} =
             Invariants.check(
               snapshot(
                 planned_runnable_keys: ["b", "a"],
                 applied_runnable_keys: ["b", "a"],
                 pending_results: [%{runnable_key: "b"}, %{runnable_key: "a"}]
               )
             )

    assert applied_report.violations == [
             %{
               code: :pending_and_applied,
               details: %{collection: :pending_results, runnable_keys: ["a", "b"]}
             }
           ]

    assert {:error, {:invariant_violations, overlap_report}} =
             Invariants.check(
               snapshot(
                 planned_runnable_keys: ["b", "a"],
                 pending_dispatches: [%{runnable_key: "b"}, %{runnable_key: "a"}],
                 pending_results: [%{runnable_key: "b"}, %{runnable_key: "a"}]
               )
             )

    assert overlap_report.violations == [
             %{
               code: :pending_in_multiple_views,
               details: %{runnable_keys: ["a", "b"]}
             }
           ]
  end

  test "accepts public retry and manual snapshots" do
    assert {:ok, retry_runtime} = Test.start_runtime(workflow: RetryWorkflow, now: @now)
    assert {:ok, retry_run} = Test.start(retry_runtime, %{})

    assert {:blocked, %{reason: :attempt_scheduled_for_later}} =
             Test.drain(retry_runtime, retry_run)

    assert {:ok, %{reason: :attempt_scheduled_for_later}} =
             Test.check_invariants(retry_runtime, retry_run)

    assert {:ok, pause_runtime} = Test.start_runtime(workflow: PauseWorkflow, now: @now)
    assert {:ok, pause_run} = Test.start(pause_runtime, %{})
    assert {:blocked, %{status: :paused}} = Test.drain(pause_runtime, pause_run)
    assert {:ok, %{status: :paused}} = Test.check_invariants(pause_runtime, pause_run)

    on_exit(fn -> Test.stop_runtime(retry_runtime) end)
    on_exit(fn -> Test.stop_runtime(pause_runtime) end)
  end

  test "checks one public snapshot without changing isolated persistence" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: CompleteWorkflow,
               queue: "priority",
               partition: "tenant-invariants",
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, completed} = Test.drain(runtime, run)
    before_state = persistence_state(runtime)

    assert {:ok, checked} = Test.check_invariants(runtime, run)
    assert checked == completed
    assert persistence_state(runtime) == before_state

    assert :ok = Test.delete_checkpoints(runtime)
    after_delete = persistence_state(runtime)
    assert {:ok, rebuilt} = Test.check_invariants(runtime, run)
    assert rebuilt == completed
    assert persistence_state(runtime) == after_delete
  end

  test "propagates inspection routing errors" do
    assert {:ok, runtime} = Test.start_runtime(workflow: CompleteWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:error, :not_found} = Test.check_invariants(runtime, Ecto.UUID.generate())
    assert :ok = Test.stop_runtime(runtime)

    assert {:error, :runtime_stopped} =
             Test.check_invariants(runtime, Ecto.UUID.generate())
  end

  defp snapshot(overrides) do
    struct!(
      Snapshot,
      Keyword.merge(
        [
          run_id: "run-1",
          partition: "tenant-1",
          workflow: "Example.Workflow",
          queue: "priority",
          status: :running,
          reason: :attempt_visible,
          terminal?: false,
          terminal_status: nil,
          terminal_at: nil,
          thread_revisions: %{run: 3, dispatch: 4}
        ],
        overrides
      )
    )
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
