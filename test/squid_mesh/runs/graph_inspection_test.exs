defmodule SquidMesh.Runs.GraphInspectionTest do
  use ExUnit.Case, async: true

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runs.GraphInspection

  defmodule ConditionalScoreWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :score_invoice, __MODULE__.ScoreInvoice
      step :escalate_review, __MODULE__.EscalateReview
      step :auto_approve, __MODULE__.AutoApprove

      transition :score_invoice,
        on: :ok,
        to: :escalate_review,
        condition: [path: [:risk, :score], greater_than: 70]

      transition :score_invoice,
        on: :ok,
        to: :auto_approve,
        condition: [path: [:risk, :score], less_than: 30]

      transition :score_invoice, on: :ok, to: :auto_approve
      transition :escalate_review, on: :ok, to: :complete
      transition :auto_approve, on: :ok, to: :complete
    end
  end

  @run_id "run_123"
  @child_run %{
    child_run_id: "child_run_123",
    child_workflow: "ChildWorkflow",
    child_trigger: "manual",
    child_key: "digest_subscription_1",
    origin: %{runnable_key: "run_123:fanout:1", step: "fanout", attempt: 1},
    metadata: %{subscription_id: "sub_123"}
  }

  test "exposes child runs as graph metadata instead of inline nodes" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      child_runs: [@child_run]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert graph.child_runs == [@child_run]
    assert graph.nodes == []

    assert %{
             child_runs: [@child_run],
             child_links: [
               %{
                 id: "fanout:child_run:child_run_123",
                 from: "fanout",
                 to: "child_run_123",
                 type: :child_run,
                 status: :linked,
                 child_run_id: "child_run_123",
                 child_workflow: "ChildWorkflow",
                 child_trigger: "manual",
                 child_key: "digest_subscription_1",
                 origin: %{runnable_key: "run_123:fanout:1", step: "fanout", attempt: 1},
                 metadata: %{subscription_id: "sub_123"}
               }
             ],
             nodes: []
           } = GraphInspection.to_map(graph)
  end

  test "skips stale child run records that cannot be linked to an origin step" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      child_runs: [
        %{child_run_id: "child_without_origin", origin: %{}},
        %{origin: %{step: "fanout"}},
        :legacy_child_run
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert graph.child_links == []
    assert %{child_links: []} = GraphInspection.to_map(graph)
  end

  test "exposes stable action identity from planned runnable metadata" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "RuntimeAuthoredWorkflow",
      queue: "default",
      status: :running,
      reason: :planned_dispatch_pending_schedule,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      planned_runnables: [
        %{
          step: :load_invoice,
          metadata: %{action: "billing.load_invoice"}
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert [%{id: "load_invoice", action: "billing.load_invoice"}] =
             Enum.map(graph.nodes, &Map.take(&1, [:id, :action]))

    assert %{nodes: [%{id: "load_invoice", action: "billing.load_invoice"}]} =
             GraphInspection.to_map(graph)
  end

  test "marks greater-than conditional transition edges from persisted route evidence" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: Atom.to_string(ConditionalScoreWorkflow),
      queue: "default",
      status: :running,
      reason: :planned_dispatch_pending_schedule,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      attempts: [
        %{
          runnable_key: "run_123:score_invoice:1",
          status: :completed,
          attempt_number: 1,
          step: "score_invoice",
          input: %{},
          visible_at: ~U[2026-05-26 00:00:00Z],
          idempotency_key: "run_123:score_invoice:1",
          result: %{"risk" => %{"score" => 71}},
          transition: %{
            "from" => "score_invoice",
            "on" => "ok",
            "to" => "escalate_review",
            "condition" => %{"path" => ["risk", "score"], "greater_than" => 70}
          },
          wakeup_emitted?: false,
          applied?: true
        }
      ],
      planned_runnables: [
        %{
          step: "escalate_review",
          metadata: %{action: "billing.escalate_review"}
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert [
             %{
               id: "score_invoice:ok:escalate_review:condition:0",
               condition: %{path: [:risk, :score], greater_than: 70},
               status: :selected,
               selected?: true
             },
             %{
               id: "score_invoice:ok:auto_approve:condition:1",
               condition: %{path: [:risk, :score], less_than: 30},
               status: :skipped,
               skipped?: true
             },
             %{
               id: "score_invoice:ok:auto_approve",
               condition: nil,
               status: :skipped,
               skipped?: true
             }
             | _remaining
           ] = GraphInspection.to_map(graph).edges
  end
end
