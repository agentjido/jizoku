defmodule Squidie.Runs.GraphInspectionTest do
  use ExUnit.Case, async: true

  alias Squidie.Inspection
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.Runs.GraphInspection

  defmodule ConditionalScoreWorkflow do
    use Squidie.Workflow

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

  test "canonical inspection graph keeps the old Runs graph shape compatible" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0}
    }

    inspection_graph = Inspection.GraphInspection.from_snapshot(snapshot, source: :read_model)
    runs_graph = GraphInspection.from_inspection_graph(inspection_graph)

    assert %Inspection.GraphInspection{} = inspection_graph
    assert %GraphInspection{} = runs_graph

    assert GraphInspection.to_map(runs_graph) ==
             Inspection.GraphInspection.to_map(inspection_graph)

    assert inspection_graph == GraphInspection.to_inspection_graph(runs_graph)
  end

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

  test "surfaces string-keyed pending dispatches as current pending graph nodes" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :planned_dispatch_pending_schedule,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      dynamic_work: [
        %{
          "dynamic_key" => "compensation",
          "nodes" => [
            %{
              "id" => "compensate:authorize_payment",
              "action" => "compensation.authorize_payment",
              "status" => "waiting"
            }
          ]
        }
      ],
      pending_dispatches: [
        %{
          "step" => "compensate:authorize_payment",
          "runnable_key" => "#{@run_id}:compensate:authorize_payment:1",
          "recovery" => %{
            "irreversible?" => false,
            "compensatable?" => false,
            "replay" => "manual_review_required",
            "recovery" => "manual_intervention"
          }
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)
    nodes_by_id = Map.new(graph.nodes, &{&1.id, &1})

    assert graph.current_node_ids == ["compensate:authorize_payment"]
    assert nodes_by_id["compensate:authorize_payment"].current?
    assert nodes_by_id["compensate:authorize_payment"].status == :pending

    assert nodes_by_id["compensate:authorize_payment"].recovery == %{
             irreversible?: false,
             compensatable?: false,
             replay: :manual_review_required,
             recovery: :manual_intervention
           }
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

  test "exposes deadline state on graph nodes" do
    deadline = %{
      status: :overdue,
      step: "load_invoice",
      due_at: ~U[2026-05-15 00:00:30Z],
      escalation: %{outcome: :diagnostic}
    }

    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "RuntimeAuthoredWorkflow",
      queue: "default",
      status: :running,
      reason: :attempt_visible,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 1},
      attempts: [
        %{
          step: "load_invoice",
          runnable_key: "#{@run_id}:load_invoice:1",
          attempt_number: 1,
          status: :available,
          deadline: deadline
        }
      ],
      visible_attempts: [
        %{
          step: "load_invoice",
          runnable_key: "#{@run_id}:load_invoice:1",
          attempt_number: 1,
          status: :available,
          deadline: deadline
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert [%{id: "load_invoice", deadline: ^deadline}] = GraphInspection.to_map(graph).nodes
  end

  test "dynamic work overlays stay consistent with stale string-keyed graph records" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      dynamic_work: [
        :legacy_dynamic_work,
        %{
          "dynamic_key" => "string_fanout",
          "status" => "recorded",
          "origin" => %{"step" => "fanout", "secret" => "internal"},
          "nodes" => [
            %{"action" => "missing.id"},
            %{"id" => "deliver_digest:chat_1", "action" => "digest.deliver"}
          ],
          "edges" => [
            %{"id" => "missing_to", "from" => "fanout"},
            %{
              "id" => "fanout:dynamic:deliver_digest:chat_1",
              "from" => "fanout",
              "to" => "deliver_digest:chat_1",
              "type" => "dynamic"
            }
          ],
          "metadata" => %{"secret" => "internal"}
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert ["deliver_digest:chat_1"] = Enum.map(graph.nodes, & &1.id)
    assert ["fanout:dynamic:deliver_digest:chat_1"] = Enum.map(graph.edges, & &1.id)

    assert [
             %{},
             %{
               dynamic_key: "string_fanout",
               origin_node_id: "fanout",
               added_node_ids: ["deliver_digest:chat_1"],
               added_edge_ids: ["fanout:dynamic:deliver_digest:chat_1"],
               node_count: 1,
               edge_count: 1
             } = overlay
           ] = graph.dynamic_work_overlays

    refute Map.has_key?(overlay, :metadata)
  end

  test "graph mutation state normalizes legacy keys and drops malformed or sensitive data" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      graph_version: 3,
      graph_provenance: %{
        "nodes" => [
          %{"id" => "safe", "provenance" => "dependency_ordered", "input" => "secret"},
          %{"id" => 1, "provenance" => "legacy_eager"},
          :malformed
        ],
        "edges" => [%{"id" => "safe-edge", "provenance" => "legacy_eager"}]
      },
      active_node_ids: ["safe", 1],
      active_edge_ids: ["safe-edge"],
      ready_node_ids: ["safe"],
      blocked_node_ids: :malformed,
      tombstoned_node_ids: ["removed"],
      tombstoned_edge_ids: ["removed-edge"],
      mutation_history: [
        %{
          "mutation_id" => "mutation-safe",
          "origin" => "host",
          "expected_version" => 2,
          "result_version" => 3,
          "added_node_ids" => ["safe"],
          "input" => %{"token" => "secret"}
        },
        :malformed
      ],
      reconciliation_status: "required"
    }

    payload =
      snapshot
      |> GraphInspection.from_snapshot(source: :read_model)
      |> GraphInspection.to_map()

    assert payload.graph_version == 3

    assert payload.graph_provenance == %{
             nodes: [%{id: "safe", provenance: :dependency_ordered}],
             edges: [%{id: "safe-edge", provenance: :legacy_eager}]
           }

    assert payload.active_node_ids == ["safe"]
    assert payload.active_edge_ids == ["safe-edge"]
    assert payload.blocked_node_ids == []
    assert payload.tombstoned_node_ids == ["removed"]
    assert payload.tombstoned_edge_ids == ["removed-edge"]
    assert payload.reconciliation_status == :required

    assert payload.mutation_history == [
             %{
               mutation_id: "mutation-safe",
               origin: "host",
               expected_version: 2,
               result_version: 3,
               added_node_ids: ["safe"]
             }
           ]

    refute inspect(payload) =~ "secret"
  end

  test "dynamic work graph inspection tolerates non-list dynamic work shapes" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      dynamic_work: :legacy_dynamic_work
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert graph.nodes == []
    assert graph.edges == []
    assert graph.dynamic_work_overlays == []
  end

  test "dynamic work overlays tolerate non-list node and edge fields" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      dynamic_work: [
        %{
          dynamic_key: "legacy_shape",
          nodes: :legacy_nodes,
          edges: :legacy_edges
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)

    assert graph.nodes == []
    assert graph.edges == []

    assert [
             %{
               dynamic_key: "legacy_shape",
               added_node_ids: [],
               added_edge_ids: [],
               node_count: 0,
               edge_count: 0
             }
           ] = graph.dynamic_work_overlays
  end

  test "normalizes dynamic node and edge status strings" do
    dynamic_nodes =
      Enum.map(
        ["recorded", "waiting", "pending", "running", "completed", "failed", "unknown"],
        fn status ->
          %{"id" => "node_#{status}", "status" => status}
        end
      )

    dynamic_edges = [
      %{
        "id" => "edge_selected",
        "from" => "node_recorded",
        "to" => "node_waiting",
        "status" => "selected"
      },
      %{
        "id" => "edge_skipped",
        "from" => "node_recorded",
        "to" => "node_pending",
        "status" => "skipped"
      },
      %{
        "id" => "edge_pending",
        "from" => "node_recorded",
        "to" => "node_running",
        "status" => "pending"
      },
      %{
        "id" => "edge_blocked",
        "from" => "node_recorded",
        "to" => "node_completed",
        "status" => "blocked"
      },
      %{
        "id" => "edge_unknown",
        "from" => "node_recorded",
        "to" => "node_failed",
        "status" => "unknown"
      },
      %{
        "id" => "edge_custom_type",
        "from" => "node_recorded",
        "to" => "node_unknown",
        "type" => "custom"
      }
    ]

    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: "MissingWorkflow",
      queue: "default",
      status: :running,
      reason: :run_started,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 0},
      dynamic_work: [
        %{
          "dynamic_key" => "status_fanout",
          "status" => "preview",
          "nodes" => dynamic_nodes,
          "edges" => dynamic_edges
        },
        %{
          "dynamic_key" => "unknown_status_fanout",
          "status" => "unknown",
          "nodes" => [],
          "edges" => []
        }
      ]
    }

    graph = GraphInspection.from_snapshot(snapshot, source: :read_model)
    nodes_by_id = Map.new(graph.nodes, &{&1.id, &1})
    edges_by_id = Map.new(graph.edges, &{&1.id, &1})

    assert nodes_by_id["node_recorded"].status == :recorded
    assert nodes_by_id["node_waiting"].status == :waiting
    assert nodes_by_id["node_pending"].status == :pending
    assert nodes_by_id["node_running"].status == :running
    assert nodes_by_id["node_completed"].status == :completed
    assert nodes_by_id["node_failed"].status == :failed
    assert nodes_by_id["node_unknown"].status == :recorded

    assert edges_by_id["edge_selected"].status == :selected
    assert edges_by_id["edge_skipped"].status == :skipped
    assert edges_by_id["edge_pending"].status == :pending
    assert edges_by_id["edge_blocked"].status == :blocked
    assert edges_by_id["edge_unknown"].status == :pending
    assert edges_by_id["edge_custom_type"].type == :dynamic

    assert [
             %{status: :preview, node_count: 7, edge_count: 6},
             %{status: :recorded, node_count: 0, edge_count: 0}
           ] = graph.dynamic_work_overlays
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
