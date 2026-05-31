defmodule SquidMesh.ReadModel.VisibilityTest do
  use ExUnit.Case, async: true

  alias SquidMesh.ReadModel.Explanation.Diagnostic
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.ReadModel.Listing.Summary
  alias SquidMesh.ReadModel.Visibility
  alias SquidMesh.Runs.GraphInspection
  alias SquidMesh.Runs.GraphInspection.Node

  defmodule RolePolicy do
    @spec visibility_scope(%{role: atom()}, term()) :: atom()
    def visibility_scope(%{role: role}, _view), do: role
  end

  defmodule OkScopePolicy do
    @spec visibility_scope(%{role: atom()}, term()) :: {:ok, atom()}
    def visibility_scope(%{role: role}, _view), do: {:ok, role}
  end

  defmodule OptsPolicy do
    @spec visibility_scope(term(), term(), keyword()) :: {:ok, atom()}
    def visibility_scope(_actor, _view, opts), do: {:ok, Keyword.fetch!(opts, :scope)}
  end

  defmodule NoCallbackPolicy do
  end

  @now ~U[2026-05-30 12:00:00Z]

  test "redacts sensitive snapshot fields for an external actor" do
    snapshot = snapshot()

    assert {:ok, redacted} = Visibility.redact(snapshot, %{role: :external}, RolePolicy)

    assert %Snapshot{} = redacted
    assert redacted.run_id == snapshot.run_id
    assert redacted.status == snapshot.status
    assert redacted.input == nil
    assert redacted.context == %{}
    assert redacted.command_history == []

    assert redacted.manual_state == %{
             step: "review_payment",
             kind: "approval",
             paused_at: @now
           }

    assert redacted.visible_attempts == [
             %{
               runnable_key: "run_123:charge_card:1",
               status: :available,
               attempt_number: 1,
               step: "charge_card",
               visible_at: @now,
               wakeup_emitted?: false,
               applied?: false
             }
           ]

    refute Map.has_key?(hd(redacted.visible_attempts), :input)
    refute Map.has_key?(hd(redacted.visible_attempts), :result)
    refute Map.has_key?(hd(redacted.visible_attempts), :error)
    refute Map.has_key?(hd(redacted.visible_attempts), :idempotency_key)
    refute Map.has_key?(hd(redacted.visible_attempts), :claim_id)
    refute Map.has_key?(hd(redacted.visible_attempts), :owner_id)

    assert [
             %{
               dynamic_key: "fanout",
               status: :recorded,
               reason: :runtime_fanout,
               origin: %{runnable_key: "run_123:charge_card:1", step: "charge_card", attempt: 1},
               nodes: [
                 %{id: "deliver_digest:chat_1", action: "digest.deliver", status: :recorded}
               ],
               edges: [
                 %{
                   id: "charge_card:dynamic:deliver_digest:chat_1",
                   from: "charge_card",
                   to: "deliver_digest:chat_1",
                   type: :dynamic,
                   status: :pending
                 }
               ]
             }
           ] = redacted.dynamic_work

    refute Map.has_key?(hd(redacted.dynamic_work), :metadata)
    refute Map.has_key?(hd(hd(redacted.dynamic_work).nodes), :metadata)
  end

  test "defaults to external visibility when no policy is supplied" do
    assert {:ok, redacted} = Visibility.redact(snapshot(), %{role: :auditor})

    assert redacted.input == nil
    assert redacted.command_history == []
  end

  test "preserves snapshot fields for an auditor actor" do
    snapshot = snapshot()

    assert {:ok, ^snapshot} = Visibility.redact(snapshot, %{role: :auditor}, RolePolicy)
  end

  test "accepts policy modules that return ok tuples" do
    assert {:ok, redacted} = Visibility.redact(snapshot(), %{role: :external}, OkScopePolicy)

    assert redacted.input == nil
    assert redacted.command_history == []
  end

  test "honors module opts policies" do
    snapshot = snapshot()

    assert {:ok, ^snapshot} =
             Visibility.redact(snapshot, %{role: :external}, {OptsPolicy, scope: :auditor})
  end

  test "returns structured errors for policy modules without callbacks" do
    assert {:error, {:invalid_visibility_policy, :missing_callback}} =
             Visibility.redact(snapshot(), %{role: :external}, NoCallbackPolicy)

    assert {:error, {:invalid_visibility_policy, :missing_callback}} =
             Visibility.redact(
               snapshot(),
               %{role: :external},
               {NoCallbackPolicy, scope: :auditor}
             )
  end

  test "redacts with direct operator scope policies" do
    assert {:ok, redacted} = Visibility.redact(snapshot(), %{role: :auditor}, :operator)

    assert redacted.input == nil
    assert redacted.command_history == []
  end

  test "redacts graph details while preserving node status and current state" do
    graph = graph()

    assert {:ok, redacted} = Visibility.redact(graph, %{role: :external}, RolePolicy)

    assert %GraphInspection{} = redacted
    assert redacted.current_node_ids == ["review_payment"]

    assert [
             %Node{
               id: "review_payment",
               status: :paused,
               current?: true,
               input: nil,
               output: nil,
               error: nil,
               attempts: [],
               origin: %{runnable_key: "run_123:charge_card:1", step: "charge_card", attempt: 1},
               metadata: %{},
               manual_state: %{step: "review_payment", kind: "approval", paused_at: @now}
             }
           ] = redacted.nodes

    assert redacted.child_runs == [
             %{run_id: "child_123", workflow: "ChildWorkflow", status: :running}
           ]

    assert [
             %{
               dynamic_key: "fanout",
               nodes: [%{id: "deliver_digest:chat_1"}],
               edges: [%{type: :dynamic}]
             }
           ] = redacted.dynamic_work
  end

  test "redacts JSON-ready graph maps by default" do
    graph_map = GraphInspection.to_map(graph())

    assert {:ok, redacted} = Visibility.redact(graph_map, %{role: :external})

    assert [%{id: "review_payment", status: :paused} = node] = redacted.nodes
    refute Map.has_key?(node, :input)
    refute Map.has_key?(node, :output)
    refute Map.has_key?(node, :error)
    refute Map.has_key?(node, :attempts)

    assert [%{run_id: "child_123", workflow: "ChildWorkflow", status: :running} = child] =
             redacted.child_runs

    refute Map.has_key?(child, :input)
  end

  test "redacts stale malformed dynamic work shapes" do
    snapshot = %Snapshot{
      snapshot()
      | dynamic_work: [
          %{
            dynamic_key: "legacy_fanout",
            origin: "legacy-origin",
            nodes: [:legacy_node],
            edges: [:legacy_edge],
            metadata: %{secret: "internal"}
          },
          %{dynamic_key: "legacy_empty", nodes: :legacy_nodes, edges: :legacy_edges},
          :legacy_dynamic_work
        ]
    }

    assert {:ok, redacted} = Visibility.redact(snapshot, %{role: :external})

    assert redacted.dynamic_work == [
             %{dynamic_key: "legacy_fanout", nodes: [%{}], edges: [%{}]},
             %{dynamic_key: "legacy_empty", nodes: [], edges: []},
             %{}
           ]
  end

  test "redacts sensitive binary-key fields in JSON-ready maps" do
    view = %{
      "run_id" => "run_123",
      "nodes" => [
        %{
          "id" => "review_payment",
          "status" => "paused",
          "input" => %{"payment_source" => "test-payment-source"},
          "metadata" => %{"secret" => "internal"}
        }
      ],
      "command_history" => [%{"payload" => %{"secret" => "internal"}}]
    }

    assert {:ok, redacted} = Visibility.redact(view, %{role: :external})

    assert %{
             "nodes" => [%{"id" => "review_payment", "status" => "paused"}],
             "run_id" => "run_123"
           } =
             redacted
  end

  test "redacts explanation evidence and manual details for an external actor" do
    diagnostic = diagnostic()

    assert {:ok, redacted} = Visibility.redact(diagnostic, %{role: :external}, RolePolicy)

    assert %Diagnostic{} = redacted
    assert redacted.summary == diagnostic.summary
    assert redacted.next_actions == diagnostic.next_actions

    assert redacted.details == %{
             step: "review_payment",
             kind: "approval",
             paused_at: @now
           }

    assert redacted.evidence == %{
             snapshot_reason: :manual_intervention_required,
             definition_version: "v1",
             terminal_status: nil,
             planned_count: 1,
             applied_count: 0,
             anomaly_count: 0
           }
  end

  test "redacts non-manual explanation details recursively" do
    diagnostic = %Diagnostic{
      diagnostic()
      | details: %{
          safe_count: 1,
          nested: %{visible: true, secret: "internal"},
          attempts: [%{status: :available, error: %{message: "internal"}}]
        },
        evidence: %{
          snapshot_reason: :attempt_visible,
          definition_version: "v1",
          terminal_status: nil,
          planned_count: 2,
          applied_count: 1,
          anomaly_count: 0
        }
    }

    assert {:ok, redacted} = Visibility.redact(diagnostic, %{role: :external})

    assert redacted.details == %{safe_count: 1, nested: %{visible: true}}

    assert redacted.evidence == %{
             snapshot_reason: :attempt_visible,
             definition_version: "v1",
             terminal_status: nil,
             planned_count: 2,
             applied_count: 1,
             anomaly_count: 0
           }
  end

  test "redacts struct elements inside list views" do
    assert {:ok, [%Snapshot{} = redacted_snapshot, %Diagnostic{} = redacted_diagnostic]} =
             Visibility.redact([snapshot(), diagnostic()], %{role: :external}, RolePolicy)

    assert redacted_snapshot.input == nil
    assert redacted_snapshot.command_history == []

    assert redacted_diagnostic.details == %{
             step: "review_payment",
             kind: "approval",
             paused_at: @now
           }

    refute Map.has_key?(redacted_diagnostic.evidence, :manual_state)
    refute Map.has_key?(redacted_diagnostic.evidence, :command_history)
  end

  test "leaves listing summaries unchanged because they are already minimal" do
    summary = %Summary{
      run_id: "run_123",
      workflow: "PaymentWorkflow",
      definition_version: "v1",
      queue: "default",
      status: :running,
      terminal?: false,
      terminal_status: nil,
      indexed_at: @now,
      thread_revision: 4,
      anomalies: [%{source: :workflow, reason: :duplicate_command}]
    }

    assert {:ok, ^summary} = Visibility.redact(summary, %{role: :external}, RolePolicy)
  end

  test "returns a structured error for unsupported policy results" do
    policy = fn _actor, _view -> :superuser end

    assert {:error, {:invalid_visibility_policy, {:scope, :superuser}}} =
             Visibility.redact(snapshot(), %{role: :external}, policy)
  end

  test "returns a structured error for unsupported policy forms" do
    assert {:error, {:invalid_visibility_policy, {:policy, "external"}}} =
             Visibility.redact(snapshot(), %{role: :external}, "external")
  end

  defp snapshot do
    %Snapshot{
      run_id: "run_123",
      workflow: "PaymentWorkflow",
      trigger: "manual",
      input: %{payment_source: "test-payment-source"},
      definition_version: "v1",
      context: %{payment: %{token: "tok_123"}},
      parent_run: %{run_id: "parent_123", input: %{secret: "parent-secret"}},
      child_runs: [%{run_id: "child_123", workflow: "ChildWorkflow", status: :running}],
      dynamic_work: [
        %{
          dynamic_key: "fanout",
          status: :recorded,
          reason: :runtime_fanout,
          origin: %{runnable_key: "run_123:charge_card:1", step: "charge_card", attempt: 1},
          nodes: [
            %{
              id: "deliver_digest:chat_1",
              action: "digest.deliver",
              status: :recorded,
              metadata: %{secret: "internal"}
            }
          ],
          edges: [
            %{
              id: "charge_card:dynamic:deliver_digest:chat_1",
              from: "charge_card",
              to: "deliver_digest:chat_1",
              type: :dynamic,
              status: :pending
            }
          ],
          metadata: %{secret: "internal"},
          recorded_at: @now
        }
      ],
      queue: "default",
      status: :paused,
      reason: :manual_intervention_required,
      terminal?: false,
      terminal_status: nil,
      manual_state: %{
        step: "review_payment",
        kind: "approval",
        paused_at: @now,
        metadata: %{output_key: "approval", secret: "internal"}
      },
      command_history: [
        %{
          signal_type: "start_run",
          payload: %{payment_source: "test-payment-source"},
          actor: "operator_123",
          idempotency_key: "start:run_123"
        }
      ],
      thread_revisions: %{run: 2, dispatch: 3},
      planned_runnables: [
        %{
          runnable_key: "run_123:charge_card:1",
          step: "charge_card",
          input: %{payment_source: "test-payment-source"}
        }
      ],
      planned_runnable_keys: ["run_123:charge_card:1"],
      applied_runnable_keys: [],
      pending_dispatches: [
        %{runnable_key: "run_123:charge_card:1", step: "charge_card", input: %{secret: true}}
      ],
      visible_attempts: [attempt(:available)],
      scheduled_attempts: [attempt(:retry_scheduled)],
      expired_claims: [attempt(:claimed)],
      attempts: [attempt(:available)],
      anomalies: [%{source: :workflow, reason: :duplicate_command, payload: %{secret: true}}]
    }
  end

  defp graph do
    %GraphInspection{
      run_id: "run_123",
      workflow: "PaymentWorkflow",
      definition_version: "v1",
      source: :read_model,
      status: :paused,
      current_node_id: "review_payment",
      current_node_ids: ["review_payment"],
      terminal?: false,
      nodes: [
        %Node{
          id: "review_payment",
          action: "review",
          status: :paused,
          current?: true,
          input: %{payment_source: "test-payment-source"},
          output: %{approval: %{secret: "yes"}},
          error: %{message: "internal"},
          manual_state: %{
            step: "review_payment",
            kind: "approval",
            paused_at: @now,
            metadata: %{secret: "yes"}
          },
          dynamic?: true,
          origin: %{
            runnable_key: "run_123:charge_card:1",
            step: "charge_card",
            attempt: 1,
            secret: "internal"
          },
          metadata: %{secret: "internal"},
          attempts: [%{attempt_number: 1, status: :available, error: %{message: "internal"}}]
        }
      ],
      child_runs: [
        %{
          run_id: "child_123",
          workflow: "ChildWorkflow",
          status: :running,
          input: %{secret: "child-secret"}
        }
      ],
      dynamic_work: [
        %{
          dynamic_key: "fanout",
          status: :recorded,
          reason: :runtime_fanout,
          origin: %{runnable_key: "run_123:charge_card:1", step: "charge_card", attempt: 1},
          nodes: [
            %{
              id: "deliver_digest:chat_1",
              action: "digest.deliver",
              status: :recorded,
              metadata: %{secret: "internal"}
            }
          ],
          edges: [
            %{
              id: "charge_card:dynamic:deliver_digest:chat_1",
              from: "charge_card",
              to: "deliver_digest:chat_1",
              type: :dynamic,
              status: :pending
            }
          ],
          metadata: %{secret: "internal"},
          recorded_at: @now
        }
      ],
      anomalies: [%{source: :workflow, reason: :duplicate_command, payload: %{secret: true}}]
    }
  end

  defp diagnostic do
    %Diagnostic{
      run_id: "run_123",
      workflow: "PaymentWorkflow",
      definition_version: "v1",
      queue: "default",
      status: :paused,
      reason: :manual_intervention_required,
      step: "review_payment",
      summary: "The run is paused for manual intervention.",
      details: %{
        step: "review_payment",
        kind: "approval",
        paused_at: @now,
        metadata: %{output_key: "approval", secret: "internal"}
      },
      next_actions: [:resolve_manual_step],
      evidence: %{
        snapshot_reason: :manual_intervention_required,
        definition_version: "v1",
        terminal_status: nil,
        manual_state: %{metadata: %{secret: "internal"}},
        command_history: [%{payload: %{payment_source: "test-payment-source"}}],
        planned_runnable_keys: ["run_123:charge_card:1"],
        applied_runnable_keys: [],
        anomaly_count: 0,
        anomalies: []
      }
    }
  end

  defp attempt(status) do
    %{
      runnable_key: "run_123:charge_card:1",
      status: status,
      attempt_number: 1,
      step: "charge_card",
      input: %{payment_source: "test-payment-source"},
      visible_at: @now,
      idempotency_key: "run_123:charge_card:1",
      claim_id: "claim_123",
      owner_id: "worker_123",
      lease_until: @now,
      result: %{auth_code: "secret"},
      error: %{message: "internal"},
      wakeup_emitted?: false,
      applied?: false
    }
  end
end
