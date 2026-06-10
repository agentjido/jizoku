defmodule Squidie.ReadModel.VisibilityTest do
  use ExUnit.Case, async: true

  alias Squidie.ReadModel.Explanation.Diagnostic
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.ReadModel.Listing.Summary
  alias Squidie.ReadModel.Timeline
  alias Squidie.ReadModel.Timeline.Event
  alias Squidie.ReadModel.Visibility
  alias Squidie.Runs.GraphInspection
  alias Squidie.Runs.GraphInspection.Node

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
    assert redacted.terminal_error == nil

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

  test "redacts timeline event details for external actors" do
    timeline = %Timeline{
      run_id: "run_123",
      workflow: "BillingWorkflow",
      queue: "default",
      status: :running,
      terminal?: false,
      terminal_status: nil,
      events: [
        %Event{
          type: :attempt_completed,
          occurred_at: @now,
          run_id: "run_123",
          step_id: "charge_card",
          runnable_key: "run_123:charge_card:1",
          status: :completed,
          summary: "charge_card attempt completed",
          details: %{
            attempt_number: 1,
            payment_id: "pay_secret_123",
            nested: %{token: "super-secret-token"}
          }
        }
      ]
    }

    assert {:ok, redacted} = Visibility.redact(timeline, %{role: :external}, RolePolicy)

    assert %Timeline{} = redacted

    assert [
             %Event{
               type: :attempt_completed,
               occurred_at: @now,
               run_id: "run_123",
               step_id: "charge_card",
               runnable_key: "run_123:charge_card:1",
               status: :completed,
               summary: "charge_card attempt completed",
               details: %{attempt_number: 1}
             }
           ] = redacted.events

    refute inspect(redacted.events) =~ "pay_secret_123"
    refute inspect(redacted.events) =~ "super-secret-token"
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

    assert redacted.child_links == [
             %{
               id: "review_payment:child_run:child_123",
               from: "review_payment",
               to: "child_123",
               type: :child_run,
               status: :linked,
               child_run_id: "child_123",
               child_workflow: "ChildWorkflow",
               child_trigger: "manual",
               child_key: "review_child",
               origin: %{
                 runnable_key: "run_123:review_payment:1",
                 step: "review_payment",
                 attempt: 1
               }
             }
           ]

    assert [
             %{
               dynamic_key: "fanout",
               nodes: [%{id: "deliver_digest:chat_1"}],
               edges: [%{type: :dynamic}]
             }
           ] = redacted.dynamic_work

    assert [
             %{
               dynamic_key: "fanout",
               origin_node_id: "charge_card",
               added_node_ids: ["deliver_digest:chat_1"],
               added_edge_ids: ["charge_card:dynamic:deliver_digest:chat_1"]
             } = overlay,
             %{}
           ] = redacted.dynamic_work_overlays

    refute Map.has_key?(overlay, :metadata)
    refute Map.has_key?(overlay.origin, :tenant_id)
    refute Map.has_key?(overlay.origin, :secret)
  end

  test "redacts JSON-ready graph maps by default" do
    mixed_key_child_link = %{
      "child_run_id" => "child_mixed",
      "origin" => %{
        "runnable_key" => "run_123:mixed:1",
        "step" => "mixed",
        "tenant_id" => "tenant_123"
      }
    }

    graph_map =
      graph()
      |> GraphInspection.to_map()
      |> Map.put("child_links", [mixed_key_child_link])
      |> Map.put("dynamic_work_overlays", [
        %{
          "dynamic_key" => "mixed_fanout",
          "origin" => %{
            "runnable_key" => "run_123:mixed:1",
            "step" => "mixed",
            "tenant_id" => "tenant_123"
          },
          "origin_node_id" => "mixed",
          "added_node_ids" => ["deliver_digest:chat_mixed"],
          "metadata" => %{"secret" => "internal"}
        }
      ])

    assert {:ok, redacted} = Visibility.redact(graph_map, %{role: :external})

    assert [%{id: "review_payment", status: :paused} = node] = redacted.nodes
    refute Map.has_key?(node, :input)
    refute Map.has_key?(node, :output)
    refute Map.has_key?(node, :error)
    refute Map.has_key?(node, :attempts)

    assert [%{run_id: "child_123", workflow: "ChildWorkflow", status: :running} = child] =
             redacted.child_runs

    refute Map.has_key?(child, :input)
    refute Map.has_key?(child, :started_at)

    assert [%{child_run_id: "child_123"} = child_link] = redacted.child_links
    refute Map.has_key?(child_link, :metadata)
    refute Map.has_key?(child_link, :started_at)
    refute Map.has_key?(child_link.origin, :tenant_id)
    refute Map.has_key?(child_link.origin, :secret)

    assert [%{child_run_id: "child_mixed"} = mixed_key_link] = redacted["child_links"]
    refute Map.has_key?(mixed_key_link.origin, :tenant_id)

    assert [%{origin_node_id: "charge_card"} = overlay, %{}] = redacted.dynamic_work_overlays
    refute Map.has_key?(overlay, :metadata)

    assert [%{origin_node_id: "mixed"} = mixed_overlay] = redacted["dynamic_work_overlays"]
    refute Map.has_key?(mixed_overlay, :metadata)
    refute Map.has_key?(mixed_overlay.origin, :tenant_id)
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
          terminal_error: %{code: "missing_input_path", path: ["secret"]},
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
      deadline: nil,
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
      terminal_error: %{code: "missing_input_path", path: ["draft", "drafts"]},
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
          input: %{secret: "child-secret"},
          started_at: @now
        }
      ],
      child_links: [
        %{
          id: "review_payment:child_run:child_123",
          from: "review_payment",
          to: "child_123",
          type: :child_run,
          status: :linked,
          child_run_id: "child_123",
          child_workflow: "ChildWorkflow",
          child_trigger: "manual",
          child_key: "review_child",
          origin: %{
            runnable_key: "run_123:review_payment:1",
            step: "review_payment",
            attempt: 1,
            tenant_id: "tenant_123",
            secret: "internal"
          },
          metadata: %{secret: "internal"},
          started_at: @now
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
      dynamic_work_overlays: [
        %{
          dynamic_key: "fanout",
          status: :recorded,
          reason: :runtime_fanout,
          origin: %{
            runnable_key: "run_123:charge_card:1",
            step: "charge_card",
            attempt: 1,
            tenant_id: "tenant_123",
            secret: "internal"
          },
          origin_node_id: "charge_card",
          added_node_ids: ["deliver_digest:chat_1"],
          added_edge_ids: ["charge_card:dynamic:deliver_digest:chat_1"],
          node_count: 1,
          edge_count: 1,
          metadata: %{secret: "internal"},
          recorded_at: @now
        },
        :legacy_overlay
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
