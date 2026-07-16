defmodule Squidie.Runtime.DispatchProtocolTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Projection
  alias Squidie.Test.TelemetryCapture

  @run_id "run_123"
  @workflow "BillingWorkflow"
  @runnable_key "run_123:charge_card:1"
  @retry_key "run_123:charge_card:2"
  @idempotency_key "run_123:charge_card:payment_456"
  @claim_id "claim_1"
  @claim_token_hash "token_hash_1"
  @stale_claim_token_hash "token_hash_stale"
  @owner_id "worker_1"
  @started_at ~U[2026-05-14 00:00:00Z]
  @visible_at ~U[2026-05-14 00:00:10Z]
  @claimed_at ~U[2026-05-14 00:00:20Z]
  @lease_until ~U[2026-05-14 00:01:00Z]
  @expired_at ~U[2026-05-14 00:02:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  test "preserves optional attempt trace without making it part of duplicate semantics" do
    assert {:ok, scheduled} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs(trace: @trace))

    other_trace = %{@trace | span_id: "b7ad6b7169203331"}

    assert {:ok, duplicate} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs(trace: other_trace))

    projection = Projection.rebuild([scheduled, duplicate])

    assert %ActionAttempt{trace: @trace} = projection.attempts[@runnable_key]
    assert Projection.anomalies(projection) == []
  end

  test "rebuilds legacy untraced entries without emitting telemetry" do
    TelemetryCapture.attach(Squidie.Telemetry.events())

    assert {:ok, scheduled} = DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())
    projection = Projection.rebuild([scheduled])

    assert %ActionAttempt{trace: nil} = projection.attempts[@runnable_key]
    refute_receive {:telemetry_event, _, _, _}
  end

  test "classifies run, dispatch, run index, and run catalog thread entries" do
    assert {:ok, run_entry} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: @workflow,
               occurred_at: @started_at
             })

    assert {:ok, dispatch_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, index_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: @workflow,
               queue: "default",
               occurred_at: @started_at
             })

    assert {:ok, catalog_entry} =
             DispatchProtocol.new_entry(:run_cataloged, %{
               run_id: @run_id,
               workflow: @workflow,
               queue: "default",
               occurred_at: @started_at
             })

    assert run_entry.thread == {:run, @run_id}
    assert dispatch_entry.thread == {:dispatch, "default"}
    assert index_entry.thread == {:run_index, @workflow}
    assert catalog_entry.thread == {:run_catalog, "all"}
  end

  test "normalizes thread identifiers for workflow modules, atom queues, and catalog facts" do
    assert {:ok, dispatch_entry} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(queue: :squidie)
             )

    assert {:ok, index_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: __MODULE__,
               queue: :squidie,
               occurred_at: @started_at
             })

    assert {:ok, catalog_entry} =
             DispatchProtocol.new_entry(:run_cataloged, %{
               run_id: @run_id,
               workflow: __MODULE__,
               queue: :squidie,
               occurred_at: @started_at
             })

    assert dispatch_entry.thread == {:dispatch, "squidie"}
    assert dispatch_entry.data.queue == "squidie"
    assert index_entry.thread == {:run_index, Atom.to_string(__MODULE__)}
    assert index_entry.data.workflow == Atom.to_string(__MODULE__)
    assert index_entry.data.queue == "squidie"
    assert catalog_entry.thread == {:run_catalog, "all"}
    assert catalog_entry.data.workflow == Atom.to_string(__MODULE__)
    assert catalog_entry.data.queue == "squidie"
  end

  test "classifies child run lineage entries on the parent run thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: "child_run_123",
               child_workflow: __MODULE__,
               child_trigger: :manual,
               child_key: :digest_subscription_1,
               origin: %{
                 runnable_key: @runnable_key,
                 step: :charge_card,
                 attempt: 1
               },
               metadata: %{subscription_id: "sub_123"},
               occurred_at: @started_at
             })

    assert entry.thread == {:run, @run_id}
    assert entry.data.child_workflow == Atom.to_string(__MODULE__)
    assert entry.data.child_trigger == "manual"
    assert entry.data.child_key == "digest_subscription_1"

    assert entry.data.origin == %{
             runnable_key: @runnable_key,
             step: "charge_card",
             attempt: 1
           }

    assert {:ok, legacy_origin_entry} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: "child_run_legacy",
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_legacy",
               origin: "legacy-origin",
               occurred_at: @started_at
             })

    assert legacy_origin_entry.data.origin == "legacy-origin"
  end

  test "normalizes dynamic work records on the run thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:dynamic_work_recorded, %{
               run_id: @run_id,
               dynamic_key: :fanout,
               origin: %{runnable_key: @runnable_key, step: :charge_card, attempt: 1},
               reason: :host_fanout,
               status: :recorded,
               nodes: [
                 %{
                   id: :deliver_digest,
                   action: :deliver,
                   metadata: %{access_token: "secret", channel: "email"}
                 },
                 :stale_node
               ],
               edges: [
                 %{id: :edge_1, from: :charge_card, to: :deliver_digest, type: :dynamic},
                 :stale_edge
               ],
               metadata: %{secret: "internal", source: "test"},
               occurred_at: @started_at
             })

    assert entry.thread == {:run, @run_id}
    assert entry.data.dynamic_key == "fanout"
    assert entry.data.reason == :host_fanout
    assert entry.data.status == :recorded
    assert entry.data.origin.step == "charge_card"

    assert entry.data.nodes == [
             %{
               id: "deliver_digest",
               action: :deliver,
               status: :recorded,
               metadata: %{access_token: "[REDACTED]", channel: "email"}
             },
             :stale_node
           ]

    assert entry.data.edges == [
             %{
               id: "edge_1",
               from: "charge_card",
               to: "deliver_digest",
               type: :dynamic,
               status: :pending
             },
             :stale_edge
           ]

    assert entry.data.metadata == %{secret: "[REDACTED]", source: "test"}

    assert {:ok, legacy_entry} =
             DispatchProtocol.new_entry(:dynamic_work_recorded, %{
               run_id: @run_id,
               dynamic_key: "legacy",
               origin: "legacy-origin",
               nodes: :legacy_nodes,
               edges: :legacy_edges,
               occurred_at: @started_at
             })

    assert legacy_entry.data.nodes == []
    assert legacy_entry.data.edges == []
  end

  test "normalizes dynamic graph mutation facts on the run thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:dynamic_graph_mutated, %{
               run_id: @run_id,
               mutation_id: :mutation_1,
               expected_version: 4,
               result_version: 5,
               origin: :charge_card,
               additions: [
                 %{
                   "kind" => "node",
                   "id" => :deliver_digest,
                   "action" => :deliver,
                   "input" => %{"account_id" => "acct_123"},
                   "queue" => :priority
                 },
                 %{
                   kind: :edge,
                   id: :charge_to_digest,
                   from: :charge_card,
                   to: :deliver_digest
                 },
                 :stale_operation
               ],
               removals: [%{"kind" => "edge", "id" => :old_edge}],
               runnable_intent_fingerprints: %{deliver_digest: :intent_1},
               occurred_at: @started_at
             })

    assert entry.thread == {:run, @run_id}
    assert entry.data.mutation_id == "mutation_1"
    assert entry.data.origin == "charge_card"

    assert entry.data.additions == [
             %{
               kind: :node,
               id: "deliver_digest",
               action: "deliver",
               input: %{"account_id" => "acct_123"},
               queue: "priority"
             },
             %{
               kind: :edge,
               id: "charge_to_digest",
               from: "charge_card",
               to: "deliver_digest"
             },
             :stale_operation
           ]

    assert entry.data.removals == [%{kind: :edge, id: "old_edge"}]
    assert entry.data.runnable_intent_fingerprints == %{"deliver_digest" => "intent_1"}
  end

  test "normalizes manual step lifecycle entries on the run thread" do
    assert {:ok, paused_entry} =
             DispatchProtocol.new_entry(:manual_step_paused, %{
               run_id: @run_id,
               step: :wait_for_review,
               kind: :approval,
               metadata: %{output_key: "approval"},
               occurred_at: @started_at
             })

    assert {:ok, resolved_entry} =
             DispatchProtocol.new_entry(:manual_step_resolved, %{
               run_id: @run_id,
               step: :wait_for_review,
               action: :approved,
               result: %{actor: "ops_123"},
               metadata: %{
                 request_id: "req_123",
                 access_token: "secret",
                 nested: %{password: "secret"}
               },
               occurred_at: @visible_at
             })

    assert paused_entry.thread == {:run, @run_id}
    assert paused_entry.data.step == "wait_for_review"
    assert paused_entry.data.kind == "approval"
    assert paused_entry.data.metadata == %{output_key: "approval"}

    assert resolved_entry.thread == {:run, @run_id}
    assert resolved_entry.data.step == "wait_for_review"
    assert resolved_entry.data.action == "approved"
    assert resolved_entry.data.result == %{actor: "ops_123"}

    assert resolved_entry.data.metadata == %{
             request_id: "req_123",
             access_token: "[REDACTED]",
             nested: %{password: "[REDACTED]"}
           }
  end

  test "normalizes runtime command receipt entries on the run thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(:run_signal_received, %{
               run_id: @run_id,
               signal_type: :approve_run,
               payload: %{run_id: @run_id, attributes: %{actor: "ops_123"}},
               metadata: %{request_id: "req_123", access_token: "secret"},
               idempotency_key: "approve:#{@run_id}",
               occurred_at: @started_at
             })

    assert entry.thread == {:run, @run_id}
    assert entry.data.signal_type == "approve_run"
    assert entry.data.payload == %{run_id: @run_id, attributes: %{actor: "ops_123"}}
    assert entry.data.metadata == %{request_id: "req_123", access_token: "[REDACTED]"}
    assert entry.data.idempotency_key == "approve:#{@run_id}"
  end

  test "rejects required fields with nil values" do
    assert {:error, {:missing_fields, [:visible_at]}} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(visible_at: nil)
             )

    assert {:error, {:missing_fields, [:lease_until]}} =
             DispatchProtocol.new_entry(
               :attempt_claimed,
               claimed_attrs(lease_until: nil)
             )

    assert {:error, {:missing_fields, [:queue]}} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               scheduled_attrs(queue: nil)
             )

    assert {:error, {:missing_fields, [:workflow, :queue]}} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: nil,
               queue: nil,
               occurred_at: @started_at
             })

    assert {:error, {:missing_fields, [:workflow, :queue]}} =
             DispatchProtocol.new_entry(:run_cataloged, %{
               run_id: @run_id,
               workflow: nil,
               queue: nil,
               occurred_at: @started_at
             })

    assert {:error, {:missing_fields, [:kind]}} =
             DispatchProtocol.new_entry(:manual_step_paused, %{
               run_id: @run_id,
               step: :wait_for_review,
               kind: nil,
               occurred_at: @started_at
             })

    assert {:error, {:missing_fields, [:action]}} =
             DispatchProtocol.new_entry(:manual_step_resolved, %{
               run_id: @run_id,
               step: :wait_for_review,
               action: nil,
               occurred_at: @visible_at
             })

    assert {:error, {:missing_fields, [:child_key, :origin]}} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: "child_run_123",
               child_workflow: @workflow,
               child_trigger: "manual",
               occurred_at: @started_at
             })

    assert {:error, {:missing_fields, [:origin]}} =
             DispatchProtocol.new_entry(:child_run_started, %{
               run_id: @run_id,
               child_run_id: "child_run_123",
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: nil,
               occurred_at: @started_at
             })
  end

  test "does not treat a live wakeup as successful before runnable intent exists" do
    projection =
      Projection.rebuild([
        entry!(:live_wakeup_emitted, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          occurred_at: @started_at
        })
      ])

    assert Projection.visible_attempts(projection, @visible_at) == []

    assert [
             %{
               reason: :unknown_runnable_intent,
               runnable_key: @runnable_key,
               entry_type: :live_wakeup_emitted
             }
           ] = Projection.anomalies(projection)
  end

  test "rebuilds visible runnable intent after restart" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs())
      ])

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "duplicate runnable intent is idempotent when the scheduled fields match" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_scheduled, scheduled_attrs())
      ])

    assert [%{runnable_key: @runnable_key}] = Projection.visible_attempts(projection, @visible_at)
    assert Projection.anomalies(projection) == []
  end

  test "conflicting runnable intent for the same key is reported" do
    conflicting_attrs =
      scheduled_attrs()
      |> Map.put(:idempotency_key, "different-idempotency-key")
      |> Map.put(:input, %{"payment_id" => "pay_999"})

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_scheduled, conflicting_attrs)
      ])

    assert [%{runnable_key: @runnable_key, idempotency_key: @idempotency_key}] =
             Projection.visible_attempts(projection, @visible_at)

    assert [
             %{
               reason: :conflicting_runnable_intent,
               runnable_key: @runnable_key,
               idempotency_key: "different-idempotency-key",
               entry_type: :attempt_scheduled
             }
           ] = Projection.anomalies(projection)
  end

  test "current leases hide attempts from redelivery and expired leases are recoverable" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs())
      ])

    assert Projection.visible_attempts(projection, @visible_at) == []
    assert Projection.expired_claims(projection, @visible_at) == []

    assert [%{runnable_key: @runnable_key, claim_id: @claim_id, owner_id: @owner_id}] =
             Projection.expired_claims(projection, @expired_at)
  end

  test "terminal run entries fence remaining visible work and later claims" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:run_terminal, %{
          run_id: @run_id,
          status: :cancelled,
          occurred_at: @expired_at
        }),
        entry!(:attempt_claimed, claimed_attrs(occurred_at: @expired_at))
      ])

    assert Projection.visible_attempts(projection, @expired_at) == []
    assert Projection.expired_claims(projection, @expired_at) == []

    assert [
             %{
               reason: :terminal_run,
               runnable_key: @runnable_key,
               run_id: @run_id,
               entry_type: :attempt_claimed
             }
           ] = Projection.anomalies(projection)
  end

  test "claims cannot be accepted before the attempt is visible" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs(occurred_at: @started_at))
      ])

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)

    assert [
             %{
               reason: :attempt_not_visible,
               runnable_key: @runnable_key,
               claim_id: @claim_id,
               entry_type: :attempt_claimed
             }
           ] = Projection.anomalies(projection)
  end

  test "claim takeover is allowed only after the current lease expires" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(
          :attempt_claimed,
          claimed_attrs(
            claim_id: "claim_2",
            claim_token_hash: "token_hash_2",
            owner_id: "worker_2",
            lease_until: ~U[2026-05-14 00:03:00Z]
          )
        ),
        entry!(
          :attempt_claimed,
          claimed_attrs(
            claim_id: "claim_3",
            claim_token_hash: "token_hash_3",
            owner_id: "worker_3",
            lease_until: ~U[2026-05-14 00:04:00Z],
            occurred_at: @expired_at
          )
        )
      ])

    assert [%{claim_id: "claim_3", claim_token_hash: "token_hash_3", owner_id: "worker_3"}] =
             Projection.expired_claims(projection, ~U[2026-05-14 00:05:00Z])

    assert [
             %{
               reason: :active_claim,
               runnable_key: @runnable_key,
               claim_id: "claim_2",
               claim_token_hash: "token_hash_2",
               entry_type: :attempt_claimed
             }
           ] = Projection.anomalies(projection)
  end

  test "heartbeats extend only the current claim lease" do
    extended_lease = ~U[2026-05-14 00:05:00Z]

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_heartbeat, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @stale_claim_token_hash,
          lease_until: ~U[2026-05-14 00:10:00Z],
          occurred_at: @started_at
        }),
        entry!(:attempt_heartbeat, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          lease_until: extended_lease,
          occurred_at: @started_at
        })
      ])

    assert Projection.expired_claims(projection, @expired_at) == []

    assert [
             %{
               reason: :stale_claim,
               runnable_key: @runnable_key,
               claim_id: @claim_id,
               claim_token_hash: @stale_claim_token_hash,
               entry_type: :attempt_heartbeat
             }
           ] = Projection.anomalies(projection)
  end

  test "expired claim owners cannot heartbeat complete fail or schedule retries" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_heartbeat, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          lease_until: ~U[2026-05-14 00:10:00Z],
          occurred_at: @expired_at
        }),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: %{"ok" => true},
          occurred_at: @expired_at
        }),
        entry!(:attempt_failed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          error: %{"reason" => "gateway_timeout"},
          retry_runnable_key: @retry_key,
          retry_visible_at: @expired_at,
          occurred_at: @expired_at
        })
      ])

    assert Projection.completed_results(projection) == []
    assert Projection.visible_attempts(projection, @expired_at) == []

    assert [
             %{reason: :expired_claim, entry_type: :attempt_heartbeat},
             %{reason: :expired_claim, entry_type: :attempt_completed},
             %{reason: :expired_claim, entry_type: :attempt_failed}
           ] = Projection.anomalies(projection)
  end

  test "duplicate completions are idempotent for the same claim and result" do
    completed = %{
      "status" => "charged",
      "payment_id" => "pay_123"
    }

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: completed,
          occurred_at: @started_at
        }),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: completed,
          occurred_at: @started_at
        })
      ])

    assert [%{runnable_key: @runnable_key, result: ^completed}] =
             Projection.completed_results(projection)

    assert Projection.anomalies(projection) == []
  end

  test "completion appended without a live wakeup remains recoverable after restart" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: %{"ok" => true},
          occurred_at: @started_at
        })
      ])

    assert [%{runnable_key: @runnable_key}] = Projection.results_ready_to_apply(projection)
  end

  test "applied completed results are removed from the apply projection" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: %{"ok" => true},
          occurred_at: @started_at
        }),
        entry!(:runnable_applied, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          occurred_at: @expired_at
        })
      ])

    assert Projection.results_ready_to_apply(projection) == []

    assert [%{runnable_key: @runnable_key, applied?: true}] =
             Projection.completed_results(projection)
  end

  test "runnable apply entries cannot precede completion" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:runnable_applied, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          occurred_at: @started_at
        })
      ])

    assert Projection.results_ready_to_apply(projection) == []

    assert [
             %{
               reason: :result_not_completed,
               runnable_key: @runnable_key,
               entry_type: :runnable_applied
             }
           ] = Projection.anomalies(projection)
  end

  test "stale completion is fenced out by claim token hash" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @stale_claim_token_hash,
          result: %{"ok" => true},
          occurred_at: @started_at
        })
      ])

    assert Projection.completed_results(projection) == []

    assert [
             %{
               reason: :stale_claim,
               runnable_key: @runnable_key,
               claim_id: @claim_id,
               claim_token_hash: @stale_claim_token_hash,
               entry_type: :attempt_completed
             }
           ] = Projection.anomalies(projection)
  end

  test "claim entries cannot reopen completed attempts" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_completed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          result: %{"ok" => true},
          occurred_at: @started_at
        }),
        entry!(:attempt_claimed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: "claim_2",
          claim_token_hash: "token_hash_2",
          owner_id: "worker_2",
          lease_until: @expired_at,
          occurred_at: @started_at
        })
      ])

    assert [%{runnable_key: @runnable_key, status: :completed}] =
             Projection.completed_results(projection)

    assert Projection.expired_claims(projection, @expired_at) == []

    assert [
             %{
               reason: :terminal_attempt,
               runnable_key: @runnable_key,
               claim_id: "claim_2",
               entry_type: :attempt_claimed
             }
           ] = Projection.anomalies(projection)
  end

  test "retry scheduling survives projection rebuild" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_failed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @claim_token_hash,
          error: %{"reason" => "gateway_timeout"},
          retry_runnable_key: @retry_key,
          retry_visible_at: @expired_at,
          occurred_at: @started_at
        })
      ])

    assert Projection.visible_attempts(projection, @visible_at) == []

    assert [%{runnable_key: @retry_key, status: :retry_scheduled, visible_at: @expired_at}] =
             Projection.visible_attempts(projection, @expired_at)
  end

  test "stale failures cannot schedule retry attempts" do
    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_failed, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          claim_id: @claim_id,
          claim_token_hash: @stale_claim_token_hash,
          error: %{"reason" => "gateway_timeout"},
          retry_runnable_key: @retry_key,
          retry_visible_at: @expired_at,
          occurred_at: @started_at
        })
      ])

    assert Projection.visible_attempts(projection, @expired_at) == []

    assert [
             %{
               reason: :stale_claim,
               runnable_key: @runnable_key,
               claim_id: @claim_id,
               claim_token_hash: @stale_claim_token_hash,
               entry_type: :attempt_failed
             }
           ] = Projection.anomalies(projection)
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        idempotency_key: @idempotency_key,
        attempt_number: 1,
        step: "charge_card",
        input: %{"payment_id" => "pay_123"},
        visible_at: @visible_at,
        occurred_at: @started_at
      },
      Map.new(attrs)
    )
  end

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        owner_id: @owner_id,
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
