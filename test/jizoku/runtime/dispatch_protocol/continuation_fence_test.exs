defmodule Jizoku.Runtime.DispatchProtocol.ContinuationFenceTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.DispatchProtocol.Projection

  @run_id "run_123"
  @successor_run_id "run_456"
  @runnable_key "run_123:monitor:1"
  @claim_id "claim_1"
  @claim_token_hash "claim-token-hash"
  @started_at ~U[2026-08-09 15:00:00Z]
  @visible_at ~U[2026-08-09 15:00:10Z]
  @claimed_at ~U[2026-08-09 15:00:20Z]
  @lease_until ~U[2026-08-09 15:01:00Z]
  @expired_at ~U[2026-08-09 15:02:00Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7"
  }

  test "normalizes a complete continuation fence on the dispatch thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(
               :run_continuation_fenced,
               continuation_fence_attrs(
                 continuation_key: :page_42,
                 workflow: __MODULE__,
                 trigger: :continue,
                 queue: :monitoring
               )
             )

    assert entry.thread == {:dispatch, "monitoring"}
    assert entry.data.continuation_key == "page_42"
    assert entry.data.workflow == Atom.to_string(__MODULE__)
    assert entry.data.trigger == "continue"
    assert entry.data.queue == "monitoring"
    assert Map.has_key?(entry.data, :definition_version)
    assert entry.data.definition_version == nil

    assert {:error, {:missing_fields, [:definition_fingerprint]}} =
             DispatchProtocol.new_entry(
               :run_continuation_fenced,
               Map.delete(continuation_fence_attrs(), :definition_fingerprint)
             )

    assert {:error, {:missing_fields, [:trace]}} =
             DispatchProtocol.new_entry(
               :run_continuation_fenced,
               Map.delete(continuation_fence_attrs(), :trace)
             )
  end

  test "normalizes a complete continuation repair on the dispatch thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(
               :run_continuation_repaired,
               continuation_repair_attrs(
                 continuation_key: :page_42,
                 workflow: __MODULE__,
                 trigger: :continue,
                 queue: :monitoring
               )
             )

    assert entry.thread == {:dispatch, "monitoring"}
    assert entry.data.continuation_key == "page_42"
    assert entry.data.workflow == Atom.to_string(__MODULE__)
    assert entry.data.trigger == "continue"
    assert entry.data.queue == "monitoring"
    assert Map.has_key?(entry.data, :definition_version)

    assert {:error, {:missing_fields, [:definition_fingerprint]}} =
             DispatchProtocol.new_entry(
               :run_continuation_repaired,
               Map.delete(continuation_repair_attrs(), :definition_fingerprint)
             )

    assert {:error, {:missing_fields, [:trace]}} =
             DispatchProtocol.new_entry(
               :run_continuation_repaired,
               Map.delete(continuation_repair_attrs(), :trace)
             )
  end

  test "normalizes a complete continuation abort on the dispatch thread" do
    assert {:ok, entry} =
             DispatchProtocol.new_entry(
               :run_continuation_aborted,
               continuation_abort_attrs(
                 continuation_key: :page_42,
                 workflow: __MODULE__,
                 trigger: :continue,
                 queue: :monitoring
               )
             )

    assert entry.thread == {:dispatch, "monitoring"}
    assert entry.data.continuation_key == "page_42"
    assert entry.data.workflow == Atom.to_string(__MODULE__)
    assert entry.data.trigger == "continue"
    assert entry.data.queue == "monitoring"
    assert entry.data.abort_reason == :predecessor_changed
    assert Map.has_key?(entry.data, :definition_version)

    assert {:error, {:missing_fields, [:abort_reason]}} =
             DispatchProtocol.new_entry(
               :run_continuation_aborted,
               Map.delete(continuation_abort_attrs(), :abort_reason)
             )

    assert {:error, {:missing_fields, [:trace]}} =
             DispatchProtocol.new_entry(
               :run_continuation_aborted,
               Map.delete(continuation_abort_attrs(), :trace)
             )
  end

  test "rebuilds exact duplicate fences idempotently and retains the first conflict" do
    first = entry!(:run_continuation_fenced, continuation_fence_attrs())

    duplicate =
      entry!(
        :run_continuation_fenced,
        continuation_fence_attrs(
          trace: %{@trace | span_id: "b7ad6b7169203331"},
          occurred_at: @visible_at
        )
      )

    conflicting =
      entry!(
        :run_continuation_fenced,
        continuation_fence_attrs(
          successor_run_id: "run_789",
          input: %{cursor: "page-99"},
          occurred_at: @claimed_at
        )
      )

    duplicate_projection = Projection.rebuild([first, duplicate])

    assert Projection.continuation_fence(duplicate_projection, @run_id) == first.data
    assert Projection.run_ids(duplicate_projection) == MapSet.new([@run_id])
    assert Projection.anomalies(duplicate_projection) == []

    conflicting_projection = Projection.replay(duplicate_projection, [conflicting])

    assert Projection.continuation_fence(conflicting_projection, @run_id) == first.data

    assert [
             %{
               reason: :conflicting_continuation_fence,
               entry_type: :run_continuation_fenced,
               run_id: @run_id
             }
           ] = Projection.anomalies(conflicting_projection)
  end

  test "classifies one-field continuation identity conflicts" do
    conflicts = [
      successor_run_id: "run_789",
      continuation_key: "page-99",
      workflow: "OtherWorkflow",
      trigger: "resume",
      input: %{cursor: "page-99"},
      request_input: %{cursor: "page-99"},
      source_runnable_key: "run_123:other:1",
      definition_version: "v2",
      definition_fingerprint: "definition-fingerprint-v2",
      queue: "priority"
    ]

    for {field, value} <- conflicts do
      projection =
        Projection.rebuild([
          entry!(:run_continuation_fenced, continuation_fence_attrs()),
          entry!(
            :run_continuation_fenced,
            continuation_fence_attrs([{field, value}, {:occurred_at, @visible_at}])
          )
        ])

      assert [%{reason: :conflicting_continuation_fence}] =
               Projection.anomalies(projection),
             "expected #{field} to participate in continuation identity"
    end
  end

  test "validates optional caller-declared continuation input" do
    assert Projection.valid_continuation_fence?(
             continuation_fence_attrs(request_input: %{cursor: "page-42"})
           )

    refute Projection.valid_continuation_fence?(
             continuation_fence_attrs(request_input: "page-42")
           )
  end

  test "validates optional native source while legacy fences remain valid" do
    assert Projection.valid_continuation_fence?(continuation_fence_attrs())

    assert Projection.valid_continuation_fence?(
             continuation_fence_attrs(source_runnable_key: @runnable_key)
           )
  end

  test "caller-declared input participates in continuation fence identity" do
    first = continuation_fence_attrs(request_input: %{cursor: "page-42"})
    conflicting = continuation_fence_attrs(request_input: %{cursor: "page-43"})

    refute Projection.same_continuation_fence?(first, conflicting)

    projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, first),
        entry!(:run_continuation_fenced, conflicting)
      ])

    assert [%{reason: :conflicting_continuation_fence}] = Projection.anomalies(projection)
  end

  test "malformed fence facts do not hide legitimate work or crash replay" do
    malformed_data = [
      Map.delete(continuation_fence_attrs(), :run_id),
      Map.delete(continuation_fence_attrs(), :trace),
      Map.delete(continuation_fence_attrs(), :definition_version),
      continuation_fence_attrs(run_id: ""),
      continuation_fence_attrs(successor_run_id: 123),
      continuation_fence_attrs(input: "bad-input"),
      continuation_fence_attrs(source_runnable_key: ""),
      continuation_fence_attrs(source_runnable_key: 123),
      continuation_fence_attrs(definition: :historical),
      continuation_fence_attrs(definition_fingerprint: 123),
      continuation_fence_attrs(trace: %{trace_id: "bad", span_id: "bad"}),
      continuation_fence_attrs(occurred_at: "not-a-datetime")
    ]

    for data <- malformed_data do
      malformed = %Entry{
        type: :run_continuation_fenced,
        thread: {:dispatch, "default"},
        data: data,
        occurred_at: @started_at
      }

      projection =
        Projection.rebuild([
          entry!(:attempt_scheduled, scheduled_attrs()),
          malformed
        ])

      assert Projection.continuation_fence(projection, @run_id) == nil

      assert [%{runnable_key: @runnable_key}] =
               Projection.visible_attempts(projection, @visible_at)

      assert [%{reason: :malformed_entry, entry_type: :run_continuation_fenced}] =
               Projection.anomalies(projection)
    end
  end

  test "malformed blocked dispatch facts record anomalies instead of falling through" do
    for data <- [%{}, "not-a-map"] do
      malformed = %Entry{
        type: :attempt_scheduled,
        thread: {:dispatch, "default"},
        data: data,
        occurred_at: @started_at
      }

      projection = Projection.rebuild([malformed])

      assert [%{reason: :malformed_entry, entry_type: :attempt_scheduled}] =
               Projection.anomalies(projection)
    end
  end

  test "fenced runs expose no visible expired or ready work" do
    completed_key = "run_123:completed:1"
    claimed_key = "run_123:claimed:1"

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: claimed_key)),
        entry!(:attempt_claimed, claimed_attrs(runnable_key: claimed_key)),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: completed_key)),
        entry!(:attempt_claimed, claimed_attrs(runnable_key: completed_key)),
        entry!(:attempt_completed, completed_attrs(runnable_key: completed_key)),
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(:run_continuation_repaired, continuation_repair_attrs())
      ])

    assert Projection.visible_attempts(projection, @expired_at) == []
    assert Projection.expired_claims(projection, @expired_at) == []
    assert Projection.results_ready_to_apply(projection) == []
    assert [%{runnable_key: ^completed_key}] = Projection.completed_results(projection)
  end

  test "later dispatch mutations are rejected after the fence" do
    late_key = "run_123:late:1"

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(:run_continuation_repaired, continuation_repair_attrs()),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: late_key)),
        entry!(:attempt_claimed, claimed_attrs()),
        entry!(:attempt_heartbeat, heartbeat_attrs()),
        entry!(:attempt_completed, completed_attrs()),
        entry!(:attempt_failed, failed_attrs()),
        entry!(:live_wakeup_emitted, %{
          run_id: @run_id,
          runnable_key: @runnable_key,
          queue: "default",
          occurred_at: @expired_at
        })
      ])

    assert Projection.visible_attempts(projection, @expired_at) == []
    assert projection.attempts[@runnable_key].status == :available
    refute Map.has_key?(projection.attempts, late_key)

    assert Enum.map(Projection.anomalies(projection), &{&1.entry_type, &1.reason}) == [
             {:attempt_scheduled, :continuation_fenced},
             {:attempt_claimed, :continuation_fenced},
             {:attempt_heartbeat, :continuation_fenced},
             {:attempt_completed, :continuation_fenced},
             {:attempt_failed, :continuation_fenced},
             {:live_wakeup_emitted, :continuation_fenced}
           ]
  end

  test "a fence does not hide another run in the same queue" do
    other_run_id = "run_999"
    other_runnable_key = "run_999:monitor:1"

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: other_run_id, runnable_key: other_runnable_key)
        ),
        entry!(:run_continuation_fenced, continuation_fence_attrs())
      ])

    assert [%{run_id: ^other_run_id, runnable_key: ^other_runnable_key}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "a pending fence hides its exposed successor until the repair receipt" do
    visible_key = "#{@successor_run_id}:visible:1"
    claimed_key = "#{@successor_run_id}:claimed:1"
    completed_key = "#{@successor_run_id}:completed:1"

    pending_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: @successor_run_id, runnable_key: visible_key)
        ),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: @successor_run_id, runnable_key: claimed_key)
        ),
        entry!(
          :attempt_claimed,
          claimed_attrs(run_id: @successor_run_id, runnable_key: claimed_key)
        ),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: @successor_run_id, runnable_key: completed_key)
        ),
        entry!(
          :attempt_claimed,
          claimed_attrs(run_id: @successor_run_id, runnable_key: completed_key)
        ),
        entry!(
          :attempt_completed,
          completed_attrs(run_id: @successor_run_id, runnable_key: completed_key)
        )
      ])

    assert Projection.visible_attempts(pending_projection, @visible_at) == []
    assert Projection.expired_claims(pending_projection, @expired_at) == []
    assert Projection.results_ready_to_apply(pending_projection) == []

    repaired_projection =
      Projection.replay(pending_projection, [
        entry!(:run_continuation_repaired, continuation_repair_attrs())
      ])

    assert [%{run_id: @successor_run_id, runnable_key: ^visible_key}] =
             Projection.visible_attempts(repaired_projection, @visible_at)

    assert [%{run_id: @successor_run_id, runnable_key: ^claimed_key}] =
             Projection.expired_claims(repaired_projection, @expired_at)

    assert [%{run_id: @successor_run_id, runnable_key: ^completed_key}] =
             Projection.results_ready_to_apply(repaired_projection)
  end

  test "malformed checkpoint fences suppress their predecessor without crashing selectors" do
    %Projection{} =
      projection =
      Projection.rebuild([entry!(:attempt_scheduled, scheduled_attrs())])

    malformed_checkpoint = %Projection{
      projection
      | continuation_fences: %{@run_id => %{}, "run_bad" => "malformed"}
    }

    assert Projection.continuation_suppressed_run_ids(malformed_checkpoint) ==
             MapSet.new([@run_id, "run_bad"])

    assert Projection.visible_attempts(malformed_checkpoint, @visible_at) == []
    assert Projection.expired_claims(malformed_checkpoint, @expired_at) == []
    assert Projection.results_ready_to_apply(malformed_checkpoint) == []
  end

  test "malformed checkpoint aborts cannot release a fenced predecessor" do
    %Projection{} =
      projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:run_continuation_fenced, continuation_fence_attrs())
      ])

    malformed_checkpoint = %Projection{
      projection
      | continuation_aborts: %{@run_id => %{abort_reason: :predecessor_changed}}
    }

    assert Projection.continuation_abort(malformed_checkpoint, @run_id) == nil
    assert Projection.continuation_fenced?(malformed_checkpoint, @run_id)
    assert [_pending] = Projection.pending_continuation_fences(malformed_checkpoint)
    assert Projection.visible_attempts(malformed_checkpoint, @visible_at) == []
  end

  test "rejects a current-shape checkpoint with an incomplete continuation fence" do
    incomplete_checkpoint = %Projection{
      Projection.new()
      | continuation_fences: %{@run_id => %{run_id: @run_id}}
    }

    refute Projection.checkpoint_compatible?(incomplete_checkpoint)
  end

  test "retains a completed continuation repair and removes it from pending fences" do
    fence = entry!(:run_continuation_fenced, continuation_fence_attrs())

    repair =
      entry!(
        :run_continuation_repaired,
        continuation_repair_attrs()
      )

    projection = Projection.rebuild([fence, repair])

    assert Projection.continuation_fence(projection, @run_id) == fence.data
    assert Projection.continuation_repair(projection, @run_id) == repair.data
    assert Projection.pending_continuation_fences(projection) == []
    assert Projection.anomalies(projection) == []
  end

  test "an abort releases predecessor work while keeping the successor suppressed" do
    predecessor_claimed_key = "#{@run_id}:claimed:1"
    predecessor_completed_key = "#{@run_id}:completed:1"
    predecessor_late_key = "#{@run_id}:late:1"
    successor_visible_key = "#{@successor_run_id}:visible:1"
    successor_claimed_key = "#{@successor_run_id}:claimed:1"
    successor_completed_key = "#{@successor_run_id}:completed:1"

    projection =
      Projection.rebuild([
        entry!(:attempt_scheduled, scheduled_attrs()),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: predecessor_claimed_key)),
        entry!(:attempt_claimed, claimed_attrs(runnable_key: predecessor_claimed_key)),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: predecessor_completed_key)),
        entry!(:attempt_claimed, claimed_attrs(runnable_key: predecessor_completed_key)),
        entry!(:attempt_completed, completed_attrs(runnable_key: predecessor_completed_key)),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(
            run_id: @successor_run_id,
            runnable_key: successor_visible_key
          )
        ),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: @successor_run_id, runnable_key: successor_claimed_key)
        ),
        entry!(
          :attempt_claimed,
          claimed_attrs(run_id: @successor_run_id, runnable_key: successor_claimed_key)
        ),
        entry!(
          :attempt_scheduled,
          scheduled_attrs(run_id: @successor_run_id, runnable_key: successor_completed_key)
        ),
        entry!(
          :attempt_claimed,
          claimed_attrs(run_id: @successor_run_id, runnable_key: successor_completed_key)
        ),
        entry!(
          :attempt_completed,
          completed_attrs(run_id: @successor_run_id, runnable_key: successor_completed_key)
        ),
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(:run_continuation_aborted, continuation_abort_attrs()),
        entry!(:attempt_scheduled, scheduled_attrs(runnable_key: predecessor_late_key))
      ])

    assert Projection.continuation_abort(projection, @run_id).abort_reason ==
             :predecessor_changed

    assert Projection.pending_continuation_fences(projection) == []
    assert Projection.continuation_fenced?(projection, @run_id) == false

    assert Projection.continuation_suppressed_run_ids(projection) ==
             MapSet.new([@successor_run_id])

    assert projection
           |> Projection.visible_attempts(@visible_at)
           |> MapSet.new(& &1.runnable_key) == MapSet.new([@runnable_key, predecessor_late_key])

    assert [%{run_id: @run_id, runnable_key: ^predecessor_claimed_key}] =
             Projection.expired_claims(projection, @expired_at)

    assert [%{run_id: @run_id, runnable_key: ^predecessor_completed_key}] =
             Projection.results_ready_to_apply(projection)

    refute Enum.any?(
             Projection.visible_attempts(projection, @visible_at),
             &(&1.run_id == @successor_run_id)
           )
  end

  test "retains the first abort idempotently" do
    first = entry!(:run_continuation_aborted, continuation_abort_attrs())

    duplicate =
      entry!(
        :run_continuation_aborted,
        continuation_abort_attrs(
          abort_reason: :predecessor_terminal,
          trace: %{@trace | span_id: "b7ad6b7169203331"},
          occurred_at: @visible_at
        )
      )

    projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        first,
        duplicate
      ])

    assert Projection.continuation_abort(projection, @run_id) == first.data
    assert Projection.anomalies(projection) == []
  end

  test "classifies one-field continuation abort identity conflicts" do
    conflicts = [
      successor_run_id: "run_789",
      continuation_key: "page-99",
      workflow: "OtherWorkflow",
      trigger: "resume",
      input: %{cursor: "page-99"},
      definition_version: "v2",
      definition_fingerprint: "definition-fingerprint-v2",
      queue: "priority"
    ]

    for {field, value} <- conflicts do
      projection =
        Projection.rebuild([
          entry!(:run_continuation_fenced, continuation_fence_attrs()),
          entry!(
            :run_continuation_aborted,
            continuation_abort_attrs([{field, value}, {:occurred_at, @visible_at}])
          )
        ])

      assert Projection.continuation_abort(projection, @run_id) == nil

      assert [%{reason: :conflicting_continuation_abort}] =
               Projection.anomalies(projection),
             "expected #{field} to participate in continuation abort identity"
    end
  end

  test "repair and abort receipts are mutually exclusive" do
    repaired_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(:run_continuation_repaired, continuation_repair_attrs()),
        entry!(:run_continuation_aborted, continuation_abort_attrs())
      ])

    assert Projection.continuation_abort(repaired_projection, @run_id) == nil
    assert [%{reason: :continuation_already_repaired}] = Projection.anomalies(repaired_projection)

    aborted_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(:run_continuation_aborted, continuation_abort_attrs()),
        entry!(:run_continuation_repaired, continuation_repair_attrs())
      ])

    assert Projection.continuation_repair(aborted_projection, @run_id) == nil
    assert [%{reason: :continuation_already_aborted}] = Projection.anomalies(aborted_projection)
  end

  test "orphaned or malformed abort facts cannot resolve a fence" do
    orphan_projection =
      Projection.rebuild([
        entry!(:run_continuation_aborted, continuation_abort_attrs())
      ])

    assert Projection.continuation_abort(orphan_projection, @run_id) == nil
    assert [%{reason: :orphaned_continuation_abort}] = Projection.anomalies(orphan_projection)

    malformed = %Entry{
      type: :run_continuation_aborted,
      thread: {:dispatch, "default"},
      data: Map.put(continuation_abort_attrs(), :abort_reason, "invalid"),
      occurred_at: @visible_at
    }

    malformed_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        malformed
      ])

    assert Projection.continuation_abort(malformed_projection, @run_id) == nil
    assert [_pending] = Projection.pending_continuation_fences(malformed_projection)
    assert [%{reason: :malformed_entry}] = Projection.anomalies(malformed_projection)

    unknown_reason = %Entry{
      type: :run_continuation_aborted,
      thread: {:dispatch, "default"},
      data: Map.put(continuation_abort_attrs(), :abort_reason, :future_reason),
      occurred_at: @visible_at
    }

    unknown_reason_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        unknown_reason
      ])

    assert Projection.continuation_abort(unknown_reason_projection, @run_id) == nil
    assert [_pending] = Projection.pending_continuation_fences(unknown_reason_projection)
    assert [%{reason: :malformed_entry}] = Projection.anomalies(unknown_reason_projection)

    non_map = %Entry{
      type: :run_continuation_aborted,
      thread: {:dispatch, "default"},
      data: "not-a-map",
      occurred_at: @visible_at
    }

    non_map_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        non_map
      ])

    assert Projection.continuation_abort(non_map_projection, @run_id) == nil
    assert [_pending] = Projection.pending_continuation_fences(non_map_projection)
    assert [%{reason: :malformed_entry}] = Projection.anomalies(non_map_projection)
  end

  test "retains the first continuation repair and classifies conflicting reuse" do
    first = entry!(:run_continuation_repaired, continuation_repair_attrs())

    duplicate =
      entry!(
        :run_continuation_repaired,
        continuation_repair_attrs(
          trace: %{@trace | span_id: "b7ad6b7169203331"},
          occurred_at: @visible_at
        )
      )

    conflicting =
      entry!(
        :run_continuation_repaired,
        continuation_repair_attrs(
          successor_run_id: "run_789",
          occurred_at: @claimed_at
        )
      )

    duplicate_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        first,
        duplicate
      ])

    assert Projection.continuation_repair(duplicate_projection, @run_id) == first.data
    assert Projection.anomalies(duplicate_projection) == []

    conflicting_projection = Projection.replay(duplicate_projection, [conflicting])

    assert Projection.continuation_repair(conflicting_projection, @run_id) == first.data

    assert [%{reason: :conflicting_continuation_repair}] =
             Projection.anomalies(conflicting_projection)
  end

  test "classifies one-field continuation repair identity conflicts" do
    conflicts = [
      successor_run_id: "run_789",
      continuation_key: "page-99",
      workflow: "OtherWorkflow",
      trigger: "resume",
      input: %{cursor: "page-99"},
      definition_version: "v2",
      definition_fingerprint: "definition-fingerprint-v2",
      queue: "priority"
    ]

    for {field, value} <- conflicts do
      projection =
        Projection.rebuild([
          entry!(:run_continuation_fenced, continuation_fence_attrs()),
          entry!(
            :run_continuation_repaired,
            continuation_repair_attrs([{field, value}, {:occurred_at, @visible_at}])
          )
        ])

      assert Projection.continuation_repair(projection, @run_id) == nil

      assert [%{reason: :conflicting_continuation_repair}] =
               Projection.anomalies(projection),
             "expected #{field} to participate in continuation repair identity"
    end
  end

  test "does not resolve an orphaned or mismatched continuation repair" do
    orphan = entry!(:run_continuation_repaired, continuation_repair_attrs())
    orphan_projection = Projection.rebuild([orphan])

    assert Projection.continuation_repair(orphan_projection, @run_id) == nil

    assert [%{reason: :orphaned_continuation_repair}] =
             Projection.anomalies(orphan_projection)

    mismatched =
      entry!(
        :run_continuation_repaired,
        continuation_repair_attrs(input: %{cursor: "page-99"})
      )

    mismatched_projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        mismatched
      ])

    assert Projection.continuation_repair(mismatched_projection, @run_id) == nil
    assert [_pending] = Projection.pending_continuation_fences(mismatched_projection)

    assert [%{reason: :conflicting_continuation_repair}] =
             Projection.anomalies(mismatched_projection)
  end

  test "malformed continuation repairs cannot resolve a valid fence" do
    malformed = %Entry{
      type: :run_continuation_repaired,
      thread: {:dispatch, "default"},
      data: Map.delete(continuation_repair_attrs(), :trace),
      occurred_at: @visible_at
    }

    projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        malformed
      ])

    assert Projection.continuation_repair(projection, @run_id) == nil
    assert [_pending] = Projection.pending_continuation_fences(projection)
    assert [%{reason: :malformed_entry}] = Projection.anomalies(projection)
  end

  test "orders only unresolved continuation fences by predecessor run id" do
    first_run_id = "run_001"
    first_successor_run_id = "run_002"
    second_run_id = "run_003"
    second_successor_run_id = "run_004"

    projection =
      Projection.rebuild([
        entry!(:run_continuation_fenced, continuation_fence_attrs()),
        entry!(
          :run_continuation_fenced,
          continuation_fence_attrs(
            run_id: second_run_id,
            successor_run_id: second_successor_run_id
          )
        ),
        entry!(
          :run_continuation_fenced,
          continuation_fence_attrs(
            run_id: first_run_id,
            successor_run_id: first_successor_run_id
          )
        ),
        entry!(:run_continuation_repaired, continuation_repair_attrs())
      ])

    assert [%{run_id: ^first_run_id}, %{run_id: ^second_run_id}] =
             Projection.pending_continuation_fences(projection)
  end

  test "normalizes checkpoints created before continuation fences" do
    legacy_projection =
      Projection.new()
      |> Map.from_struct()
      |> Map.delete(:continuation_fences)
      |> Map.put(:__struct__, Projection)

    refute Projection.checkpoint_compatible?(legacy_projection)
    assert Projection.normalize(legacy_projection).continuation_fences == %{}
  end

  test "rejects checkpoints created before continuation repair receipts" do
    legacy_projection =
      Projection.new()
      |> Map.from_struct()
      |> Map.delete(:continuation_repairs)
      |> Map.put(:__struct__, Projection)

    refute Projection.checkpoint_compatible?(legacy_projection)
    assert Projection.normalize(legacy_projection).continuation_repairs == %{}
  end

  test "rejects checkpoints created before continuation abort receipts" do
    legacy_projection =
      Projection.new()
      |> Map.from_struct()
      |> Map.delete(:continuation_aborts)
      |> Map.put(:__struct__, Projection)

    refute Projection.checkpoint_compatible?(legacy_projection)
    assert Projection.normalize(legacy_projection).continuation_aborts == %{}
  end

  test "rejects malformed or contradictory continuation resolution checkpoints" do
    non_map_abort_projection = %Projection{Projection.new() | continuation_aborts: nil}

    refute Projection.checkpoint_compatible?(non_map_abort_projection)
    assert Projection.normalize(non_map_abort_projection).continuation_aborts == %{}

    fence = entry!(:run_continuation_fenced, continuation_fence_attrs())
    repair = entry!(:run_continuation_repaired, continuation_repair_attrs())
    %Projection{} = repaired_projection = Projection.rebuild([fence, repair])

    contradictory_projection = %Projection{
      repaired_projection
      | continuation_aborts: %{@run_id => continuation_abort_attrs()}
    }

    refute Projection.checkpoint_compatible?(contradictory_projection)

    assert Projection.continuation_suppressed_run_ids(contradictory_projection) ==
             MapSet.new([@run_id, @successor_run_id])

    assert [_pending] = Projection.pending_continuation_fences(contradictory_projection)

    %Projection{} = fenced_projection = Projection.rebuild([fence])

    mismatched_abort_projection = %Projection{
      fenced_projection
      | continuation_aborts: %{
          @run_id => continuation_abort_attrs(input: %{cursor: "page-99"})
        }
    }

    refute Projection.checkpoint_compatible?(mismatched_abort_projection)

    wrong_key_repair_projection = %Projection{
      fenced_projection
      | continuation_repairs: %{"wrong-key" => continuation_repair_attrs()}
    }

    wrong_key_abort_projection = %Projection{
      fenced_projection
      | continuation_aborts: %{"wrong-key" => continuation_abort_attrs()}
    }

    refute Projection.checkpoint_compatible?(wrong_key_repair_projection)
    refute Projection.checkpoint_compatible?(wrong_key_abort_projection)
  end

  defp continuation_fence_attrs(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        successor_run_id: @successor_run_id,
        continuation_key: "page-42",
        workflow: "MonitoringWorkflow",
        trigger: "continue",
        input: %{cursor: "page-42"},
        definition: :current,
        definition_version: nil,
        definition_fingerprint: "definition-fingerprint-v1",
        queue: "default",
        trace: @trace,
        occurred_at: @started_at
      },
      Map.new(attrs)
    )
  end

  defp continuation_repair_attrs(attrs \\ []) do
    Map.merge(continuation_fence_attrs(), Map.new(attrs))
  end

  defp continuation_abort_attrs(attrs \\ []) do
    continuation_fence_attrs()
    |> Map.put(:abort_reason, :predecessor_changed)
    |> Map.merge(Map.new(attrs))
  end

  defp scheduled_attrs(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        idempotency_key: "monitor-page-42",
        attempt_number: 1,
        step: "monitor",
        input: %{cursor: "page-42"},
        queue: "default",
        visible_at: @visible_at,
        occurred_at: @started_at
      },
      Map.new(attrs)
    )
  end

  defp claimed_attrs(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        owner_id: "worker-1",
        queue: "default",
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp heartbeat_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: @claim_id,
      claim_token_hash: @claim_token_hash,
      queue: "default",
      lease_until: @expired_at,
      occurred_at: @expired_at
    }
  end

  defp completed_attrs(attrs \\ []) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        queue: "default",
        result: %{cursor: "page-42"},
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp failed_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: @claim_id,
      claim_token_hash: @claim_token_hash,
      queue: "default",
      error: %{reason: "failed"},
      occurred_at: @expired_at
    }
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
