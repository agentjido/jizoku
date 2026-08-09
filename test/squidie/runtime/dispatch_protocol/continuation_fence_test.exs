defmodule Squidie.Runtime.DispatchProtocol.ContinuationFenceTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.DispatchProtocol.Projection

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

  test "malformed fence facts do not hide legitimate work or crash replay" do
    malformed_data = [
      Map.delete(continuation_fence_attrs(), :run_id),
      Map.delete(continuation_fence_attrs(), :trace),
      Map.delete(continuation_fence_attrs(), :definition_version),
      continuation_fence_attrs(run_id: ""),
      continuation_fence_attrs(successor_run_id: 123),
      continuation_fence_attrs(input: "bad-input"),
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
        entry!(:run_continuation_fenced, continuation_fence_attrs())
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

  test "normalizes checkpoints created before continuation fences" do
    legacy_projection =
      Projection.new()
      |> Map.from_struct()
      |> Map.delete(:continuation_fences)
      |> Map.put(:__struct__, Projection)

    refute Projection.checkpoint_compatible?(legacy_projection)
    assert Projection.normalize(legacy_projection).continuation_fences == %{}
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
