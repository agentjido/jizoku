defmodule Squidie.Runtime.RunIndexProjectionTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.RunIndexProjection

  @workflow "BillingWorkflow"
  @run_id "run_123"
  @second_run_id "run_456"
  @queue "critical"
  @started_at ~U[2026-05-14 00:00:00Z]
  @later_started_at ~U[2026-05-14 00:00:10Z]

  test "rebuilds deterministic run summaries from index entries" do
    projection =
      RunIndexProjection.rebuild([
        entry!(:run_indexed, %{run_id: @second_run_id, occurred_at: @later_started_at}),
        entry!(:run_indexed, %{run_id: @run_id, occurred_at: @started_at})
      ])

    assert RunIndexProjection.workflow(projection) == @workflow
    assert RunIndexProjection.run_ids(projection) == [@run_id, @second_run_id]

    assert RunIndexProjection.runs(projection) == [
             %{run_id: @run_id, workflow: @workflow, queue: @queue, indexed_at: @started_at},
             %{
               run_id: @second_run_id,
               workflow: @workflow,
               queue: @queue,
               indexed_at: @later_started_at
             }
           ]

    assert RunIndexProjection.anomalies(projection) == []
  end

  test "treats duplicate index entries as idempotent" do
    projection =
      RunIndexProjection.rebuild([
        entry!(:run_indexed, %{run_id: @run_id}),
        entry!(:run_indexed, %{run_id: @run_id})
      ])

    assert RunIndexProjection.run_ids(projection) == [@run_id]
    assert RunIndexProjection.anomalies(projection) == []
  end

  test "reports conflicting index facts for the same run id" do
    projection =
      RunIndexProjection.rebuild([
        entry!(:run_indexed, %{run_id: @run_id, occurred_at: @started_at}),
        entry!(:run_indexed, %{run_id: @run_id, occurred_at: @later_started_at})
      ])

    assert RunIndexProjection.runs(projection) == [
             %{run_id: @run_id, workflow: @workflow, queue: @queue, indexed_at: @started_at}
           ]

    assert [
             %{
               entry_type: :run_indexed,
               reason: :conflicting_run_index,
               run_id: @run_id,
               workflow: @workflow
             }
           ] = RunIndexProjection.anomalies(projection)
  end

  test "reports malformed persisted index entries instead of raising" do
    malformed_entry = %Entry{
      type: :run_indexed,
      thread: {:run_index, @workflow},
      data: %{run_id: nil, workflow: @workflow},
      occurred_at: @started_at
    }

    projection = RunIndexProjection.rebuild([malformed_entry])

    assert RunIndexProjection.runs(projection) == []

    assert [
             %{
               entry_type: :run_indexed,
               reason: :malformed_entry,
               workflow: @workflow
             }
           ] = RunIndexProjection.anomalies(projection)
  end

  test "reports entries for a different workflow in the same index projection" do
    conflicting_entry = %Entry{
      type: :run_indexed,
      thread: {:run_index, @workflow},
      data: %{run_id: @second_run_id, workflow: "OtherWorkflow", queue: @queue},
      occurred_at: @later_started_at
    }

    projection =
      RunIndexProjection.rebuild([
        entry!(:run_indexed, %{run_id: @run_id}),
        conflicting_entry
      ])

    assert RunIndexProjection.run_ids(projection) == [@run_id]

    assert [
             %{
               entry_type: :run_indexed,
               reason: :conflicting_workflow,
               run_id: @second_run_id,
               workflow: "OtherWorkflow"
             }
           ] = RunIndexProjection.anomalies(projection)
  end

  defp entry!(type, attrs) do
    attrs =
      Map.merge(%{workflow: @workflow, queue: @queue, occurred_at: @started_at}, Map.new(attrs))

    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
