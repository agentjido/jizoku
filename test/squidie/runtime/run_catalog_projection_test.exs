defmodule Squidie.Runtime.RunCatalogProjectionTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.RunCatalogProjection

  @run_id "run_123"
  @workflow "BillingWorkflow"
  @queue "default"
  @started_at ~U[2026-05-14 00:00:00Z]
  @later_started_at ~U[2026-05-14 00:00:01Z]

  test "catalogs runs newest deterministically by durable timestamp" do
    projection =
      RunCatalogProjection.rebuild([
        entry!(%{run_id: "run_2", occurred_at: @later_started_at}),
        entry!(%{run_id: "run_1", occurred_at: @started_at})
      ])

    assert RunCatalogProjection.run_ids(projection) == ["run_1", "run_2"]
  end

  test "keeps duplicate catalog facts idempotent" do
    projection =
      RunCatalogProjection.rebuild([
        entry!(%{run_id: @run_id}),
        entry!(%{run_id: @run_id})
      ])

    assert RunCatalogProjection.run_ids(projection) == [@run_id]
    assert RunCatalogProjection.anomalies(projection) == []
  end

  test "surfaces conflicting catalog facts for one run id" do
    projection =
      RunCatalogProjection.rebuild([
        entry!(%{run_id: @run_id, queue: "first"}),
        entry!(%{run_id: @run_id, queue: "second"})
      ])

    assert RunCatalogProjection.run_ids(projection) == [@run_id]

    assert [
             %{
               entry_type: :run_cataloged,
               reason: :conflicting_run_catalog,
               run_id: @run_id,
               workflow: @workflow,
               queue: "second"
             }
           ] = RunCatalogProjection.anomalies(projection)
  end

  test "surfaces malformed catalog facts" do
    projection =
      RunCatalogProjection.rebuild([
        %Entry{
          type: :run_cataloged,
          thread: {:run_catalog, "all"},
          data: %{run_id: @run_id, workflow: @workflow},
          occurred_at: @started_at
        }
      ])

    assert RunCatalogProjection.run_ids(projection) == []

    assert [
             %{
               entry_type: :run_cataloged,
               reason: :malformed_entry,
               run_id: @run_id,
               workflow: @workflow
             }
           ] = RunCatalogProjection.anomalies(projection)
  end

  defp entry!(attrs) do
    occurred_at = Map.get(attrs, :occurred_at, @started_at)
    data = Map.drop(attrs, [:occurred_at])

    %Entry{
      type: :run_cataloged,
      thread: {:run_catalog, "all"},
      data: Map.merge(%{run_id: @run_id, workflow: @workflow, queue: @queue}, data),
      occurred_at: occurred_at
    }
  end
end
