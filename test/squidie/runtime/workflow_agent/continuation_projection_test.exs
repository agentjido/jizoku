defmodule Squidie.Runtime.WorkflowAgent.ContinuationProjectionTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry
  alias Squidie.Runtime.WorkflowAgent.Projection

  @predecessor_run_id "run_123"
  @middle_run_id "run_456"
  @successor_run_id "run_789"
  @workflow "BillingWorkflow"
  @occurred_at ~U[2026-08-09 12:00:00Z]

  test "rebuilds predecessor continuation intent and terminal state" do
    projection =
      Projection.rebuild([
        run_started(@predecessor_run_id),
        continuation_requested(@predecessor_run_id, @middle_run_id, "page-42"),
        entry!(:run_terminal, %{
          run_id: @predecessor_run_id,
          status: :continued,
          occurred_at: @occurred_at
        })
      ])

    assert projection.terminal_status == :continued
    assert projection.continued_to_run_id == @middle_run_id

    assert Projection.continuation(projection) == %{
             continued_from: nil,
             continued_to: %{run_id: @middle_run_id, continuation_key: "page-42"}
           }
  end

  test "rebuilds successor lineage from explicit durable evidence" do
    projection =
      Projection.rebuild([
        run_started(@middle_run_id),
        continued_from(@middle_run_id, @predecessor_run_id, "page-42")
      ])

    assert projection.continued_from_run_id == @predecessor_run_id
    assert projection.continued_to_run_id == nil

    assert Projection.continuation(projection) == %{
             continued_from: %{run_id: @predecessor_run_id, continuation_key: "page-42"},
             continued_to: nil
           }
  end

  test "keeps independent incoming and outgoing keys for a middle run" do
    projection =
      Projection.rebuild([
        run_started(@middle_run_id),
        continued_from(@middle_run_id, @predecessor_run_id, "page-42"),
        continuation_requested(@middle_run_id, @successor_run_id, "page-43")
      ])

    assert Projection.continuation(projection) == %{
             continued_from: %{run_id: @predecessor_run_id, continuation_key: "page-42"},
             continued_to: %{run_id: @successor_run_id, continuation_key: "page-43"}
           }

    assert Projection.anomalies(projection) == []
  end

  test "deduplicates matching continuation facts within one run thread" do
    origin = continued_from(@middle_run_id, @predecessor_run_id, "page-42")
    request = continuation_requested(@middle_run_id, @successor_run_id, "page-43")

    projection =
      Projection.rebuild([run_started(@middle_run_id), origin, origin, request, request])

    assert Projection.continuation(projection) == %{
             continued_from: %{run_id: @predecessor_run_id, continuation_key: "page-42"},
             continued_to: %{run_id: @successor_run_id, continuation_key: "page-43"}
           }

    assert Projection.anomalies(projection) == []
  end

  test "retains first lineage and records conflicting continuation facts as anomalies" do
    projection =
      Projection.rebuild([
        run_started(@middle_run_id),
        continued_from(@middle_run_id, @predecessor_run_id, "page-42"),
        continued_from(@middle_run_id, "run_conflict", "page-42"),
        continuation_requested(@middle_run_id, @successor_run_id, "page-43"),
        continuation_requested(@middle_run_id, "run_conflict", "page-43")
      ])

    assert Projection.continuation(projection) == %{
             continued_from: %{run_id: @predecessor_run_id, continuation_key: "page-42"},
             continued_to: %{run_id: @successor_run_id, continuation_key: "page-43"}
           }

    assert Projection.anomalies(projection) == [
             %{
               reason: :conflicting_continuation,
               entry_type: :run_continued_from,
               run_id: @middle_run_id
             },
             %{
               reason: :conflicting_continuation,
               entry_type: :run_continuation_requested,
               run_id: @middle_run_id
             }
           ]
  end

  test "treats a changed resolved definition fingerprint as conflicting continuation intent" do
    request = continuation_requested(@middle_run_id, @successor_run_id, "page-43")

    conflicting_request =
      put_in(request.data.definition_fingerprint, "definition-fingerprint-v2")

    projection = Projection.rebuild([run_started(@middle_run_id), request, conflicting_request])

    assert projection.continuation_request.definition_fingerprint ==
             "definition-fingerprint-v1"

    assert Projection.anomalies(projection) == [
             %{
               reason: :conflicting_continuation,
               entry_type: :run_continuation_requested,
               run_id: @middle_run_id
             }
           ]
  end

  test "rebuilds continuation intent for an unversioned workflow" do
    request =
      @middle_run_id
      |> continuation_requested(@successor_run_id, "page-43")
      |> put_in([Access.key(:data), Access.key(:definition_version)], nil)

    projection = Projection.rebuild([run_started(@middle_run_id), request])

    assert projection.continuation_request.definition_version == nil

    assert Projection.continuation(projection) == %{
             continued_from: nil,
             continued_to: %{run_id: @successor_run_id, continuation_key: "page-43"}
           }

    assert Projection.anomalies(projection) == []
  end

  test "records malformed continuation facts without mutating lineage" do
    malformed_origin = %Entry{
      type: :run_continued_from,
      thread: {:run, @middle_run_id},
      data: %{run_id: @middle_run_id},
      occurred_at: @occurred_at
    }

    malformed_request = %Entry{
      type: :run_continuation_requested,
      thread: {:run, @middle_run_id},
      data: %{
        run_id: @middle_run_id,
        successor_run_id: @successor_run_id,
        continuation_key: "page-43",
        workflow: @workflow,
        trigger: "created",
        input: "not-a-map",
        definition: :current,
        definition_version: "2026-08-v1",
        definition_fingerprint: "definition-fingerprint-v1"
      },
      occurred_at: @occurred_at
    }

    projection = Projection.rebuild([malformed_origin, malformed_request])

    assert Projection.continuation(projection) == %{continued_from: nil, continued_to: nil}

    assert Projection.anomalies(projection) == [
             %{
               reason: :malformed_entry,
               entry_type: :run_continued_from,
               run_id: @middle_run_id
             },
             %{
               reason: :malformed_entry,
               entry_type: :run_continuation_requested,
               run_id: @middle_run_id
             }
           ]
  end

  test "rejects wrong-typed lineage identifiers before they can win reconstruction" do
    malformed_request =
      @middle_run_id
      |> continuation_requested(@successor_run_id, "page-43")
      |> put_in([Access.key(:data), Access.key(:successor_run_id)], :not_a_run_id)

    malformed_origin =
      @middle_run_id
      |> continued_from(@predecessor_run_id, "page-42")
      |> put_in([Access.key(:data), Access.key(:continuation_key)], :not_a_key)

    projection = Projection.rebuild([malformed_request, malformed_origin])

    assert Projection.continuation(projection) == %{continued_from: nil, continued_to: nil}

    assert Enum.map(Projection.anomalies(projection), & &1.reason) == [
             :malformed_entry,
             :malformed_entry
           ]
  end

  test "upgrades a legacy checkpoint missing continuation fields" do
    legacy_projection =
      %Projection{run_id: @middle_run_id, workflow: @workflow}
      |> Map.delete(:continued_from_run_id)
      |> Map.delete(:continued_from_key)
      |> Map.delete(:continued_to_run_id)
      |> Map.delete(:continued_to_key)
      |> Map.delete(:continuation_request)
      |> Map.delete(:continuation_origin)

    projection = Projection.upgrade(legacy_projection)

    assert projection.run_id == @middle_run_id
    assert projection.workflow == @workflow
    assert Projection.continuation(projection) == %{continued_from: nil, continued_to: nil}
  end

  defp run_started(run_id) do
    entry!(:run_started, %{
      run_id: run_id,
      workflow: @workflow,
      occurred_at: @occurred_at
    })
  end

  defp continuation_requested(run_id, successor_run_id, continuation_key) do
    entry!(:run_continuation_requested, %{
      run_id: run_id,
      successor_run_id: successor_run_id,
      continuation_key: continuation_key,
      workflow: @workflow,
      trigger: "created",
      input: %{cursor: continuation_key},
      definition: :current,
      definition_version: "2026-08-v1",
      definition_fingerprint: "definition-fingerprint-v1",
      occurred_at: @occurred_at
    })
  end

  defp continued_from(run_id, predecessor_run_id, continuation_key) do
    entry!(:run_continued_from, %{
      run_id: run_id,
      predecessor_run_id: predecessor_run_id,
      continuation_key: continuation_key,
      occurred_at: @occurred_at
    })
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
