defmodule Jizoku.Operations.CollectorTest do
  use ExUnit.Case, async: true

  alias Jizoku.Operations.Collector
  alias Jizoku.Runtime.DispatchProtocol.ActionAttempt
  alias Jizoku.Runtime.DispatchProtocol.Projection

  @now ~U[2026-07-11 12:00:00Z]

  test "rejects invalid collection options" do
    assert {:error, {:invalid_option, {:opts, :invalid}}} = Collector.collect(:invalid)
    assert {:error, {:invalid_option, {:now, :invalid}}} = Collector.collect(now: :tomorrow)
  end

  test "returns only completed results that belong to planned unapplied runnables" do
    attempts = [
      attempt("ready", "run-1"),
      attempt("already-applied", "run-1"),
      attempt("not-planned", "run-1"),
      attempt("other-run", "run-2")
    ]

    queue = %{
      queue: "default",
      projection: %Projection{attempts: Map.new(attempts, &{&1.runnable_key, &1})},
      attempts: attempts
    }

    run = %{
      run_id: "run-1",
      planned_runnables: [%{runnable_key: "ready"}, %{runnable_key: "already-applied"}],
      applied_runnable_keys: MapSet.new(["already-applied"])
    }

    assert [%{runnable_key: "ready"}] = Collector.pending_results(run, queue)
  end

  test "normalizes atom and missing runnable queues when finding pending dispatches" do
    dispatched = attempt("dispatched", "run-1")

    queue = %{
      queue: "critical",
      projection: %Projection{attempts: %{"dispatched" => dispatched}},
      attempts: [dispatched]
    }

    run = %{
      queue: "critical",
      planned_runnables: [
        %{runnable_key: "atom-queue", queue: :critical},
        %{runnable_key: "default-queue"},
        %{runnable_key: "other-queue", queue: "other"},
        %{runnable_key: "dispatched", queue: :critical},
        %{runnable_key: "applied", queue: :critical}
      ],
      applied_runnable_keys: MapSet.new(["applied"])
    }

    assert [%{runnable_key: "atom-queue"}, %{runnable_key: "default-queue"}] =
             Collector.pending_dispatches(run, queue)
  end

  test "fails closed when operational run and queue partitions differ" do
    attempt = attempt("ready", "run-1")

    queue = %{
      partition: "tenant_globex",
      queue: "default",
      projection: %Projection{attempts: %{attempt.runnable_key => attempt}},
      attempts: [attempt]
    }

    run = %{
      partition: "tenant_acme",
      queue: "default",
      planned_runnables: [%{runnable_key: "ready"}],
      applied_runnable_keys: MapSet.new()
    }

    assert Collector.pending_dispatches(run, queue) == []
    assert Collector.pending_results(run, queue) == []
  end

  defp attempt(key, run_id) do
    %ActionAttempt{
      run_id: run_id,
      runnable_key: key,
      idempotency_key: "idempotency-#{key}",
      attempt_number: 1,
      step: key,
      input: %{},
      visible_at: @now,
      status: :completed,
      result: %{"ok" => true}
    }
  end
end
