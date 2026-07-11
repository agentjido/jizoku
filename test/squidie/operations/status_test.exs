defmodule Squidie.Operations.StatusTest do
  use ExUnit.Case, async: true

  alias Squidie.Config
  alias Squidie.Operations.Collector
  alias Squidie.Operations.Status
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Projection

  @now ~U[2026-07-11 12:00:00Z]

  test "builds deterministic redacted run and queue health metrics" do
    visible = attempt("visible", :available, ~U[2026-07-11 11:59:00Z])
    scheduled = attempt("scheduled", :retry_scheduled, ~U[2026-07-11 12:05:00Z])

    claimed =
      attempt("claimed", :claimed, ~U[2026-07-11 11:55:00Z],
        claimed_at: ~U[2026-07-11 11:50:00Z],
        owner_id: "secret-worker",
        claim_token_hash: "secret-token"
      )

    projection = %Projection{
      attempts: Map.new([visible, scheduled, claimed], &{&1.runnable_key, &1}),
      queued_run_ids: MapSet.new(["run-1"]),
      terminal_runs: MapSet.new()
    }

    collector = %Collector{
      config: %Config{queue: "default"},
      now: @now,
      catalog_anomalies: [],
      runs: [
        %{
          run_id: "run-1",
          workflow: "BillingWorkflow",
          queue: "default",
          status: :running,
          terminal?: false,
          manual_state: %{step: "review"},
          planned_runnables: [
            %{runnable_key: "visible", queue: "default"},
            %{runnable_key: "scheduled", queue: "default"},
            %{runnable_key: "claimed", queue: "default"},
            %{runnable_key: "missing", queue: "default"}
          ],
          applied_runnable_keys: MapSet.new(),
          anomalies: []
        }
      ],
      queues: %{
        "default" => %{
          queue: "default",
          projection: projection,
          attempts: [claimed, scheduled, visible]
        }
      }
    }

    report = Status.from_collector(collector)

    assert report.schema_version == 1
    assert report.generated_at == "2026-07-11T12:00:00Z"
    assert report.totals == %{runs: 1, queues: 1, anomalies: 0}

    assert report.run_counts == [
             %{workflow: "BillingWorkflow", queue: "default", status: :running, count: 1}
           ]

    assert [queue] = report.queues
    assert queue.visible_attempt_depth == 1
    assert queue.scheduled_attempt_depth == 1
    assert queue.next_visible_at == "2026-07-11T12:05:00Z"
    assert queue.claimed_attempt_depth == 1
    assert queue.oldest_claim_age_seconds == 600
    assert queue.pending_dispatch_count == 1
    assert queue.pending_result_count == 0
    assert queue.manual_intervention_count == 1

    encoded = Jason.encode!(report)
    refute encoded =~ "secret-worker"
    refute encoded =~ "secret-token"
    refute encoded =~ "\"input\":"
    refute encoded =~ "\"result\":"
  end

  test "keeps the configured queue visible for an empty runtime" do
    collector = %Collector{
      config: %Config{queue: "critical"},
      now: @now,
      catalog_anomalies: [],
      runs: [],
      queues: %{
        "critical" => %{
          queue: "critical",
          projection: Projection.new(),
          attempts: []
        }
      }
    }

    assert %{
             totals: %{runs: 0, queues: 1},
             run_counts: [],
             queues: [
               %{
                 queue: "critical",
                 visible_attempt_depth: 0,
                 scheduled_attempt_depth: 0,
                 claimed_attempt_depth: 0
               }
             ]
           } = Status.from_collector(collector)
  end

  test "reports no claim age when active claims lack a claimed timestamp" do
    claimed = attempt("claimed", :claimed, @now)

    collector = %Collector{
      config: %Config{queue: "default"},
      now: @now,
      catalog_anomalies: [],
      runs: [],
      queues: %{
        "default" => %{
          queue: "default",
          projection: %Projection{
            attempts: %{claimed.runnable_key => claimed},
            terminal_runs: MapSet.new()
          },
          attempts: [claimed]
        }
      }
    }

    assert [%{claimed_attempt_depth: 1, oldest_claim_age_seconds: nil}] =
             Status.from_collector(collector).queues
  end

  defp attempt(key, status, visible_at, attrs \\ []) do
    struct!(
      ActionAttempt,
      Keyword.merge(
        [
          run_id: "run-1",
          runnable_key: key,
          idempotency_key: "idempotency-#{key}",
          attempt_number: 1,
          step: key,
          input: %{secret: true},
          visible_at: visible_at,
          status: status
        ],
        attrs
      )
    )
  end
end
