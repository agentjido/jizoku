defmodule Squidie.Operations.DoctorTest do
  use ExUnit.Case, async: false

  alias Squidie.Operations.Doctor
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal

  @now ~U[2026-07-11 12:00:00Z]
  @earlier ~U[2026-07-11 11:00:00Z]
  @storage {Jido.Storage.ETS, table: :squidie_doctor_findings_test}

  setup do
    :ok = Jido.Storage.ETS.Owner.ensure_tables(table: :squidie_doctor_findings_test)
    on_exit(&drop_tables/0)
    :ok
  end

  test "reports an empty custom-storage runtime as healthy and JSON-safe" do
    storage = {Jido.Storage.ETS, table: :squidie_doctor_empty_test}

    assert {:ok, report} =
             Doctor.report(
               now: @now,
               journal_storage: storage,
               partition: "tenant_acme",
               queue: "critical"
             )

    assert report.schema_version == 1
    assert report.partition == "tenant_acme"
    assert report.generated_at == "2026-07-11T12:00:00Z"
    assert report.healthy
    assert report.summary.fail == 0

    assert %{status: :pass, details: %{status: :not_applicable}} =
             Enum.find(report.checks, &(&1.id == :schema))

    assert is_binary(Jason.encode!(report))
  end

  test "turns invalid configuration into an actionable diagnostic" do
    assert {:ok, report} = Doctor.report(now: @now, runtime: :unsupported)
    refute report.healthy

    assert [
             %{
               id: :configuration,
               status: :fail,
               next_actions: [:configure_squidie_repo_and_queue],
               details: %{reason: %{type: :invalid_config, keys: [:runtime]}}
             }
           ] = report.checks
  end

  test "identifies only behind or incompatible schema results as drift" do
    for status <- [:behind, :incompatible] do
      assert Doctor.drift?(%{checks: [%{id: :schema, details: %{status: status}}]})
    end

    refute Doctor.drift?(%{checks: [%{id: :schema, details: %{status: :unavailable}}]})
  end

  test "rejects invalid report options" do
    assert {:error, {:invalid_option, {:now, :invalid}}} = Doctor.report(now: :tomorrow)
    assert {:error, {:invalid_option, {:opts, :invalid}}} = Doctor.report(:invalid)
  end

  test "reports missing configuration without exposing configuration values" do
    assert {:ok, report} = Doctor.report(now: @now, journal_storage: nil)

    assert [
             %{
               id: :configuration,
               status: :fail,
               message: "Missing Squidie configuration: :journal_storage.",
               details: %{reason: %{type: :missing_config, keys: [:journal_storage]}}
             }
           ] = report.checks
  end

  test "reports actionable journal-backed runtime hazards without sensitive attempt data" do
    append_catalog!("expired-run")
    append_catalog!("overdue-run")
    append_catalog!("terminal-run")

    append_run!("expired-run", [
      run_started("expired-run"),
      runnables_planned("expired-run", "expired-step")
    ])

    append_run!("overdue-run", [
      run_started("overdue-run"),
      runnables_planned("overdue-run", "overdue-step"),
      entry!(:manual_step_paused, %{
        run_id: "overdue-run",
        step: :review,
        kind: :approval,
        metadata: %{secret: "manual-secret"},
        occurred_at: @earlier
      })
    ])

    append_run!("terminal-run", [
      run_started("terminal-run"),
      runnables_planned("terminal-run", "terminal-step"),
      entry!(:run_terminal, %{
        run_id: "terminal-run",
        status: :completed,
        occurred_at: @now
      })
    ])

    expired = scheduled("expired-run", "expired-step")
    overdue = scheduled("overdue-run", "overdue-step")

    conflicting_overdue =
      overdue
      |> put_in([Access.key!(:data), :idempotency_key], "conflicting-idempotency")
      |> put_in([Access.key!(:data), :input], %{secret: "conflicting-input"})

    append_dispatch!([
      expired,
      entry!(:attempt_claimed, %{
        run_id: "expired-run",
        runnable_key: "expired-step",
        claim_id: "claim-expired",
        claim_token_hash: "secret-claim-token",
        owner_id: "secret-worker",
        queue: "critical",
        lease_until: ~U[2026-07-11 11:10:00Z],
        occurred_at: ~U[2026-07-11 11:05:00Z]
      }),
      overdue,
      conflicting_overdue
    ])

    assert {:ok, report} =
             Doctor.report(now: @now, journal_storage: @storage, queue: "critical")

    checks = Map.new(report.checks, &{&1.id, &1})

    assert %{status: :fail, details: %{count: 1}} = checks.expired_claims
    assert %{status: :warn, details: %{count: 1}} = checks.overdue_attempts

    assert %{
             status: :fail,
             details: %{
               findings: [
                 %{
                   run_id: "terminal-run",
                   pending_dispatch_count: 1,
                   pending_result_count: 0
                 }
               ]
             }
           } = checks.terminal_pending_facts

    assert %{status: :warn, details: %{count: 1}} = checks.manual_intervention
    assert %{status: :warn, details: %{count: 1}} = checks.projection_anomalies

    encoded = Jason.encode!(report)
    refute encoded =~ "secret-claim-token"
    refute encoded =~ "secret-worker"
    refute encoded =~ "manual-secret"
    refute encoded =~ "conflicting-input"
  end

  defp append_catalog!(run_id) do
    append!([
      entry!(:run_cataloged, %{
        run_id: run_id,
        workflow: "BillingWorkflow",
        queue: "critical",
        occurred_at: @earlier
      })
    ])
  end

  defp append_run!(_run_id, entries), do: append!(entries)
  defp append_dispatch!(entries), do: append!(entries)

  defp append!(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp run_started(run_id) do
    entry!(:run_started, %{
      run_id: run_id,
      workflow: "BillingWorkflow",
      occurred_at: @earlier
    })
  end

  defp runnables_planned(run_id, runnable_key) do
    entry!(:runnables_planned, %{
      run_id: run_id,
      runnables: [
        %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: "idempotency-#{runnable_key}",
          attempt_number: 1,
          queue: "critical",
          step: runnable_key,
          input: %{secret: "input-secret"},
          visible_at: @earlier
        }
      ],
      occurred_at: @earlier
    })
  end

  defp scheduled(run_id, runnable_key) do
    entry!(:attempt_scheduled, %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: "idempotency-#{runnable_key}",
      attempt_number: 1,
      queue: "critical",
      step: runnable_key,
      input: %{secret: "input-secret"},
      visible_at: @earlier,
      occurred_at: @earlier
    })
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp drop_tables do
    [:checkpoints, :threads, :thread_meta]
    |> Enum.map(&String.to_existing_atom("squidie_doctor_findings_test_#{&1}"))
    |> Enum.each(fn table ->
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)
  end
end
