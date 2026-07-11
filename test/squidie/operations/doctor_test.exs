defmodule Squidie.Operations.DoctorTest do
  use ExUnit.Case, async: false

  alias Squidie.Operations.Doctor

  @now ~U[2026-07-11 12:00:00Z]

  test "reports an empty custom-storage runtime as healthy and JSON-safe" do
    storage = {Jido.Storage.ETS, table: :squidie_doctor_empty_test}

    assert {:ok, report} = Doctor.report(now: @now, journal_storage: storage, queue: "critical")
    assert report.schema_version == 1
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
end
