defmodule Jizoku.Workflow.MigrationTest do
  use ExUnit.Case, async: true

  alias Jizoku.Workflow.Migration

  defmodule ValidMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "valid-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(state) do
      {:ok, %{context: Map.put(state.context, :version, 2), manual_step: :gate_v2}}
    end
  end

  defmodule SameVersionMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "same-version"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v1"
    end

    @impl Migration
    def migrate(state) do
      {:ok, %{context: state.context, manual_step: :gate}}
    end
  end

  defmodule OversizedMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "oversized-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(_state) do
      {:ok, %{context: %{payload: String.duplicate("x", 65_537)}, manual_step: :gate}}
    end
  end

  defmodule MissingCallbackMigration do
    def key do
      "missing-callback"
    end
  end

  defmodule InvalidIdentifierMigration do
    @behaviour Migration

    @impl Migration
    def key do
      ""
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(state) do
      {:ok, %{context: state.context}}
    end
  end

  defmodule DefaultStepMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "default-step-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(state) do
      {:ok, %{context: Map.put(state.context, :version, 2)}}
    end
  end

  defmodule FailedMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "failed-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(_state) do
      {:error, :not_applicable}
    end
  end

  defmodule InvalidReturnMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "invalid-return-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(_state) do
      :invalid
    end
  end

  defmodule RaisingMigration do
    @behaviour Migration

    @impl Migration
    def key do
      "raising-v1-to-v2"
    end

    @impl Migration
    def source_version do
      "v1"
    end

    @impl Migration
    def target_version do
      "v2"
    end

    @impl Migration
    def migrate(_state) do
      raise "host callback failure"
    end
  end

  test "loads a valid host-owned contract and normalizes its bounded result" do
    assert {:ok, contract} = Migration.contract(ValidMigration)

    assert contract == %{
             module: ValidMigration,
             key: "valid-v1-to-v2",
             source_version: "v1",
             target_version: "v2"
           }

    assert {:ok, %{context: %{version: 2}, manual_step: :gate_v2}} =
             Migration.apply(contract, state())
  end

  test "rejects malformed contracts and same-version migrations" do
    assert {:error, {:invalid_workflow_migration, :invalid_module}} =
             Migration.contract("untrusted-module-name")

    assert {:error, {:invalid_workflow_migration, :same_version}} =
             Migration.contract(SameVersionMigration)
  end

  test "rejects unloaded modules, missing callbacks, and invalid identifiers" do
    assert {:error, {:invalid_workflow_migration, :invalid_module}} =
             Migration.contract(__MODULE__.NotLoaded)

    assert {:error, {:invalid_workflow_migration, :missing_callback}} =
             Migration.contract(MissingCallbackMigration)

    assert {:error, {:invalid_workflow_migration, :key}} =
             Migration.contract(InvalidIdentifierMigration)
  end

  test "defaults the target manual step to the current durable boundary" do
    assert {:ok, contract} = Migration.contract(DefaultStepMigration)

    assert {:ok, %{context: %{version: 2}, manual_step: "gate"}} =
             Migration.apply(contract, state())
  end

  test "normalizes host failures and invalid callback returns" do
    assert {:ok, failed_contract} = Migration.contract(FailedMigration)

    assert {:error, {:workflow_migration_failed, :not_applicable}} =
             Migration.apply(failed_contract, state())

    assert {:ok, invalid_contract} = Migration.contract(InvalidReturnMigration)

    assert {:error, {:invalid_workflow_migration_result, :return}} =
             Migration.apply(invalid_contract, state())
  end

  test "contains exceptions raised by host migration callbacks" do
    assert {:ok, contract} = Migration.contract(RaisingMigration)

    assert {:error, {:workflow_migration_failed, {:callback_exception, RuntimeError}}} =
             Migration.apply(contract, state())
  end

  test "rejects transformed context beyond the durable command bound" do
    assert {:ok, contract} = Migration.contract(OversizedMigration)

    assert {:error, {:invalid_workflow_migration_result, :context_bounds}} =
             Migration.apply(contract, state())
  end

  defp state do
    %{
      context: %{},
      manual_state: %{step: "gate", kind: "pause"},
      source_version: "v1",
      source_fingerprint: "source-fingerprint",
      target_version: "v2",
      target_fingerprint: "target-fingerprint"
    }
  end
end
