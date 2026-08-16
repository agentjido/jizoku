defmodule Jizoku.Workflow.VersionRegistryTest do
  use ExUnit.Case, async: true

  alias Jizoku.Config
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.VersionRegistry

  defmodule StepV1 do
    use Jizoku.Step, name: "version_registry_step_v1", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 1}}
    end
  end

  defmodule StepV2 do
    use Jizoku.Step, name: "version_registry_step_v2", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 2}}
    end
  end

  defmodule HistoricalV1 do
    use Jizoku.Workflow

    workflow do
      version "v1"

      trigger :manual do
        manual()
      end

      step :process, Jizoku.Workflow.VersionRegistryTest.StepV1
      transition :process, on: :ok, to: :complete
    end
  end

  defmodule CurrentWorkflow do
    use Jizoku.Workflow

    workflow do
      version "v2"

      trigger :manual do
        manual()
      end

      step :process, Jizoku.Workflow.VersionRegistryTest.StepV2
      transition :process, on: :ok, to: :complete
    end
  end

  test "resolves only a trusted registered implementation with an exact fingerprint" do
    fingerprint = Definition.fingerprint(HistoricalV1.workflow_definition())

    assert {:ok, CurrentWorkflow, definition} =
             VersionRegistry.resolve(
               CurrentWorkflow,
               "v1",
               fingerprint,
               workflow_versions()
             )

    assert definition.definition_version == "v1"
    assert Definition.fingerprint(definition) == fingerprint
  end

  test "fails closed when the persisted version is not registered" do
    assert {:error,
            %{
              code: "workflow_version_unavailable",
              workflow: workflow,
              requested_version: "v0",
              available_versions: ["v1", "v2"]
            }} =
             VersionRegistry.resolve(CurrentWorkflow, "v0", "fingerprint", workflow_versions())

    assert workflow == inspect(CurrentWorkflow)
  end

  test "fails closed when the registered implementation fingerprint does not match" do
    assert {:error,
            %{
              code: "workflow_version_fingerprint_mismatch",
              requested_version: "v1",
              persisted_definition_fingerprint: "wrong",
              resolved_definition_fingerprint: resolved
            }} =
             VersionRegistry.resolve(CurrentWorkflow, "v1", "wrong", workflow_versions())

    assert resolved == Definition.fingerprint(HistoricalV1.workflow_definition())
  end

  test "validates version keys against implementation definitions" do
    invalid = %{CurrentWorkflow => %{"v9" => HistoricalV1}}

    assert {:error,
            {:invalid_workflow_versions,
             [
               %{
                 code: :definition_version_mismatch,
                 configured_version: "v9",
                 definition_version: "v1"
               }
             ]}} = VersionRegistry.validate(invalid)
  end

  test "rejects malformed registry values without loading persisted module names" do
    assert {:error, {:invalid_workflow_versions, [%{code: :invalid_registry}]}} =
             VersionRegistry.validate([])

    assert {:error, {:invalid_workflow_versions, [%{code: :invalid_versions}]}} =
             VersionRegistry.validate(%{CurrentWorkflow => ["v1"]})

    assert {:error, {:invalid_workflow_versions, [%{code: :invalid_implementation}]}} =
             VersionRegistry.validate(%{CurrentWorkflow => %{"v1" => "Elixir.Untrusted"}})
  end

  test "validates the registry at the application configuration boundary" do
    registry = workflow_versions()

    assert {:ok, %Config{workflow_versions: ^registry}} =
             Config.load(workflow_versions: registry)

    assert {:error, {:invalid_config, [workflow_versions: [%{code: :invalid_registry}]]}} =
             Config.load(workflow_versions: [])
  end

  defp workflow_versions do
    %{CurrentWorkflow => %{"v1" => HistoricalV1, "v2" => CurrentWorkflow}}
  end
end
