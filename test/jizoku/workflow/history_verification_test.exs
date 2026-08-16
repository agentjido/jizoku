defmodule Jizoku.Workflow.HistoryVerificationTest do
  use ExUnit.Case, async: true

  alias Jizoku.Workflow.Definition

  defmodule StepV1 do
    use Jizoku.Step, name: "history_verification_v1", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 1}}
    end
  end

  defmodule StepV2 do
    use Jizoku.Step, name: "history_verification_v2", input_schema: []

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

      step :process, StepV1
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

      step :process, StepV2
      transition :process, on: :ok, to: :complete
    end
  end

  test "verifies checked-in golden histories against exact registered code" do
    fixture = fixture(Definition.fingerprint(HistoricalV1.workflow_definition()))

    assert {:ok,
            %{
              schema_version: 1,
              total: 1,
              verified: 1,
              incompatible: 0,
              histories: [
                %{
                  status: :verified,
                  workflow: workflow,
                  definition_version: "v1",
                  event_count: 2
                }
              ]
            }} = Jizoku.Workflow.verify_history_fixtures([fixture], registry())

    assert workflow == inspect(CurrentWorkflow)
  end

  test "reports unavailable and fingerprint-mismatched historical code" do
    fingerprint = Definition.fingerprint(HistoricalV1.workflow_definition())

    assert {:error, %{incompatible: 1, histories: [missing]}} =
             Jizoku.Workflow.verify_history_fixtures(
               [fixture(fingerprint)],
               %{CurrentWorkflow => %{"v2" => CurrentWorkflow}}
             )

    assert missing.status == :incompatible
    assert missing.error.code == "workflow_version_unavailable"
    assert missing.error.available_versions == ["v2"]

    assert {:error, %{incompatible: 1, histories: [mismatch]}} =
             Jizoku.Workflow.verify_history_fixtures([fixture("0" <> fingerprint)], registry())

    assert mismatch.error.code == "workflow_version_fingerprint_mismatch"
    refute Map.has_key?(mismatch, :golden_history)
  end

  test "rejects malformed fixtures without turning persisted names into atoms" do
    workflow_name = "Elixir.UntrustedHistory#{System.unique_integer([:positive])}"

    assert_raise ArgumentError, fn -> String.to_existing_atom(workflow_name) end

    fixture = fixture(Definition.fingerprint(HistoricalV1.workflow_definition()))

    malformed =
      fixture
      |> Map.put(:workflow, workflow_name)
      |> put_in([:golden_history, :workflow], workflow_name)

    assert {:error, %{histories: [%{error: error}]}} =
             Jizoku.Workflow.verify_history_fixtures([malformed], registry())

    assert error == %{code: "invalid_history_fixture", fields: [:workflow]}
    assert_raise ArgumentError, fn -> String.to_existing_atom(workflow_name) end
  end

  test "rejects placeholder fixtures without durable events" do
    fingerprint = Definition.fingerprint(HistoricalV1.workflow_definition())
    fixture = put_in(fixture(fingerprint), [:golden_history, :events], [])

    assert {:error, %{histories: [%{error: error}]}} =
             Jizoku.Workflow.verify_history_fixtures([fixture], registry())

    assert error == %{code: "invalid_history_fixture", fields: [:golden_history]}
  end

  defp fixture(fingerprint) do
    %{
      workflow: CurrentWorkflow,
      definition_version: "v1",
      definition_fingerprint: fingerprint,
      golden_history: %{
        schema_version: 1,
        workflow: Atom.to_string(CurrentWorkflow),
        queue: "default",
        partition: nil,
        status: :completed,
        terminal_status: :completed,
        events: [
          %{type: :run_started, offset_us: 0, run: "run-1", status: :running},
          %{type: :run_terminal, offset_us: 1, run: "run-1", status: :completed}
        ]
      }
    }
  end

  defp registry do
    %{CurrentWorkflow => %{"v1" => HistoricalV1, "v2" => CurrentWorkflow}}
  end
end
