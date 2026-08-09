defmodule Squidie.Runtime.Journal.ContinuationIntentTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.Journal.ContinuationIntent
  alias Squidie.Workflow.Definition

  defmodule Noop do
    use Jido.Action,
      name: "continuation_intent_noop",
      description: "No-op action for continuation intent validation",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:ok, %{}}
    end
  end

  defmodule MultiTriggerWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :alpha do
        manual()

        payload do
          field :alpha_value, :string
        end
      end

      trigger :beta do
        manual()

        payload do
          field :beta_value, :integer
        end
      end

      step :noop, Noop
      transition :noop, on: :ok, to: :complete
    end
  end

  defmodule DefaultedWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :continue do
        manual()

        payload do
          field :cursor, :string
          field :batch_size, :integer, default: 100
        end
      end

      step :noop, Noop
      transition :noop, on: :ok, to: :complete
    end
  end

  test "validates resolved input against only the selected trigger contract" do
    assert {:ok, definition} = Definition.load(MultiTriggerWorkflow)

    intent = %ContinuationIntent{
      run_id: "11111111-1111-5111-8111-111111111111",
      successor_run_id: "22222222-2222-5222-8222-222222222222",
      continuation_key: "page-42",
      workflow: Definition.serialize_workflow(MultiTriggerWorkflow),
      trigger: "alpha",
      input: %{alpha_value: "next"},
      definition: :current,
      definition_version: definition.definition_version,
      definition_fingerprint: Definition.fingerprint(definition),
      queue: "default",
      trace: %{
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
        span_id: "00f067aa0ba902b7"
      },
      occurred_at: ~U[2026-08-09 17:00:00Z]
    }

    assert :ok = ContinuationIntent.validate_current_target(intent)
  end

  test "rejects persisted target input whose declared defaults were not resolved" do
    assert {:ok, definition} = Definition.load(DefaultedWorkflow)

    intent = %ContinuationIntent{
      run_id: "11111111-1111-5111-8111-111111111111",
      successor_run_id: "22222222-2222-5222-8222-222222222222",
      continuation_key: "page-42",
      workflow: Definition.serialize_workflow(DefaultedWorkflow),
      trigger: "continue",
      input: %{cursor: "next"},
      definition: :current,
      definition_version: definition.definition_version,
      definition_fingerprint: Definition.fingerprint(definition),
      queue: "default",
      trace: %{
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
        span_id: "00f067aa0ba902b7"
      },
      occurred_at: ~U[2026-08-09 17:00:00Z]
    }

    assert {:error, {:invalid_continuation_target, :unresolved_input}} =
             ContinuationIntent.validate_current_target(intent)
  end
end
