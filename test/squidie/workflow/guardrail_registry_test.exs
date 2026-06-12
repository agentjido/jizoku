defmodule Squidie.Workflow.GuardrailRegistryTest do
  use ExUnit.Case, async: true

  defmodule AllowAmount do
    @spec validate_guardrail(map(), map()) :: :ok
    def validate_guardrail(_value, _context), do: :ok
  end

  defmodule DenyAmount do
    @spec validate_guardrail(map(), map()) :: {:error, map()}
    def validate_guardrail(_value, context) do
      {:error,
       %{
         message: "amount is outside host policy",
         placement: context.placement
       }}
    end
  end

  defmodule IncompatibleGuardrail do
    @spec run(map(), map()) :: :ok
    def run(_value, _context), do: :ok
  end

  describe "catalog/1" do
    test "exposes editor-safe guardrail metadata" do
      registry = %{
        "billing.amount_limit" => [
          module: AllowAmount,
          display_name: "Amount limit",
          category: "Billing",
          description: "Checks host-owned amount policy",
          input_contract: %{amount: [type: :integer]},
          config_schema: %{max: [type: :integer]},
          credentials: %{api_key: "secret"}
        ]
      }

      assert {:ok, [entry]} = Squidie.Workflow.guardrail_catalog(registry)

      assert entry == %{
               key: "billing.amount_limit",
               display_name: "Amount limit",
               category: "Billing",
               description: "Checks host-owned amount policy",
               enabled?: true,
               input_contract: %{"amount" => %{"type" => "integer"}},
               config_schema: %{"max" => %{"type" => "integer"}}
             }

      refute inspect(entry) =~ inspect(AllowAmount)
      refute inspect(entry) =~ "secret"
    end

    test "keeps disabled guardrails visible but unavailable at validation time" do
      registry = %{"billing.amount_limit" => [module: AllowAmount, enabled?: false]}

      assert {:ok, [entry]} = Squidie.Workflow.GuardrailRegistry.catalog(registry)
      assert entry.enabled? == false

      assert {:error, :disabled_guardrail_key} =
               Squidie.Workflow.GuardrailRegistry.validate_guardrail(
                 "billing.amount_limit",
                 registry
               )
    end

    test "rejects incompatible catalog entries" do
      registry = %{"billing.amount_limit" => IncompatibleGuardrail}

      assert {:error, {:invalid_guardrail_catalog, errors}} =
               Squidie.Workflow.GuardrailRegistry.catalog(registry)

      assert %{
               path: [:guardrails, "billing.amount_limit"],
               code: :incompatible_guardrail_module,
               message:
                 "guardrail \"billing.amount_limit\" references an incompatible validator module",
               details: %{guardrail: "billing.amount_limit"}
             } in errors
    end
  end

  describe "validate_spec/2 with a guardrail registry" do
    test "accepts input, action, and output guardrail placements" do
      spec =
        spec_with_guardrails(
          input: ["billing.amount_limit"],
          action: [[key: "billing.amount_limit", policy: :route_error]],
          output: [%{key: "billing.amount_limit", policy: :route_error}]
        )

      assert :ok =
               Squidie.Workflow.validate_spec(spec,
                 guardrail_registry: %{"billing.amount_limit" => AllowAmount}
               )
    end

    test "rejects unknown guardrail keys before publish or activation" do
      spec = spec_with_guardrails(input: ["billing.missing"])

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, guardrail_registry: %{})

      assert %{
               path: [:steps, 0, :opts, :guardrails, :input, 0, :key],
               code: :unknown_guardrail_key,
               message: "step :charge_card references unknown input guardrail key",
               details: %{step: :charge_card, guardrail: "billing.missing", placement: :input}
             } in errors
    end

    test "rejects disabled guardrail keys before publish or activation" do
      spec = spec_with_guardrails(action: ["billing.amount_limit"])
      registry = %{"billing.amount_limit" => [module: AllowAmount, enabled?: false]}

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, guardrail_registry: registry)

      assert %{
               path: [:steps, 0, :opts, :guardrails, :action, 0, :key],
               code: :disabled_guardrail_key,
               message: "step :charge_card references disabled action guardrail key",
               details: %{
                 step: :charge_card,
                 guardrail: "billing.amount_limit",
                 placement: :action
               }
             } in errors
    end

    test "rejects invalid guardrail policy values" do
      spec = spec_with_guardrails(output: [[key: "billing.amount_limit", policy: :retry]])

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec,
                 guardrail_registry: %{"billing.amount_limit" => AllowAmount}
               )

      assert %{
               path: [:steps, 0, :opts, :guardrails, :output, 0, :policy],
               code: :invalid_guardrail_policy,
               message: "step :charge_card defines an invalid output guardrail policy",
               details: %{step: :charge_card, guardrail: "billing.amount_limit", policy: :retry}
             } in errors
    end

    test "rejects policies that cannot be honored for the placement" do
      spec =
        spec_with_guardrails(
          input: [[key: "billing.amount_limit", policy: :route_error]],
          action: [[key: "billing.amount_limit", policy: :block_run_start]]
        )

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec,
                 guardrail_registry: %{"billing.amount_limit" => AllowAmount}
               )

      assert %{
               path: [:steps, 0, :opts, :guardrails, :input, 0, :policy],
               code: :invalid_guardrail_policy,
               message: "step :charge_card defines an invalid input guardrail policy",
               details: %{
                 step: :charge_card,
                 guardrail: "billing.amount_limit",
                 policy: :route_error
               }
             } in errors

      assert %{
               path: [:steps, 0, :opts, :guardrails, :action, 0, :policy],
               code: :invalid_guardrail_policy,
               message: "step :charge_card defines an invalid action guardrail policy",
               details: %{
                 step: :charge_card,
                 guardrail: "billing.amount_limit",
                 policy: :block_run_start
               }
             } in errors
    end

    test "rejects malformed guardrail config" do
      spec = spec_with_guardrails(input: [[key: "billing.amount_limit", config: "strict"]])

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec,
                 guardrail_registry: %{"billing.amount_limit" => AllowAmount}
               )

      assert %{
               path: [:steps, 0, :opts, :guardrails, :input, 0, :config],
               code: :invalid_guardrail_config,
               message: "step :charge_card defines an invalid input guardrail",
               details: %{
                 step: :charge_card,
                 placement: :input,
                 guardrail: %{key: "billing.amount_limit", config: "strict"},
                 config: "strict"
               }
             } in errors
    end
  end

  defp spec_with_guardrails(guardrails) do
    %Squidie.Workflow.Spec{
      workflow: __MODULE__.RuntimeAuthoredWorkflow,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :amount, type: :integer, opts: []}]
        }
      ],
      payload: [%{name: :amount, type: :integer, opts: []}],
      steps: [
        %{
          name: :charge_card,
          module: AllowAmount,
          opts: [guardrails: guardrails]
        }
      ],
      transitions: [%{from: :charge_card, on: :ok, to: :complete}],
      retries: [],
      entry_steps: [:charge_card],
      initial_step: :charge_card,
      entry_step: :charge_card
    }
  end
end
