defmodule Squidie.Workflow.ActionRegistryTest do
  use ExUnit.Case, async: true

  defmodule NativeLoadInvoice do
    use Squidie.Step,
      name: :load_invoice,
      input_schema: [
        invoice_id: [type: :string, required: true]
      ],
      output_schema: [
        invoice: [type: :map, required: true]
      ]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{invoice: %{id: "inv_123"}}}
  end

  defmodule JidoSendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends an invoice reminder email"

    @impl Jido.Action
    def run(_params, _context), do: {:ok, %{sent?: true}}
  end

  defmodule IncompatibleAction do
    def run(_params, _context), do: {:ok, %{}}
  end

  describe "validate_spec/2 with an action registry" do
    test "accepts runtime specs that reference configured action keys" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, action: "billing.load_invoice", opts: []},
          %{name: :send_email, action: "billing.send_email", opts: []}
        ])

      registry = %{
        "billing.load_invoice" => NativeLoadInvoice,
        "billing.send_email" => JidoSendEmail
      }

      assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: registry)
    end

    test "rejects unknown action keys before activation" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, action: "billing.missing", opts: []}
        ])

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, action_registry: %{})

      assert %{
               path: [:steps, 0, :action],
               code: :unknown_action_key,
               message: "step :load_invoice references unknown action key",
               details: %{step: :load_invoice, action: "billing.missing"}
             } in errors
    end

    test "rejects disabled action keys" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, action: "billing.load_invoice", opts: []}
        ])

      registry = %{"billing.load_invoice" => [module: NativeLoadInvoice, enabled?: false]}

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, action_registry: registry)

      assert %{
               path: [:steps, 0, :action],
               code: :disabled_action_key,
               message: "step :load_invoice references disabled action key",
               details: %{step: :load_invoice, action: "billing.load_invoice"}
             } in errors
    end

    test "rejects action modules that do not satisfy an executable step contract" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, action: "billing.load_invoice", opts: []}
        ])

      registry = %{"billing.load_invoice" => IncompatibleAction}

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, action_registry: registry)

      assert %{
               path: [:steps, 0, :action],
               code: :incompatible_action_module,
               message: "step :load_invoice references an incompatible action module",
               details: %{
                 step: :load_invoice,
                 action: "billing.load_invoice",
                 module: IncompatibleAction
               }
             } in errors
    end

    test "keeps module-authored workflow specs working without registry entries" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, module: NativeLoadInvoice, opts: []}
        ])

      assert :ok = Squidie.Workflow.validate_spec(spec)
    end

    test "keeps module-authored workflow specs working when shared registry opts are present" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, module: NativeLoadInvoice, opts: []}
        ])

      assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: %{})
      assert {:ok, ^spec} = Squidie.Workflow.resolve_spec_actions(spec, action_registry: %{})
    end

    test "routes string-key runtime step collections through the registry wrapper" do
      step = Map.put(%{name: :load_invoice, opts: []}, "action", "billing.load_invoice")

      spec =
        spec_with_steps([
          %{name: :load_invoice, action: "billing.load_invoice", opts: []}
        ])
        |> Map.from_struct()
        |> Map.delete(:steps)
        |> Map.put("steps", [step])

      registry = %{"billing.load_invoice" => NativeLoadInvoice}

      assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: registry)

      assert {:ok, resolved} =
               Squidie.Workflow.resolve_spec_actions(spec, action_registry: registry)

      assert [
               %{
                 name: :load_invoice,
                 action: "billing.load_invoice",
                 module: NativeLoadInvoice,
                 metadata: %{action: "billing.load_invoice"}
               }
             ] = resolved.steps

      refute Map.has_key?(resolved, "steps")
    end

    test "rejects raw module atoms when the action registry boundary is called directly" do
      spec =
        spec_with_steps([
          %{name: :load_invoice, module: NativeLoadInvoice, opts: []}
        ])

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.ActionRegistry.validate_spec(spec, %{})

      assert %{
               path: [:steps, 0, :action],
               code: :missing_action_key,
               message: "step :load_invoice must reference an action key",
               details: %{step: :load_invoice, module: NativeLoadInvoice}
             } in errors
    end

    test "keeps built-in runtime steps valid inside registry-validated specs" do
      spec =
        spec_with_steps([
          %{name: :announce, module: :log, opts: [message: "hello"]}
        ])

      assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: %{})
    end

    test "keeps malformed step collections visible to structural validation" do
      spec = %{
        spec_with_steps([%{name: :load_invoice, action: "billing.load_invoice", opts: []}])
        | steps: "bad"
      }

      assert {:error, {:invalid_workflow_spec, errors}} =
               Squidie.Workflow.validate_spec(spec, action_registry: %{})

      assert %{
               path: [:steps],
               code: :invalid_collection,
               message: "steps must be a list",
               details: %{field: :steps, value: "bad"}
             } in errors
    end
  end

  test "resolve_spec_actions/2 resolves action keys to modules and preserves stable identity" do
    spec =
      spec_with_steps([
        %{name: :load_invoice, action: "billing.load_invoice", opts: []}
      ])

    registry = %{"billing.load_invoice" => NativeLoadInvoice}

    assert {:ok, resolved} =
             Squidie.Workflow.resolve_spec_actions(spec, action_registry: registry)

    assert [
             %{
               name: :load_invoice,
               action: "billing.load_invoice",
               module: NativeLoadInvoice,
               metadata: %{action: "billing.load_invoice"}
             }
           ] = resolved.steps
  end

  defp spec_with_steps(steps) do
    transitions =
      case steps do
        [%{name: only_step}] ->
          [%{from: only_step, on: :ok, to: :complete}]

        [%{name: first_step}, %{name: second_step}] ->
          [%{from: first_step, on: :ok, to: second_step}]
      end

    %Squidie.Workflow.Spec{
      workflow: __MODULE__.RuntimeAuthoredWorkflow,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :invoice_id, type: :string, opts: []}]
        }
      ],
      payload: [%{name: :invoice_id, type: :string, opts: []}],
      steps: steps,
      transitions: transitions,
      retries: [],
      entry_steps: [hd(steps).name],
      initial_step: hd(steps).name,
      entry_step: hd(steps).name
    }
  end
end
