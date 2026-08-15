defmodule BedrockMinimalHostApp.ActionRegistryTest do
  use ExUnit.Case, async: true

  alias BedrockMinimalHostApp.Steps
  alias BedrockMinimalHostApp.WorkflowRuns
  alias BedrockMinimalHostApp.Workflows.PaymentRecovery
  alias BedrockMinimalHostApp.Workflows.RawJidoWorkflow

  test "validates runtime-authored specs through host-owned action keys" do
    spec = %Squidie.Workflow.Spec{
      workflow: BedrockMinimalHostApp.RuntimeAuthoredPaymentRecovery,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [
            %{name: :account_id, type: :string, opts: []},
            %{name: :invoice_id, type: :string, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :account_id, type: :string, opts: []},
        %{name: :invoice_id, type: :string, opts: []}
      ],
      steps: [
        %{name: :load_invoice, action: "payment.load_invoice", opts: []},
        %{name: :notify_customer, action: "payment.notify_customer", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :notify_customer},
        %{from: :notify_customer, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }

    registry = %{
      "payment.load_invoice" => Steps.LoadInvoice,
      "payment.notify_customer" => Steps.NotifyCustomer
    }

    assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: registry)

    assert {:ok, resolved} =
             Squidie.Workflow.resolve_spec_actions(spec, action_registry: registry)

    assert Enum.map(resolved.steps, &{&1.name, &1.module, &1.metadata.action}) == [
             {:load_invoice, Steps.LoadInvoice, "payment.load_invoice"},
             {:notify_customer, Steps.NotifyCustomer, "payment.notify_customer"}
           ]
  end

  test "starts a runtime-authored workflow through the host boundary" do
    storage = {Jido.Storage.ETS, table: :bedrock_minimal_host_app_runtime_spec_test}
    run_id = "00000000-0000-4000-8000-000000000257"

    assert {:ok, run} =
             WorkflowRuns.start_runtime_digest(
               %{channel: "ops", digest_date: "2026-05-30"},
               journal_storage: storage,
               run_id: run_id
             )

    assert run.workflow == "Elixir.BedrockMinimalHostApp.RuntimeAuthoredDigest"
    assert run.trigger == "manual_digest"
    assert run.definition_version == "bedrock-minimal-host-runtime-digest-v1"
    assert [%{step: "record_digest_delivery", status: :available}] = run.visible_attempts

    assert {:ok, completed_run} = Squidie.execute_next(journal_storage: storage)
    assert completed_run.status == :completed
  end

  test "executes a raw Jido action through the normal journal runtime" do
    assert {:ok, runtime} =
             Squidie.Test.start_runtime(
               workflow: RawJidoWorkflow,
               now: ~U[2026-08-10 12:00:00Z]
             )

    on_exit(fn -> Squidie.Test.stop_runtime(runtime) end)

    assert {:ok, run} = Squidie.Test.start(runtime, %{value: "bedrock"})
    assert {:completed, completed} = Squidie.Test.drain(runtime, run)

    assert completed.context.jido_result == %{
             value: "BEDROCK",
             run_id: run.run_id
           }
  end

  test "compiled payment recovery workflow exposes numeric gateway routing condition" do
    assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

    assert Enum.any?(spec.transitions, fn
             %{
               from: :check_gateway_status,
               on: :ok,
               to: :notify_customer,
               condition: %{path: [:gateway_check, :status_code], greater_than: 199}
             } ->
               true

             _transition ->
               false
           end)

    assert Enum.any?(spec.transitions, fn transition ->
             match?(
               %{from: :check_gateway_status, on: :ok, to: :issue_gateway_credit},
               transition
             ) and is_nil(Map.get(transition, :condition))
           end)
  end
end
