defmodule BedrockMinimalHostApp.Workflows.PaymentRecovery do
  @moduledoc """
  Example workflow used by the host app harness.
  """

  use Squidie.Workflow

  workflow do
    trigger :payment_recovery do
      manual()

      payload do
        field :account_id, :string
        field :invoice_id, :string
        field :attempt_id, :string
        field :gateway_url, :string
      end
    end

    step :load_invoice, BedrockMinimalHostApp.Steps.LoadInvoice

    step :check_gateway_status, BedrockMinimalHostApp.Steps.CheckGatewayStatus,
      retry: [max_attempts: 5, backoff: [type: :exponential, min: 1_000, max: 1_000]],
      deadline: [within: 30_000, due_soon: 5_000, escalation: :diagnostic]

    step :issue_gateway_credit, BedrockMinimalHostApp.Steps.IssueGatewayCredit
    step :notify_customer, BedrockMinimalHostApp.Steps.NotifyCustomer, compensatable: false

    transition :load_invoice, on: :ok, to: :check_gateway_status

    transition :check_gateway_status,
      on: :ok,
      to: :notify_customer,
      condition: [path: [:gateway_check, :status_code], greater_than: 199]

    transition :check_gateway_status, on: :ok, to: :issue_gateway_credit

    transition :check_gateway_status,
      on: :error,
      to: :issue_gateway_credit,
      recovery: :compensation

    transition :issue_gateway_credit, on: :ok, to: :complete
    transition :notify_customer, on: :ok, to: :complete
  end
end
