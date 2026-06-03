defmodule MinimalHostApp.Workflows.DependencyRecovery do
  @moduledoc """
  Example dependency-based workflow with two roots and one join step.
  """

  use Squidie.Workflow

  workflow do
    version "2026-05-26.dependency-recovery"

    trigger :dependency_recovery do
      manual()

      payload do
        field :account_id, :string
        field :invoice_id, :string
        field :attempt_id, :string
      end
    end

    step :load_account, MinimalHostApp.Steps.LoadAccount
    step :load_invoice, MinimalHostApp.Steps.LoadInvoice

    step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
      after: [:load_account, :load_invoice],
      input: [
        account_id: [:account, :id],
        invoice_id: [:invoice, :id],
        account_tier: [:account, :tier]
      ]
  end
end
