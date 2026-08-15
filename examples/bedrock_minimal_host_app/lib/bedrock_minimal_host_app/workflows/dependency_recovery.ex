defmodule BedrockMinimalHostApp.Workflows.DependencyRecovery do
  @moduledoc """
  Example dependency-based workflow with two roots and one join step.
  """

  use Jizoku.Workflow

  workflow do
    trigger :dependency_recovery do
      manual()

      payload do
        field :account_id, :string
        field :invoice_id, :string
        field :attempt_id, :string
      end
    end

    step :load_account, BedrockMinimalHostApp.Steps.LoadAccount
    step :load_invoice, BedrockMinimalHostApp.Steps.LoadInvoice

    step :prepare_notification, BedrockMinimalHostApp.Steps.PrepareNotification,
      after: [:load_account, :load_invoice]
  end
end
