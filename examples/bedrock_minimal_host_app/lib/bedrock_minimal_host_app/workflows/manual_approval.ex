defmodule BedrockMinimalHostApp.Workflows.ManualApproval do
  @moduledoc """
  Example workflow that waits for an explicit operator approval or rejection.
  """

  use Squidie.Workflow

  workflow do
    trigger :manual_approval do
      manual()

      payload do
        field :account_id, :string
      end
    end

    approval_step :wait_for_approval,
      output: :approval,
      deadline: [within: 300_000, due_soon: 60_000, escalation: :operator_action]

    step :record_approval, BedrockMinimalHostApp.Steps.RecordApproval,
      input: [:account_id, :approval],
      output: :approval

    step :record_rejection, BedrockMinimalHostApp.Steps.RecordRejection,
      input: [:account_id, :approval],
      output: :approval

    transition :wait_for_approval, on: :ok, to: :record_approval
    transition :wait_for_approval, on: :error, to: :record_rejection
    transition :record_approval, on: :ok, to: :complete
    transition :record_rejection, on: :ok, to: :complete
  end
end
