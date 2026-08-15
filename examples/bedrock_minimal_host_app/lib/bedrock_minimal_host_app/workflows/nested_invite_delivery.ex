defmodule BedrockMinimalHostApp.Workflows.NestedInviteDelivery do
  @moduledoc """
  Parent workflow used by the nested workflow smoke path.
  """

  use Jizoku.Workflow

  workflow do
    trigger :nested_invite_delivery do
      manual()

      payload do
        field :party_id, :string
        field :guest_id, :string
        field :child_queue, :string
        field :fail_after_child_start, :boolean, default: false
        field :fail_child_once, :boolean, default: false
      end
    end

    trigger :scheduled_nested_invite do
      cron "@reboot", timezone: "Etc/UTC", idempotency: :return_existing_run

      payload do
        field :party_id, :string, default: "party_bedrock"
        field :guest_id, :string, default: "guest_bedrock"
        field :child_queue, :string, default: "bedrock_nested_child"
        field :fail_after_child_start, :boolean, default: true
        field :fail_child_once, :boolean, default: true
      end
    end

    step :start_nested_invite, BedrockMinimalHostApp.Steps.StartNestedInvite,
      retry: [max_attempts: 2]

    transition :start_nested_invite, on: :ok, to: :complete
  end
end
