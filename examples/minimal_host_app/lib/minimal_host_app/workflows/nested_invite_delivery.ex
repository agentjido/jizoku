defmodule MinimalHostApp.Workflows.NestedInviteDelivery do
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

    step :start_nested_invite, MinimalHostApp.Steps.StartNestedInvite, retry: [max_attempts: 2]

    transition :start_nested_invite, on: :ok, to: :complete
  end
end
