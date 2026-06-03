defmodule MinimalHostApp.Workflows.InviteDelivery do
  @moduledoc """
  Child workflow used by the nested workflow smoke path.
  """

  use Squidie.Workflow

  workflow do
    trigger :deliver_invite do
      manual()

      payload do
        field :party_id, :string
        field :guest_id, :string
        field :fail_child_once, :boolean, default: false
      end
    end

    step :deliver_invite, MinimalHostApp.Steps.DeliverInvite, retry: [max_attempts: 2]

    transition :deliver_invite, on: :ok, to: :complete
  end
end
