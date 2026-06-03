defmodule MinimalHostApp.Steps.DeliverInvite do
  @moduledoc """
  Example child workflow step that records an invite delivery.
  """

  use Squidie.Step,
    name: :deliver_invite,
    description: "Records an invite delivery for a nested workflow",
    input_schema: [
      party_id: [type: :string, required: true],
      guest_id: [type: :string, required: true],
      fail_child_once: [type: :boolean, required: false]
    ],
    output_schema: [
      invite_delivery: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()} | {:retry, map()}
  def run(
        %{party_id: party_id, guest_id: guest_id} = input,
        %Squidie.Step.Context{attempt: attempt}
      ) do
    if Map.get(input, :fail_child_once, false) and attempt == 1 do
      {:retry, %{message: "retry child invite delivery", code: "retry_child_invite"}}
    else
      {:ok,
       %{
         invite_delivery: %{
           party_id: party_id,
           guest_id: guest_id,
           status: "delivered"
         }
       }}
    end
  end
end
