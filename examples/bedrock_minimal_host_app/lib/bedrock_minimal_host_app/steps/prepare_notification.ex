defmodule BedrockMinimalHostApp.Steps.PrepareNotification do
  @moduledoc """
  Example join step that waits for account and invoice context.
  """

  use Squidie.Step,
    name: :prepare_notification,
    description: "Builds notification context once dependencies are ready",
    input_schema: [
      account: [type: :map, required: true],
      invoice: [type: :map, required: true]
    ],
    output_schema: [
      notification: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()}
  def run(%{account: account, invoice: invoice}, _context) do
    {:ok,
     %{
       notification: %{
         channel: "email",
         account_id: account.id,
         invoice_id: invoice.id,
         account_tier: account.tier
       }
     }}
  end
end
