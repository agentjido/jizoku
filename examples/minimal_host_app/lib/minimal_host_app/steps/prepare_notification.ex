defmodule MinimalHostApp.Steps.PrepareNotification do
  @moduledoc """
  Example join step that waits for account and invoice context.
  """

  use Squidie.Step,
    name: :prepare_notification,
    description: "Builds notification context once dependencies are ready",
    input_schema: [
      account_id: [type: :string, required: true],
      invoice_id: [type: :string, required: true],
      account_tier: [type: :string, required: true]
    ],
    output_schema: [
      notification: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()}
  def run(%{account_id: account_id, invoice_id: invoice_id, account_tier: account_tier}, _context) do
    {:ok,
     %{
       notification: %{
         channel: "email",
         account_id: account_id,
         invoice_id: invoice_id,
         account_tier: account_tier
       }
     }}
  end
end
