defmodule MinimalHostApp.Steps.NotifyCustomer do
  @moduledoc """
  Example step that records customer notification intent.
  """

  use Squidie.Step,
    name: :notify_customer,
    description: "Records notification intent",
    input_schema: [
      invoice: [type: :map, required: true],
      gateway_check: [type: :map, required: true]
    ],
    output_schema: [
      notification: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()}
  def run(%{invoice: invoice, gateway_check: gateway_check}, _context) do
    {:ok,
     %{
       notification: %{
         channel: "email",
         invoice_id: invoice.id,
         gateway_status: gateway_check.status
       }
     }}
  end
end
