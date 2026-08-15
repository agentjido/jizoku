defmodule BedrockMinimalHostApp.Steps.VoidPaymentAuthorization do
  @moduledoc """
  Voids a payment authorization created by the saga checkout workflow.

  This compensation callback demonstrates a business-level inverse operation
  rather than a same-step fallback.
  """

  use Jizoku.Step,
    name: :void_payment_authorization,
    description: "Voids a previous payment authorization",
    input_schema: [
      step: [type: :map, required: true]
    ],
    output_schema: [
      voided_payment_authorization: [type: :map, required: true]
    ]

  @impl true
  def run(%{step: %{output: %{payment_authorization: authorization}}}, _context) do
    {:ok, %{voided_payment_authorization: Map.put(authorization, :status, "voided")}}
  end
end
