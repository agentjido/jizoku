defmodule MinimalHostApp.Steps.AuthorizePayment do
  @moduledoc """
  Creates a reversible payment authorization for the saga checkout example.

  The authorization is intentionally separate from capture so the workflow can
  void it during compensation if capture fails.
  """

  use Jizoku.Step,
    name: :authorize_payment,
    description: "Authorizes payment for an order",
    input_schema: [
      account_id: [type: :string, required: true],
      order_id: [type: :string, required: true]
    ],
    output_schema: [
      payment_authorization: [type: :map, required: true]
    ]

  @impl true
  def run(%{account_id: account_id, order_id: order_id}, _context) do
    {:ok,
     %{
       payment_authorization: %{
         account_id: account_id,
         order_id: order_id,
         authorization_id: "auth_#{order_id}",
         status: "authorized"
       }
     }}
  end
end
