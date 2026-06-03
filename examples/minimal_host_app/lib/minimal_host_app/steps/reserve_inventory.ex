defmodule MinimalHostApp.Steps.ReserveInventory do
  @moduledoc """
  Reserves inventory for the saga checkout example.

  The reservation is reversible through `MinimalHostApp.Steps.ReleaseInventory`
  if a downstream checkout step fails after retries are exhausted.
  """

  use Squidie.Step,
    name: :reserve_inventory,
    description: "Reserves inventory for an order",
    input_schema: [
      order_id: [type: :string, required: true]
    ],
    output_schema: [
      inventory_reservation: [type: :map, required: true]
    ]

  @impl true
  def run(%{order_id: order_id}, _context) do
    {:ok, %{inventory_reservation: %{order_id: order_id, status: "reserved"}}}
  end
end
