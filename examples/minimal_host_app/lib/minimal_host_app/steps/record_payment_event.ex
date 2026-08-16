defmodule MinimalHostApp.Steps.RecordPaymentEvent do
  @moduledoc """
  Records the verified provider event selected by the durable wait.
  """

  use Jizoku.Step,
    name: "record_payment_event",
    input_schema: [event: [type: :map, required: true]]

  @impl Jizoku.Step
  def run(%{event: event}, _context) do
    {:ok,
     %{
       settled_payment_id: event.payment_id,
       settlement_status: event.status
     }}
  end
end
