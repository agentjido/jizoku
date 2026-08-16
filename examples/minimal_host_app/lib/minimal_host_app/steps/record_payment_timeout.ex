defmodule MinimalHostApp.Steps.RecordPaymentTimeout do
  @moduledoc """
  Records the timeout continuation selected for a missing provider event.
  """

  use Jizoku.Step,
    name: "record_payment_timeout",
    input_schema: [event_timeout: [type: :map, required: true]]

  @impl Jizoku.Step
  def run(%{event_timeout: event_timeout}, _context) do
    {:ok,
     %{
       timed_out_event: event_timeout.event,
       timed_out_payment_id: event_timeout.correlation
     }}
  end
end
