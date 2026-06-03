defmodule BedrockMinimalHostApp.Steps.CapturePayment do
  @moduledoc """
  Fails payment capture for the saga checkout smoke path.

  The deliberate failure lets the host app example verify retry exhaustion,
  terminal failure persistence, compensation dispatch, and rollback inspection.
  """

  use Squidie.Step,
    name: :capture_payment,
    description: "Captures an authorized payment"

  @impl true
  def run(_params, _context) do
    {:retry, %{message: "payment capture declined", code: "capture_declined"}}
  end
end
