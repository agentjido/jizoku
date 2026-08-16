defmodule MinimalHostApp.Workflows.PaymentWebhook do
  @moduledoc """
  Waits durably for a verified payment-provider callback.
  """

  use Jizoku.Workflow

  workflow do
    trigger :payment_webhook do
      manual()

      payload do
        field :payment_id, :string
      end
    end

    step :await_payment, :await_event,
      event: "payment.completed",
      correlation: [:payment_id],
      output: :event,
      timeout: [after: 300_000, on_timeout: :record_timeout]

    step :record_event, MinimalHostApp.Steps.RecordPaymentEvent, input: [:event]

    step :record_timeout, MinimalHostApp.Steps.RecordPaymentTimeout, input: [:event_timeout]

    transition :await_payment, on: :ok, to: :record_event
    transition :record_event, on: :ok, to: :complete
    transition :record_timeout, on: :ok, to: :complete
  end
end
