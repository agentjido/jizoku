defmodule MinimalHostApp.Workflows.SagaCheckout do
  @moduledoc """
  Example saga workflow that rolls back completed steps after downstream failure.
  """

  use Jizoku.Workflow

  workflow do
    trigger :saga_checkout do
      manual()

      payload do
        field :account_id, :string
        field :order_id, :string
      end
    end

    step :reserve_inventory, MinimalHostApp.Steps.ReserveInventory,
      compensate: MinimalHostApp.Steps.ReleaseInventory

    step :authorize_payment, MinimalHostApp.Steps.AuthorizePayment,
      compensate: MinimalHostApp.Steps.VoidPaymentAuthorization

    step :capture_payment, MinimalHostApp.Steps.CapturePayment, retry: [max_attempts: 2]

    transition :reserve_inventory, on: :ok, to: :authorize_payment
    transition :authorize_payment, on: :ok, to: :capture_payment
    transition :capture_payment, on: :ok, to: :complete
  end
end
