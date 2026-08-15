defmodule MinimalHostApp.Workflows.CancellableWait do
  @moduledoc """
  Example workflow used to verify cancellation convergence after delayed waits.
  """

  use Jizoku.Workflow

  workflow do
    trigger :manual do
      manual()

      payload do
        field :account_id, :string
      end
    end

    step :wait_for_cancellation, :wait, duration: 10
    step :record_delivery, :log, message: "delivery recorded", level: :info

    transition :wait_for_cancellation, on: :ok, to: :record_delivery
    transition :record_delivery, on: :ok, to: :complete
  end
end
