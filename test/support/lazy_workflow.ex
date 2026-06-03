defmodule Squidie.TestSupport.LazyWorkflow do
  @moduledoc false

  use Squidie.Workflow

  workflow do
    trigger :manual do
      manual()

      payload do
        field :account_id, :string
      end
    end

    step :load_invoice, Squidie.TestSupport.LazyWorkflow.LoadInvoice, retry: [max_attempts: 1]
    transition :load_invoice, on: :ok, to: :complete
  end
end
