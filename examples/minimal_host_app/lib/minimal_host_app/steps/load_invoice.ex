defmodule MinimalHostApp.Steps.LoadInvoice do
  @moduledoc """
  Example step that loads invoice context for a recovery run.
  """

  use Squidie.Step,
    name: :load_invoice,
    description: "Loads invoice context",
    input_schema: [
      account_id: [type: :string, required: true],
      invoice_id: [type: :string, required: true],
      attempt_id: [type: :string, required: true]
    ],
    output_schema: [
      invoice: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()}
  def run(%{account_id: account_id, invoice_id: invoice_id, attempt_id: attempt_id}, _context) do
    {:ok,
     %{
       invoice: %{id: invoice_id, account_id: account_id, attempt_id: attempt_id}
     }}
  end
end
