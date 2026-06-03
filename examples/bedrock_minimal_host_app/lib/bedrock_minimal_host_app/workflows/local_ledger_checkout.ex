defmodule BedrockMinimalHostApp.Workflows.LocalLedgerCheckout do
  @moduledoc """
  Example workflow for a same-process local repo transaction group.
  """

  use Squidie.Workflow

  workflow do
    trigger :local_ledger_checkout do
      manual()

      payload do
        field :account_id, :string
        field :fail_after_reserve, :boolean, default: false
      end
    end

    step :post_local_ledger_entries, BedrockMinimalHostApp.Steps.PostLocalLedgerEntries,
      transaction: :repo

    transition :post_local_ledger_entries, on: :ok, to: :complete
  end
end
