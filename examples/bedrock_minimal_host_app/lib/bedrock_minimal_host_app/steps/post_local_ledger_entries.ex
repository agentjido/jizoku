defmodule BedrockMinimalHostApp.Steps.PostLocalLedgerEntries do
  @moduledoc """
  Writes a small local ledger group inside the host repo transaction.
  """

  use Squidie.Step,
    name: :post_local_ledger_entries,
    description: "Posts two local ledger entries in one host repo transaction",
    input_schema: [
      account_id: [type: :string, required: true],
      fail_after_reserve: [type: :boolean, required: true]
    ],
    output_schema: [
      local_ledger: [type: :map, required: true]
    ]

  alias BedrockMinimalHostApp.Repo

  @impl true
  def run(
        %{account_id: account_id, fail_after_reserve: fail_after_reserve?},
        %Squidie.Step.Context{run_id: run_id}
      ) do
    insert_entry!(run_id, account_id, "reserve")

    if fail_after_reserve? do
      {:error, %{message: "local ledger capture failed"}}
    else
      insert_entry!(run_id, account_id, "capture")
      {:ok, %{local_ledger: %{status: "committed", entries: 2}}}
    end
  end

  defp insert_entry!(run_id, account_id, entry) do
    now = NaiveDateTime.truncate(NaiveDateTime.utc_now(), :second)

    Repo.insert_all("local_ledger_entries", [
      %{
        run_id: run_id,
        account_id: account_id,
        entry: entry,
        inserted_at: now,
        updated_at: now
      }
    ])

    :ok
  end
end
