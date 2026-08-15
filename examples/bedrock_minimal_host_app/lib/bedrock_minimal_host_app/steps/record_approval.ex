defmodule BedrockMinimalHostApp.Steps.RecordApproval do
  @moduledoc """
  Example step that records a manual approval decision after a paused gate.
  """

  use Jizoku.Step,
    name: :record_approval,
    description: "Records an approved manual review result",
    input_schema: [
      account_id: [type: :string, required: true],
      approval: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Jizoku.Step.Context.t()) :: {:ok, map()}
  def run(%{account_id: account_id, approval: approval}, _context) do
    {:ok,
     approval
     |> Map.put(:account_id, account_id)
     |> Map.put(:status, "approved")}
  end
end
