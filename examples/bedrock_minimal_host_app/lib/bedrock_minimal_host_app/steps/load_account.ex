defmodule BedrockMinimalHostApp.Steps.LoadAccount do
  @moduledoc """
  Example step that loads account context for dependency-based workflows.
  """

  use Jizoku.Step,
    name: :load_account,
    description: "Loads account context",
    input_schema: [
      account_id: [type: :string, required: true]
    ],
    output_schema: [
      account: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Jizoku.Step.Context.t()) :: {:ok, map()}
  def run(%{account_id: account_id}, _context) do
    {:ok,
     %{
       account: %{id: account_id, tier: "standard"}
     }}
  end
end
