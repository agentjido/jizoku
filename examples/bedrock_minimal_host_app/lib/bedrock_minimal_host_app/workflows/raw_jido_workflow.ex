defmodule BedrockMinimalHostApp.Workflows.RawJidoWorkflow do
  @moduledoc """
  Demonstrates an ordinary raw Jido action on the journal executor path.
  """

  use Squidie.Workflow

  defmodule Normalize do
    use Jido.Action,
      name: "normalize_jido_value",
      description: "Normalizes one value through a raw Jido action",
      schema: [value: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{value: value}, context) do
      {:ok, %{jido_result: %{value: String.upcase(value), run_id: context.run_id}}, []}
    end
  end

  workflow do
    trigger :manual do
      manual()

      payload do
        field :value, :string
      end
    end

    step :normalize, Normalize
    transition :normalize, on: :ok, to: :complete
  end
end
