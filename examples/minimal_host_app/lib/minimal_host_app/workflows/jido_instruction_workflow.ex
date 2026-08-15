defmodule MinimalHostApp.Workflows.JidoInstructionWorkflow do
  @moduledoc """
  Battle-tests direct `Jido.Instruction` scheduling through durable dynamic work.
  """

  use Jizoku.Workflow

  defmodule Prepare do
    use Jizoku.Step, name: :prepare_jido_instruction

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{prepared: true}}
    end
  end

  defmodule Enrich do
    use Jido.Action,
      name: "enrich_jido_instruction",
      description: "Enriches a sample order from durable Jido work",
      schema: [order_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{order_id: order_id}, context) do
      {:ok, %{instruction_order: %{id: order_id, request_id: context.request_id}}}
    end
  end

  defmodule Finish do
    use Jizoku.Step, name: :finish_jido_instruction

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{instruction_workflow_finished: true}}
    end
  end

  workflow do
    trigger :manual do
      manual()
    end

    step :prepare, Prepare
    step :wait, :wait, duration: 60_000
    step :finish, Finish

    transition :prepare, on: :ok, to: :wait
    transition :wait, on: :ok, to: :finish
    transition :finish, on: :ok, to: :complete
  end
end
