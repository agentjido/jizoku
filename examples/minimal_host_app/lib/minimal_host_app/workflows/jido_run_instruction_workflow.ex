defmodule MinimalHostApp.Workflows.JidoRunInstructionWorkflow do
  @moduledoc """
  Battle-tests a raw Jido `RunInstruction` directive through durable recovery.
  """

  use Jizoku.Workflow

  alias MinimalHostApp.Workflows.JidoInstructionWorkflow

  defmodule RequestFollowup do
    use Jido.Action,
      name: "request_jido_instruction",
      description: "Returns a durable Jido RunInstruction directive",
      schema: []

    @impl Jido.Action
    def run(_input, _context) do
      instruction =
        Jido.Instruction.new!(
          id: "sample-followup",
          action: JidoInstructionWorkflow.Enrich,
          params: %{order_id: "order-from-directive"},
          context: %{request_id: "sample-directive-request"}
        )

      {:ok, %{prepared: true}, [Jido.Agent.Directive.run_instruction(instruction)]}
    end
  end

  workflow do
    trigger :manual do
      manual()
    end

    step :request_followup, RequestFollowup
    transition :request_followup, on: :ok, to: :complete
  end
end
