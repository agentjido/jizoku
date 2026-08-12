defmodule MinimalHostApp.Workflows.JidoErrorRecovery do
  @moduledoc """
  Battle-tests native `Jido.Agent.Directive.Error` workflow recovery.
  """

  use Squidie.Workflow

  defmodule Reject do
    use Jido.Action,
      name: "reject_with_jido_error",
      description: "Returns a Jido error directive for workflow recovery",
      schema: []

    @impl Jido.Action
    def run(_input, _context) do
      error = Jido.Error.execution_error("sample-secret", details: %{secret: "sample-secret"})

      {:ok, %{must_not_be_applied: true},
       [%Jido.Agent.Directive.Error{error: error, context: :action}]}
    end
  end

  defmodule Recover do
    use Squidie.Step, name: :recover_jido_error

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{jido_error_recovered: true}}
    end
  end

  workflow do
    trigger :manual do
      manual()
    end

    step :reject, Reject
    step :recover, Recover

    transition :reject, on: :error, to: :recover
    transition :recover, on: :ok, to: :complete
  end
end
