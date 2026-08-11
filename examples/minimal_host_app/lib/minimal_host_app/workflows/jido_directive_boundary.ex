defmodule MinimalHostApp.Workflows.JidoDirectiveBoundary do
  @moduledoc """
  Battle-tests fail-closed handling for raw Jido action directives.
  """

  use Squidie.Workflow

  alias MinimalHostApp.Steps.UnsupportedJidoDirective

  workflow do
    trigger :manual do
      manual()
    end

    step :unsupported_directive, UnsupportedJidoDirective
    transition :unsupported_directive, on: :ok, to: :complete
  end
end
