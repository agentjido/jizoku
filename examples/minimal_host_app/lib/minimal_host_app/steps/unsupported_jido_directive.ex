defmodule MinimalHostApp.Steps.UnsupportedJidoDirective do
  @moduledoc """
  Raw Jido action used to exercise Squidie's directive compatibility boundary.

  The emitted directive is deliberately unsupported in the first Jido
  interoperability slice, so the sample workflow must fail without applying
  the action output or exposing the signal payload.
  """

  use Jido.Action,
    name: "unsupported_jido_directive",
    description: "Returns a Jido directive that Squidie must reject explicitly",
    schema: []

  @impl Jido.Action
  def run(_input, _context) do
    {:ok, %{accepted: true}, [%Jido.Agent.Directive.Emit{signal: %{secret: "sample-secret"}}]}
  end
end
