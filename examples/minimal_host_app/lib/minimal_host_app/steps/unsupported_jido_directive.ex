defmodule MinimalHostApp.Steps.UnsupportedJidoDirective do
  @moduledoc """
  Raw Jido action used to exercise Squidie's directive compatibility boundary.

  The custom directive is deliberately unsupported, so the sample workflow
  must fail without applying the action output or exposing its payload.
  """

  use Jido.Action,
    name: "unsupported_jido_directive",
    description: "Returns a Jido directive that Squidie must reject explicitly",
    schema: []

  defmodule CustomDirective do
    @moduledoc false

    defstruct [:secret]
  end

  @impl Jido.Action
  def run(_input, _context) do
    {:ok, %{accepted: true}, [%CustomDirective{secret: "sample-secret"}]}
  end
end
