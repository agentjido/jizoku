defmodule MinimalHostApp.Steps.VersionedRoutingV2 do
  @moduledoc false

  use Jizoku.Step, name: "versioned_routing_v2", input_schema: []

  @impl Jizoku.Step
  def run(_input, _context) do
    {:ok, %{implementation: "v2"}}
  end
end
