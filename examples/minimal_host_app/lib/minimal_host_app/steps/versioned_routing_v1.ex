defmodule MinimalHostApp.Steps.VersionedRoutingV1 do
  @moduledoc false

  use Jizoku.Step, name: "versioned_routing_v1", input_schema: []

  @impl Jizoku.Step
  def run(_input, context) do
    {:ok,
     %{
       implementation: "v1",
       workflow: Atom.to_string(context.workflow)
     }}
  end
end
