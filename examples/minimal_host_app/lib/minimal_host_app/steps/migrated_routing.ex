defmodule MinimalHostApp.Steps.MigratedRoutingV1 do
  @moduledoc false

  use Jizoku.Step, name: "minimal_host_migrated_routing_v1", input_schema: []

  @impl Jizoku.Step
  def run(input, context) do
    {:ok,
     %{
       implementation: "v1",
       schema: input.schema,
       workflow: Atom.to_string(context.workflow)
     }}
  end
end

defmodule MinimalHostApp.Steps.MigratedRoutingV2 do
  @moduledoc false

  use Jizoku.Step, name: "minimal_host_migrated_routing_v2", input_schema: []

  @impl Jizoku.Step
  def run(input, context) do
    {:ok,
     %{
       implementation: "v2",
       schema: input.schema,
       workflow: Atom.to_string(context.workflow)
     }}
  end
end
