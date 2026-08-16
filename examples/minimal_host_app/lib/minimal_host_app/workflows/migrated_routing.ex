defmodule MinimalHostApp.Workflows.MigratedRouting.V1 do
  @moduledoc false

  use Jizoku.Workflow

  workflow do
    version "v1"

    trigger :manual do
      manual()
    end

    step :legacy_gate, :pause
    step :legacy_finish, MinimalHostApp.Steps.MigratedRoutingV1
    transition :legacy_gate, on: :ok, to: :legacy_finish
    transition :legacy_finish, on: :ok, to: :complete
  end
end

defmodule MinimalHostApp.Workflows.MigratedRouting do
  @moduledoc "Current definition for the minimal host's safe-point migration example."

  use Jizoku.Workflow

  workflow do
    version "v2"

    trigger :manual do
      manual()
    end

    step :gate, :pause
    step :finish, MinimalHostApp.Steps.MigratedRoutingV2
    transition :gate, on: :ok, to: :finish
    transition :finish, on: :ok, to: :complete
  end
end
