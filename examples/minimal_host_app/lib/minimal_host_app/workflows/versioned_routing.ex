defmodule MinimalHostApp.Workflows.VersionedRouting.V1 do
  @moduledoc false

  use Jizoku.Workflow

  workflow do
    version "v1"

    trigger :manual do
      manual()
    end

    step :record_version, MinimalHostApp.Steps.VersionedRoutingV1
    transition :record_version, on: :ok, to: :complete
  end
end

defmodule MinimalHostApp.Workflows.VersionedRouting do
  @moduledoc """
  Current implementation for the minimal host's blue/green routing example.
  """

  use Jizoku.Workflow

  workflow do
    version "v2"

    trigger :manual do
      manual()
    end

    step :record_version, MinimalHostApp.Steps.VersionedRoutingV2
    transition :record_version, on: :ok, to: :complete
  end
end
