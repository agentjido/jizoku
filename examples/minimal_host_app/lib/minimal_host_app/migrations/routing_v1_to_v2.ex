defmodule MinimalHostApp.Migrations.RoutingV1ToV2 do
  @moduledoc "Migration contract used by the minimal host smoke path."

  @behaviour Jizoku.Workflow.Migration

  @impl Jizoku.Workflow.Migration
  def key do
    "minimal-host-routing-v1-to-v2"
  end

  @impl Jizoku.Workflow.Migration
  def source_version do
    "v1"
  end

  @impl Jizoku.Workflow.Migration
  def target_version do
    "v2"
  end

  @impl Jizoku.Workflow.Migration
  def migrate(%{context: context, manual_state: %{step: "legacy_gate"}}) do
    {:ok,
     %{
       context:
         context
         |> Map.delete(:legacy)
         |> Map.delete(:legacy_output)
         |> Map.put(:schema, 2),
       manual_step: :gate
     }}
  end
end
