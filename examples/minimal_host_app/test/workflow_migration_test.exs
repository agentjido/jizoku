defmodule MinimalHostApp.WorkflowMigrationTest do
  use MinimalHostApp.DataCase, async: false

  alias MinimalHostApp.Verification.WorkflowMigration
  alias MinimalHostApp.Workflows.MigratedRouting

  test "migrates a paused v1 run and completes through v2 code" do
    snapshot = WorkflowMigration.run!()
    stable_workflow = Atom.to_string(MigratedRouting)

    assert snapshot.definition_version == "v2"
    assert snapshot.context.schema == 2

    assert [%{migration_key: "minimal-host-routing-v1-to-v2"}] =
             snapshot.definition_migrations

    assert Enum.any?(snapshot.attempts, fn attempt ->
             attempt.result == %{
               implementation: "v2",
               schema: 2,
               workflow: stable_workflow
             }
           end)
  end
end
