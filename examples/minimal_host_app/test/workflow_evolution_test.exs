defmodule MinimalHostApp.WorkflowEvolutionTest do
  use MinimalHostApp.DataCase, async: false

  alias MinimalHostApp.Verification.WorkflowEvolution

  test "a v1 run executes through its registered implementation after v2 deploys" do
    snapshot = WorkflowEvolution.run!()

    assert snapshot.definition_version == "v1"

    expected = %{
      implementation: "v1",
      workflow: Atom.to_string(MinimalHostApp.Workflows.VersionedRouting)
    }

    assert snapshot.context == expected
    assert [%{status: :completed, result: ^expected}] = snapshot.attempts
  end
end
