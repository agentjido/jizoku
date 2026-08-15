defmodule Jizoku.Runtime.DeterministicIdentityTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.DeterministicIdentity
  alias Jizoku.Runtime.ScheduleIdentity

  test "preserves the public durable schedule identities" do
    assert ScheduleIdentity.run_id("Example.Workflow", "run", "signal-42") ==
             {:ok, "591cfd56-2dc5-57d5-a823-c19d6361f6c6"}

    payload = %{
      "intended_window" => %{
        "start_at" => "2026-05-26T12:00:00Z",
        "end_at" => "2026-05-26T13:00:00Z"
      }
    }

    assert ScheduleIdentity.signal_id("Example.Workflow", "run", payload) ==
             {:ok, "sha256:DzOKNNYz7CYBBFNL87WjX6QzVELQhmLt_R0xZ2vmY4s"}
  end

  test "length prefixes keep component boundaries unambiguous" do
    refute DeterministicIdentity.encode_parts(["a", "bc"]) ==
             DeterministicIdentity.encode_parts(["ab", "c"])

    refute DeterministicIdentity.uuid(["a", "bc"]) ==
             DeterministicIdentity.uuid(["ab", "c"])
  end
end
