defmodule Squidie.Executor.LeasesTest do
  use ExUnit.Case, async: true

  alias Squidie.Executor.Leases
  alias Squidie.Executor.Leases.Claim

  test "declares the lease adapter callbacks" do
    assert Leases.required_callbacks() == [
             claim: 4,
             heartbeat: 3,
             complete: 3,
             fail: 4
           ]
  end

  test "claim keeps backend lease details opaque" do
    backend_ref = %{lease_id: "lease_123", backend: :bedrock}

    assert %Claim{
             id: "lease_123",
             queue: "squidie",
             item_id: "job_123",
             owner: "worker_a",
             lease_until: 1_800_000_000_000,
             payload: %{kind: :step},
             backend_ref: ^backend_ref,
             metadata: %{}
           } = %Claim{
             id: "lease_123",
             queue: "squidie",
             item_id: "job_123",
             owner: "worker_a",
             lease_until: 1_800_000_000_000,
             payload: %{kind: :step},
             backend_ref: backend_ref
           }
  end
end
