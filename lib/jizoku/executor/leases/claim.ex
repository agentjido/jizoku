defmodule Jizoku.Executor.Leases.Claim do
  @moduledoc """
  Backend-neutral worker claim returned by a lease adapter.

  `backend_ref` is intentionally opaque. It lets an adapter keep the native
  backend lease data it needs to heartbeat, complete, or fail the claim without
  making Jizoku depend on that backend's structs.
  """

  @type t :: %__MODULE__{
          id: String.t() | binary(),
          queue: String.t(),
          item_id: String.t() | binary(),
          owner: String.t(),
          lease_until: non_neg_integer(),
          payload: term(),
          metadata: map(),
          backend_ref: term()
        }

  @enforce_keys [:id, :queue, :item_id, :owner, :lease_until, :payload, :backend_ref]
  defstruct [:id, :queue, :item_id, :owner, :lease_until, :payload, :backend_ref, metadata: %{}]
end
