defmodule Squidie.Executor.Leases do
  @moduledoc """
  Behaviour for backends that own worker claims and lease extension.

  `Squidie.Executor` covers durable job delivery. This behaviour is the
  separate worker-lifecycle boundary for queue backends that can claim work,
  extend active claims, complete delivered work, and return failed work to the
  backend's retry or dead-letter policy.

  The journal-backed runtime does not require a lease adapter. It exists as the
  public contract for adapters that want to expose leasing semantics through
  a durable delivery backend.
  """

  alias Squidie.Config
  alias Squidie.Executor.Leases.Claim

  @type queue :: String.t()
  @type owner :: String.t()
  @type lease_error :: term()

  @type claim_opts :: [
          {:limit, pos_integer()}
          | {:lease_duration_ms, pos_integer()}
          | {:now, non_neg_integer()}
        ]

  @type heartbeat_opts :: [
          {:lease_duration_ms, pos_integer()}
          | {:now, non_neg_integer()}
        ]

  @type fail_opts :: [
          {:base_delay, pos_integer()}
          | {:max_delay, pos_integer()}
          | {:now, non_neg_integer()}
        ]

  @doc """
  Claims visible work from a queue for one owner.

  Returning `{:ok, []}` means no work is currently visible. Returned claims
  should include the queued payload and an opaque backend reference that the same
  adapter can use for heartbeat, completion, and failure.
  """
  @callback claim(Config.t(), queue(), owner(), claim_opts()) ::
              {:ok, [Claim.t()]} | {:error, lease_error()}

  @doc """
  Extends the lease for an active claim.
  """
  @callback heartbeat(Config.t(), Claim.t(), heartbeat_opts()) ::
              {:ok, Claim.t()} | {:error, lease_error()}

  @doc """
  Completes a claimed item and removes it from backend delivery.
  """
  @callback complete(Config.t(), Claim.t(), keyword()) :: :ok | {:error, lease_error()}

  @doc """
  Marks a claimed item failed and lets the backend apply retry/dead-letter policy.
  """
  @callback fail(Config.t(), Claim.t(), term(), fail_opts()) ::
              {:ok, :requeued | :dead_lettered} | {:error, lease_error()}

  @required_callbacks [
    claim: 4,
    heartbeat: 3,
    complete: 3,
    fail: 4
  ]

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec required_callbacks() :: keyword(pos_integer())
  def required_callbacks, do: @required_callbacks
end
