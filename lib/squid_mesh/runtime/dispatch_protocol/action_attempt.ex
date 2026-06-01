defmodule SquidMesh.Runtime.DispatchProtocol.ActionAttempt do
  @moduledoc """
  Rebuildable projection of one dispatch attempt.

  This is not mutable runtime state. It is the compact read model obtained by
  replaying journal entries for one runnable key.
  """

  @type status :: :available | :retry_scheduled | :claimed | :completed | :failed

  @type t :: %__MODULE__{
          run_id: String.t(),
          runnable_key: String.t(),
          idempotency_key: String.t(),
          attempt_number: pos_integer(),
          step: String.t(),
          input: map(),
          visible_at: DateTime.t(),
          status: status(),
          claim_id: String.t() | nil,
          claim_token_hash: String.t() | nil,
          owner_id: String.t() | nil,
          lease_until: DateTime.t() | nil,
          result: map() | nil,
          execution_opts: keyword(),
          completed_at: DateTime.t() | nil,
          transition: map() | nil,
          error: map() | nil,
          wakeup_emitted?: boolean(),
          applied?: boolean()
        }

  @enforce_keys [
    :run_id,
    :runnable_key,
    :idempotency_key,
    :attempt_number,
    :step,
    :input,
    :visible_at,
    :status
  ]
  defstruct [
    :run_id,
    :runnable_key,
    :idempotency_key,
    :attempt_number,
    :step,
    :input,
    :visible_at,
    :status,
    :claim_id,
    :claim_token_hash,
    :owner_id,
    :lease_until,
    :result,
    :completed_at,
    :transition,
    :error,
    execution_opts: [],
    wakeup_emitted?: false,
    applied?: false
  ]
end
