defmodule Squidie.ReadModel.Listing.Summary do
  @moduledoc """
  Redacted journal-backed run listing row.

  Listing intentionally exposes only lookup and state fields. Detailed attempt
  inputs, results, errors, claims, and idempotency keys stay behind
  `Squidie.inspect_run/2`.
  """

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: String.t(),
          definition_version: String.t() | nil,
          queue: String.t(),
          status: atom(),
          terminal?: boolean(),
          terminal_status: atom() | nil,
          deadline: map() | nil,
          indexed_at: DateTime.t(),
          thread_revision: non_neg_integer(),
          anomalies: [map()]
        }

  @enforce_keys [
    :run_id,
    :workflow,
    :definition_version,
    :queue,
    :status,
    :terminal?,
    :terminal_status,
    :deadline,
    :indexed_at,
    :thread_revision
  ]

  defstruct [
    :run_id,
    :workflow,
    :definition_version,
    :queue,
    :status,
    :terminal?,
    :terminal_status,
    :deadline,
    :indexed_at,
    :thread_revision,
    anomalies: []
  ]
end
