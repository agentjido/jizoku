defmodule Jizoku.Retention.Receipt do
  @moduledoc "A payload-free aggregate receipt for one completed retention plan."

  @type t :: %__MODULE__{
          plan_digest: String.t(),
          partition: String.t() | nil,
          run_ids: [String.t()],
          run_count: pos_integer(),
          run_entries_deleted: non_neg_integer(),
          dispatch_entries_deleted: non_neg_integer(),
          applied_at: DateTime.t(),
          idempotent?: boolean()
        }

  @enforce_keys [
    :plan_digest,
    :partition,
    :run_ids,
    :run_count,
    :run_entries_deleted,
    :dispatch_entries_deleted,
    :applied_at,
    :idempotent?
  ]

  defstruct @enforce_keys
end
