defmodule Jizoku.Retention.Plan.Candidate do
  @moduledoc "A non-sensitive retention candidate pinned to durable source revisions."

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: String.t(),
          queue: String.t(),
          terminal_status: atom(),
          terminal_at: DateTime.t(),
          archived_at: DateTime.t(),
          run_revision: non_neg_integer(),
          dispatch_revision: non_neg_integer(),
          dispatch_entry_count: non_neg_integer(),
          affected: map(),
          estimated_entries: non_neg_integer()
        }

  @enforce_keys [
    :run_id,
    :workflow,
    :queue,
    :terminal_status,
    :terminal_at,
    :archived_at,
    :run_revision,
    :dispatch_revision,
    :dispatch_entry_count,
    :affected,
    :estimated_entries
  ]

  defstruct @enforce_keys
end

defmodule Jizoku.Retention.Plan.Blocked do
  @moduledoc "A non-sensitive retention exclusion and its ordered safety reasons."

  @type t :: %__MODULE__{
          run_id: String.t(),
          terminal_status: atom(),
          terminal_at: DateTime.t(),
          reasons: [atom()]
        }

  @enforce_keys [:run_id, :terminal_status, :terminal_at, :reasons]
  defstruct @enforce_keys
end

defmodule Jizoku.Retention.Plan do
  @moduledoc "Deterministic, expiring evidence for one partition-scoped retention preview."

  alias Jizoku.Retention.Plan.Blocked
  alias Jizoku.Retention.Plan.Candidate

  @version 1

  @type t :: %__MODULE__{
          version: pos_integer(),
          partition: String.t() | nil,
          created_at: DateTime.t(),
          expires_at: DateTime.t(),
          terminal_before: DateTime.t(),
          statuses: [atom()],
          limit: pos_integer(),
          catalog_revision: non_neg_integer(),
          eligible: [Candidate.t()],
          blocked: [Blocked.t()],
          confirmation_token: String.t()
        }

  @enforce_keys [
    :partition,
    :created_at,
    :expires_at,
    :terminal_before,
    :statuses,
    :limit,
    :catalog_revision,
    :eligible,
    :blocked,
    :confirmation_token
  ]

  defstruct [:version | @enforce_keys]

  @doc "Builds a versioned plan and binds its confirmation token to all plan fields."
  @spec new(map()) :: t()
  def new(attrs) when is_map(attrs) do
    attrs = Map.put(attrs, :version, @version)
    %__MODULE__{} = plan = struct!(__MODULE__, Map.put(attrs, :confirmation_token, ""))
    %{plan | confirmation_token: digest(plan)}
  end

  @doc "Validates a plan token and rejects expired or mutated plan evidence."
  @spec valid_confirmation?(t(), String.t(), DateTime.t()) :: boolean()
  def valid_confirmation?(%__MODULE__{} = plan, token, %DateTime{} = now)
      when is_binary(token) do
    DateTime.compare(now, plan.expires_at) == :lt and
      digest_matches?(plan.confirmation_token, token) and
      digest_matches?(plan.confirmation_token, digest(plan))
  end

  def valid_confirmation?(%__MODULE__{}, _token, %DateTime{}), do: false

  @doc "Validates that a token matches the exact, unmodified plan evidence."
  @spec confirmation_matches?(t(), String.t()) :: boolean()
  def confirmation_matches?(%__MODULE__{} = plan, token) when is_binary(token) do
    digest_matches?(plan.confirmation_token, token) and
      digest_matches?(plan.confirmation_token, digest(plan))
  end

  def confirmation_matches?(%__MODULE__{}, _token), do: false

  defp digest_matches?(left, right) when is_binary(left) and is_binary(right) do
    byte_size(left) == byte_size(right) and
      :crypto.hash(:sha256, left) == :crypto.hash(:sha256, right)
  end

  defp digest(%__MODULE__{} = plan) do
    plan
    |> Map.from_struct()
    |> Map.put(:confirmation_token, "")
    |> :erlang.term_to_binary([:deterministic, {:minor_version, 2}])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end
end
