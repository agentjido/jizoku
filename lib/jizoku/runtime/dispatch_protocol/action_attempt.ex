# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.DispatchProtocol.ActionAttempt do
  @moduledoc """
  Rebuildable projection of one dispatch attempt.

  This is not mutable runtime state. It is the compact read model obtained by
  replaying journal entries for one runnable key.
  """

  @type status :: :available | :retry_scheduled | :claimed | :completed | :failed

  alias Jizoku.Runtime.Trace

  @completion_encoding_key "completion_encoding"

  @type t :: %__MODULE__{
          run_id: String.t(),
          workflow: String.t() | nil,
          runnable_key: String.t(),
          idempotency_key: String.t(),
          attempt_number: pos_integer(),
          step: String.t(),
          input: map(),
          trace: Trace.t() | nil,
          scheduled_at: DateTime.t() | nil,
          visible_at: DateTime.t(),
          status: status(),
          claim_id: String.t() | nil,
          claim_token_hash: String.t() | nil,
          owner_id: String.t() | nil,
          lease_until: DateTime.t() | nil,
          claimed_at: DateTime.t() | nil,
          result: map() | nil,
          guardrails: [map()],
          execution_opts: keyword(),
          deadline: map() | nil,
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
    :workflow,
    :runnable_key,
    :idempotency_key,
    :attempt_number,
    :step,
    :input,
    :trace,
    :scheduled_at,
    :visible_at,
    :status,
    :claim_id,
    :claim_token_hash,
    :owner_id,
    :lease_until,
    :claimed_at,
    :result,
    :deadline,
    :completed_at,
    :transition,
    :error,
    execution_opts: [],
    guardrails: [],
    wakeup_emitted?: false,
    applied?: false
  ]

  @doc false
  @spec upgrade(t()) :: t()
  def upgrade(%__MODULE__{} = attempt) do
    completion_encoding = Map.get(attempt, @completion_encoding_key)
    attributes = Map.delete(attempt, :__struct__)

    __MODULE__
    |> struct(attributes)
    |> put_completion_encoding(completion_encoding)
  end

  @doc false
  @spec completion_encoding(t()) :: term()
  def completion_encoding(%__MODULE__{} = attempt) do
    Map.get(attempt, @completion_encoding_key)
  end

  @doc false
  @spec put_completion_encoding(t(), term()) :: t()
  def put_completion_encoding(%__MODULE__{} = attempt, nil), do: attempt

  def put_completion_encoding(%__MODULE__{} = attempt, encoding) do
    Map.put(attempt, @completion_encoding_key, encoding)
  end
end
