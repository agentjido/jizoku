# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Step.Context do
  @moduledoc """
  Durable runtime context passed to native Jizoku steps.

  The context intentionally exposes Jizoku concepts only. It gives steps the
  current run identity, workflow module, step name, attempt number, and the
  durable run state available before the current attempt started.
  """

  @enforce_keys [:run_id, :workflow, :step, :attempt, :state]
  defstruct [
    :run_id,
    :partition,
    :workflow,
    :step,
    :attempt,
    :runnable_key,
    :idempotency_key,
    :claim_id,
    :trace,
    :step_opts,
    state: %{}
  ]

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          partition: String.t() | nil,
          workflow: module(),
          step: atom(),
          runnable_key: String.t() | nil,
          idempotency_key: String.t() | nil,
          claim_id: String.t() | nil,
          trace: Jizoku.Runtime.Trace.t() | nil,
          step_opts: keyword(),
          attempt: pos_integer() | nil,
          state: map()
        }

  @doc false
  @spec from_map(map()) :: t()
  def from_map(context) when is_map(context) do
    %__MODULE__{
      run_id: Map.fetch!(context, :run_id),
      partition: Map.get(context, :partition),
      workflow: Map.fetch!(context, :workflow),
      step: Map.fetch!(context, :step),
      attempt: Map.get(context, :attempt),
      runnable_key: Map.get(context, :runnable_key),
      idempotency_key: Map.get(context, :idempotency_key),
      claim_id: Map.get(context, :claim_id),
      trace: Map.get(context, :trace),
      step_opts: Map.get(context, :step_opts, []),
      state: Map.get(context, :state, %{})
    }
  end
end
