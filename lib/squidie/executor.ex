# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Executor do
  @moduledoc """
  Behaviour implemented by host applications to schedule Squidie cron work.

  Jido-native execution is pulled through `Squidie.execute_next/1`. Host
  applications that use external cron schedulers may still enqueue trigger
  activations as plain `Squidie.Executor.Payload.cron/3` maps and deliver
  them to `Squidie.Runtime.Runner.perform/2`.
  """

  alias Squidie.Config

  @type metadata :: map()
  @type enqueue_error :: term()
  @type schedule_window :: %{
          optional(:start_at) => String.t(),
          optional(:end_at) => String.t(),
          optional(String.t()) => String.t()
        }
  @type cron_enqueue_opts :: [
          {:schedule_in, pos_integer()}
          | {:signal_id, String.t()}
          | {:intended_window, schedule_window()}
        ]

  @doc """
  Enqueues or schedules a cron trigger activation.

  Host schedulers can call this callback when a declared cron trigger fires, or
  can enqueue `Squidie.Executor.Payload.cron/3` directly and deliver it to
  `Squidie.Runtime.Runner.perform/2`.

  When the scheduler knows the logical schedule window, pass `:signal_id` and
  `:intended_window` through to `Squidie.Executor.Payload.cron/3`. Squidie
  persists those values as run context before workflow processing starts, so
  delayed workers do not need to infer the intended window from wall-clock time.

  Cron triggers that opt into idempotency use the scheduler `:signal_id`, or a
  deterministic id derived from a complete `:intended_window`, as the duplicate
  start key. If the host omits both, the runtime rejects the start because it
  cannot safely distinguish a new activation from a redelivery.
  """
  @callback enqueue_cron(Config.t(), module(), atom(), cron_enqueue_opts()) ::
              {:ok, metadata()} | {:error, enqueue_error()}

  @required_callbacks [enqueue_cron: 4]

  @doc false
  @spec required_callbacks() :: keyword(pos_integer())
  def required_callbacks, do: @required_callbacks
end
