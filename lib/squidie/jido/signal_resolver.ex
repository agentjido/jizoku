defmodule Squidie.Jido.SignalResolver do
  @moduledoc """
  Host-owned routing boundary for domain `Jido.Signal` values.

  A resolver maps one validated domain signal to a closed Squidie lifecycle
  command. It cannot choose storage, queues, runtime modules, dispatch
  adapters, or journal facts. Start commands require a compiled workflow
  module so untrusted signal strings never become executable module names.

  The original Jido envelope remains authoritative for command identity,
  source, subject, occurrence time, trace correlation, and idempotency.
  Rejections use atom reasons so resolver errors cannot leak raw application
  data through the public compatibility boundary.
  """

  @type command ::
          {:start_run, module(), atom() | nil, map()}
          | {:cancel_run, Ecto.UUID.t()}
          | {:resume_run, Ecto.UUID.t(), map()}
          | {:approve_run, Ecto.UUID.t(), map()}
          | {:reject_run, Ecto.UUID.t(), map()}
          | {:replay_run, Ecto.UUID.t(), boolean()}

  @type rejection_reason :: atom()

  @callback resolve(Jido.Signal.t()) ::
              {:ok, command()} | {:error, rejection_reason()}
end
