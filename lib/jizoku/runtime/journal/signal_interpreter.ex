defmodule Jizoku.Runtime.Journal.SignalInterpreter do
  @moduledoc """
  Backward-compatible alias for journal command signal dispatch.

  New code should call `Jizoku.Runtime.Journal.Commands.SignalInterpreter`.
  This module preserves the older runtime namespace for existing host
  integrations and tests.
  """

  alias Jizoku.Runtime.Journal.Commands
  alias Jizoku.Runtime.Signal

  @doc """
  Applies a normalized runtime command signal to the journal runtime.
  """
  @spec apply(Signal.t(), keyword()) :: {:ok, term()} | {:error, term()}
  defdelegate apply(signal, opts), to: Commands.SignalInterpreter
end
