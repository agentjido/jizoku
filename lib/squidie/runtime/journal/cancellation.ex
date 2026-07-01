defmodule Squidie.Runtime.Journal.Cancellation do
  @moduledoc """
  Backward-compatible alias for journal cancellation commands.

  New code should call `Squidie.Runtime.Journal.Commands.Cancellation`. This
  module preserves the older runtime namespace for existing host integrations
  and tests.
  """

  alias Squidie.Runtime.Journal.Commands
  alias Squidie.Runtime.Signal

  @type cancel_error :: Commands.Cancellation.cancel_error()

  @doc """
  Requests cancellation for a journal-backed workflow run.
  """
  @spec cancel(String.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, cancel_error()}
  defdelegate cancel(run_id, opts \\ []), to: Commands.Cancellation

  @doc """
  Applies a normalized cancellation signal at the journal command boundary.
  """
  @spec apply_signal(Signal.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, cancel_error()}
  defdelegate apply_signal(signal, opts), to: Commands.Cancellation
end
