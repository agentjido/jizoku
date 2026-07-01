defmodule Squidie.Runtime.Journal.ManualControl do
  @moduledoc """
  Backward-compatible alias for journal manual-control commands.

  New code should call `Squidie.Runtime.Journal.Commands.ManualControl`. This
  module preserves the older runtime namespace for existing host integrations
  and tests.
  """

  alias Squidie.Runtime.Journal.Commands
  alias Squidie.Runtime.Signal

  @type control_error :: Commands.ManualControl.control_error()

  @doc """
  Resumes a paused journal-backed workflow run.
  """
  @spec resume(String.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, control_error()}
  defdelegate resume(run_id, attrs, opts \\ []), to: Commands.ManualControl

  @doc """
  Approves a pending manual approval boundary.
  """
  @spec approve(String.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, control_error()}
  defdelegate approve(run_id, attrs, opts \\ []), to: Commands.ManualControl

  @doc """
  Rejects a pending manual approval boundary.
  """
  @spec reject(String.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, control_error()}
  defdelegate reject(run_id, attrs, opts \\ []), to: Commands.ManualControl

  @doc """
  Applies a normalized manual-control signal at the journal command boundary.
  """
  @spec apply_signal(Signal.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, control_error()}
  defdelegate apply_signal(signal, opts), to: Commands.ManualControl
end
