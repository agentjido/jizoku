defmodule Jizoku.Runtime.Journal.Replay do
  @moduledoc """
  Backward-compatible alias for journal replay commands.

  New code should call `Jizoku.Runtime.Journal.Commands.Replay`. This module
  preserves the older runtime namespace for existing host integrations and
  tests.
  """

  alias Jizoku.Runtime.Journal.Commands

  @type replay_error :: Commands.Replay.replay_error()

  @doc """
  Replays an eligible journal-backed workflow run.
  """
  @spec replay(String.t(), keyword(), keyword()) ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, replay_error()}
  defdelegate replay(run_id, replay_opts \\ [], config_opts \\ []), to: Commands.Replay
end
