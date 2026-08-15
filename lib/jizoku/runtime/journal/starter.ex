defmodule Jizoku.Runtime.Journal.Starter do
  @moduledoc """
  Backward-compatible alias for journal start commands.

  New code should call `Jizoku.Runtime.Journal.Commands.Starter`. This module
  preserves the older runtime namespace for existing host integrations and
  tests.
  """

  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.Journal.Commands
  alias Jizoku.Workflow.Spec

  @type start_error :: Commands.Starter.start_error()

  @doc """
  Starts a module-authored workflow through the journal runtime.
  """
  @spec start_run(module(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Inspection.Snapshot.t()}}
          | {:error, start_error()}
  defdelegate start_run(workflow, trigger_name, payload, opts), to: Commands.Starter

  @doc """
  Starts a runtime-authored workflow spec through the journal runtime.
  """
  @spec start_spec_run(Spec.t() | map(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Inspection.Snapshot.t()}}
          | {:error, start_error()}
  defdelegate start_spec_run(spec, trigger_name, payload, opts), to: Commands.Starter
end
