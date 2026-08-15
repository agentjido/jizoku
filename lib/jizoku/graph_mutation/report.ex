defmodule Jizoku.GraphMutation.Report do
  @moduledoc """
  Durable apply or reconciliation outcome for a graph mutation.

  The contract distinguishes a complete commit from one that needs explicit
  post-commit dispatch reconciliation.
  """

  use Jizoku.GraphMutation.Outcome,
    statuses: [:committed, :duplicate, :rejected, :committed_needs_reconciliation]
end
