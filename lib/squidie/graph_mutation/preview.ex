defmodule Squidie.GraphMutation.Preview do
  @moduledoc """
  Read-only outcome contract for a proposed graph mutation.

  Applied operations are stored as redacted summaries so the struct is safe for
  host tooling without another serialization step.
  """

  use Squidie.GraphMutation.Outcome,
    statuses: [:applicable, :duplicate, :invalid]
end
