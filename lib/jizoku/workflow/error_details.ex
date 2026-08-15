defmodule Jizoku.Workflow.ErrorDetails do
  @moduledoc false

  @doc """
  Builds transition recovery details for validation errors.
  """
  @spec transition_recovery(term(), term(), term()) :: map()
  def transition_recovery(from, on, recovery) do
    Map.new(from: from, on: on, recovery: recovery)
  end
end
