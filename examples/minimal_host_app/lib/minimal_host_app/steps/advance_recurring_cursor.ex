defmodule MinimalHostApp.Steps.AdvanceRecurringCursor do
  @moduledoc """
  Advances one recurring cursor by continuing into a fresh workflow run.
  """

  use Squidie.Step,
    name: :advance_recurring_cursor,
    description: "Continues the first cursor page and completes the successor"

  @impl Squidie.Step
  @spec run(map(), Squidie.Step.Context.t()) :: Squidie.Step.result()
  def run(%{cursor: 0}, _context) do
    {:continue_as_new, %{cursor: 1}, key: "cursor-1", definition: :current}
  end

  def run(%{cursor: cursor}, _context) when is_integer(cursor) and cursor > 0 do
    {:ok, %{completed_cursor: cursor}}
  end
end
