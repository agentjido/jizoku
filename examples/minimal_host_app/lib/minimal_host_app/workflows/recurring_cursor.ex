defmodule MinimalHostApp.Workflows.RecurringCursor do
  @moduledoc """
  Example recurring workflow that continues with fresh bounded run history.
  """

  use Jizoku.Workflow

  workflow do
    version "2026-08-09.recurring-cursor"

    trigger :recurring_cursor do
      manual()

      payload do
        field :cursor, :integer
      end
    end

    step :advance_recurring_cursor, MinimalHostApp.Steps.AdvanceRecurringCursor
    transition :advance_recurring_cursor, on: :ok, to: :complete
  end
end
