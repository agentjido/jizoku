defmodule Squidie.ReadModel.Timeline.Event do
  @moduledoc """
  Redaction-safe event for chronological operator run timelines.
  """

  @type t :: %__MODULE__{
          type: atom(),
          occurred_at: DateTime.t(),
          run_id: String.t(),
          step_id: String.t() | nil,
          runnable_key: String.t() | nil,
          status: atom() | nil,
          summary: String.t(),
          details: map()
        }

  @enforce_keys [:type, :occurred_at, :run_id, :summary]
  defstruct [
    :type,
    :occurred_at,
    :run_id,
    :step_id,
    :runnable_key,
    :status,
    :summary,
    details: %{}
  ]
end
