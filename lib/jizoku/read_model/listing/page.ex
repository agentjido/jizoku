defmodule Jizoku.ReadModel.Listing.Page do
  @moduledoc """
  One stable page of redacted run-listing summaries.
  """

  alias Jizoku.ReadModel.Listing.Summary

  @type t :: %__MODULE__{
          items: [Summary.t()],
          next_cursor: String.t() | nil
        }

  @enforce_keys [:items, :next_cursor]
  defstruct [:items, :next_cursor]
end
