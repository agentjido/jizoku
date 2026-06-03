defmodule Squidie.Tools.Result do
  @moduledoc """
  Normalized successful tool result.
  """

  @enforce_keys [:adapter, :payload]
  defstruct [:adapter, :payload, metadata: %{}]

  @type t :: %__MODULE__{
          adapter: module(),
          payload: map(),
          metadata: map()
        }
end
