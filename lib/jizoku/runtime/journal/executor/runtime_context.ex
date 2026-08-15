defmodule Jizoku.Runtime.Journal.Executor.RuntimeContext do
  @moduledoc false

  defstruct [:storage, :queue, :now]

  @type t :: %__MODULE__{
          storage: term(),
          queue: String.t(),
          now: DateTime.t()
        }

  @doc """
  Builds an executor runtime context for journal storage, queue, and clock.
  """
  @spec new(term(), String.t(), DateTime.t()) :: t()
  def new(storage, queue, %DateTime{} = now) when is_binary(queue) do
    %__MODULE__{storage: storage, queue: queue, now: now}
  end
end
