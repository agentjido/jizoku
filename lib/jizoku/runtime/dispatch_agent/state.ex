defmodule Jizoku.Runtime.DispatchAgent.State do
  @moduledoc false

  alias Jizoku.Runtime.DispatchProtocol.Projection

  defstruct [:partition, :queue, :projection, :thread_rev]

  @type t :: %__MODULE__{
          queue: String.t(),
          partition: String.t() | nil,
          projection: Projection.t(),
          thread_rev: non_neg_integer()
        }

  @doc """
  Builds dispatch-agent state from a queue projection and journal revision.
  """
  @spec new(String.t(), Projection.t(), non_neg_integer()) :: t()
  def new(queue, %Projection{} = projection, thread_rev)
      when is_binary(queue) and is_integer(thread_rev) and thread_rev >= 0 do
    new(nil, queue, projection, thread_rev)
  end

  @doc false
  @spec new(String.t() | nil, String.t(), Projection.t(), non_neg_integer()) :: t()
  def new(partition, queue, %Projection{} = projection, thread_rev)
      when (is_nil(partition) or is_binary(partition)) and is_binary(queue) and
             is_integer(thread_rev) and thread_rev >= 0 do
    %__MODULE__{
      partition: partition,
      queue: queue,
      projection: projection,
      thread_rev: thread_rev
    }
  end
end
