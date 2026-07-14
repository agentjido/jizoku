defmodule Squidie.Telemetry.CommitBuffer do
  @moduledoc false

  @enforce_keys [:owner, :ref]
  defstruct [:owner, :ref]

  @type intent :: {[atom()], map()}
  @type t :: %__MODULE__{owner: pid(), ref: reference()}

  @doc """
  Creates a telemetry intent buffer owned by the calling process.
  """
  @spec new() :: t()
  def new, do: %__MODULE__{owner: self(), ref: make_ref()}

  @doc """
  Queues telemetry intents for later draining by the buffer owner.
  """
  @spec enqueue(t(), [intent()]) :: :ok
  def enqueue(%__MODULE__{owner: owner, ref: ref}, intents) when is_list(intents) do
    send(owner, {__MODULE__, ref, intents})
    :ok
  end

  @doc """
  Drains queued intents when called by the buffer owner.

  Calls from any other process return an empty list without consuming intents.
  """
  @spec drain(t()) :: [intent()]
  def drain(%__MODULE__{owner: owner, ref: ref}) when owner == self() do
    ref
    |> drain([])
    |> Enum.reverse()
    |> List.flatten()
  end

  def drain(%__MODULE__{}), do: []

  @doc """
  Discards all queued intents owned by the calling process.
  """
  @spec discard(t()) :: :ok
  def discard(%__MODULE__{} = buffer) do
    _intents = drain(buffer)
    :ok
  end

  defp drain(ref, intents) do
    receive do
      {__MODULE__, ^ref, queued} when is_list(queued) -> drain(ref, [queued | intents])
    after
      0 -> intents
    end
  end
end
