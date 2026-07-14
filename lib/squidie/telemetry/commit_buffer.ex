defmodule Squidie.Telemetry.CommitBuffer do
  @moduledoc false

  @enforce_keys [:owner, :ref]
  defstruct [:owner, :ref]

  @type intent :: {[atom()], map()}
  @type t :: %__MODULE__{owner: pid(), ref: reference()}

  @spec new() :: t()
  def new, do: %__MODULE__{owner: self(), ref: make_ref()}

  @spec enqueue(t(), [intent()]) :: :ok
  def enqueue(%__MODULE__{owner: owner, ref: ref}, intents) when is_list(intents) do
    send(owner, {__MODULE__, ref, intents})
    :ok
  end

  @spec drain(t()) :: [intent()]
  def drain(%__MODULE__{owner: owner, ref: ref}) when owner == self() do
    ref
    |> drain([])
    |> Enum.reverse()
    |> List.flatten()
  end

  def drain(%__MODULE__{}), do: []

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
