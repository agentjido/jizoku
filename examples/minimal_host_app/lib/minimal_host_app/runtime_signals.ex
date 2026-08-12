defmodule MinimalHostApp.RuntimeSignals do
  @moduledoc """
  Host-app boundary for runtime command signals.

  Application code can use `Squidie.Runtime.Signal` directly. Jido-facing
  routers or agents can exchange `Jido.Signal` envelopes and hand them to this
  module at the boundary.
  """

  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Signal.JidoAdapter

  @type apply_result :: {:ok, Inspection.Snapshot.t()} | {:error, term()}

  @spec apply(Signal.t() | Jido.Signal.t()) :: apply_result()
  def apply(%Signal{} = signal), do: Squidie.apply_signal(signal)

  def apply(%Jido.Signal{} = signal), do: Squidie.apply_signal(signal)

  @spec to_jido(Signal.t()) :: {:ok, Jido.Signal.t()} | {:error, term()}
  def to_jido(%Signal{} = signal), do: JidoAdapter.to_jido(signal)
end
