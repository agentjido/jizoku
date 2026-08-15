defmodule MinimalHostApp.RuntimeSignals do
  @moduledoc """
  Host-app boundary for runtime command signals.

  Application code can use `Jizoku.Runtime.Signal` directly. Jido-facing
  routers or agents can exchange `Jido.Signal` envelopes and hand them to this
  module at the boundary.
  """

  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Signal.JidoAdapter

  @type apply_result :: {:ok, Inspection.Snapshot.t()} | {:error, term()}

  @spec apply(Signal.t() | Jido.Signal.t()) :: apply_result()
  def apply(%Signal{} = signal), do: Jizoku.apply_signal(signal)

  def apply(%Jido.Signal{} = signal), do: Jizoku.apply_signal(signal)

  @doc """
  Routes an allowlisted domain Jido signal into a bounded Jizoku command.
  """
  @spec apply_domain(Jido.Signal.t()) :: apply_result()
  def apply_domain(%Jido.Signal{} = signal) do
    Jizoku.apply_signal(signal, jido_signal_resolver: MinimalHostApp.JidoSignalRoutes)
  end

  @spec to_jido(Signal.t()) :: {:ok, Jido.Signal.t()} | {:error, term()}
  def to_jido(%Signal{} = signal), do: JidoAdapter.to_jido(signal)
end
