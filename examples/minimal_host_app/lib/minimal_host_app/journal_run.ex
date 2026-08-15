defmodule MinimalHostApp.JournalRun do
  @moduledoc """
  Small host-owned worker loop that drains Jizoku journal attempts.

  Production hosts should replace this with their preferred capacity,
  back-pressure, and deployment model. The example keeps the loop explicit so a
  normally started host app has a real execution surface instead of relying on
  tests or scripts to call `Jizoku.execute_next/1`.
  """

  use GenServer

  @idle_interval_ms 100
  @error_interval_ms 1_000
  @heartbeat_interval_ms 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl GenServer
  def init(opts) do
    state = %{
      owner_id: Keyword.get(opts, :owner_id, "minimal-host-app-journal-run"),
      idle_interval_ms: Keyword.get(opts, :idle_interval_ms, @idle_interval_ms),
      error_interval_ms: Keyword.get(opts, :error_interval_ms, @error_interval_ms),
      heartbeat_interval_ms: Keyword.get(opts, :heartbeat_interval_ms, @heartbeat_interval_ms)
    }

    {:ok, state, {:continue, :drain}}
  end

  @impl GenServer
  def handle_continue(:drain, state) do
    {:noreply, drain_once(state)}
  end

  @impl GenServer
  def handle_info(:drain, state) do
    {:noreply, drain_once(state)}
  end

  defp drain_once(state) do
    case Jizoku.execute_next(execute_options(state)) do
      {:ok, :none} ->
        schedule_drain(state.idle_interval_ms)

      {:ok, _snapshot} ->
        schedule_drain(0)

      {:error, _reason} ->
        schedule_drain(state.error_interval_ms)
    end

    state
  end

  defp execute_options(state) do
    [owner_id: state.owner_id]
    |> maybe_put_heartbeat_interval(state.heartbeat_interval_ms)
  end

  defp maybe_put_heartbeat_interval(opts, nil), do: opts

  defp maybe_put_heartbeat_interval(opts, heartbeat_interval_ms) do
    Keyword.put(opts, :heartbeat_interval_ms, heartbeat_interval_ms)
  end

  defp schedule_drain(interval_ms) do
    Process.send_after(self(), :drain, interval_ms)
  end
end
