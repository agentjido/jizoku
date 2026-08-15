defmodule Jizoku.Test.TelemetryCapture do
  @moduledoc false

  @spec attach([:telemetry.event_name()], pid()) :: term()
  def attach(events, owner \\ self()) do
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        events,
        fn event, measurements, metadata, pid ->
          send(pid, {:telemetry_event, event, measurements, metadata})
        end,
        owner
      )

    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end
end
