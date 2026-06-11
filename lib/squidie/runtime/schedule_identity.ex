defmodule Squidie.Runtime.ScheduleIdentity do
  @moduledoc """
  Builds stable identities for scheduled workflow activations.

  Cron delivery has to survive worker retries, duplicate job delivery, and code
  deploys between the time a scheduler queues an activation and the time Squidie
  receives it. This module keeps the scheduler-supplied or derived signal
  identity independent from the current workflow definition so an already
  persisted scheduled run can still be found after workflow code drifts.
  """

  import Bitwise, only: [band: 2, bor: 2]

  @spec run_id(String.t(), String.t(), String.t()) ::
          {:ok, Ecto.UUID.t()} | {:error, {:invalid_schedule_identity, term()}}
  @doc """
  Derives the deterministic run id used to fence one scheduled activation.

  The inputs are serialized workflow and trigger names plus a stable signal id.
  """
  def run_id(workflow, trigger, signal_id)
      when is_binary(workflow) and is_binary(trigger) and is_binary(signal_id) and
             workflow != "" and trigger != "" and signal_id != "" do
    {:ok,
     [workflow, trigger, signal_id]
     |> stable_identity_parts()
     |> deterministic_uuid()}
  end

  def run_id(_workflow, _trigger, _signal_id) do
    {:error, {:invalid_schedule_identity, :invalid}}
  end

  @spec signal_id(String.t(), String.t(), map()) :: {:ok, String.t()} | {:error, term()}
  @doc """
  Returns the scheduler signal id from the payload or derives one from a window.
  """
  def signal_id(workflow, trigger, payload)
      when is_binary(workflow) and is_binary(trigger) and is_map(payload) do
    with {:ok, intended_window} <- Squidie.Runtime.SchedulePayload.intended_window(payload) do
      case Squidie.Runtime.SchedulePayload.value(payload, "signal_id") do
        nil ->
          derived_signal_id(workflow, trigger, intended_window)

        signal_id when is_binary(signal_id) and signal_id != "" ->
          {:ok, signal_id}

        invalid_signal_id ->
          {:error, {:invalid_schedule_signal_id, invalid_signal_id}}
      end
    end
  end

  defp derived_signal_id(workflow, trigger, %{start_at: start_at, end_at: end_at}) do
    signal_parts = stable_identity_parts([workflow, trigger, start_at, end_at])
    digest = :crypto.hash(:sha256, signal_parts)

    {:ok, "sha256:" <> Base.url_encode64(digest, padding: false)}
  end

  defp derived_signal_id(_workflow, _trigger, _intended_window),
    do: {:error, {:invalid_schedule_identity, :missing_signal_id}}

  defp stable_identity_parts(parts) do
    parts
    |> Enum.map(fn part -> [Integer.to_string(byte_size(part)), ":", part] end)
    |> Enum.intersperse("|")
    |> IO.iodata_to_binary()
  end

  defp deterministic_uuid(identity) when is_binary(identity) do
    <<a::32, b::16, c::16, d::16, e::48, _rest::binary>> = :crypto.hash(:sha256, identity)
    version = bor(band(c, 0x0FFF), 0x5000)
    variant = bor(band(d, 0x3FFF), 0x8000)
    uuid_binary = <<a::32, b::16, version::16, variant::16, e::48>>
    {:ok, uuid} = Ecto.UUID.load(uuid_binary)
    uuid
  end
end
