defmodule Squidie.Runtime.ScheduleMetadata do
  @moduledoc """
  Normalizes scheduler metadata for cron-triggered workflow starts.

  Cron activation is intentionally host-owned: a host app decides when a
  declared cron trigger fires and queues a `Squidie.Executor.Payload.cron/3`
  payload. This module translates that delivery payload plus the compiled
  workflow trigger definition into the durable context stored on the new run.

  The persisted shape is reserved under `run.context.schedule` and is meant to
  answer two different questions:

  - what logical schedule window was intended by the scheduler
  - when Squidie actually received and started processing the signal
  - which stable idempotency key, when configured, protects the logical start

  Keeping both timestamps matters because delayed delivery is normal in durable
  runtime systems. Workflow steps should not infer their schedule window from current
  wall-clock time; they should read the intended window from the run context.

  The metadata is stored in run context rather than workflow payload so it does
  not participate in the workflow's business input contract. It also means the
  metadata survives reload, inspection, explanation, and replay without adding a
  database column for one trigger kind.
  """

  alias Squidie.Runtime.ScheduleIdentity

  @type t :: %{
          required(:workflow) => String.t(),
          required(:trigger_name) => String.t(),
          required(:cron_expression) => String.t(),
          required(:timezone) => String.t(),
          required(:received_at) => String.t(),
          optional(:signal_id) => String.t(),
          optional(:idempotency) => :return_existing_run | :skip_duplicate,
          optional(:idempotency_key) => String.t(),
          optional(:intended_window) => map()
        }

  @doc """
  Builds the durable run context for one cron activation.

  The workflow and trigger definition contribute stable declarative data such
  as the workflow name, trigger name, cron expression, and timezone. The
  payload contributes scheduler-delivery data such as `signal_id` and
  `intended_window`. If the scheduler omits `signal_id`, Squidie derives one
  from the workflow, trigger, and intended window when both window bounds are
  present. The runtime adds
  `received_at` at activation delivery time, so operators can compare scheduler
  intent against actual processing. Any `received_at` value in the payload is
  ignored because this timestamp belongs to the runner boundary.

  When the cron trigger opts into idempotency, the runtime also stores
  `idempotency` and `idempotency_key` under the schedule context. The key is the
  scheduler `signal_id`, either supplied by the host scheduler or derived from a
  complete intended window. Without that identity, Squidie rejects the start
  because it cannot prove whether the activation is new or a duplicate.
  """
  @spec cron_context(module(), Squidie.Workflow.Definition.trigger(), map()) ::
          {:ok, %{schedule: t()}}
          | {:error, {:invalid_schedule_signal_id, term()}}
          | {:error, {:invalid_schedule_intended_window, term()}}
          | {:error, {:missing_schedule_idempotency_key, atom()}}
  def cron_context(workflow, %{name: trigger_name, type: :cron, config: config}, payload)
      when is_atom(workflow) and is_map(config) and is_map(payload) do
    workflow_name = Squidie.Workflow.Definition.serialize_workflow(workflow)
    raw_trigger_name = trigger_name
    trigger_name = Squidie.Workflow.Definition.serialize_trigger(trigger_name)
    idempotency = Map.get(config, :idempotency)

    with {:ok, intended_window} <- intended_window(payload),
         {:ok, signal_id} <- signal_id(workflow_name, trigger_name, intended_window, payload),
         {:ok, idempotency_key} <- idempotency_key(idempotency, signal_id, raw_trigger_name) do
      {:ok,
       %{
         schedule:
           %{
             workflow: workflow_name,
             trigger_name: trigger_name,
             # Cron workflow validation guarantees expression and timezone for
             # every compiled cron trigger before the runtime can load it.
             cron_expression: Map.fetch!(config, :expression),
             timezone: Map.fetch!(config, :timezone),
             received_at: received_at()
           }
           |> maybe_put_signal_id(signal_id)
           |> maybe_put_idempotency(idempotency, idempotency_key)
           |> maybe_put(:intended_window, intended_window)
       }}
    end
  end

  defp idempotency_key(nil, _signal_id, _trigger_name), do: {:ok, :none}

  defp idempotency_key(_idempotency, :none, trigger_name),
    do: {:error, {:missing_schedule_idempotency_key, trigger_name}}

  defp idempotency_key(_idempotency, signal_id, _trigger_name), do: {:ok, signal_id}

  defp signal_id(workflow_name, trigger_name, intended_window, payload) do
    case ScheduleIdentity.signal_id(workflow_name, trigger_name, %{
           "signal_id" => payload_value(payload, "signal_id"),
           "intended_window" => intended_window
         }) do
      {:ok, signal_id} -> {:ok, signal_id}
      {:error, {:invalid_schedule_identity, :missing_signal_id}} -> {:ok, :none}
      {:error, _reason} = error -> error
    end
  end

  defp intended_window(payload) do
    case payload_value(payload, "intended_window") do
      %{} = window ->
        normalize_intended_window(window)

      nil ->
        {:ok, nil}

      invalid ->
        {:error, {:invalid_schedule_intended_window, invalid}}
    end
  end

  defp normalize_intended_window(window) do
    with {:ok, start_at} <- window_value(window, :start_at),
         {:ok, end_at} <- window_value(window, :end_at) do
      %{}
      |> maybe_put(:start_at, start_at)
      |> maybe_put(:end_at, end_at)
      |> case do
        empty when map_size(empty) == 0 -> {:ok, nil}
        intended_window -> {:ok, intended_window}
      end
    end
  end

  defp window_value(window, key) when is_atom(key) do
    case value_with_fallback(window, Atom.to_string(key), key) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      invalid -> {:error, {:invalid_schedule_intended_window, %{key => invalid}}}
    end
  end

  defp received_at do
    DateTime.to_iso8601(DateTime.utc_now(:second))
  end

  defp payload_value(payload, "signal_id"),
    do: value_with_fallback(payload, "signal_id", :signal_id)

  defp payload_value(payload, "intended_window") do
    value_with_fallback(payload, "intended_window", :intended_window)
  end

  defp value_with_fallback(map, preferred_key, fallback_key) do
    case Map.fetch(map, preferred_key) do
      {:ok, value} -> value
      :error -> Map.get(map, fallback_key)
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp maybe_put_signal_id(map, :none), do: map
  defp maybe_put_signal_id(map, signal_id), do: Map.put(map, :signal_id, signal_id)

  defp maybe_put_idempotency(map, nil, :none), do: map

  defp maybe_put_idempotency(map, idempotency, idempotency_key) do
    map
    |> Map.put(:idempotency, idempotency)
    |> Map.put(:idempotency_key, idempotency_key)
  end
end
