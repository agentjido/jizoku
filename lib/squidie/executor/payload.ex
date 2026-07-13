defmodule Squidie.Executor.Payload do
  @moduledoc """
  Backend-neutral cron payloads that host schedulers can hand to their queue.

  Payloads are plain maps with string keys so host applications can store them
  in job systems without depending on Squidie structs or atoms. A queued job
  should deliver the stored payload back to `Squidie.Runtime.Runner.perform/2`;
  the runner is responsible for loading the workflow definition and applying the
  runtime rules.

  Cron payloads identify a workflow trigger activation that will create a new
  run when delivered. Cron payloads may also include scheduler metadata such as
  a stable signal id and intended schedule window. That metadata is not workflow
  input; the runtime persists it under `run.context.schedule` before dispatching
  the first workflow step.
  """

  @type t :: %{
          required(String.t()) => String.t() | boolean() | map()
        }

  @doc """
  Builds the cron payload for a trigger activation.

  `workflow` and `trigger` are serialized into strings so the payload can cross
  a queue boundary. When the job is delivered, the runtime loads the workflow
  definition, validates the trigger, resolves trigger payload defaults, persists
  a new run, and dispatches the first step.

  Supported options:

  - `:signal_id` - a stable scheduler signal id for this activation. When it is
    omitted, Squidie derives a deterministic id from the trigger and intended
    window if both window bounds are present; otherwise the signal id is omitted
    so the missing scheduler identity is explicit.
  - `:intended_window` - a map describing the logical schedule window the
    scheduler meant to run, usually `%{start_at: iso8601, end_at: iso8601}`.

  The intended window is distinct from the worker execution time. If a cron job
  is delayed, retried, or delivered after a restart, steps should use
  `context.state.schedule.intended_window` instead of `DateTime.utc_now/0` to
  understand the scheduled period being processed.
  """
  @spec cron(module(), atom() | String.t()) :: t()
  @spec cron(module(), atom() | String.t(), keyword()) :: t()
  def cron(workflow, trigger, opts \\ []) when is_atom(workflow) and is_list(opts) do
    %{
      "kind" => "cron",
      "workflow" => Squidie.Workflow.Definition.serialize_workflow(workflow),
      "trigger" => Squidie.Workflow.Definition.serialize_trigger(trigger)
    }
    |> maybe_put_partition(opts)
    |> maybe_put("signal_id", Keyword.get(opts, :signal_id))
    |> maybe_put("intended_window", Keyword.get(opts, :intended_window))
  end

  defp maybe_put(payload, _key, nil), do: payload
  defp maybe_put(payload, key, value), do: Map.put(payload, key, value)

  defp maybe_put_partition(payload, opts) do
    case Keyword.fetch(opts, :partition) do
      {:ok, partition} -> Map.put(payload, "partition", partition)
      :error -> payload
    end
  end
end
