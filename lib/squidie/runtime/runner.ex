defmodule Squidie.Runtime.Runner do
  @moduledoc """
  Backend-neutral runtime entrypoints for host scheduler jobs.

  Cron scheduler jobs should call this module when a serialized cron activation
  is delivered. Step execution is claimed through `Squidie.execute_next/1`.
  """

  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.ScheduleIdentity
  alias Squidie.Runtime.ScheduleMetadata
  alias Squidie.Workflow.Definition

  @doc """
  Executes one queued runtime payload.

  Host job backends should store payloads produced by
  `Squidie.Executor.Payload` and pass the payload back here when the job is
  delivered. The runner accepts cron activation payloads only.

  Cron payloads create a new run. Any scheduler metadata carried by
  `Squidie.Executor.Payload.cron/3` is persisted into the run context before
  the first workflow step is dispatched.
  """
  @spec perform(map(), keyword()) :: :ok | {:error, term()}
  def perform(args, overrides \\ [])

  def perform(%{"kind" => "cron", "workflow" => workflow, "trigger" => trigger} = args, overrides)
      when is_binary(workflow) and is_binary(trigger) do
    case start_cron_trigger(workflow, trigger, args, overrides) do
      {:ok, {:duplicate_schedule_start, _run_id}} -> :ok
      {:ok, {:skipped_schedule_start, _run_id}} -> :ok
      result -> result
    end
  end

  def perform(args, _overrides) do
    {:error, {:invalid_runtime_payload, args}}
  end

  @doc """
  Starts a workflow run from a serialized cron trigger.

  This arity is useful for host schedulers that only know the workflow and
  trigger names. It records schedule metadata, including the actual receive
  timestamp. A signal id is recorded only when the scheduler supplies one or an
  intended window is available for deterministic derivation.
  """
  @spec start_cron_trigger(String.t(), String.t(), keyword()) ::
          :ok
          | {:ok, {:duplicate_schedule_start, Ecto.UUID.t()}}
          | {:ok, {:skipped_schedule_start, Ecto.UUID.t()}}
          | {:error, term()}
  def start_cron_trigger(workflow_name, trigger_name, overrides \\ [])
      when is_binary(workflow_name) and is_binary(trigger_name) do
    start_cron_trigger(workflow_name, trigger_name, %{}, overrides)
  end

  @doc """
  Starts a workflow run from a serialized cron trigger and scheduler payload.

  `signal_payload` is the scheduler metadata subset from a cron payload. When
  it contains `"signal_id"` and `"intended_window"`, the runtime stores those
  values under `run.context.schedule` before dispatching the first step,
  making delayed delivery and restart recovery observable to workflow steps
  and operators.
  """
  @spec start_cron_trigger(String.t(), String.t(), map(), keyword()) ::
          :ok
          | {:ok, {:duplicate_schedule_start, Ecto.UUID.t()}}
          | {:ok, {:skipped_schedule_start, Ecto.UUID.t()}}
          | {:error, term()}
  def start_cron_trigger(workflow_name, trigger_name, signal_payload, overrides)
      when is_binary(workflow_name) and is_binary(trigger_name) and is_map(signal_payload) and
             is_list(overrides) do
    with {:ok, overrides} <- cron_partition_options(signal_payload, overrides) do
      case existing_journal_schedule_start(workflow_name, trigger_name, signal_payload, overrides) do
        {:ok, result} -> result
        :miss -> start_new_cron_trigger(workflow_name, trigger_name, signal_payload, overrides)
        {:error, _reason} = error -> error
      end
    end
  end

  defp cron_partition_options(signal_payload, overrides) do
    reconcile_cron_partition(
      Map.fetch(signal_payload, "partition"),
      Keyword.fetch(overrides, :partition),
      overrides
    )
  end

  defp reconcile_cron_partition(:error, :error, overrides),
    do: {:ok, Keyword.put(overrides, :partition, nil)}

  defp reconcile_cron_partition(:error, {:ok, partition}, overrides) do
    with {:ok, partition} <- Options.partition(partition) do
      {:ok, Keyword.put(overrides, :partition, partition)}
    end
  end

  defp reconcile_cron_partition({:ok, partition}, :error, overrides) do
    with {:ok, partition} <- Options.partition(partition) do
      {:ok, Keyword.put(overrides, :partition, partition)}
    end
  end

  defp reconcile_cron_partition({:ok, partition}, {:ok, partition}, overrides) do
    with {:ok, partition} <- Options.partition(partition) do
      {:ok, Keyword.put(overrides, :partition, partition)}
    end
  end

  defp reconcile_cron_partition({:ok, payload_partition}, {:ok, override_partition}, _overrides) do
    with {:ok, _payload_partition} <- Options.partition(payload_partition),
         {:ok, _override_partition} <- Options.partition(override_partition) do
      {:error, {:partition_mismatch, :cron_payload}}
    end
  end

  defp start_new_cron_trigger(workflow_name, trigger_name, signal_payload, overrides) do
    with {:ok, now} <- Options.now_from_opts(overrides),
         overrides = Keyword.put(overrides, :now, now),
         {:ok, workflow, definition} <-
           Definition.load_serialized(workflow_name),
         trigger when is_atom(trigger) <-
           Definition.deserialize_trigger(definition, trigger_name),
         {:ok, trigger_definition} <- Definition.trigger(definition, trigger),
         {:ok, schedule_context} <-
           ScheduleMetadata.cron_context(workflow, trigger_definition, signal_payload, now),
         {:ok, run_result} <- start_cron_run(workflow, trigger, schedule_context, overrides) do
      cron_start_result(run_result)
    else
      {:error, reason} ->
        {:error, reason}

      invalid_trigger ->
        {:error, {:invalid_trigger, invalid_trigger}}
    end
  end

  defp existing_journal_schedule_start(workflow_name, trigger_name, signal_payload, overrides) do
    with {:ok, run_id} <- schedule_run_id(workflow_name, trigger_name, signal_payload),
         {:ok, %Snapshot{} = snapshot} <-
           Squidie.inspect_run(run_id, journal_inspection_options(overrides)) do
      {:ok, cron_start_result({:duplicate_schedule_start, snapshot})}
    else
      {:error, :not_found} -> :miss
      {:error, {:invalid_schedule_identity, _reason}} -> :miss
      {:error, _reason} = error -> error
    end
  end

  defp schedule_run_id(workflow_name, trigger_name, signal_payload) do
    with {:ok, signal_id} <-
           ScheduleIdentity.signal_id(workflow_name, trigger_name, signal_payload) do
      ScheduleIdentity.run_id(workflow_name, trigger_name, signal_id)
    end
  end

  defp journal_inspection_options(overrides) do
    overrides
    |> Keyword.put(:runtime, :journal)
    |> Keyword.put(:read_model, :read_model)
  end

  defp start_cron_run(workflow, trigger, schedule_context, overrides) do
    Squidie.start_run_with_initial_context(
      workflow,
      trigger,
      %{},
      schedule_context,
      overrides
    )
  end

  defp cron_start_result({:duplicate_schedule_start, %Snapshot{run_id: run_id, context: context}}) do
    case schedule_idempotency(context) do
      :skip_duplicate -> {:ok, {:skipped_schedule_start, run_id}}
      _other -> {:ok, {:duplicate_schedule_start, run_id}}
    end
  end

  defp cron_start_result(%Snapshot{}), do: :ok

  defp schedule_idempotency(context) when is_map(context) do
    context
    |> Squidie.Runtime.ScheduleContext.get()
    |> Squidie.Runtime.ScheduleContext.value(:idempotency)
    |> case do
      "skip_duplicate" -> :skip_duplicate
      :skip_duplicate -> :skip_duplicate
      strategy -> strategy
    end
  end
end
