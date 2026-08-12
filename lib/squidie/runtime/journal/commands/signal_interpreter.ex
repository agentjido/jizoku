defmodule Squidie.Runtime.Journal.Commands.SignalInterpreter do
  @moduledoc """
  Routes normalized runtime command signals to journal command modules.

  The interpreter keeps signal envelope parsing at one boundary so start,
  replay, cancellation, and manual-control commands can stay focused on their
  own journal mutations.
  """

  alias Squidie.MapField
  alias Squidie.Runtime.Journal.Commands.Cancellation
  alias Squidie.Runtime.Journal.Commands.ManualControl
  alias Squidie.Runtime.Journal.Commands.Replay
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.ScheduleIdentity
  alias Squidie.Runtime.ScheduleMetadata
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Trace
  alias Squidie.Telemetry.Emitter
  alias Squidie.Workflow.Definition

  @manual_signal_types [:approve_run, :reject_run, :resume_run]
  @start_signal_types [:start_run, :start_cron]
  @max_source_bytes 1_024

  @doc """
  Applies a normalized runtime command signal to the journal runtime.
  """
  @spec apply(Signal.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def apply(%Signal{} = signal, opts) when is_list(opts) do
    with :ok <- validate_source(signal.source),
         {:ok, signal} <- ensure_trace(signal) do
      apply_with_span(signal, opts)
    end
  end

  def apply(%Signal{}, _opts), do: {:error, {:invalid_option, {:opts, :invalid}}}
  def apply(_signal, _opts), do: {:error, :invalid_signal}

  defp apply_with_span(%Signal{} = signal, opts) do
    Emitter.span(
      [:squidie, :runtime, :command, :apply],
      command_span_metadata(signal),
      fn -> apply_partitioned(signal, opts) end
    )
  end

  defp apply_partitioned(%Signal{} = signal, opts) do
    with {:ok, opts} <- partition_options(signal, opts) do
      do_apply(signal, opts)
    end
  end

  defp ensure_trace(%Signal{trace: nil} = signal) do
    with {:ok, trace} <- Trace.new_root() do
      {:ok, %Signal{signal | trace: trace}}
    end
  end

  defp ensure_trace(%Signal{trace: trace} = signal) do
    case Trace.normalize(trace) do
      {:ok, trace} -> {:ok, %Signal{signal | trace: trace}}
      {:error, {:invalid_trace, reason}} -> {:error, {:invalid_signal, {:trace, reason}}}
    end
  end

  defp validate_source(nil), do: :ok

  defp validate_source(source)
       when is_binary(source) and source != "" and byte_size(source) <= @max_source_bytes do
    if String.valid?(source), do: :ok, else: {:error, {:invalid_signal, {:source, :invalid}}}
  end

  defp validate_source(_source), do: {:error, {:invalid_signal, {:source, :invalid}}}

  defp command_span_metadata(%Signal{} = signal) do
    %{
      command_type: signal.type,
      signal_id: signal.id,
      trace: signal.trace,
      partition: signal.partition,
      run_id: MapField.get(signal.payload, :run_id)
    }
  end

  defp do_apply(%Signal{type: type} = signal, opts)
       when type in @start_signal_types and is_list(opts) do
    start_from_signal(signal, opts)
  end

  defp do_apply(%Signal{type: :replay_run} = signal, opts) when is_list(opts) do
    replay_from_signal(signal, opts)
  end

  defp do_apply(%Signal{type: :cancel_run} = signal, opts) when is_list(opts) do
    Cancellation.apply_signal(signal, opts)
  end

  defp do_apply(%Signal{type: type} = signal, opts)
       when type in @manual_signal_types and is_list(opts) do
    ManualControl.apply_signal(signal, opts)
  end

  defp do_apply(%Signal{type: type}, opts) when is_list(opts),
    do: {:error, {:unsupported_signal, type}}

  defp partition_options(%Signal{partition: nil}, opts), do: {:ok, opts}

  defp partition_options(%Signal{partition: partition}, opts) when is_binary(partition) do
    with {:ok, partition} <- Options.partition(partition) do
      reconcile_partition(opts, partition)
    end
  end

  defp partition_options(%Signal{}, _opts),
    do: {:error, {:invalid_signal, {:partition, :invalid}}}

  defp reconcile_partition(opts, signal_partition) do
    case Keyword.fetch(opts, :partition) do
      :error ->
        {:ok, Keyword.put(opts, :partition, signal_partition)}

      {:ok, nil} ->
        {:ok, Keyword.put(opts, :partition, signal_partition)}

      {:ok, ^signal_partition} ->
        {:ok, opts}

      {:ok, _other_partition} ->
        {:error, {:partition_mismatch, :signal}}
    end
  end

  defp start_from_signal(
         %Signal{
           type: type,
           payload: payload
         } = signal,
         opts
       )
       when is_map(payload) do
    workflow_name = MapField.get(payload, :workflow)
    trigger_name = MapField.get(payload, :trigger)
    input = MapField.get(payload, :input)

    with workflow_name when is_binary(workflow_name) <- workflow_name,
         input when is_map(input) <- input,
         {:ok, workflow, definition} <- Definition.load_serialized(workflow_name),
         {:ok, trigger} <- signal_trigger(definition, trigger_name, type),
         {:ok, input} <- normalize_start_input(definition, input),
         {:ok, start_input, start_opts} <-
           start_arguments(signal, workflow, definition, trigger, input, opts) do
      start_result(Starter.start_run(workflow, trigger, start_input, start_opts))
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_signal, type}}
    end
  end

  defp start_from_signal(%Signal{type: type}, _opts), do: {:error, {:invalid_signal, type}}

  defp normalize_start_input(%{payload: fields}, input)
       when is_list(fields) and is_map(input) do
    names_by_string = Map.new(fields, &{Atom.to_string(&1.name), &1.name})

    Enum.reduce_while(input, {:ok, %{}}, fn {key, value}, {:ok, normalized} ->
      normalized_key = Map.get(names_by_string, key, key)

      if Map.has_key?(normalized, normalized_key) do
        {:halt, {:error, {:invalid_signal, {:input, :conflicting_keys}}}}
      else
        {:cont, {:ok, Map.put(normalized, normalized_key, value)}}
      end
    end)
  end

  defp normalize_start_input(_definition, _input) do
    {:error, {:invalid_signal, {:input, :invalid}}}
  end

  defp replay_from_signal(
         %Signal{
           type: :replay_run,
           payload: %{run_id: run_id, allow_irreversible: allow_irreversible}
         } = signal,
         opts
       )
       when is_binary(run_id) and is_boolean(allow_irreversible) do
    with {:ok, signal_opts} <-
           command_idempotency_options(
             signal,
             "Squidie.Runtime.Signal",
             "replay_run:#{run_id}",
             opts
           ) do
      signal_opts = signal_options(signal, signal_opts)

      Replay.replay(run_id, [allow_irreversible: allow_irreversible], signal_opts)
    end
  end

  defp replay_from_signal(%Signal{type: type}, _opts), do: {:error, {:invalid_signal, type}}

  defp signal_trigger(definition, nil, :start_run),
    do: {:ok, Definition.default_trigger(definition)}

  defp signal_trigger(definition, trigger_name, type) when is_binary(trigger_name) do
    case Definition.deserialize_trigger(definition, trigger_name) do
      trigger when is_atom(trigger) -> {:ok, trigger}
      _invalid -> {:error, {:invalid_signal, type}}
    end
  end

  defp signal_trigger(_definition, _trigger_name, type), do: {:error, {:invalid_signal, type}}

  defp start_arguments(
         %Signal{type: :start_cron} = signal,
         workflow,
         definition,
         trigger,
         input,
         opts
       ) do
    with {:ok, _validated_opts} <- start_options(signal, workflow, trigger, opts),
         {:ok, trigger_definition} <- Definition.trigger(definition, trigger),
         {:ok, schedule_context} <-
           ScheduleMetadata.cron_context(
             workflow,
             trigger_definition,
             cron_schedule_payload(signal, input),
             signal.occurred_at
           ),
         {:ok, start_opts} <-
           start_options(
             signal,
             workflow,
             trigger,
             Keyword.put(opts, :initial_context, schedule_context)
           ) do
      {:ok, input, start_opts}
    end
  end

  defp start_arguments(signal, workflow, _definition, trigger, input, opts) do
    with {:ok, start_opts} <- start_options(signal, workflow, trigger, opts) do
      {:ok, input, start_opts}
    end
  end

  defp start_options(%Signal{type: :start_cron} = signal, workflow, trigger, opts) do
    with {:ok, opts} <- cron_idempotency_options(workflow, trigger, opts) do
      {:ok, signal_options(signal, opts)}
    end
  end

  defp start_options(%Signal{type: :start_run} = signal, workflow, trigger, opts) do
    workflow_name = Definition.serialize_workflow(workflow)
    trigger_name = signal_trigger_name(trigger)

    with {:ok, opts} <- command_idempotency_options(signal, workflow_name, trigger_name, opts) do
      {:ok, signal_options(signal, opts)}
    end
  end

  defp start_result({:ok, {:duplicate_schedule_start, snapshot}}), do: {:ok, snapshot}
  defp start_result(result), do: result

  defp signal_options(%Signal{} = signal, opts) do
    opts
    |> Keyword.put(:now, signal.occurred_at)
    |> Keyword.put(:command_signal, signal)
  end

  defp cron_idempotency_options(workflow, trigger, opts) do
    case schedule_idempotency_key(Keyword.get(opts, :initial_context, %{})) do
      {:ok, nil} ->
        {:ok, opts}

      {:ok, idempotency_key} ->
        workflow_name = Definition.serialize_workflow(workflow)
        trigger_name = Definition.serialize_trigger(trigger)

        with {:ok, run_id} <-
               ScheduleIdentity.run_id(workflow_name, trigger_name, idempotency_key) do
          opts =
            opts
            |> Keyword.put(:run_id, run_id)
            |> Keyword.put(:duplicate_schedule_start, true)

          {:ok, opts}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp command_idempotency_options(%Signal{idempotency_key: nil}, _workflow, _trigger, opts) do
    {:ok, opts}
  end

  defp command_idempotency_options(
         %Signal{idempotency_key: idempotency_key},
         workflow,
         trigger,
         opts
       )
       when is_binary(idempotency_key) and idempotency_key != "" do
    with {:ok, run_id} <- ScheduleIdentity.run_id(workflow, trigger, idempotency_key) do
      {:ok, Keyword.put(opts, :run_id, run_id)}
    end
  end

  defp command_idempotency_options(%Signal{}, _workflow, _trigger, _opts) do
    {:error, {:invalid_signal, {:idempotency_key, :expected_non_empty_string}}}
  end

  defp signal_trigger_name(nil), do: "__default__"

  defp signal_trigger_name(trigger) when is_atom(trigger),
    do: Definition.serialize_trigger(trigger)

  defp cron_schedule_payload(%Signal{idempotency_key: nil}, input) do
    input
  end

  defp cron_schedule_payload(%Signal{idempotency_key: idempotency_key}, input)
       when is_binary(idempotency_key) do
    input
    |> Map.delete("signal_id")
    |> Map.put(:signal_id, idempotency_key)
  end

  defp schedule_idempotency_key(context) when is_map(context) do
    context
    |> Squidie.Runtime.ScheduleContext.get()
    |> Squidie.Runtime.ScheduleContext.value(:idempotency_key)
    |> validate_schedule_idempotency_key()
  end

  defp schedule_idempotency_key(_context), do: {:ok, nil}

  defp validate_schedule_idempotency_key(nil), do: {:ok, nil}

  defp validate_schedule_idempotency_key(key) when is_binary(key) and key != "", do: {:ok, key}

  defp validate_schedule_idempotency_key(_key) do
    {:error, {:invalid_option, {:schedule_idempotency_key, :invalid}}}
  end
end
