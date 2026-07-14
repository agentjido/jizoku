defmodule Squidie.Telemetry.JournalEvents do
  @moduledoc false

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Telemetry.CommitBuffer
  alias Squidie.Telemetry.Emitter

  @spec commit(Journal.storage_config(), [DispatchProtocol.Entry.t()]) :: :ok
  def commit(storage, entries) when is_list(entries) do
    intents = sanitized_intents(entries, storage)

    case Squidie.Runtime.Journal.Storage.commit_buffer(storage) do
      %CommitBuffer{} = buffer -> CommitBuffer.enqueue(buffer, intents)
      nil -> emit_intents(intents)
    end
  catch
    _kind, _reason -> :ok
  end

  def commit(_storage, _entries), do: :ok

  @doc "Flushes lifecycle intents after a Squidie-owned transaction commits."
  @spec flush(CommitBuffer.t()) :: :ok
  def flush(%CommitBuffer{} = buffer) do
    buffer
    |> CommitBuffer.drain()
    |> emit_intents()
  end

  @doc "Discards lifecycle intents after a Squidie-owned transaction rolls back."
  @spec discard(CommitBuffer.t()) :: :ok
  def discard(%CommitBuffer{} = buffer), do: CommitBuffer.discard(buffer)

  defp intents(entries, storage) when is_list(entries) do
    context = build_context(storage, entries)
    Enum.flat_map(entries, &entry_intents(&1, context))
  end

  defp sanitized_intents(entries, storage) do
    entries
    |> intents(storage)
    |> Enum.map(fn {event, metadata} -> {event, Emitter.sanitize_metadata(metadata)} end)
  end

  defp emit_intents(intents) do
    Enum.each(intents, fn {event, metadata} -> Emitter.point(event, metadata) end)
    :ok
  end

  defp build_context(storage, entries) do
    run_ids =
      entries
      |> Enum.map(&Map.get(&1.data, :run_id))
      |> Enum.filter(&is_binary/1)
      |> Enum.uniq()

    %{
      partition: Squidie.Runtime.Journal.Storage.partition(storage),
      runs: Map.new(run_ids, &{&1, run_context(storage, &1)}),
      attempts: dispatch_attempts(storage, entries)
    }
  end

  defp run_context(storage, run_id) do
    case Journal.load_entries(storage, {:run, run_id}) do
      {:ok, entries} ->
        projection = WorkflowAgent.Projection.rebuild(entries)

        %{
          workflow: projection.workflow,
          planned_runnables: projection.planned_runnables
        }

      {:error, _reason} ->
        %{}
    end
  end

  defp dispatch_attempts(storage, [%{thread: {:dispatch, queue}} | _entries]) do
    case Journal.load_entries(storage, {:dispatch, queue}) do
      {:ok, entries} -> DispatchProtocol.Projection.rebuild(entries).attempts
      {:error, _reason} -> %{}
    end
  end

  defp dispatch_attempts(_storage, _entries), do: %{}

  defp entry_intents(%{type: :run_signal_received, data: data}, context) do
    point(:command, :received, data, context, %{
      command_type: command_type(Map.get(data, :signal_type)),
      signal_id: Map.get(data, :signal_id)
    })
  end

  defp entry_intents(%{type: :run_started, data: data}, context) do
    point(:run, :started, data, context, %{workflow: Map.get(data, :workflow)})
  end

  defp entry_intents(%{type: :run_terminal, data: data}, context) do
    point(:run, :terminal, data, context, %{status: Map.get(data, :status)})
  end

  defp entry_intents(%{type: :runnables_planned, data: data}, context) do
    data
    |> Map.get(:runnables, [])
    |> Enum.flat_map(fn runnable ->
      metadata = %{
        queue: value(runnable, :queue),
        step: value(runnable, :step),
        attempt_number: value(runnable, :attempt_number),
        runnable_key: value(runnable, :runnable_key),
        trace: value(runnable, :trace)
      }

      point(:runnable, :planned, data, context, metadata)
    end)
  end

  defp entry_intents(%{type: :runnable_applied, data: data}, context) do
    runnable = planned_runnable(context, data)

    point(:runnable, :applied, data, context, %{
      queue: value(runnable, :queue),
      step: value(runnable, :step),
      outcome: applied_outcome(Map.get(data, :transition)),
      runnable_key: Map.get(data, :runnable_key)
    })
  end

  defp entry_intents(%{type: :attempt_scheduled, data: data}, context) do
    attempt_point(:scheduled, data, context, :initial)
  end

  defp entry_intents(%{type: :attempt_claimed, data: data}, context) do
    attempt_point(:claimed, data, context)
  end

  defp entry_intents(%{type: :attempt_heartbeat, data: data}, context) do
    attempt_point(:heartbeat, data, context)
  end

  defp entry_intents(%{type: :attempt_completed, data: data}, context) do
    attempt_point(:completed, data, context)
  end

  defp entry_intents(%{type: :attempt_failed, data: data}, context) do
    attempt_point(:failed, data, context) ++ retry_intent(data, context)
  end

  defp entry_intents(%{type: :manual_step_paused, data: data}, context) do
    point(:manual, :paused, data, context, %{
      step: Map.get(data, :step),
      kind: Map.get(data, :kind)
    })
  end

  defp entry_intents(%{type: :manual_step_resolved, data: data}, context) do
    point(:manual, :resolved, data, context, %{
      step: Map.get(data, :step),
      action: Map.get(data, :action)
    })
  end

  defp entry_intents(%{type: :child_run_started, data: data}, context) do
    point(:child, :started, data, context, %{child_run_id: Map.get(data, :child_run_id)})
  end

  defp entry_intents(%{type: :dynamic_work_recorded, data: data}, context) do
    point(:dynamic_work, :recorded, data, context, %{
      dynamic_key: Map.get(data, :dynamic_key)
    })
  end

  defp entry_intents(_entry, _context), do: []

  defp retry_intent(%{retry_runnable_key: retry_key} = data, context)
       when is_binary(retry_key) do
    retry_data = %{
      run_id: Map.get(data, :run_id),
      runnable_key: retry_key,
      trace: Map.get(data, :retry_trace)
    }

    attempt_point(:retry_scheduled, retry_data, context, :retry)
  end

  defp retry_intent(_data, _context), do: []

  defp attempt_point(event, data, context, retry_state \\ nil) do
    attempt = Map.get(context.attempts, Map.get(data, :runnable_key))

    point(:attempt, event, data, context, %{
      queue: attempt_value(attempt, :queue) || Map.get(data, :queue),
      step: attempt_value(attempt, :step) || Map.get(data, :step),
      attempt_number: attempt_value(attempt, :attempt_number) || Map.get(data, :attempt_number),
      runnable_key: Map.get(data, :runnable_key),
      retry_state: retry_state,
      trace: Map.get(data, :trace) || attempt_value(attempt, :trace)
    })
  end

  defp point(domain, event, data, context, metadata) do
    run_id = Map.get(data, :run_id)
    run = Map.get(context.runs, run_id, %{})

    metadata =
      metadata
      |> Map.put_new(:run_id, run_id)
      |> Map.put_new(:workflow, Map.get(run, :workflow))
      |> Map.put_new(:partition, context.partition)
      |> Map.put_new(:trace, Map.get(data, :trace))

    [{[:squidie, :runtime, domain, event], metadata}]
  end

  defp planned_runnable(context, data) do
    run_id = Map.get(data, :run_id)
    runnable_key = Map.get(data, :runnable_key)

    context.runs
    |> Map.get(run_id, %{})
    |> Map.get(:planned_runnables, %{})
    |> Map.get(runnable_key, %{})
  end

  defp applied_outcome(transition) when is_map(transition) do
    case value(transition, :on) do
      :error -> :error
      "error" -> :error
      _other -> :ok
    end
  end

  defp applied_outcome(_transition), do: :ok

  defp attempt_value(nil, _key), do: nil
  defp attempt_value(attempt, key), do: Map.get(attempt, key)

  defp value(map, key) when is_map(map), do: Squidie.MapField.get(map, key)
  defp value(_map, _key), do: nil

  defp command_type(:start_run), do: :start_run
  defp command_type(:start_cron), do: :start_cron
  defp command_type(:replay_run), do: :replay_run
  defp command_type(:cancel_run), do: :cancel_run
  defp command_type(:resume_run), do: :resume_run
  defp command_type(:approve_run), do: :approve_run
  defp command_type(:reject_run), do: :reject_run
  defp command_type("start_run"), do: :start_run
  defp command_type("start_cron"), do: :start_cron
  defp command_type("replay_run"), do: :replay_run
  defp command_type("cancel_run"), do: :cancel_run
  defp command_type("resume_run"), do: :resume_run
  defp command_type("approve_run"), do: :approve_run
  defp command_type("reject_run"), do: :reject_run
  defp command_type(_type), do: nil
end
