defmodule Jizoku.ReadModel.Timeline do
  @moduledoc """
  Chronological, redaction-safe event projection for one workflow run.
  """

  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.ReadModel.Timeline.Event

  @event_order %{
    command_received: 0,
    run_started: 1,
    workflow_definition_migrated: 2,
    run_continued_from: 2,
    attempt_claimed: 2,
    attempt_completed: 3,
    attempt_failed: 3,
    runnable_applied: 4,
    jido_signal_enqueued: 5,
    jido_signal_delivered: 8,
    attempt_scheduled: 5,
    manual_step_paused: 6,
    run_continued_to: 7,
    run_terminal: 7
  }

  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          workflow: String.t() | nil,
          definition_version: String.t() | nil,
          definition_fingerprint: String.t() | nil,
          definition_resolution: map() | nil,
          queue: String.t(),
          status: atom(),
          terminal?: boolean(),
          terminal_status: atom() | nil,
          events: [Event.t()]
        }

  @enforce_keys [:run_id, :workflow, :queue, :status, :terminal?, :terminal_status]
  defstruct [
    :run_id,
    :partition,
    :workflow,
    :definition_version,
    :definition_fingerprint,
    :definition_resolution,
    :queue,
    :status,
    :terminal?,
    :terminal_status,
    events: []
  ]

  @doc """
  Projects a stable timeline from the public inspection snapshot.
  """
  @spec from_snapshot(Snapshot.t()) :: t()
  def from_snapshot(%Snapshot{} = snapshot) do
    %__MODULE__{
      run_id: snapshot.run_id,
      partition: snapshot.partition,
      workflow: snapshot.workflow,
      definition_version: snapshot.definition_version,
      definition_fingerprint: snapshot.definition_fingerprint,
      definition_resolution: snapshot.definition_resolution,
      queue: snapshot.queue,
      status: snapshot.status,
      terminal?: snapshot.terminal?,
      terminal_status: snapshot.terminal_status,
      events:
        snapshot
        |> events()
        |> Enum.sort_by(&event_sort_key/1)
    }
  end

  defp events(%Snapshot{} = snapshot) do
    snapshot
    |> command_events()
    |> Kernel.++(run_started_events(snapshot))
    |> Kernel.++(migration_events(snapshot))
    |> Kernel.++(continuation_events(snapshot))
    |> Kernel.++(attempt_events(snapshot))
    |> Kernel.++(applied_events(snapshot))
    |> Kernel.++(jido_signal_events(snapshot))
    |> Kernel.++(manual_events(snapshot))
    |> Kernel.++(terminal_events(snapshot))
  end

  defp jido_signal_events(%Snapshot{run_id: run_id, jido_signals: %{items: items}}) do
    Enum.flat_map(items, fn item ->
      enqueued =
        case Map.get(item, :enqueued_at) do
          %DateTime{} = at ->
            [
              event(:jido_signal_enqueued, at, run_id,
                summary: "Jido signal enqueued for delivery",
                details: jido_signal_details(item)
              )
            ]

          _missing ->
            []
        end

      delivered =
        case Map.get(item, :delivered_at) do
          %DateTime{} = at ->
            [
              event(:jido_signal_delivered, at, run_id,
                summary: "Jido signal delivery acknowledged",
                details: jido_signal_details(item)
              )
            ]

          _missing ->
            []
        end

      enqueued ++ delivered
    end)
  end

  defp jido_signal_events(%Snapshot{}) do
    []
  end

  defp jido_signal_details(item) do
    Map.take(item, [:outbox_id, :signal_id, :signal_type, :route, :status])
  end

  defp command_events(%Snapshot{run_id: run_id, command_history: commands}) do
    Enum.flat_map(commands, fn command ->
      case value(command, :occurred_at) do
        %DateTime{} = occurred_at ->
          signal_type = value(command, :signal_type)

          [
            event(:command_received, occurred_at, run_id,
              summary: "#{signal_type} command received",
              details: put_detail(%{}, :signal_type, signal_type)
            )
          ]

        _missing ->
          []
      end
    end)
  end

  defp run_started_events(%Snapshot{run_id: run_id, started_at: %DateTime{} = started_at}) do
    [event(:run_started, started_at, run_id, status: :running, summary: "run started")]
  end

  defp run_started_events(%Snapshot{}), do: []

  defp migration_events(%Snapshot{run_id: run_id, definition_migrations: migrations}) do
    Enum.flat_map(migrations, fn migration ->
      case value(migration, :occurred_at) do
        %DateTime{} = occurred_at ->
          [
            event(:workflow_definition_migrated, occurred_at, run_id,
              status: :migrated,
              summary:
                "workflow definition migrated from #{value(migration, :source_version)} to #{value(migration, :target_version)}",
              details:
                Map.take(migration, [
                  :migration_key,
                  :source_version,
                  :source_fingerprint,
                  :target_version,
                  :target_fingerprint,
                  :source_manual_step,
                  :target_manual_step
                ])
            )
          ]

        _missing ->
          []
      end
    end)
  end

  defp continuation_events(%Snapshot{} = snapshot) do
    continuation = snapshot.continuation || %{}

    Enum.reject(
      [
        continuation_event(
          :run_continued_from,
          snapshot.started_at,
          snapshot.run_id,
          Map.get(continuation, :continued_from)
        ),
        continuation_event(
          :run_continued_to,
          snapshot.terminal_at,
          snapshot.run_id,
          Map.get(continuation, :continued_to)
        )
      ],
      &is_nil/1
    )
  end

  defp continuation_event(type, %DateTime{} = occurred_at, run_id, edge)
       when type in [:run_continued_from, :run_continued_to] and is_map(edge) do
    linked_run_id = value(edge, :run_id)
    continuation_key = value(edge, :continuation_key)

    if is_binary(linked_run_id) and is_binary(continuation_key) do
      event(type, occurred_at, run_id,
        status: :linked,
        summary: continuation_summary(type),
        details: %{run_id: linked_run_id, continuation_key: continuation_key}
      )
    end
  end

  defp continuation_event(_type, _occurred_at, _run_id, _edge), do: nil

  defp continuation_summary(:run_continued_from), do: "run continued from predecessor"
  defp continuation_summary(:run_continued_to), do: "run continued to successor"

  defp attempt_events(%Snapshot{run_id: run_id, attempts: attempts}) do
    Enum.flat_map(attempts, fn attempt ->
      step = value(attempt, :step)
      runnable_key = value(attempt, :runnable_key)
      attempt_number = value(attempt, :attempt_number)

      Enum.reject(
        [
          attempt_event(
            :attempt_scheduled,
            value(attempt, :scheduled_at),
            run_id,
            step,
            runnable_key,
            status: scheduled_status(attempt),
            summary: "#{step} attempt scheduled",
            details: %{attempt_number: attempt_number, visible_at: value(attempt, :visible_at)}
          ),
          attempt_event(:attempt_claimed, value(attempt, :claimed_at), run_id, step, runnable_key,
            status: :claimed,
            summary: "#{step} attempt claimed",
            details: %{attempt_number: attempt_number}
          ),
          completed_or_failed_event(run_id, attempt, step, runnable_key, attempt_number)
        ],
        &is_nil/1
      )
    end)
  end

  defp completed_or_failed_event(run_id, attempt, step, runnable_key, attempt_number) do
    case {value(attempt, :status), value(attempt, :completed_at)} do
      {:completed, %DateTime{} = completed_at} ->
        attempt_event(:attempt_completed, completed_at, run_id, step, runnable_key,
          status: :completed,
          summary: "#{step} attempt completed",
          details: %{attempt_number: attempt_number}
        )

      {:failed, %DateTime{} = completed_at} ->
        attempt_event(:attempt_failed, completed_at, run_id, step, runnable_key,
          status: :failed,
          summary: "#{step} attempt failed",
          details: %{attempt_number: attempt_number}
        )

      _other ->
        nil
    end
  end

  defp applied_events(%Snapshot{
         run_id: run_id,
         applied_at: applied_at,
         planned_runnables: runnables
       }) do
    step_by_key =
      Map.new(runnables, fn runnable ->
        {value(runnable, :runnable_key), value(runnable, :step)}
      end)

    Enum.flat_map(applied_at, fn {runnable_key, occurred_at} ->
      case occurred_at do
        %DateTime{} = occurred_at ->
          step = Map.get(step_by_key, runnable_key)

          [
            attempt_event(:runnable_applied, occurred_at, run_id, step, runnable_key,
              status: :applied,
              summary: "#{step} result applied"
            )
          ]

        _missing ->
          []
      end
    end)
  end

  defp manual_events(%Snapshot{run_id: run_id, manual_state: manual_state})
       when is_map(manual_state) do
    case value(manual_state, :paused_at) do
      %DateTime{} = paused_at ->
        step = value(manual_state, :step)

        [
          event(:manual_step_paused, paused_at, run_id,
            step_id: step,
            status: :paused,
            summary: "#{step} paused for manual intervention",
            details:
              %{}
              |> put_detail(:kind, value(manual_state, :kind))
              |> put_detail(:reason, value(manual_state, :reason))
          )
        ]

      _missing ->
        []
    end
  end

  defp manual_events(%Snapshot{}), do: []

  defp terminal_events(%Snapshot{
         terminal_status: nil
       }),
       do: []

  defp terminal_events(%Snapshot{
         run_id: run_id,
         terminal_at: %DateTime{} = terminal_at,
         terminal_status: status
       })
       when is_atom(status) do
    [event(:run_terminal, terminal_at, run_id, status: status, summary: "run #{status}")]
  end

  defp terminal_events(%Snapshot{}), do: []

  defp attempt_event(type, %DateTime{} = occurred_at, run_id, step, runnable_key, opts) do
    event(type, occurred_at, run_id,
      step_id: step,
      runnable_key: runnable_key,
      status: Keyword.get(opts, :status),
      summary: Keyword.fetch!(opts, :summary),
      details: Keyword.get(opts, :details, %{})
    )
  end

  defp attempt_event(_type, _occurred_at, _run_id, _step, _runnable_key, _opts), do: nil

  defp event(type, %DateTime{} = occurred_at, run_id, opts) do
    %Event{
      type: type,
      occurred_at: occurred_at,
      run_id: run_id,
      step_id: Keyword.get(opts, :step_id),
      runnable_key: Keyword.get(opts, :runnable_key),
      status: Keyword.get(opts, :status),
      summary: Keyword.fetch!(opts, :summary),
      details: compact_details(Keyword.get(opts, :details, %{}))
    }
  end

  defp event_sort_key(%Event{} = event) do
    {
      DateTime.to_unix(event.occurred_at, :microsecond),
      Map.get(@event_order, event.type, 100),
      event.step_id || "",
      event.runnable_key || ""
    }
  end

  defp scheduled_status(attempt) do
    case value(attempt, :status) do
      :retry_scheduled -> :retry_scheduled
      _status -> :available
    end
  end

  defp value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp put_detail(details, key, value), do: Map.put(details, key, value)

  defp compact_details(details) when is_map(details) do
    Map.reject(details, fn {_key, value} -> is_nil(value) end)
  end

  defp compact_details(_details), do: %{}
end
