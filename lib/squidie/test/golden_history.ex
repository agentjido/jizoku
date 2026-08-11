# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Test.GoldenHistory do
  @moduledoc false

  alias Squidie.ReadModel.Timeline
  alias Squidie.ReadModel.Timeline.Event

  @schema_version 1
  @signal_types ~w(start_run start_cron approve_run reject_run resume_run cancel_run replay_run)

  @doc false
  @spec from_timeline(Timeline.t()) :: map()
  def from_timeline(%Timeline{} = timeline) do
    base_time = base_time(timeline.events)

    aliases = %{
      run_ids: %{timeline.run_id => "run-1"},
      runnable_keys: %{},
      next_run: 2,
      next_runnable: 1
    }

    {events, _aliases} =
      Enum.map_reduce(timeline.events, aliases, fn event, aliases ->
        golden_event(event, base_time, aliases)
      end)

    %{
      schema_version: @schema_version,
      workflow: timeline.workflow,
      queue: timeline.queue,
      partition: timeline.partition,
      status: safe_classification(timeline.status),
      terminal_status: safe_classification(timeline.terminal_status),
      events: events
    }
  end

  defp base_time(events) do
    case Enum.find(events, &(&1.type == :run_started)) do
      %Event{occurred_at: %DateTime{} = occurred_at} ->
        occurred_at

      _missing ->
        events
        |> List.first()
        |> event_time()
    end
  end

  defp event_time(%Event{occurred_at: %DateTime{} = occurred_at}), do: occurred_at
  defp event_time(_missing), do: nil

  defp golden_event(%Event{} = event, base_time, aliases) do
    {run_alias, aliases_after_run} =
      alias_id(aliases, :run_ids, :next_run, "run", event.run_id)

    {runnable_alias, aliases_after_runnable} =
      alias_id(
        aliases_after_run,
        :runnable_keys,
        :next_runnable,
        "runnable",
        event.runnable_key
      )

    {details, final_aliases} = golden_details(event, base_time, aliases_after_runnable)

    golden =
      %{
        type: safe_classification(event.type),
        offset_us: offset_us(event.occurred_at, base_time),
        run: run_alias
      }
      |> put_optional(:step, safe_identifier(event.step_id))
      |> put_optional(:runnable, runnable_alias)
      |> put_optional(:status, safe_classification(event.status))
      |> put_optional(:details, details, &(&1 != %{}))

    {golden, final_aliases}
  end

  defp golden_details(%Event{type: :command_received, details: details}, _base_time, aliases) do
    {%{signal_type: safe_signal_type(value(details, :signal_type))}, aliases}
  end

  defp golden_details(
         %Event{type: type, details: details},
         base_time,
         aliases
       )
       when type in [:attempt_scheduled, :attempt_claimed, :attempt_completed, :attempt_failed] do
    details =
      %{}
      |> put_optional(:attempt_number, safe_integer(value(details, :attempt_number)))
      |> put_optional(
        :visible_offset_us,
        offset_us(value(details, :visible_at), base_time)
      )

    {details, aliases}
  end

  defp golden_details(
         %Event{type: type, details: details},
         _base_time,
         aliases
       )
       when type in [:run_continued_from, :run_continued_to] do
    {linked_run, aliases} =
      alias_id(aliases, :run_ids, :next_run, "run", value(details, :run_id))

    {put_optional(%{}, :linked_run, linked_run), aliases}
  end

  defp golden_details(%Event{type: :manual_step_paused, details: details}, _base_time, aliases) do
    details = put_optional(%{}, :kind, safe_classification(value(details, :kind)))
    {details, aliases}
  end

  defp golden_details(%Event{}, _base_time, aliases) do
    {%{}, aliases}
  end

  defp alias_id(aliases, _ids_key, _counter_key, _prefix, nil), do: {nil, aliases}

  defp alias_id(aliases, ids_key, counter_key, prefix, value) when is_binary(value) do
    case get_in(aliases, [ids_key, value]) do
      nil ->
        counter = Map.fetch!(aliases, counter_key)
        alias_value = "#{prefix}-#{counter}"

        aliases =
          aliases
          |> put_in([ids_key, value], alias_value)
          |> Map.put(counter_key, counter + 1)

        {alias_value, aliases}

      alias_value ->
        {alias_value, aliases}
    end
  end

  defp alias_id(aliases, _ids_key, _counter_key, _prefix, _value) do
    {:malformed, aliases}
  end

  defp offset_us(%DateTime{} = occurred_at, %DateTime{} = base_time) do
    DateTime.diff(occurred_at, base_time, :microsecond)
  end

  defp offset_us(_occurred_at, _base_time), do: nil

  defp safe_signal_type(value) when is_atom(value) do
    value
    |> Atom.to_string()
    |> safe_signal_type()
  end

  defp safe_signal_type(value) when value in @signal_types, do: value
  defp safe_signal_type(_value), do: :unknown

  defp safe_classification(nil), do: nil
  defp safe_classification(value) when is_atom(value), do: value
  defp safe_classification(_value), do: :malformed

  defp safe_identifier(nil), do: nil
  defp safe_identifier(value) when is_binary(value), do: value
  defp safe_identifier(value) when is_atom(value), do: Atom.to_string(value)
  defp safe_identifier(_value), do: nil

  defp safe_integer(value) when is_integer(value), do: value
  defp safe_integer(_value), do: nil

  defp put_optional(map, _key, nil), do: map
  defp put_optional(map, key, value), do: Map.put(map, key, value)

  defp put_optional(map, key, value, predicate) do
    if predicate.(value), do: Map.put(map, key, value), else: map
  end

  defp value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp value(_map, _key), do: nil
end
