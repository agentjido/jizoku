# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Test.Assertions do
  @moduledoc false

  alias Squidie.ReadModel.Inspection.Snapshot

  @doc false
  @spec raise_status_failure(
          atom(),
          term(),
          Snapshot.t() | nil,
          map() | :unavailable | nil
        ) :: no_return()
  def raise_status_failure(expected_status, result, snapshot, golden) do
    raise ExUnit.AssertionError,
      message: failure_message(expected_status, result, snapshot, golden)
  end

  defp failure_message(expected_status, result, %Snapshot{} = snapshot, golden) do
    base =
      "expected Squidie workflow status #{inspect(expected_status)}, " <>
        "got #{inspect(snapshot.status)}\n" <>
        "run_id: #{snapshot.run_id}\n" <>
        "reason: #{inspect(result_reason(result))}"

    append_timeline(base, golden)
  end

  defp failure_message(expected_status, result, nil, golden) do
    base =
      "expected Squidie workflow status #{inspect(expected_status)}, " <>
        "but execution did not return a snapshot\n" <>
        "reason: #{inspect(result_reason(result))}"

    append_timeline(base, golden)
  end

  defp append_timeline(message, nil) do
    message
  end

  defp append_timeline(message, %{schema_version: version, events: events})
       when is_integer(version) and is_list(events) do
    rendered_events = Enum.map_join(events, "\n", &render_event/1)

    message <> "\n\ntimeline (schema v#{version}):\n" <> rendered_events
  end

  defp append_timeline(message, _unavailable) do
    message <> "\n\ntimeline unavailable"
  end

  defp render_event(%{type: type, offset_us: offset_us} = event) do
    fields =
      ""
      |> add_field(:step, Map.get(event, :step))
      |> add_field(:runnable, Map.get(event, :runnable))
      |> add_field(:status, Map.get(event, :status))
      |> add_details(Map.get(event, :details, %{}))

    "  #{format_offset(offset_us)} #{format_value(type)}#{fields}"
  end

  defp render_event(_malformed) do
    "  ? malformed_event"
  end

  defp add_details(fields, details) when is_map(details) do
    fields
    |> add_field(:signal_type, Map.get(details, :signal_type))
    |> add_field(:attempt, Map.get(details, :attempt_number))
    |> add_field(:visible_offset_us, Map.get(details, :visible_offset_us))
    |> add_field(:manual_kind, Map.get(details, :kind))
    |> add_field(:linked_run, Map.get(details, :linked_run))
  end

  defp add_details(fields, _details) do
    fields
  end

  defp add_field(fields, _name, nil) do
    fields
  end

  defp add_field(fields, name, value) do
    fields <> " #{name}=#{format_value(value)}"
  end

  defp format_offset(offset_us) when is_integer(offset_us) and offset_us >= 0 do
    "+#{offset_us}us"
  end

  defp format_offset(offset_us) when is_integer(offset_us) do
    "#{offset_us}us"
  end

  defp format_offset(_offset_us) do
    "?"
  end

  defp format_value(value) when is_atom(value) or is_integer(value) do
    to_string(value)
  end

  defp format_value(value) when is_binary(value) do
    value
  end

  defp format_value(_value) do
    "malformed"
  end

  defp result_reason({reason, %Snapshot{}})
       when reason in [:cancelled, :completed, :continued, :failed] do
    :terminal
  end

  defp result_reason({reason, %Snapshot{}}) when is_atom(reason) do
    reason
  end

  defp result_reason({:error, {:execution_limit_reached, _details}}) do
    :execution_limit_reached
  end

  defp result_reason({:error, _reason}) do
    :execution_error
  end

  defp result_reason(_result) do
    :unknown
  end
end
