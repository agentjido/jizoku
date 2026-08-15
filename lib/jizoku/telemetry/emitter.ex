defmodule Jizoku.Telemetry.Emitter do
  @moduledoc false

  alias Jizoku.Runtime.Trace
  alias Jizoku.Telemetry

  @metadata_keys [
    :queue,
    :workflow,
    :step,
    :outcome,
    :status,
    :command_type,
    :retry_state,
    :partition,
    :attempt_number,
    :action,
    :kind,
    :run_id,
    :signal_id,
    :runnable_key,
    :trace_id,
    :span_id,
    :parent_span_id,
    :causation_id,
    :child_run_id,
    :dynamic_key,
    :outbox_id,
    :route
  ]

  @max_metadata_value_bytes 255

  @doc """
  Emits a supported lifecycle point event with sanitized metadata.

  Unsupported event names are ignored.
  """
  @spec point([atom()], map()) :: :ok
  def point(event, metadata) when is_map(metadata) do
    if Telemetry.point_event?(event) do
      :telemetry.execute(
        event,
        %{count: 1, system_time: System.system_time()},
        sanitize_metadata(metadata)
      )
    end

    :ok
  end

  def point(_event, _metadata), do: :ok

  @doc """
  Instruments an operation for a supported runtime span prefix.

  Unsupported prefixes execute the operation without telemetry. Results and
  raised, thrown, or exited terms retain their original behavior.
  """
  @spec span([atom()], map(), (-> result)) :: result when result: term()
  def span(prefix, metadata, operation) when is_map(metadata) and is_function(operation, 0) do
    if Telemetry.span_prefix?(prefix) do
      execute_span(prefix, metadata, operation)
    else
      operation.()
    end
  end

  @doc """
  Filters metadata to the runtime allowlist and expands valid trace fields.

  Empty, oversized, malformed, and unsupported values are omitted.
  """
  @spec sanitize_metadata(map()) :: map()
  def sanitize_metadata(metadata) when is_map(metadata) do
    metadata
    |> expand_trace()
    |> Map.take(@metadata_keys)
    |> Enum.reduce(%{}, &put_safe_metadata/2)
  end

  defp execute_span(prefix, metadata, operation) do
    started_at = System.monotonic_time()
    start_metadata = metadata_with_outcome(metadata, :unknown)
    start_event = Enum.concat(prefix, [:start])

    :telemetry.execute(
      start_event,
      %{system_time: System.system_time(), monotonic_time: started_at},
      start_metadata
    )

    try do
      result = operation.()
      finished_at = System.monotonic_time()
      stop_event = Enum.concat(prefix, [:stop])
      stop_metadata = metadata_with_outcome(metadata, outcome(result))

      :telemetry.execute(
        stop_event,
        %{duration: finished_at - started_at, monotonic_time: finished_at},
        stop_metadata
      )

      result
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        finished_at = System.monotonic_time()
        exception_event = Enum.concat(prefix, [:exception])
        exception_metadata = metadata_with_outcome(metadata, :exception)

        :telemetry.execute(
          exception_event,
          %{duration: finished_at - started_at, monotonic_time: finished_at},
          exception_metadata
        )

        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp outcome({:error, _reason}), do: :error
  defp outcome(_result), do: :ok

  defp metadata_with_outcome(metadata, outcome) do
    metadata
    |> Map.put(:outcome, outcome)
    |> sanitize_metadata()
  end

  defp expand_trace(%{trace: trace} = metadata) do
    metadata = Map.delete(metadata, :trace)

    case Trace.normalize(trace) do
      {:ok, trace} -> Map.merge(metadata, Map.drop(trace, [:tracestate]))
      {:error, _reason} -> metadata
    end
  end

  defp expand_trace(metadata), do: metadata

  defp put_safe_metadata({key, value}, metadata) do
    if safe_metadata_value?(value) do
      Map.put(metadata, key, value)
    else
      metadata
    end
  end

  defp safe_metadata_value?(nil), do: false
  defp safe_metadata_value?(value) when is_atom(value), do: true
  defp safe_metadata_value?(value) when is_integer(value), do: true

  defp safe_metadata_value?(value) when is_binary(value) and value != "" do
    byte_size(value) <= @max_metadata_value_bytes and String.valid?(value)
  end

  defp safe_metadata_value?(_value), do: false
end
