defmodule Squidie.Runtime.Signal.JidoAdapter do
  @moduledoc """
  Converts Squidie runtime command signals to and from `Jido.Signal`.

  The adapter keeps `Squidie.Runtime.Signal` as the product-level contract and
  treats `Jido.Signal` as a boundary envelope. It does not dispatch, persist, or
  apply runtime commands.
  """

  alias Squidie.Runtime.Partition
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Trace

  @source "/squidie/runtime/commands"
  @datacontenttype "application/vnd.squidie.runtime-signal+json"

  @type error :: {:invalid_signal_adapter, term()}

  @start_commands [:start_run, :start_cron]
  @run_commands [:approve_run, :reject_run, :resume_run, :cancel_run, :replay_run]

  @type_string_by_command %{
    start_run: "squidie.runtime.command.start_run",
    start_cron: "squidie.runtime.command.start_cron",
    approve_run: "squidie.runtime.command.approve_run",
    reject_run: "squidie.runtime.command.reject_run",
    resume_run: "squidie.runtime.command.resume_run",
    cancel_run: "squidie.runtime.command.cancel_run",
    replay_run: "squidie.runtime.command.replay_run"
  }

  @command_by_type_string Map.new(@type_string_by_command, fn {command, type} ->
                            {type, command}
                          end)
  @command_by_name Map.new(@type_string_by_command, fn {command, _type} ->
                     {Atom.to_string(command), command}
                   end)

  @doc """
  Converts a Squidie runtime command signal to a `Jido.Signal`.
  """
  @spec to_jido(Signal.t()) :: {:ok, Jido.Signal.t()} | {:error, error()}
  def to_jido(%Signal{
        id: id,
        type: type,
        payload: payload,
        partition: partition,
        trace: trace,
        metadata: metadata,
        occurred_at: occurred_at,
        idempotency_key: idempotency_key
      }) do
    with {:ok, jido_type} <- jido_type(type),
         {:ok, subject} <- subject(payload),
         {:ok, data} <-
           transport_data(type, payload, metadata, occurred_at, idempotency_key, partition),
         {:ok, id} <- adapter_id(id),
         {:ok, trace} <- adapter_trace(trace),
         {:ok, jido_signal} <-
           normalize_jido_result(
             Jido.Signal.new(
               jido_type,
               data,
               id: id,
               source: @source,
               subject: subject,
               time: DateTime.to_iso8601(occurred_at),
               datacontenttype: @datacontenttype
             )
           ) do
      put_trace(jido_signal, trace)
    end
  end

  def to_jido(_signal), do: invalid(:signal, :expected_squidie_signal)

  @doc """
  Converts a `Jido.Signal` produced by this adapter back to a Squidie signal.
  """
  @spec from_jido(Jido.Signal.t()) :: {:ok, Signal.t()} | {:error, error()}
  def from_jido(%Jido.Signal{
        id: id,
        source: @source,
        type: jido_type,
        data: data,
        subject: subject,
        extensions: extensions
      }) do
    with {:ok, command_type} <- command_type(jido_type),
         {:ok, signal_data} <- signal_data(data),
         :ok <- matching_command_type(command_type, signal_data),
         {:ok, payload} <- fetch_payload(command_type, signal_data),
         :ok <- validate_subject(command_type, subject, payload),
         {:ok, metadata} <- fetch_map(signal_data, :metadata),
         {:ok, occurred_at} <- fetch_occurred_at(signal_data),
         {:ok, idempotency_key} <- fetch_idempotency_key(signal_data),
         {:ok, partition} <- fetch_partition(signal_data),
         {:ok, id} <- adapter_id(id),
         {:ok, trace} <- fetch_trace(extensions) do
      {:ok,
       %Signal{
         id: id,
         type: command_type,
         payload: payload,
         partition: partition,
         trace: trace,
         metadata: metadata,
         occurred_at: occurred_at,
         idempotency_key: idempotency_key
       }}
    end
  end

  def from_jido(%Jido.Signal{}), do: invalid(:source, :unsupported)
  def from_jido(_signal), do: invalid(:signal, :expected_jido_signal)

  defp jido_type(type) do
    case Map.fetch(@type_string_by_command, type) do
      {:ok, jido_type} -> {:ok, jido_type}
      :error -> invalid(:type, :unsupported)
    end
  end

  defp command_type(jido_type) do
    case Map.fetch(@command_by_type_string, jido_type) do
      {:ok, command_type} -> {:ok, command_type}
      :error -> invalid(:type, :unsupported)
    end
  end

  defp subject(%{run_id: run_id}) when is_binary(run_id), do: {:ok, run_id}
  defp subject(%{workflow: workflow}) when is_binary(workflow), do: {:ok, workflow}
  defp subject(_payload), do: invalid(:payload, :missing_subject_identity)

  defp transport_data(type, payload, metadata, occurred_at, idempotency_key, partition) do
    with {:ok, payload} <- transport_payload(type, payload),
         {:ok, partition} <- adapter_partition(partition) do
      data = %{
        "type" => Atom.to_string(type),
        "payload" => payload,
        "metadata" => metadata,
        "occurred_at" => DateTime.to_iso8601(occurred_at),
        "idempotency_key" => idempotency_key
      }

      {:ok, put_transport_partition(data, partition)}
    end
  end

  defp put_transport_partition(data, nil), do: data
  defp put_transport_partition(data, partition), do: Map.put(data, "partition", partition)

  defp transport_payload(:start_run, payload) do
    with {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string_or_nil(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, %{"workflow" => workflow, "trigger" => trigger, "input" => input}}
    end
  end

  defp transport_payload(:start_cron, payload) do
    with {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, %{"workflow" => workflow, "trigger" => trigger, "input" => input}}
    end
  end

  defp transport_payload(type, payload) when type in [:approve_run, :reject_run, :resume_run] do
    with {:ok, run_id} <- fetch_string(payload, :run_id),
         {:ok, attributes} <- fetch_map(payload, :attributes) do
      {:ok, %{"run_id" => run_id, "attributes" => attributes}}
    end
  end

  defp transport_payload(:cancel_run, payload) do
    with {:ok, run_id} <- fetch_string(payload, :run_id) do
      {:ok, %{"run_id" => run_id}}
    end
  end

  defp transport_payload(:replay_run, payload) do
    with {:ok, run_id} <- fetch_string(payload, :run_id),
         {:ok, allow_irreversible} <- fetch_boolean(payload, :allow_irreversible) do
      {:ok, %{"run_id" => run_id, "allow_irreversible" => allow_irreversible}}
    end
  end

  defp signal_data(data) when is_map(data) and map_size(data) > 0, do: {:ok, data}
  defp signal_data(_data), do: invalid(:data, :missing_signal_payload)

  defp matching_command_type(command_type, data) do
    case fetch_value(data, :type) do
      {:ok, value} -> matching_command_value(command_type, value)
      :error -> invalid(:type, :missing)
    end
  end

  defp matching_command_value(command_type, command_type), do: :ok

  defp matching_command_value(command_type, value) when is_binary(value) do
    case Map.fetch(@command_by_name, value) do
      {:ok, ^command_type} -> :ok
      {:ok, other_type} -> invalid(:type, {:mismatch, other_type})
      :error -> invalid(:type, {:mismatch, value})
    end
  end

  defp matching_command_value(_command_type, value), do: invalid(:type, {:mismatch, value})

  defp fetch_payload(command_type, data) do
    with {:ok, payload} <- fetch_map(data, :payload) do
      normalize_payload(command_type, payload)
    end
  end

  defp normalize_payload(:start_run, payload) do
    with {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string_or_nil(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, Signal.start_payload(workflow, trigger, input)}
    end
  end

  defp normalize_payload(:start_cron, payload) do
    with {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, Signal.start_payload(workflow, trigger, input)}
    end
  end

  defp normalize_payload(type, payload) when type in [:approve_run, :reject_run, :resume_run] do
    with {:ok, run_id} <- fetch_uuid(payload, :run_id),
         {:ok, attributes} <- fetch_map(payload, :attributes) do
      {:ok, %{run_id: run_id, attributes: attributes}}
    end
  end

  defp normalize_payload(:cancel_run, payload) do
    with {:ok, run_id} <- fetch_uuid(payload, :run_id) do
      {:ok, %{run_id: run_id}}
    end
  end

  defp normalize_payload(:replay_run, payload) do
    with {:ok, run_id} <- fetch_uuid(payload, :run_id),
         {:ok, allow_irreversible} <- fetch_boolean(payload, :allow_irreversible) do
      {:ok, %{run_id: run_id, allow_irreversible: allow_irreversible}}
    end
  end

  defp validate_subject(type, subject, %{workflow: workflow}) when type in @start_commands do
    if subject == workflow, do: :ok, else: invalid(:subject, :mismatch)
  end

  defp validate_subject(type, subject, %{run_id: run_id}) when type in @run_commands do
    if subject == run_id, do: :ok, else: invalid(:subject, :mismatch)
  end

  defp fetch_map(data, field) do
    case fetch_value(data, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, _value} -> invalid(field, :expected_map)
      :error -> invalid(field, :missing)
    end
  end

  defp fetch_string(data, field) do
    case fetch_value(data, field) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _value} -> invalid(field, :expected_non_empty_string)
      :error -> invalid(field, :missing)
    end
  end

  defp fetch_uuid(data, field) do
    with {:ok, value} <- fetch_string(data, field) do
      case Ecto.UUID.cast(value) do
        {:ok, uuid} -> {:ok, uuid}
        :error -> invalid(field, :invalid)
      end
    end
  end

  defp fetch_partition(data) do
    case fetch_value(data, :partition) do
      {:ok, partition} -> adapter_partition(partition)
      :error -> {:ok, nil}
    end
  end

  defp adapter_partition(partition) do
    case Partition.normalize(partition) do
      {:ok, partition} -> {:ok, partition}
      {:error, _reason} -> invalid(:partition, :invalid)
    end
  end

  defp adapter_id(id) when is_binary(id) and id != "" do
    if byte_size(id) <= 255 and String.valid?(id) do
      {:ok, id}
    else
      invalid(:id, :invalid)
    end
  end

  defp adapter_id(_id), do: invalid(:id, :invalid)

  defp adapter_trace(nil), do: {:ok, nil}

  defp adapter_trace(trace) do
    case Trace.normalize(trace) do
      {:ok, trace} -> {:ok, trace}
      {:error, {:invalid_trace, reason}} -> invalid(:trace, reason)
    end
  end

  defp fetch_trace(extensions) when is_map(extensions) do
    case {Map.fetch(extensions, "correlation"), Map.fetch(extensions, :correlation)} do
      {:error, :error} -> {:ok, nil}
      {{:ok, trace}, :error} -> adapter_trace(trace)
      {:error, {:ok, trace}} -> adapter_trace(trace)
      {{:ok, trace}, {:ok, trace}} -> adapter_trace(trace)
      {{:ok, _string_trace}, {:ok, _atom_trace}} -> invalid(:trace, :ambiguous)
    end
  end

  defp fetch_trace(_extensions), do: invalid(:extensions, :expected_map)

  defp put_trace(signal, nil), do: {:ok, signal}

  defp put_trace(signal, trace) do
    case Jido.Signal.put_extension(signal, "correlation", trace) do
      {:ok, signal} -> {:ok, signal}
      {:error, _reason} -> invalid(:trace, :invalid)
    end
  end

  defp fetch_string_or_nil(data, field) do
    case fetch_value(data, field) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _value} -> invalid(field, :expected_non_empty_string_or_nil)
      :error -> invalid(field, :missing)
    end
  end

  defp fetch_boolean(data, field) do
    case fetch_value(data, field) do
      {:ok, value} when is_boolean(value) -> {:ok, value}
      {:ok, _value} -> invalid(field, :expected_boolean)
      :error -> invalid(field, :missing)
    end
  end

  defp fetch_occurred_at(data) do
    case fetch_value(data, :occurred_at) do
      {:ok, %DateTime{} = occurred_at} ->
        {:ok, occurred_at}

      {:ok, occurred_at} when is_binary(occurred_at) ->
        parse_occurred_at(occurred_at)

      {:ok, _occurred_at} ->
        invalid(:occurred_at, :expected_datetime)

      :error ->
        invalid(:occurred_at, :missing)
    end
  end

  defp parse_occurred_at(occurred_at) do
    case DateTime.from_iso8601(occurred_at) do
      {:ok, parsed, _offset} -> {:ok, parsed}
      {:error, _reason} -> invalid(:occurred_at, :expected_datetime)
    end
  end

  defp fetch_idempotency_key(data) do
    case fetch_value(data, :idempotency_key) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, _value} -> invalid(:idempotency_key, :expected_non_empty_string)
      :error -> {:ok, nil}
    end
  end

  defp fetch_value(data, field) do
    string_field = Atom.to_string(field)

    cond do
      Map.has_key?(data, field) -> {:ok, Map.fetch!(data, field)}
      Map.has_key?(data, string_field) -> {:ok, Map.fetch!(data, string_field)}
      true -> :error
    end
  end

  defp normalize_jido_result({:ok, %Jido.Signal{} = signal}), do: {:ok, signal}
  defp normalize_jido_result({:error, reason}), do: invalid(:jido_signal, reason)

  defp invalid(field, reason), do: {:error, {:invalid_signal_adapter, {field, reason}}}
end
