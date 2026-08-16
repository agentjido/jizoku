defmodule Jizoku.Runtime.Signal.JidoAdapter do
  @moduledoc """
  Converts Jizoku runtime command signals to and from `Jido.Signal`.

  The adapter keeps `Jizoku.Runtime.Signal` as the product-level contract and
  treats `Jido.Signal` as a boundary envelope. Public callers may pass
  recognized command envelopes directly to `Jizoku.apply_signal/2`; this
  module remains the conversion boundary and does not dispatch, persist, or
  apply runtime commands itself.
  """

  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Partition
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Trace

  @default_source "/jizoku/runtime/commands"
  @datacontenttype "application/vnd.jizoku.runtime-signal+json"
  @max_source_bytes 1_024
  @signal_data_fields [:type, :payload, :metadata, :occurred_at, :idempotency_key, :partition]
  @manual_attribute_fields [:actor, :comment, :metadata]

  @type error :: {:invalid_signal_adapter, term()}
  @type domain_envelope :: %{
          id: String.t(),
          source: String.t(),
          subject: String.t() | nil,
          type: String.t(),
          occurred_at: DateTime.t(),
          trace: Trace.t() | nil,
          event_key: String.t(),
          identity_key: String.t(),
          envelope_fingerprint: String.t()
        }

  @start_commands [:start_run, :start_cron]
  @run_commands [:approve_run, :reject_run, :resume_run, :cancel_run, :replay_run, :signal_run]

  @type_string_by_command %{
    start_run: "jizoku.runtime.command.start_run",
    start_cron: "jizoku.runtime.command.start_cron",
    approve_run: "jizoku.runtime.command.approve_run",
    reject_run: "jizoku.runtime.command.reject_run",
    resume_run: "jizoku.runtime.command.resume_run",
    cancel_run: "jizoku.runtime.command.cancel_run",
    replay_run: "jizoku.runtime.command.replay_run",
    signal_run: "jizoku.runtime.command.signal_run"
  }

  @command_by_type_string Map.new(@type_string_by_command, fn {command, type} ->
                            {type, command}
                          end)
  @command_by_name Map.new(@type_string_by_command, fn {command, _type} ->
                     {Atom.to_string(command), command}
                   end)

  @doc """
  Converts a Jizoku runtime command signal to a `Jido.Signal`.
  """
  @spec to_jido(Signal.t()) :: {:ok, Jido.Signal.t()} | {:error, error()}
  def to_jido(%Signal{
        id: id,
        source: source,
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
         {:ok, source} <- outbound_source(source),
         {:ok, trace} <- adapter_trace(trace),
         {:ok, jido_signal} <-
           normalize_jido_result(
             Jido.Signal.new(
               jido_type,
               data,
               id: id,
               source: source,
               subject: subject,
               time: DateTime.to_iso8601(occurred_at),
               datacontenttype: @datacontenttype
             )
           ) do
      put_trace(jido_signal, trace)
    end
  end

  def to_jido(_signal), do: invalid(:signal, :expected_jizoku_signal)

  @doc """
  Converts a recognized Jido command signal to a Jizoku signal.

  The CloudEvents source is retained as audit provenance. It is not an
  authorization decision; hosts must authenticate and authorize inbound
  signals before applying them.
  """
  @spec from_jido(Jido.Signal.t()) :: {:ok, Signal.t()} | {:error, error()}
  def from_jido(%Jido.Signal{
        id: id,
        source: source,
        specversion: specversion,
        type: jido_type,
        data: data,
        datacontenttype: datacontenttype,
        subject: subject,
        time: time,
        extensions: extensions
      }) do
    with {:ok, command_type} <- command_type(jido_type),
         :ok <- validate_specversion(specversion),
         {:ok, source} <- inbound_source(source),
         {:ok, signal_data} <- signal_data(data),
         :ok <- reject_alias_collisions(signal_data, @signal_data_fields),
         :ok <- matching_command_type(command_type, signal_data),
         {:ok, payload} <- fetch_payload(command_type, signal_data),
         :ok <- validate_subject(command_type, subject, payload),
         {:ok, metadata} <- fetch_optional_map(signal_data, :metadata, %{}),
         {:ok, occurred_at, occurred_at_source} <- fetch_occurred_at(signal_data, time),
         :ok <- validate_time(datacontenttype, time, occurred_at, occurred_at_source),
         {:ok, idempotency_key} <- fetch_idempotency_key(signal_data),
         {:ok, partition} <- fetch_partition(signal_data),
         {:ok, id} <- adapter_id(id),
         {:ok, trace} <- fetch_trace(extensions) do
      {:ok,
       %Signal{
         id: id,
         source: source,
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

  def from_jido(_signal), do: invalid(:signal, :expected_jido_signal)

  @doc false
  @spec domain_envelope(Jido.Signal.t()) :: {:ok, domain_envelope()} | {:error, error()}
  def domain_envelope(%Jido.Signal{
        id: id,
        source: source,
        specversion: specversion,
        subject: subject,
        time: time,
        type: type,
        datacontenttype: datacontenttype,
        dataschema: dataschema,
        data: data,
        extensions: extensions
      }) do
    with :ok <- validate_specversion(specversion),
         {:ok, id} <- adapter_id(id),
         {:ok, source} <- validate_source(source),
         {:ok, type} <- domain_type(type),
         {:ok, subject} <- domain_subject(subject),
         {:ok, occurred_at} <- parse_outer_time(time),
         {:ok, trace} <- fetch_trace(extensions),
         :ok <- validate_domain_value(data, :data),
         :ok <- validate_domain_value(extensions, :extensions),
         :ok <- validate_optional_domain_string(datacontenttype, :datacontenttype),
         :ok <- validate_optional_domain_string(dataschema, :dataschema) do
      event_key = identity_hash({source, id})

      envelope_fingerprint =
        identity_hash(%{
          id: id,
          source: source,
          subject: subject,
          type: type,
          specversion: specversion,
          occurred_at: DateTime.to_iso8601(occurred_at),
          datacontenttype: datacontenttype,
          dataschema: dataschema,
          data: data,
          extensions: extensions
        })

      {:ok,
       %{
         id: id,
         source: source,
         subject: subject,
         type: type,
         occurred_at: occurred_at,
         trace: trace,
         event_key: event_key,
         identity_key: "jido:" <> event_key,
         envelope_fingerprint: envelope_fingerprint
       }}
    end
  end

  def domain_envelope(_signal), do: invalid(:signal, :expected_jido_signal)

  defp validate_specversion("1.0.2"), do: :ok
  defp validate_specversion(_specversion), do: invalid(:specversion, :unsupported)

  defp inbound_source(source) do
    with {:ok, source} <- validate_source(source) do
      if source == @default_source, do: {:ok, nil}, else: {:ok, source}
    end
  end

  defp outbound_source(nil), do: {:ok, @default_source}
  defp outbound_source(source), do: validate_source(source)

  defp validate_source(source)
       when is_binary(source) and source != "" and byte_size(source) <= @max_source_bytes do
    if String.valid?(source), do: {:ok, source}, else: invalid(:source, :invalid)
  end

  defp validate_source(_source), do: invalid(:source, :invalid)

  defp domain_type(type)
       when is_binary(type) and type != "" and byte_size(type) <= 255 do
    if String.valid?(type), do: {:ok, type}, else: invalid(:type, :invalid)
  end

  defp domain_type(_type), do: invalid(:type, :invalid)

  defp domain_subject(nil), do: {:ok, nil}

  defp domain_subject(subject)
       when is_binary(subject) and subject != "" and byte_size(subject) <= 1_024 do
    if String.valid?(subject), do: {:ok, subject}, else: invalid(:subject, :invalid)
  end

  defp domain_subject(_subject), do: invalid(:subject, :invalid)

  defp validate_domain_value(value, field) do
    if Options.storage_safe_value?(value), do: :ok, else: invalid(field, :unsupported_term)
  end

  defp validate_optional_domain_string(nil, _field), do: :ok

  defp validate_optional_domain_string(value, field)
       when is_binary(value) and value != "" and byte_size(value) <= 1_024 do
    if String.valid?(value), do: :ok, else: invalid(field, :invalid)
  end

  defp validate_optional_domain_string(_value, field), do: invalid(field, :invalid)

  defp identity_hash(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

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

  defp transport_payload(:signal_run, payload) do
    with {:ok, run_id} <- fetch_string(payload, :run_id),
         {:ok, event} <- fetch_string(payload, :event),
         {:ok, correlation} <- fetch_string(payload, :correlation),
         {:ok, event_payload} <- fetch_map(payload, :event_payload) do
      {:ok,
       %{
         "run_id" => run_id,
         "event" => event,
         "correlation" => correlation,
         "event_payload" => event_payload
       }}
    end
  end

  defp signal_data(data) when is_map(data) and map_size(data) > 0, do: {:ok, data}
  defp signal_data(_data), do: invalid(:data, :missing_signal_payload)

  defp matching_command_type(command_type, data) do
    case fetch_value(data, :type) do
      {:ok, value} -> matching_command_value(command_type, value)
      :error -> :ok
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
    with :ok <- reject_alias_collisions(payload, [:workflow, :trigger, :input]),
         {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string_or_nil(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, Signal.start_payload(workflow, trigger, input)}
    end
  end

  defp normalize_payload(:start_cron, payload) do
    with :ok <- reject_alias_collisions(payload, [:workflow, :trigger, :input]),
         {:ok, workflow} <- fetch_string(payload, :workflow),
         {:ok, trigger} <- fetch_string(payload, :trigger),
         {:ok, input} <- fetch_map(payload, :input) do
      {:ok, Signal.start_payload(workflow, trigger, input)}
    end
  end

  defp normalize_payload(type, payload) when type in [:approve_run, :reject_run, :resume_run] do
    with :ok <- reject_alias_collisions(payload, [:run_id, :attributes]),
         {:ok, run_id} <- fetch_uuid(payload, :run_id),
         {:ok, attributes} <- fetch_map(payload, :attributes),
         {:ok, attributes} <- normalize_manual_attributes(attributes) do
      {:ok, %{run_id: run_id, attributes: attributes}}
    end
  end

  defp normalize_payload(:cancel_run, payload) do
    with :ok <- reject_alias_collisions(payload, [:run_id]),
         {:ok, run_id} <- fetch_uuid(payload, :run_id) do
      {:ok, %{run_id: run_id}}
    end
  end

  defp normalize_payload(:replay_run, payload) do
    with :ok <- reject_alias_collisions(payload, [:run_id, :allow_irreversible]),
         {:ok, run_id} <- fetch_uuid(payload, :run_id),
         {:ok, allow_irreversible} <- fetch_boolean(payload, :allow_irreversible) do
      {:ok, %{run_id: run_id, allow_irreversible: allow_irreversible}}
    end
  end

  defp normalize_payload(:signal_run, payload) do
    with :ok <-
           reject_alias_collisions(payload, [:run_id, :event, :correlation, :event_payload]),
         {:ok, run_id} <- fetch_uuid(payload, :run_id),
         {:ok, event} <- fetch_string(payload, :event),
         {:ok, correlation} <- fetch_string(payload, :correlation),
         {:ok, event_payload} <- fetch_map(payload, :event_payload) do
      {:ok,
       %{
         run_id: run_id,
         event: event,
         correlation: correlation,
         event_payload: event_payload
       }}
    end
  end

  defp normalize_manual_attributes(attributes) do
    with :ok <- reject_alias_collisions(attributes, @manual_attribute_fields) do
      {:ok, normalize_known_keys(attributes, @manual_attribute_fields)}
    end
  end

  defp normalize_known_keys(map, fields) do
    Enum.reduce(fields, map, fn field, normalized ->
      string_field = Atom.to_string(field)

      case Map.fetch(normalized, string_field) do
        :error ->
          normalized

        {:ok, value} ->
          normalized
          |> Map.delete(string_field)
          |> Map.put(field, value)
      end
    end)
  end

  defp reject_alias_collisions(map, fields) do
    case Enum.find(fields, &alias_collision?(map, &1)) do
      nil -> :ok
      field -> invalid(field, :ambiguous)
    end
  end

  defp alias_collision?(map, field) do
    Map.has_key?(map, field) and Map.has_key?(map, Atom.to_string(field))
  end

  defp validate_subject(_type, nil, _payload), do: :ok

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

  defp fetch_optional_map(data, field, default) do
    case fetch_value(data, field) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, _value} -> invalid(field, :expected_map)
      :error -> {:ok, default}
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

  defp validate_time(_datacontenttype, _time, _occurred_at, :outer) do
    :ok
  end

  defp validate_time(@datacontenttype, nil, _occurred_at, :embedded) do
    :ok
  end

  defp validate_time(@datacontenttype, time, %DateTime{} = occurred_at, :embedded)
       when is_binary(time) do
    case DateTime.from_iso8601(time) do
      {:ok, outer_time, _offset} -> validate_parsed_time(outer_time, occurred_at)
      _invalid -> invalid(:time, :invalid)
    end
  end

  defp validate_time(@datacontenttype, _time, _occurred_at, :embedded) do
    invalid(:time, :invalid)
  end

  defp validate_time(_legacy_datacontenttype, _time, _occurred_at, :embedded), do: :ok

  defp validate_parsed_time(outer_time, occurred_at) do
    if DateTime.compare(outer_time, occurred_at) == :eq do
      :ok
    else
      invalid(:time, :mismatch)
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

  defp fetch_occurred_at(data, outer_time) do
    case fetch_value(data, :occurred_at) do
      {:ok, %DateTime{} = occurred_at} ->
        {:ok, occurred_at, :embedded}

      {:ok, occurred_at} when is_binary(occurred_at) ->
        with {:ok, occurred_at} <- parse_occurred_at(occurred_at) do
          {:ok, occurred_at, :embedded}
        end

      {:ok, _occurred_at} ->
        invalid(:occurred_at, :expected_datetime)

      :error ->
        with {:ok, occurred_at} <- parse_outer_time(outer_time) do
          {:ok, occurred_at, :outer}
        end
    end
  end

  defp parse_outer_time(time) when is_binary(time), do: parse_occurred_at(time)
  defp parse_outer_time(_time), do: invalid(:time, :missing)

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
