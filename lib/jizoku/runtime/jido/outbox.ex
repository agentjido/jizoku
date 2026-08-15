# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Jido.Outbox do
  @moduledoc false

  alias Jido.Agent
  alias Jido.Agent.Directive
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.DispatchProtocol.ActionAttempt
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @items_key "items"
  @anomalies_key "anomalies"
  @outbox_id_key "outbox_id"
  @source_runnable_key "source_runnable_key"
  @route_key "route"
  @fingerprint_key "signal_fingerprint"
  @max_signal_bytes 1_048_576
  @max_route_bytes 255
  @anomaly_reasons %{
    "conflicting_enqueue" => :conflicting_jido_signal_enqueue,
    "invalid_acknowledgement" => :invalid_jido_signal_delivery_acknowledgement,
    "malformed_entry" => :malformed_jido_outbox_entry,
    "malformed_projection" => :malformed_jido_outbox_projection
  }
  @anomaly_entry_types %{
    "jido_signal_enqueued" => :jido_signal_enqueued,
    "jido_signal_delivery_acknowledged" => :jido_signal_delivery_acknowledged
  }

  @type intent :: %{
          required(:outbox_id) => String.t(),
          required(:run_id) => String.t(),
          required(:source_runnable_key) => String.t(),
          required(:signal_id) => String.t(),
          required(:signal) => map(),
          required(:route) => String.t(),
          required(:signal_fingerprint) => String.t()
        }

  @doc false
  @spec prepare(Jido.Signal.t(), String.t(), String.t(), String.t()) ::
          {:ok, intent()} | {:error, {:invalid_jido_emit, atom()}}
  def prepare(signal, run_id, source_runnable_key, route \\ "default")

  def prepare(%Jido.Signal{} = signal, run_id, source_runnable_key, route) do
    with :ok <- non_empty_string(run_id, :run_id),
         :ok <- non_empty_string(source_runnable_key, :source_runnable_key),
         :ok <- non_empty_string(signal.id, :signal_id),
         :ok <- route(route),
         {:ok, encoded_signal} <- encode_signal(signal) do
      outbox_id = fingerprint({run_id, source_runnable_key, signal.id})
      signal_fingerprint = fingerprint({encoded_signal, route})

      {:ok,
       %{
         outbox_id: outbox_id,
         run_id: run_id,
         source_runnable_key: source_runnable_key,
         signal_id: signal.id,
         signal: encoded_signal,
         route: route,
         signal_fingerprint: signal_fingerprint
       }}
    end
  end

  def prepare(_signal, _run_id, _source_runnable_key, _route) do
    invalid(:signal)
  end

  @doc false
  @spec prepare_directive(Directive.Emit.t(), ActionAttempt.t()) ::
          {:ok, intent()} | {:error, {:invalid_jido_emit, atom()}}
  def prepare_directive(
        %Directive.Emit{signal: %Jido.Signal{} = signal, dispatch: nil},
        %ActionAttempt{} = attempt
      ) do
    prepare(signal, attempt.run_id, attempt.runnable_key)
  end

  def prepare_directive(%Directive.Emit{dispatch: dispatch}, %ActionAttempt{})
      when dispatch != nil do
    invalid(:dispatch)
  end

  def prepare_directive(%Directive.Emit{}, %ActionAttempt{}) do
    invalid(:signal)
  end

  @doc false
  @spec encode_intent(intent()) :: map()
  def encode_intent(intent) when is_map(intent) do
    %{
      @outbox_id_key => Map.get(intent, :outbox_id),
      "run_id" => Map.get(intent, :run_id),
      @source_runnable_key => Map.get(intent, :source_runnable_key),
      "signal_id" => Map.get(intent, :signal_id),
      "signal" => Map.get(intent, :signal),
      @route_key => Map.get(intent, :route),
      @fingerprint_key => Map.get(intent, :signal_fingerprint)
    }
  end

  @doc false
  @spec decode_intent(map()) :: {:ok, intent()} | {:error, {:invalid_jido_emit, atom()}}
  def decode_intent(encoded) when is_map(encoded) and map_size(encoded) == 7 do
    with {:ok, outbox_id} <- non_empty_value(encoded, @outbox_id_key),
         {:ok, run_id} <- non_empty_value(encoded, "run_id"),
         {:ok, source_runnable_key} <- non_empty_value(encoded, @source_runnable_key),
         {:ok, signal_id} <- non_empty_value(encoded, "signal_id"),
         signal when is_map(signal) <- Map.get(encoded, "signal"),
         {:ok, %Jido.Signal{id: ^signal_id}} <- decode_signal(signal),
         {:ok, route} <- non_empty_value(encoded, @route_key),
         :ok <- route(route),
         {:ok, signal_fingerprint} <- non_empty_value(encoded, @fingerprint_key),
         true <- fingerprint({run_id, source_runnable_key, signal_id}) == outbox_id,
         true <- fingerprint({signal, route}) == signal_fingerprint do
      {:ok,
       %{
         outbox_id: outbox_id,
         run_id: run_id,
         source_runnable_key: source_runnable_key,
         signal_id: signal_id,
         signal: signal,
         route: route,
         signal_fingerprint: signal_fingerprint
       }}
    else
      _invalid -> invalid(:intent)
    end
  end

  def decode_intent(_encoded) do
    invalid(:intent)
  end

  @doc false
  @spec durable_entries(map(), ActionAttempt.t(), Agent.t(), DateTime.t()) ::
          {:ok, [Entry.t()]} | {:error, {:invalid_jido_emit, atom()}}
  def durable_entries(
        encoded_intent,
        %ActionAttempt{} = attempt,
        workflow_agent,
        %DateTime{} = now
      ) do
    with {:ok, intent} <- decode_intent(encoded_intent),
         true <- intent.run_id == attempt.run_id,
         true <- intent.source_runnable_key == attempt.runnable_key,
         {:ok, entry} <- enqueue_entry(intent, now) do
      classify_durable_entry(intent, entry, workflow_agent)
    else
      {:error, {:invalid_jido_emit, _reason}} = error -> error
      _invalid -> invalid(:intent)
    end
  end

  @doc false
  @spec recorded?(map(), ActionAttempt.t(), Agent.t()) :: boolean()
  def recorded?(encoded_intent, %ActionAttempt{} = attempt, workflow_agent) do
    with {:ok, intent} <- decode_intent(encoded_intent),
         %{"enqueued_at" => %DateTime{} = enqueued_at} <-
           find_item(workflow_agent, intent.outbox_id),
         {:ok, []} <- durable_entries(encoded_intent, attempt, workflow_agent, enqueued_at) do
      true
    else
      _not_recorded -> false
    end
  end

  @doc false
  @spec enqueue_entry(intent(), DateTime.t()) :: {:ok, Entry.t()} | {:error, term()}
  def enqueue_entry(intent, %DateTime{} = now) when is_map(intent) do
    DispatchProtocol.new_entry(:jido_signal_enqueued, %{
      @outbox_id_key => Map.get(intent, :outbox_id),
      @source_runnable_key => Map.get(intent, :source_runnable_key),
      @route_key => Map.get(intent, :route),
      @fingerprint_key => Map.get(intent, :signal_fingerprint),
      run_id: Map.get(intent, :run_id),
      signal_id: Map.get(intent, :signal_id),
      resolved_signal: Map.get(intent, :signal),
      occurred_at: now
    })
  end

  @doc false
  @spec acknowledge_entry(intent(), DateTime.t()) :: {:ok, Entry.t()} | {:error, term()}
  def acknowledge_entry(intent, %DateTime{} = now) when is_map(intent) do
    DispatchProtocol.new_entry(:jido_signal_delivery_acknowledged, %{
      @outbox_id_key => Map.get(intent, :outbox_id),
      @fingerprint_key => Map.get(intent, :signal_fingerprint),
      @route_key => Map.get(intent, :route),
      run_id: Map.get(intent, :run_id),
      signal_id: Map.get(intent, :signal_id),
      occurred_at: now
    })
  end

  @doc false
  @spec new_projection() :: map()
  def new_projection do
    %{@items_key => %{}, @anomalies_key => []}
  end

  @doc false
  @spec valid_projection?(term()) :: boolean()
  def valid_projection?(%{@items_key => items, @anomalies_key => anomalies}) do
    is_map(items) and Enum.all?(items, &valid_projected_item?/1) and is_list(anomalies) and
      Enum.all?(anomalies, &valid_projected_anomaly?/1)
  end

  def valid_projection?(_projection), do: false

  @doc false
  @spec apply_entry(map(), Entry.t()) :: map()
  def apply_entry(projection, %Entry{type: :jido_signal_enqueued} = entry) do
    apply_enqueued(projection, entry)
  end

  def apply_entry(projection, %Entry{type: :jido_signal_delivery_acknowledged} = entry) do
    apply_acknowledged(projection, entry)
  end

  def apply_entry(projection, %Entry{}), do: projection

  @doc false
  @spec apply_entry_observed(map(), Entry.t()) :: {map(), map() | nil}
  def apply_entry_observed(projection, %Entry{} = entry) do
    previous_anomalies = raw_anomalies(projection)
    updated = apply_entry(projection, entry)

    case raw_anomalies(updated) do
      [anomaly | tail] when tail == previous_anomalies ->
        {updated, normalize_projected_anomaly(anomaly)}

      _unchanged ->
        {updated, nil}
    end
  end

  @doc false
  @spec pending(map()) :: [map()]
  def pending(%{@items_key => items}) when is_map(items) do
    items
    |> Map.values()
    |> Enum.filter(&(Map.get(&1, "status") == "pending"))
    |> Enum.sort_by(&{Map.get(&1, "enqueued_at"), Map.get(&1, @outbox_id_key)})
  end

  def pending(_projection), do: []

  @doc false
  @spec items(map()) :: [map()]
  def items(%{@items_key => items}) when is_map(items) do
    items
    |> Map.values()
    |> Enum.sort_by(&{Map.get(&1, "enqueued_at"), Map.get(&1, @outbox_id_key)})
  end

  def items(_projection), do: []

  @doc false
  @spec anomalies(map()) :: [map()]
  def anomalies(%{@anomalies_key => anomalies}) when is_list(anomalies) do
    Enum.reverse(anomalies)
  end

  def anomalies(_projection), do: []

  @doc false
  @spec projection_anomalies(map()) :: [map()]
  def projection_anomalies(projection) do
    Enum.map(anomalies(projection), &normalize_projected_anomaly/1)
  end

  @doc false
  @spec public_summary(map()) :: %{
          required(:pending_count) => non_neg_integer(),
          required(:delivered_count) => non_neg_integer(),
          required(:items) => [map()]
        }
  def public_summary(projection) do
    public_items = Enum.map(items(projection), &public_item/1)

    %{
      pending_count: Enum.count(public_items, &(Map.get(&1, :status) == :pending)),
      delivered_count: Enum.count(public_items, &(Map.get(&1, :status) == :delivered)),
      items: public_items
    }
  end

  @doc false
  @spec fetch_projected_item(map(), String.t()) :: {:ok, map()} | :error
  def fetch_projected_item(projection, outbox_id) when is_binary(outbox_id) do
    fetch_item(projection, outbox_id)
  end

  @doc false
  @spec intent_from_item(map()) :: {:ok, intent()} | {:error, {:invalid_jido_emit, atom()}}
  def intent_from_item(item) when is_map(item) do
    decode_intent(%{
      @outbox_id_key => Map.get(item, @outbox_id_key),
      "run_id" => Map.get(item, "run_id"),
      @source_runnable_key => Map.get(item, @source_runnable_key),
      "signal_id" => Map.get(item, "signal_id"),
      "signal" => Map.get(item, "signal"),
      @route_key => Map.get(item, @route_key),
      @fingerprint_key => Map.get(item, @fingerprint_key)
    })
  end

  def intent_from_item(_item) do
    invalid(:intent)
  end

  @doc false
  @spec decode_signal(map()) :: {:ok, Jido.Signal.t()} | {:error, {:invalid_jido_emit, atom()}}
  def decode_signal(encoded) when is_map(encoded) do
    case Jido.Signal.from_map(encoded) do
      {:ok, %Jido.Signal{} = signal} -> {:ok, signal}
      {:error, _reason} -> invalid(:signal)
    end
  end

  def decode_signal(_encoded), do: invalid(:signal)

  defp encode_signal(%Jido.Signal{jido_dispatch: nil} = signal) do
    encoded = %{
      "specversion" => signal.specversion,
      "id" => signal.id,
      "source" => signal.source,
      "type" => signal.type,
      "subject" => signal.subject,
      "time" => signal.time,
      "datacontenttype" => signal.datacontenttype,
      "dataschema" => signal.dataschema,
      "data" => signal.data,
      "extensions" => signal.extensions
    }

    with :ok <- persistable_signal(encoded),
         {:ok, _signal} <- decode_signal(encoded) do
      {:ok, encoded}
    end
  end

  defp encode_signal(%Jido.Signal{}), do: invalid(:embedded_dispatch)

  defp persistable_signal(encoded) do
    if Options.storage_safe_value?(encoded) and
         byte_size(:erlang.term_to_binary(encoded)) <= @max_signal_bytes do
      :ok
    else
      invalid(:signal)
    end
  end

  defp apply_enqueued(projection, %Entry{data: data} = entry) do
    with true <- projection_shape?(projection),
         {:ok, item} <- projected_item(data, entry.occurred_at) do
      put_enqueued_item(projection, entry, item)
    else
      _invalid -> add_anomaly(safe_projection(projection), entry, "malformed_entry", nil)
    end
  end

  defp apply_acknowledged(projection, %Entry{data: data} = entry) do
    with true <- projection_shape?(projection),
         {:ok, outbox_id} <- non_empty_value(data, @outbox_id_key),
         {:ok, signal_fingerprint} <- non_empty_value(data, @fingerprint_key),
         {:ok, route} <- non_empty_value(data, @route_key),
         {:ok, item} <- fetch_item(projection, outbox_id),
         true <- valid_projected_item?({outbox_id, item}),
         true <- Map.get(item, @fingerprint_key) == signal_fingerprint,
         true <- Map.get(item, @route_key) == route,
         true <- Map.get(item, "run_id") == Map.get(data, :run_id),
         true <- Map.get(item, "signal_id") == Map.get(data, :signal_id),
         true <- Map.get(data, :occurred_at) == entry.occurred_at,
         true <- delivery_after_enqueue?(item, entry.occurred_at) do
      acknowledge_item(projection, item, entry.occurred_at)
    else
      _invalid ->
        add_anomaly(
          safe_projection(projection),
          entry,
          "invalid_acknowledgement",
          Map.get(data, @outbox_id_key)
        )
    end
  end

  defp projected_item(data, enqueued_at) when is_map(data) do
    with {:ok, run_id} <- non_empty_value(data, :run_id),
         {:ok, signal_id} <- non_empty_value(data, :signal_id),
         {:ok, outbox_id} <- non_empty_value(data, @outbox_id_key),
         {:ok, source_runnable_key} <- non_empty_value(data, @source_runnable_key),
         {:ok, route} <- non_empty_value(data, @route_key),
         {:ok, signal_fingerprint} <- non_empty_value(data, @fingerprint_key),
         signal when is_map(signal) <- Map.get(data, :resolved_signal),
         {:ok, %Jido.Signal{id: ^signal_id}} <- decode_signal(signal),
         true <- Map.get(data, :occurred_at) == enqueued_at,
         true <- match?(%DateTime{}, enqueued_at),
         true <- fingerprint({run_id, source_runnable_key, signal_id}) == outbox_id,
         true <- fingerprint({signal, route}) == signal_fingerprint do
      {:ok,
       %{
         @outbox_id_key => outbox_id,
         "run_id" => run_id,
         "source_runnable_key" => source_runnable_key,
         "signal_id" => signal_id,
         "signal" => signal,
         "route" => route,
         @fingerprint_key => signal_fingerprint,
         "status" => "pending",
         "enqueued_at" => enqueued_at,
         "delivered_at" => nil
       }}
    else
      _invalid -> {:error, :invalid}
    end
  end

  defp projected_item(_data, _enqueued_at), do: {:error, :invalid}

  defp acknowledge_item(projection, %{"status" => "delivered"}, _delivered_at) do
    projection
  end

  defp acknowledge_item(projection, item, delivered_at) do
    delivered = %{item | "status" => "delivered", "delivered_at" => delivered_at}
    put_in(projection, [@items_key, Map.fetch!(item, @outbox_id_key)], delivered)
  end

  defp fetch_item(%{@items_key => items}, outbox_id) do
    Map.fetch(items, outbox_id)
  end

  defp put_enqueued_item(projection, entry, item) do
    outbox_id = Map.fetch!(item, @outbox_id_key)

    case Map.fetch(Map.fetch!(projection, @items_key), outbox_id) do
      :error ->
        put_in(projection, [@items_key, outbox_id], item)

      {:ok, existing} ->
        if valid_projected_item?({outbox_id, existing}) do
          classify_duplicate_enqueue(projection, entry, existing, item)
        else
          add_anomaly(projection, entry, "malformed_projection", outbox_id)
        end
    end
  end

  defp classify_durable_entry(intent, entry, workflow_agent) do
    workflow_projection = workflow_agent.state.projection
    projection = Projection.jido_outbox(workflow_projection)
    {updated, anomaly} = apply_entry_observed(projection, entry)

    cond do
      not is_nil(anomaly) -> invalid(:conflict)
      updated == projection -> exact_existing_intent(intent, projection)
      Projection.terminal?(workflow_projection) -> invalid(:terminal_run)
      true -> {:ok, [entry]}
    end
  end

  defp exact_existing_intent(intent, projection) do
    case fetch_item(projection, intent.outbox_id) do
      {:ok, _existing} -> {:ok, []}
      :error -> invalid(:intent)
    end
  end

  defp find_item(workflow_agent, outbox_id) do
    projection = Projection.jido_outbox(workflow_agent.state.projection)

    case fetch_item(projection, outbox_id) do
      {:ok, item} -> item
      :error -> nil
    end
  end

  defp classify_duplicate_enqueue(projection, _entry, existing, item)
       when existing == item do
    projection
  end

  defp classify_duplicate_enqueue(projection, entry, existing, item) do
    if same_enqueue?(existing, item) do
      projection
    else
      add_anomaly(projection, entry, "conflicting_enqueue", Map.fetch!(item, @outbox_id_key))
    end
  end

  defp same_enqueue?(existing, candidate) do
    Map.drop(existing, ["status", "enqueued_at", "delivered_at"]) ==
      Map.drop(candidate, ["status", "enqueued_at", "delivered_at"])
  end

  defp valid_projected_item?({outbox_id, item}) when is_binary(outbox_id) and is_map(item) do
    valid_projected_identity?(outbox_id, item) and valid_projected_state?(item)
  end

  defp valid_projected_item?(_item), do: false

  defp valid_projected_identity?(outbox_id, item) do
    with true <- Map.get(item, @outbox_id_key) == outbox_id,
         true <-
           Enum.all?(
             ["run_id", "source_runnable_key", "signal_id", "route", @fingerprint_key],
             &non_empty_projected_field?(item, &1)
           ),
         signal when is_map(signal) <- Map.get(item, "signal"),
         {:ok, %Jido.Signal{id: signal_id}} <- decode_signal(signal),
         true <- signal_id == Map.get(item, "signal_id"),
         true <-
           fingerprint({Map.get(item, "run_id"), Map.get(item, "source_runnable_key"), signal_id}) ==
             outbox_id do
      fingerprint({signal, Map.get(item, "route")}) == Map.get(item, @fingerprint_key)
    else
      _invalid -> false
    end
  end

  defp valid_projected_state?(item) do
    enqueued_at = Map.get(item, "enqueued_at")

    Map.get(item, "status") in ["pending", "delivered"] and
      match?(%DateTime{}, enqueued_at) and valid_delivered_at?(item, enqueued_at)
  end

  defp non_empty_projected_field?(item, field) do
    value = Map.get(item, field)
    is_binary(value) and value != ""
  end

  defp valid_delivered_at?(%{"status" => "pending", "delivered_at" => nil}, _enqueued_at) do
    true
  end

  defp valid_delivered_at?(
         %{"status" => "delivered", "delivered_at" => %DateTime{} = delivered_at},
         %DateTime{} = enqueued_at
       ) do
    DateTime.compare(delivered_at, enqueued_at) != :lt
  end

  defp valid_delivered_at?(_item, _enqueued_at) do
    false
  end

  defp delivery_after_enqueue?(%{"enqueued_at" => %DateTime{} = enqueued_at}, %DateTime{} = at) do
    DateTime.compare(at, enqueued_at) != :lt
  end

  defp delivery_after_enqueue?(_item, _at), do: false

  defp non_empty_value(map, key) do
    case Map.get(map, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, :invalid}
    end
  end

  defp non_empty_string(value, _field) when is_binary(value) and value != "", do: :ok
  defp non_empty_string(_value, field), do: invalid(field)

  defp route(value)
       when is_binary(value) and value != "" and byte_size(value) <= @max_route_bytes do
    if String.valid?(value), do: :ok, else: invalid(:route)
  end

  defp route(_value), do: invalid(:route)

  defp add_anomaly(projection, %Entry{} = entry, reason, _outbox_id) do
    anomaly = %{
      "reason" => reason,
      "entry_type" => Atom.to_string(entry.type)
    }

    Map.update!(projection, @anomalies_key, &[anomaly | &1])
  end

  defp safe_projection(projection) do
    if projection_shape?(projection), do: projection, else: new_projection()
  end

  defp projection_shape?(%{@items_key => items, @anomalies_key => anomalies}) do
    # Entry replay validates only the affected item so rebuilding N outbox facts stays O(N).
    # Checkpoint admission continues to use valid_projection?/1 for a full semantic scan.
    is_map(items) and is_list(anomalies)
  end

  defp projection_shape?(_projection), do: false

  defp valid_projected_anomaly?(%{"reason" => reason, "entry_type" => entry_type} = anomaly) do
    map_size(anomaly) == 2 and Map.has_key?(@anomaly_reasons, reason) and
      Map.has_key?(@anomaly_entry_types, entry_type)
  end

  defp valid_projected_anomaly?(_anomaly) do
    false
  end

  defp public_item(item) do
    signal = Map.get(item, "signal", %{})

    %{
      outbox_id: Map.get(item, @outbox_id_key),
      signal_id: Map.get(item, "signal_id"),
      signal_type: Map.get(signal, "type"),
      route: Map.get(item, "route"),
      status: public_status(Map.get(item, "status")),
      enqueued_at: Map.get(item, "enqueued_at"),
      delivered_at: Map.get(item, "delivered_at")
    }
  end

  defp public_status("pending"), do: :pending
  defp public_status("delivered"), do: :delivered
  defp public_status(_status), do: :unknown

  defp normalize_projected_anomaly(anomaly) do
    %{
      reason: Map.fetch!(@anomaly_reasons, Map.fetch!(anomaly, "reason")),
      entry_type: Map.fetch!(@anomaly_entry_types, Map.fetch!(anomaly, "entry_type")),
      component: :jido_outbox
    }
  end

  defp raw_anomalies(%{@anomalies_key => anomalies}) when is_list(anomalies) do
    anomalies
  end

  defp raw_anomalies(_projection) do
    []
  end

  defp fingerprint(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp invalid(field), do: {:error, {:invalid_jido_emit, field}}
end
