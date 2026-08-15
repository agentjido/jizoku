# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Signal.JidoIngress do
  @moduledoc false

  defmodule PersistedSignal do
    @moduledoc false

    @type t :: %__MODULE__{
            id: String.t() | nil,
            source: String.t(),
            type: Jizoku.Runtime.Signal.command_type(),
            payload: Jizoku.Runtime.Signal.payload(),
            occurred_at: DateTime.t(),
            partition: String.t() | nil,
            trace: Jizoku.Runtime.Trace.t() | nil,
            metadata: map(),
            idempotency_key: String.t()
          }

    defstruct [
      :id,
      :source,
      :type,
      :payload,
      :occurred_at,
      :partition,
      :trace,
      :metadata,
      :idempotency_key
    ]
  end

  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Commands.SignalInterpreter
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Signal.JidoAdapter
  alias Jizoku.Runtime.Signal.JidoResolver

  @max_attempts 25

  @doc false
  @spec apply(module(), Jido.Signal.t(), keyword()) ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  def apply(resolver, %Jido.Signal{} = signal, opts) when is_atom(resolver) and is_list(opts) do
    with {:ok, envelope} <- JidoAdapter.domain_envelope(signal),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, intent} <- resolve_intent(storage, queue, resolver, signal, envelope) do
      intent_opts = Keyword.put(opts, :queue, intent.queue)
      SignalInterpreter.apply(intent.signal, intent_opts)
    end
  end

  defp resolve_intent(storage, queue, resolver, signal, envelope) do
    resolve_intent(storage, queue, resolver, signal, envelope, nil, @max_attempts)
  end

  defp resolve_intent(_storage, _queue, _resolver, _signal, _envelope, _candidate, 0) do
    {:error, :conflict}
  end

  defp resolve_intent(storage, queue, resolver, signal, envelope, candidate, attempts_left) do
    case load_intent(storage, envelope) do
      {:ok, intent} ->
        {:ok, intent}

      {:error, :not_found} ->
        with {:ok, candidate} <- candidate(candidate, resolver, signal, envelope, queue),
             {:ok, entry} <- intent_entry(candidate, envelope) do
          persist_candidate(
            storage,
            queue,
            resolver,
            signal,
            envelope,
            candidate,
            entry,
            attempts_left
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp persist_candidate(
         storage,
         queue,
         resolver,
         signal,
         envelope,
         candidate,
         entry,
         attempts_left
       ) do
    case Journal.append_entries(storage, [entry], expected_rev: 0) do
      {:ok, _thread} ->
        {:ok, candidate}

      {:error, :conflict} ->
        resolve_intent(
          storage,
          queue,
          resolver,
          signal,
          envelope,
          candidate,
          attempts_left - 1
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp candidate(nil, resolver, signal, envelope, queue) do
    with {:ok, resolved_signal} <- JidoResolver.resolve(resolver, signal, envelope) do
      {:ok, %{signal: resolved_signal, queue: queue}}
    end
  end

  defp candidate(candidate, _resolver, _signal, _envelope, _queue), do: {:ok, candidate}

  defp intent_entry(%{signal: %Signal{} = signal, queue: queue}, envelope) do
    DispatchProtocol.new_entry(:jido_signal_resolved, %{
      event_key: envelope.event_key,
      signal_id: envelope.id,
      source: envelope.source,
      envelope_fingerprint: envelope.envelope_fingerprint,
      resolved_signal: serialize_signal(signal),
      queue: queue,
      occurred_at: envelope.occurred_at
    })
  end

  defp load_intent(storage, envelope) do
    case Journal.load_thread(storage, {:jido_signal, envelope.event_key}) do
      {:ok, %{rev: 1, entries: [%Entry{type: :jido_signal_resolved, data: data}]}} ->
        decode_intent(data, envelope)

      {:ok, _thread} ->
        invalid_ingress()

      {:error, _reason} = error ->
        error
    end
  end

  defp decode_intent(data, envelope) do
    with true <- Map.get(data, :event_key) == envelope.event_key,
         true <- Map.get(data, :signal_id) == envelope.id,
         true <- Map.get(data, :source) == envelope.source,
         :ok <- matching_fingerprint(data, envelope),
         {:ok, queue} <- persisted_queue(data),
         {:ok, signal} <- persisted_signal(data),
         :ok <- matching_signal_envelope(signal, envelope) do
      {:ok, %{signal: signal, queue: queue}}
    else
      false -> invalid_ingress()
      {:error, _reason} = error -> error
    end
  end

  defp matching_fingerprint(data, envelope) do
    case Map.get(data, :envelope_fingerprint) do
      fingerprint when fingerprint == envelope.envelope_fingerprint -> :ok
      fingerprint when is_binary(fingerprint) -> {:error, {:conflicting_jido_signal, :envelope}}
      _invalid -> invalid_ingress()
    end
  end

  defp matching_signal_envelope(%Signal{} = signal, envelope) do
    expected = %{
      id: envelope.id,
      source: envelope.source,
      occurred_at: envelope.occurred_at,
      trace: envelope.trace,
      metadata: %{"jido" => maybe_subject(%{"type" => envelope.type}, envelope.subject)},
      idempotency_key: envelope.identity_key,
      partition: nil
    }

    actual =
      Map.take(signal, [
        :id,
        :source,
        :occurred_at,
        :trace,
        :metadata,
        :idempotency_key,
        :partition
      ])

    if actual == expected, do: :ok, else: invalid_ingress()
  end

  defp maybe_subject(metadata, nil), do: metadata
  defp maybe_subject(metadata, subject), do: Map.put(metadata, "subject", subject)

  defp persisted_queue(data) do
    case Map.fetch(data, :queue) do
      {:ok, queue} ->
        case Options.queue(queue) do
          {:ok, normalized} -> {:ok, normalized}
          {:error, _reason} -> invalid_ingress()
        end

      :error ->
        invalid_ingress()
    end
  end

  defp persisted_signal(data) do
    case Map.get(data, :resolved_signal) do
      %{
        id: id,
        source: source,
        type: type,
        payload: payload,
        occurred_at: %DateTime{} = occurred_at,
        partition: partition,
        trace: trace,
        metadata: metadata,
        idempotency_key: idempotency_key
      } ->
        rebuild_persisted_signal(%PersistedSignal{
          id: id,
          source: source,
          type: type,
          payload: payload,
          occurred_at: occurred_at,
          partition: partition,
          trace: trace,
          metadata: metadata,
          idempotency_key: idempotency_key
        })

      _invalid ->
        invalid_ingress()
    end
  end

  defp rebuild_persisted_signal(attrs) do
    if valid_persisted_signal?(attrs) do
      opts =
        [
          id: attrs.id,
          occurred_at: attrs.occurred_at,
          partition: attrs.partition,
          trace: attrs.trace,
          metadata: attrs.metadata,
          idempotency_key: attrs.idempotency_key
        ]

      case rebuild_signal(attrs.type, attrs.payload, opts) do
        {:ok, %Signal{} = signal} -> {:ok, %Signal{signal | source: attrs.source}}
        {:error, _reason} -> invalid_ingress()
      end
    else
      invalid_ingress()
    end
  end

  defp valid_persisted_signal?(attrs) do
    Enum.all?([
      optional_binary?(attrs.id),
      is_binary(attrs.source),
      is_atom(attrs.type),
      is_map(attrs.payload),
      match?(%DateTime{}, attrs.occurred_at),
      optional_binary?(attrs.partition),
      is_nil(attrs.trace) or is_map(attrs.trace),
      is_map(attrs.metadata),
      is_binary(attrs.idempotency_key)
    ])
  end

  defp optional_binary?(value), do: is_nil(value) or is_binary(value)

  defp rebuild_signal(:start_run, %{workflow: workflow, trigger: trigger, input: input}, opts) do
    Signal.start_run(workflow, trigger, input, opts)
  end

  defp rebuild_signal(:cancel_run, %{run_id: run_id}, opts) do
    Signal.cancel_run(run_id, opts)
  end

  defp rebuild_signal(:resume_run, %{run_id: run_id, attributes: attributes}, opts) do
    Signal.resume_run(run_id, attributes, opts)
  end

  defp rebuild_signal(:approve_run, %{run_id: run_id, attributes: attributes}, opts) do
    Signal.approve_run(run_id, attributes, opts)
  end

  defp rebuild_signal(:reject_run, %{run_id: run_id, attributes: attributes}, opts) do
    Signal.reject_run(run_id, attributes, opts)
  end

  defp rebuild_signal(
         :replay_run,
         %{run_id: run_id, allow_irreversible: allow_irreversible},
         opts
       ) do
    Signal.replay_run(run_id, Keyword.put(opts, :allow_irreversible, allow_irreversible))
  end

  defp rebuild_signal(_type, _payload, _opts), do: invalid_ingress()

  defp serialize_signal(%Signal{} = signal) do
    signal
    |> then(&struct!(PersistedSignal, Map.from_struct(&1)))
    |> Map.from_struct()
  end

  defp invalid_ingress do
    {:error, {:invalid_jido_signal_ingress, :malformed}}
  end
end
