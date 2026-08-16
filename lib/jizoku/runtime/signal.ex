defmodule Jizoku.Runtime.Signal do
  @moduledoc """
  Jizoku-native runtime command signal envelope.

  These signals describe product-level runtime commands before any adapter turns
  them into a backend primitive such as `Jido.Signal`. Workflow authors and host
  apps should not need to construct raw backend signals.

  | type | payload |
  | --- | --- |
  | `:start_run` | `%{workflow: String.t(), trigger: String.t() | nil, input: map()}` |
  | `:start_cron` | `%{workflow: String.t(), trigger: String.t(), input: map()}` |
  | `:approve_run` | `%{run_id: Ecto.UUID.t(), attributes: map()}` |
  | `:reject_run` | `%{run_id: Ecto.UUID.t(), attributes: map()}` |
  | `:resume_run` | `%{run_id: Ecto.UUID.t(), attributes: map()}` |
  | `:cancel_run` | `%{run_id: Ecto.UUID.t()}` |
  | `:replay_run` | `%{run_id: Ecto.UUID.t(), allow_irreversible: boolean()}` |
  | `:signal_run` | `%{run_id: Ecto.UUID.t(), event: String.t(), correlation: String.t(), event_payload: map()}` |

  Every signal carries caller metadata, an occurrence timestamp, and an optional
  idempotency key. Signals adapted from an external envelope may also carry its
  source as audit provenance. Cron signals derive the key from scheduler
  identity when the caller does not provide one.
  """

  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Partition
  alias Jizoku.Runtime.ScheduleIdentity
  alias Jizoku.Runtime.Trace
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.EventWait

  @common_options [:id, :trace, :metadata, :occurred_at, :idempotency_key, :partition]
  @replay_options [:allow_irreversible | @common_options]
  @event_options [:correlation | @common_options]
  @max_id_bytes 255
  @max_event_payload_bytes 1_048_576

  @type command_type ::
          :start_run
          | :start_cron
          | :approve_run
          | :reject_run
          | :resume_run
          | :cancel_run
          | :replay_run
          | :signal_run

  @type payload :: %{
          optional(:workflow) => String.t(),
          optional(:trigger) => String.t() | nil,
          optional(:input) => map(),
          optional(:run_id) => Ecto.UUID.t(),
          optional(:attributes) => map(),
          optional(:allow_irreversible) => boolean(),
          optional(:event) => String.t(),
          optional(:correlation) => String.t(),
          optional(:event_payload) => map()
        }

  @type t :: %__MODULE__{
          id: String.t() | nil,
          source: String.t() | nil,
          type: command_type(),
          payload: payload(),
          trace: Trace.t() | nil,
          partition: String.t() | nil,
          metadata: map(),
          occurred_at: DateTime.t(),
          idempotency_key: String.t() | nil
        }

  @type error :: {:invalid_signal, term()}

  @enforce_keys [:type, :payload, :metadata, :occurred_at]
  defstruct [
    :id,
    :source,
    :type,
    :payload,
    :occurred_at,
    :partition,
    :trace,
    metadata: %{},
    idempotency_key: nil
  ]

  @doc """
  Builds a command signal for starting a workflow run.
  """
  @spec start_run(module() | String.t(), atom() | String.t() | nil, map(), keyword()) ::
          {:ok, t()} | {:error, error()}
  def start_run(workflow, trigger, input, opts \\ []) do
    with {:ok, input} <- map_value(input, :payload),
         {:ok, workflow} <- workflow_name(workflow),
         {:ok, trigger} <- trigger_name(trigger),
         {:ok, envelope} <- envelope(opts) do
      new(:start_run, start_payload(workflow, trigger, input), envelope)
    end
  end

  @doc """
  Builds a command signal for starting a workflow run from a cron activation.
  """
  @spec start_cron(module() | String.t(), atom() | String.t(), map(), keyword()) ::
          {:ok, t()} | {:error, error()}
  def start_cron(workflow, trigger, input, opts \\ []) do
    with {:ok, input} <- map_value(input, :payload),
         {:ok, workflow} <- workflow_name(workflow),
         {:ok, trigger} <- required_trigger_name(trigger),
         {:ok, envelope} <- envelope(opts),
         {:ok, idempotency_key} <-
           cron_idempotency_key(envelope.idempotency_key, workflow, trigger, input) do
      new(:start_cron, start_payload(workflow, trigger, input), %{
        envelope
        | idempotency_key: idempotency_key
      })
    end
  end

  @doc false
  @spec start_payload(String.t(), String.t() | nil, map()) :: map()
  def start_payload(workflow, trigger, input) do
    Map.new(workflow: workflow, trigger: trigger, input: input)
  end

  @doc """
  Builds a command signal for approving a blocked run.
  """
  @spec approve_run(Ecto.UUID.t(), map(), keyword()) :: {:ok, t()} | {:error, error()}
  def approve_run(run_id, attributes, opts \\ []) do
    run_attributes_signal(:approve_run, run_id, attributes, opts)
  end

  @doc """
  Builds a command signal for rejecting a blocked run.
  """
  @spec reject_run(Ecto.UUID.t(), map(), keyword()) :: {:ok, t()} | {:error, error()}
  def reject_run(run_id, attributes, opts \\ []) do
    run_attributes_signal(:reject_run, run_id, attributes, opts)
  end

  @doc """
  Builds a command signal for resuming a blocked run.
  """
  @spec resume_run(Ecto.UUID.t(), map(), keyword()) :: {:ok, t()} | {:error, error()}
  def resume_run(run_id, attributes, opts \\ []) do
    run_attributes_signal(:resume_run, run_id, attributes, opts)
  end

  @doc """
  Builds a command signal for canceling a run.
  """
  @spec cancel_run(Ecto.UUID.t(), keyword()) :: {:ok, t()} | {:error, error()}
  def cancel_run(run_id, opts \\ []) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, envelope} <- envelope(opts) do
      new(:cancel_run, %{run_id: run_id}, envelope)
    end
  end

  @doc """
  Builds a command signal for replaying a run.
  """
  @spec replay_run(Ecto.UUID.t(), keyword()) :: {:ok, t()} | {:error, error()}
  def replay_run(run_id, opts \\ []) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, envelope} <- envelope(opts, @replay_options),
         {:ok, allow_irreversible} <- allow_irreversible(opts) do
      new(:replay_run, %{run_id: run_id, allow_irreversible: allow_irreversible}, envelope)
    end
  end

  @doc """
  Builds an idempotent command signal for delivering one named external event.
  """
  @spec signal_run(Ecto.UUID.t(), String.t(), map(), keyword()) ::
          {:ok, t()} | {:error, error()}
  def signal_run(run_id, event, event_payload, opts \\ []) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, event} <- event_name(event),
         {:ok, event_payload} <- event_payload(event_payload),
         {:ok, envelope} <- envelope(opts, @event_options),
         {:ok, correlation} <- correlation(Keyword.get(opts, :correlation)),
         :ok <- require_idempotency_key(envelope.idempotency_key) do
      new(
        :signal_run,
        %{
          run_id: run_id,
          event: event,
          correlation: correlation,
          event_payload: event_payload
        },
        envelope
      )
    end
  end

  defp run_attributes_signal(type, run_id, attributes, opts) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, attributes} <- map_value(attributes, :attributes),
         {:ok, envelope} <- envelope(opts) do
      new(type, %{run_id: run_id, attributes: attributes}, envelope)
    end
  end

  defp new(type, payload, envelope) do
    {:ok,
     struct!(__MODULE__,
       id: envelope.id,
       type: type,
       payload: payload,
       partition: envelope.partition,
       trace: envelope.trace,
       metadata: envelope.metadata,
       occurred_at: envelope.occurred_at,
       idempotency_key: envelope.idempotency_key
     )}
  end

  defp workflow_name(nil), do: invalid(:workflow, :invalid)

  defp workflow_name(workflow) when is_atom(workflow) and not is_boolean(workflow) do
    workflow
    |> Definition.serialize_workflow()
    |> non_empty_string(:workflow)
  end

  defp workflow_name(workflow) when is_binary(workflow), do: non_empty_string(workflow, :workflow)
  defp workflow_name(_workflow), do: invalid(:workflow, :invalid)

  defp trigger_name(nil), do: {:ok, nil}

  defp trigger_name(trigger)
       when (is_atom(trigger) and not is_boolean(trigger)) or is_binary(trigger) do
    trigger
    |> Definition.serialize_trigger()
    |> non_empty_string(:trigger)
  end

  defp trigger_name(_trigger), do: invalid(:trigger, :invalid)

  defp required_trigger_name(trigger) do
    case trigger_name(trigger) do
      {:ok, nil} -> invalid(:trigger, :required)
      result -> result
    end
  end

  defp event_name(value) do
    validate_event_identity(value, :event, &EventWait.valid_event?/1)
  end

  defp correlation(value) do
    validate_event_identity(value, :correlation, &EventWait.valid_correlation_value?/1)
  end

  defp event_payload(value) when is_map(value) do
    cond do
      not Options.storage_safe_value?(value) ->
        invalid(:event_payload, :unsupported_term)

      byte_size(:erlang.term_to_binary(value)) > @max_event_payload_bytes ->
        invalid(:event_payload, :too_large)

      true ->
        {:ok, value}
    end
  end

  defp event_payload(_value) do
    invalid(:event_payload, :expected_map)
  end

  defp validate_event_identity(value, field, validator) when is_function(validator, 1) do
    if validator.(value) do
      {:ok, value}
    else
      invalid(field, :invalid)
    end
  end

  defp require_idempotency_key(value) when is_binary(value) do
    :ok
  end

  defp require_idempotency_key(_value) do
    invalid(:idempotency_key, :required)
  end

  defp run_id(run_id) when is_binary(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, normalized_run_id} -> {:ok, normalized_run_id}
      :error -> invalid(:run_id, :invalid)
    end
  end

  defp run_id(_run_id), do: invalid(:run_id, :invalid)

  defp map_value(value, _field) when is_map(value), do: {:ok, value}
  defp map_value(_value, field), do: invalid(field, :expected_map)

  defp envelope(opts, allowed_options \\ @common_options) do
    with {:ok, opts} <- keyword_options(opts),
         :ok <- supported_options(opts, allowed_options),
         {:ok, metadata} <- metadata(Keyword.get(opts, :metadata, %{})),
         {:ok, occurred_at} <- occurred_at(Keyword.get(opts, :occurred_at)),
         {:ok, idempotency_key} <- idempotency_key(Keyword.get(opts, :idempotency_key)),
         {:ok, partition} <- signal_partition(Keyword.get(opts, :partition)),
         {:ok, id} <- signal_id(Keyword.get(opts, :id)),
         {:ok, trace} <- signal_trace(Keyword.get(opts, :trace)) do
      {:ok,
       %{
         id: id,
         trace: trace,
         metadata: metadata,
         occurred_at: occurred_at,
         idempotency_key: idempotency_key,
         partition: partition
       }}
    end
  end

  defp keyword_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      {:ok, opts}
    else
      invalid(:options, :expected_keyword)
    end
  end

  defp keyword_options(_opts), do: invalid(:options, :expected_keyword)

  defp supported_options(opts, allowed_options) do
    case Enum.find(Keyword.keys(opts), &(&1 not in allowed_options)) do
      nil -> :ok
      option -> invalid(option, :unsupported)
    end
  end

  defp metadata(metadata) when is_map(metadata), do: {:ok, metadata}
  defp metadata(_metadata), do: invalid(:metadata, :expected_map)

  defp occurred_at(nil), do: {:ok, DateTime.utc_now()}
  defp occurred_at(%DateTime{} = occurred_at), do: {:ok, occurred_at}
  defp occurred_at(_occurred_at), do: invalid(:occurred_at, :expected_datetime)

  defp idempotency_key(nil), do: {:ok, nil}

  defp idempotency_key(value) when is_binary(value) and value != "", do: {:ok, value}

  defp idempotency_key(_value), do: invalid(:idempotency_key, :expected_non_empty_string)

  defp signal_id(nil), do: {:ok, Ecto.UUID.generate()}

  defp signal_id(value) when is_binary(value) and value != "" do
    cond do
      byte_size(value) > @max_id_bytes -> invalid(:id, :too_long)
      not String.valid?(value) -> invalid(:id, :invalid)
      true -> {:ok, value}
    end
  end

  defp signal_id(_value), do: invalid(:id, :expected_non_empty_string)

  defp signal_trace(nil), do: {:ok, nil}

  defp signal_trace(trace) do
    case Trace.normalize(trace) do
      {:ok, trace} -> {:ok, trace}
      {:error, {:invalid_trace, reason}} -> invalid(:trace, reason)
    end
  end

  defp signal_partition(partition) do
    case Partition.normalize(partition) do
      {:ok, partition} -> {:ok, partition}
      {:error, _reason} -> invalid(:partition, :invalid)
    end
  end

  defp cron_idempotency_key(key, _workflow, _trigger, _input) when is_binary(key), do: {:ok, key}

  defp cron_idempotency_key(nil, workflow, trigger, input) do
    case ScheduleIdentity.signal_id(workflow, trigger, input) do
      {:ok, signal_id} -> {:ok, signal_id}
      {:error, {:invalid_schedule_identity, :missing_signal_id}} -> {:ok, nil}
      {:error, reason} -> {:error, {:invalid_signal, {:schedule_identity, reason}}}
    end
  end

  defp allow_irreversible(opts) do
    case Keyword.get(opts, :allow_irreversible, false) do
      value when is_boolean(value) -> {:ok, value}
      _value -> invalid(:allow_irreversible, :expected_boolean)
    end
  end

  defp non_empty_string(value, _field) when is_binary(value) and value != "", do: {:ok, value}
  defp non_empty_string(_value, field), do: invalid(field, :invalid)

  defp invalid(field, reason), do: {:error, {:invalid_signal, {field, reason}}}
end
