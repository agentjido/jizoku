# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.ContinuationIntent do
  @moduledoc false

  alias Squidie.Runtime.DispatchProtocol.Projection
  alias Squidie.Workflow.Definition

  @enforce_keys [
    :run_id,
    :successor_run_id,
    :continuation_key,
    :workflow,
    :trigger,
    :input,
    :definition,
    :definition_version,
    :definition_fingerprint,
    :queue,
    :trace,
    :occurred_at
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          run_id: String.t(),
          successor_run_id: String.t(),
          continuation_key: String.t(),
          workflow: String.t(),
          trigger: String.t(),
          input: map(),
          definition: :current,
          definition_version: String.t() | nil,
          definition_fingerprint: String.t(),
          queue: String.t(),
          trace: map(),
          occurred_at: DateTime.t()
        }
  @type current_target :: %{workflow: module(), trigger: atom()}

  @request_fields [
    :run_id,
    :successor_run_id,
    :continuation_key,
    :workflow,
    :trigger,
    :input,
    :definition,
    :definition_version,
    :definition_fingerprint
  ]

  @doc false
  @spec from_fence(term()) :: {:ok, t()} | {:error, {:invalid_continuation, :invalid}}
  def from_fence(fence) when is_map(fence) do
    if Projection.valid_continuation_fence?(fence) do
      {:ok, struct!(__MODULE__, Map.take(fence, @enforce_keys))}
    else
      {:error, {:invalid_continuation, :invalid}}
    end
  end

  def from_fence(_fence) do
    {:error, {:invalid_continuation, :invalid}}
  end

  @doc false
  @spec request(t()) :: map()
  def request(%__MODULE__{} = intent) do
    intent
    |> Map.from_struct()
    |> Map.take(@request_fields)
  end

  @doc false
  @spec validate_current_target(t()) :: :ok | {:error, term()}
  def validate_current_target(%__MODULE__{} = intent) do
    with {:ok, _target} <- resolve_current_target(intent) do
      :ok
    end
  end

  @doc false
  @spec resolve_current_target(t()) :: {:ok, current_target()} | {:error, term()}
  def resolve_current_target(%__MODULE__{} = intent) do
    with {:ok, workflow, definition} <- Definition.load_serialized(intent.workflow),
         :ok <- validate_definition_identity(definition, intent),
         {:ok, trigger} <- target_trigger(definition, intent.trigger),
         {:ok, resolved_input} <- Definition.resolve_payload(trigger, intent.input) do
      if resolved_input == intent.input do
        {:ok, %{workflow: workflow, trigger: trigger.name}}
      else
        {:error, {:invalid_continuation_target, :unresolved_input}}
      end
    end
  end

  defp validate_definition_identity(definition, intent) do
    cond do
      definition.definition_version != intent.definition_version ->
        {:error, {:invalid_continuation_target, :definition_version}}

      Definition.fingerprint(definition) != intent.definition_fingerprint ->
        {:error, {:invalid_continuation_target, :definition_fingerprint}}

      true ->
        :ok
    end
  end

  defp target_trigger(definition, trigger_name) do
    case Definition.deserialize_trigger(definition, trigger_name) do
      trigger when is_atom(trigger) -> Definition.trigger(definition, trigger)
      _unknown -> {:error, {:invalid_continuation_target, :trigger}}
    end
  end
end
