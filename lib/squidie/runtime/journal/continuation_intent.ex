# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.ContinuationIntent do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.Runtime.ContinuationIdentity
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.Trace
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
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
  @spec prepare_current(
          Squidie.Runtime.Journal.storage_config(),
          Agent.t(),
          map(),
          String.t(),
          String.t(),
          DateTime.t(),
          keyword()
        ) :: {:ok, t()} | {:error, term()}
  def prepare_current(
        storage,
        %Agent{
          agent_module: WorkflowAgent,
          state: %{run_id: run_id, projection: %Projection{} = projection}
        },
        request_input,
        continuation_key,
        queue,
        %DateTime{} = now,
        opts \\ []
      )
      when is_map(request_input) and is_binary(continuation_key) and is_binary(queue) and
             is_list(opts) do
    with {:ok, definition, resolved_input} <- current_target(projection, request_input),
         {:ok, successor_run_id} <-
           ContinuationIdentity.successor_run_id(%{
             partition: Storage.partition(storage),
             predecessor_run_id: run_id,
             continuation_key: continuation_key,
             workflow: projection.workflow,
             trigger: projection.trigger,
             definition_version: definition.definition_version,
             definition_fingerprint: Definition.fingerprint(definition)
           }),
         {:ok, trace} <-
           Trace.child_of(
             Keyword.get(opts, :parent_trace, Projection.trace(projection)),
             "continuation:#{successor_run_id}"
           ) do
      {:ok,
       %__MODULE__{
         run_id: run_id,
         successor_run_id: successor_run_id,
         continuation_key: continuation_key,
         workflow: projection.workflow,
         trigger: projection.trigger,
         input: resolved_input,
         definition: :current,
         definition_version: definition.definition_version,
         definition_fingerprint: Definition.fingerprint(definition),
         queue: queue,
         trace: trace,
         occurred_at: now
       }}
    end
  end

  @doc false
  @spec fence_attrs(t(), map(), map()) :: map()
  def fence_attrs(%__MODULE__{} = intent, request_input, extra \\ %{})
      when is_map(request_input) and is_map(extra) do
    intent
    |> Map.from_struct()
    |> Map.drop([:queue, :occurred_at])
    |> Map.put(:request_input, request_input)
    |> Map.merge(Map.take(extra, [:source_runnable_key]))
  end

  @doc false
  @spec resolve_current_input(Projection.t(), map()) ::
          {:ok, Definition.t(), map()} | {:error, term()}
  def resolve_current_input(%Projection{} = projection, input) when is_map(input) do
    current_target(projection, input)
  end

  @doc false
  @spec from_fence(term()) :: {:ok, t()} | {:error, {:invalid_continuation, :invalid}}
  def from_fence(fence) when is_map(fence) do
    if DispatchProtocol.Projection.valid_continuation_fence?(fence) do
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

  defp current_target(%Projection{} = projection, input) do
    with {:ok, _workflow, definition} <- Definition.load_serialized(projection.workflow),
         {:ok, trigger_name} <- target_trigger_name(definition, projection.trigger),
         {:ok, trigger} <- Definition.trigger(definition, trigger_name),
         {:ok, resolved_input} <- Definition.resolve_payload(trigger, input) do
      {:ok, definition, resolved_input}
    end
  end

  defp target_trigger_name(definition, serialized_trigger) do
    case Definition.deserialize_trigger(definition, serialized_trigger) do
      trigger_name when is_atom(trigger_name) -> {:ok, trigger_name}
      _unknown -> {:error, {:invalid_continuation_target, :trigger}}
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
