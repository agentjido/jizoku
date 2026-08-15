defmodule Jizoku.Runtime.Journal.GraphMutation.Materializer.Result do
  @moduledoc false

  alias Jizoku.Runtime.Journal.GraphMutation.Topology

  @type t :: %__MODULE__{
          topology: Topology.Result.t(),
          mutation_attrs: map(),
          runnables: [map()],
          runnable_intent_fingerprints: %{optional(String.t()) => String.t()}
        }

  @enforce_keys [
    :topology,
    :mutation_attrs,
    :runnables,
    :runnable_intent_fingerprints
  ]
  defstruct @enforce_keys
end

defmodule Jizoku.Runtime.Journal.GraphMutation.Materializer do
  @moduledoc false

  alias Jizoku.GraphMutation
  alias Jizoku.Runtime.Journal.GraphMutation.Materializer.Result
  alias Jizoku.Runtime.Journal.GraphMutation.Topology
  alias Jizoku.Runtime.Journal.GraphMutation.ValidationContext
  alias Jizoku.Step
  alias Jizoku.Workflow.ActionRegistry

  @intent_version 1

  @type evaluation_result ::
          {:ok, Result.t() | :duplicate}
          | {:error, {:invalid_graph_mutation, {atom(), term()}}}

  @doc false
  @spec evaluate(
          String.t(),
          GraphMutation.t(),
          ValidationContext.t(),
          ActionRegistry.registry(),
          String.t(),
          DateTime.t()
        ) :: evaluation_result()
  def evaluate(
        run_id,
        %GraphMutation{} = mutation,
        %ValidationContext{} = context,
        registry,
        default_queue,
        %DateTime{} = now
      )
      when is_binary(run_id) and run_id != "" and is_binary(default_queue) and
             default_queue != "" do
    case Topology.evaluate(mutation, context) do
      {:ok, :duplicate} ->
        {:ok, :duplicate}

      {:ok, %Topology.Result{} = topology} ->
        materialize(run_id, mutation, topology, registry, default_queue, now)

      {:error, _reason} = error ->
        error
    end
  end

  def evaluate(
        _run_id,
        %GraphMutation{},
        %ValidationContext{},
        _registry,
        _default_queue,
        %DateTime{}
      ) do
    invalid(:materialization, :invalid)
  end

  defp materialize(run_id, mutation, topology, registry, default_queue, now) do
    mutation.additions
    |> Enum.filter(&(&1.kind == :node))
    |> Enum.reduce_while({:ok, []}, fn operation, {:ok, runnables} ->
      case materialize_node(run_id, mutation, operation, registry, default_queue, now) do
        {:ok, runnable} -> {:cont, {:ok, [runnable | runnables]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> materialization_result(mutation, topology, run_id)
  end

  defp materialize_node(run_id, mutation, operation, registry, default_queue, now) do
    with {:ok, module} <- resolve_action(operation, registry),
         {:ok, action_opts} <- resolve_action_opts(operation, registry),
         :ok <- validate_action_input(operation, module, action_opts) do
      queue = operation.queue || default_queue
      runnable = runnable(run_id, mutation, operation, module, action_opts, queue, now)
      fingerprint = intent_fingerprint(runnable)

      {:ok,
       Map.put(runnable, :graph_mutation, %{
         mutation_id: mutation.mutation_id,
         node_id: operation.id,
         intent_fingerprint: fingerprint
       })}
    end
  end

  defp resolve_action(operation, registry) do
    case ActionRegistry.resolve_action(operation.action, registry) do
      {:ok, module} ->
        {:ok, module}

      {:error, reason} ->
        node_error(operation.id, {:action, reason})
    end
  end

  defp resolve_action_opts(operation, registry) do
    case ActionRegistry.resolve_action_opts(operation.action, registry) do
      {:ok, action_opts} ->
        {:ok, action_opts}

      {:error, reason} ->
        node_error(operation.id, {:action, reason})
    end
  end

  defp validate_action_input(operation, module, action_opts) do
    result =
      cond do
        function_exported?(module, :validate_action_input, 2) ->
          module.validate_action_input(operation.input, action_opts)

        Step.native_step?(module) ->
          Step.validate_input(module, operation.input)

        function_exported?(module, :validate_params, 1) ->
          module.validate_params(operation.input)

        true ->
          :ok
      end

    case result do
      :ok ->
        :ok

      {:ok, _validated} ->
        :ok

      _invalid ->
        node_error(operation.id, {:input, :invalid})
    end
  end

  defp runnable(run_id, mutation, operation, module, action_opts, queue, now) do
    runnable_key = Enum.join([run_id, operation.id, 1], ":")

    %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: 1,
      queue: queue,
      step: operation.id,
      input: operation.input,
      visible_at: now,
      recovery: recovery(operation.action),
      dynamic?: true,
      dynamic_work: %{
        mutation_id: mutation.mutation_id,
        action: operation.action,
        module: module,
        action_opts: persisted_action_opts(module, action_opts)
      }
    }
  end

  defp persisted_action_opts(module, action_opts) do
    if function_exported?(module, :persisted_action_opts, 1) do
      module.persisted_action_opts(action_opts)
    else
      action_opts
    end
  end

  defp recovery(action) do
    %{
      irreversible?: true,
      compensatable?: false,
      replay: :manual_review_required,
      recovery: :manual_intervention,
      dynamic?: true,
      action: action
    }
  end

  defp intent_fingerprint(runnable) do
    dynamic_work = Map.fetch!(runnable, :dynamic_work)

    content = {
      :jizoku_graph_mutation_intent,
      @intent_version,
      Map.fetch!(runnable, :run_id),
      Map.fetch!(runnable, :runnable_key),
      Map.fetch!(runnable, :idempotency_key),
      Map.fetch!(runnable, :attempt_number),
      Map.fetch!(runnable, :queue),
      Map.fetch!(runnable, :step),
      Map.fetch!(runnable, :input),
      Map.fetch!(runnable, :recovery),
      Map.fetch!(dynamic_work, :mutation_id),
      Map.fetch!(dynamic_work, :action),
      Map.fetch!(dynamic_work, :module),
      Map.fetch!(dynamic_work, :action_opts)
    }

    content
    |> :erlang.term_to_binary()
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp materialization_result({:error, _reason} = error, _mutation, _topology, _run_id) do
    error
  end

  defp materialization_result({:ok, reversed}, mutation, topology, run_id) do
    runnables = Enum.reverse(reversed)

    fingerprints =
      Map.new(runnables, fn runnable ->
        metadata = Map.fetch!(runnable, :graph_mutation)
        {Map.fetch!(metadata, :node_id), Map.fetch!(metadata, :intent_fingerprint)}
      end)

    mutation_attrs =
      mutation
      |> GraphMutation.to_map()
      |> Map.merge(%{
        run_id: run_id,
        result_version: mutation.expected_version + 1,
        runnable_intent_fingerprints: fingerprints
      })

    {:ok,
     %Result{
       topology: topology,
       mutation_attrs: mutation_attrs,
       runnables: runnables,
       runnable_intent_fingerprints: fingerprints
     }}
  end

  defp node_error(node_id, reason) do
    invalid(:additions, {:node, node_id, reason})
  end

  defp invalid(field, reason) do
    {:error, {:invalid_graph_mutation, {field, reason}}}
  end
end
