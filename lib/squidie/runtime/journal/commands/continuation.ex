# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.Commands.Continuation do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Compensation
  alias Squidie.Runtime.Journal.ContinuationIntent
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.Journal.WorkflowDefinitionLoader
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  @run_append_retries 25
  @repair_receipt_retries 25
  @type commit_result :: %{
          required(:created?) => boolean(),
          required(:intent) => ContinuationIntent.t(),
          required(:workflow_agent) => Agent.t()
        }
  @type repair_result :: %{
          required(:predecessor) => commit_result(),
          required(:successor) => Squidie.ReadModel.Inspection.Snapshot.t(),
          required(:receipt_created?) => boolean()
        }

  @doc false
  @spec commit_predecessor(Journal.storage_config(), String.t(), String.t()) ::
          {:ok, commit_result()} | {:error, term()}
  def commit_predecessor(storage, run_id, queue)
      when is_binary(run_id) and run_id != "" and is_binary(queue) and queue != "" do
    with {:ok, queue} <- Options.queue_from_opts(queue: queue) do
      commit_predecessor(storage, run_id, queue, @run_append_retries)
    end
  end

  def commit_predecessor(_storage, _queue, _run_id) do
    {:error, {:invalid_continuation, :invalid}}
  end

  @doc false
  @spec repair_fenced_run(Journal.storage_config(), String.t(), String.t()) ::
          {:ok, repair_result()} | {:error, term()}
  def repair_fenced_run(storage, run_id, queue)
      when is_binary(run_id) and run_id != "" and is_binary(queue) and queue != "" do
    with {:ok, predecessor} <- commit_predecessor(storage, run_id, queue),
         {:ok, successor} <- ensure_successor(storage, predecessor.intent),
         {:ok, receipt} <-
           acknowledge_repair(
             storage,
             predecessor.intent,
             @repair_receipt_retries
           ) do
      {:ok,
       %{
         predecessor: predecessor,
         successor: successor,
         receipt_created?: receipt.created?
       }}
    end
  end

  def repair_fenced_run(_storage, _run_id, _queue) do
    {:error, {:invalid_continuation, :invalid}}
  end

  @doc false
  @spec abort_reason(Journal.storage_config(), Agent.t(), Agent.t()) ::
          {:ok, DispatchProtocol.Projection.continuation_abort_reason()} | :not_abortable
  def abort_reason(
        storage,
        %Agent{
          agent_module: DispatchAgent,
          state: %{queue: queue}
        } = dispatch_agent,
        %Agent{
          agent_module: WorkflowAgent,
          state: %{run_id: run_id, projection: %Projection{} = projection}
        } = workflow_agent
      ) do
    with {:ok, fence} <- continuation_fence(dispatch_agent, run_id),
         {:ok, intent} <- ContinuationIntent.from_fence(fence),
         :ok <- validate_static_boundary(storage, workflow_agent, intent, queue),
         :ok <- ContinuationIntent.validate_current_target(intent),
         :ok <- validate_abort_integrity(dispatch_agent, projection, run_id),
         {:ok, reason} <- durable_abort_reason(projection) do
      {:ok, reason}
    else
      _not_abortable -> :not_abortable
    end
  end

  def abort_reason(_storage, _dispatch_agent, _workflow_agent) do
    :not_abortable
  end

  defp commit_predecessor(_storage, _run_id, _queue, 0) do
    {:error, :conflict}
  end

  defp commit_predecessor(storage, run_id, queue, retries_left) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, fence} <- continuation_fence(dispatch_agent, run_id),
         {:ok, intent} <- ContinuationIntent.from_fence(fence),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, mode} <- predecessor_mode(workflow_agent, intent),
         :ok <- validate_static_boundary(storage, workflow_agent, intent, queue),
         :ok <- validate_mode(storage, dispatch_agent, workflow_agent, intent, queue, mode) do
      persist_predecessor(
        mode,
        storage,
        run_id,
        queue,
        dispatch_agent,
        workflow_agent,
        intent,
        retries_left
      )
    end
  end

  defp continuation_fence(dispatch_agent, run_id) do
    case DispatchAgent.active_continuation_fence(dispatch_agent, run_id) do
      nil -> {:error, {:continuation_fence_not_found, run_id}}
      fence -> {:ok, fence}
    end
  end

  defp predecessor_mode(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}},
         intent
       ) do
    expected_request = ContinuationIntent.request(intent)

    case {projection.continuation_request, Projection.terminal_status(projection)} do
      {^expected_request, :continued} ->
        {:ok, :existing}

      {nil, nil} ->
        {:ok, :new}

      {^expected_request, nil} ->
        {:error, {:incomplete_continuation_commit, intent.run_id}}

      {nil, :continued} ->
        {:error, {:incomplete_continuation_commit, intent.run_id}}

      {nil, terminal_status} ->
        {:error, {:conflicting_continuation_terminal, terminal_status}}

      {_request, _terminal_status} ->
        {:error, :conflicting_continuation}
    end
  end

  defp validate_mode(_storage, _dispatch_agent, _workflow_agent, _intent, _queue, :existing) do
    :ok
  end

  defp validate_mode(storage, dispatch_agent, workflow_agent, intent, _queue, :new) do
    with :ok <- ContinuationIntent.validate_current_target(intent),
         :ok <- validate_workflow_boundary(storage, workflow_agent) do
      validate_dispatch_boundary(dispatch_agent, intent.run_id)
    end
  end

  defp validate_static_boundary(storage, workflow_agent, intent, queue) do
    with :ok <- validate_fence_identity(workflow_agent, intent, queue),
         :ok <- single_queue_plan(workflow_agent.state.projection, queue) do
      module_authored_workflow(storage, intent.run_id)
    end
  end

  defp validate_fence_identity(
         %Agent{
           agent_module: WorkflowAgent,
           state: %{run_id: run_id, projection: %Projection{} = projection}
         },
         intent,
         queue
       ) do
    cond do
      intent.run_id != run_id ->
        unsafe(:run_id_mismatch)

      intent.successor_run_id == run_id ->
        unsafe(:successor_reuses_predecessor)

      intent.queue != queue ->
        unsafe(:queue_mismatch)

      intent.workflow != projection.workflow ->
        unsafe(:workflow_mismatch)

      intent.trigger != projection.trigger ->
        unsafe(:trigger_mismatch)

      true ->
        :ok
    end
  end

  defp validate_dispatch_boundary(
         %Agent{agent_module: DispatchAgent, state: %{projection: dispatch_projection}},
         run_id
       ) do
    case DispatchProtocol.Projection.continuation_blockers(dispatch_projection, run_id) do
      [] -> :ok
      blockers -> unsafe({:dispatch_blockers, blockers})
    end
  end

  defp validate_abort_integrity(dispatch_agent, projection, run_id) do
    with :ok <- workflow_without_anomalies(projection) do
      dispatch_agent.state.projection
      |> DispatchProtocol.Projection.continuation_blockers(run_id)
      |> Enum.filter(&(&1.reason == :dispatch_anomaly))
      |> case do
        [] -> :ok
        blockers -> unsafe({:dispatch_blockers, blockers})
      end
    end
  end

  defp validate_workflow_boundary(
         storage,
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}}
       ) do
    with :ok <- active_workflow(projection),
         :ok <- workflow_without_anomalies(projection),
         :ok <- workflow_without_manual_state(projection),
         :ok <- workflow_without_unsafe_dynamic_work(projection),
         :ok <- workflow_without_compensation(projection),
         :ok <- applied_plan(projection) do
      linked_children_started(storage, projection)
    end
  end

  defp active_workflow(%Projection{} = projection) do
    if Projection.terminal?(projection) do
      unsafe(:terminal_run)
    else
      :ok
    end
  end

  defp workflow_without_anomalies(%Projection{} = projection) do
    case Projection.anomalies(projection) do
      [] -> :ok
      anomalies -> unsafe({:workflow_anomalies, anomalies})
    end
  end

  defp workflow_without_manual_state(%Projection{} = projection) do
    if Projection.manual_state(projection) do
      unsafe(:manual_state)
    else
      :ok
    end
  end

  defp applied_plan(%Projection{} = projection) do
    planned_keys = MapSet.new(Projection.planned_runnable_keys(projection))
    applied_keys = Projection.applied_runnable_keys(projection)

    if MapSet.subset?(planned_keys, applied_keys) do
      :ok
    else
      unsafe(:unapplied_runnables)
    end
  end

  defp single_queue_plan(%Projection{} = projection, queue) do
    queues =
      projection
      |> Projection.planned_runnables()
      |> Enum.map(&Squidie.MapField.get(&1, :queue, "default"))
      |> Enum.uniq()

    if Enum.all?(queues, &(&1 == queue)) do
      :ok
    else
      unsafe(:multiple_queues)
    end
  end

  defp workflow_without_unsafe_dynamic_work(%Projection{} = projection) do
    dynamic_work? = Projection.dynamic_work(projection) != []
    graph_mutation? = map_size(projection.graph.mutation_history) > 0

    if dynamic_work? or graph_mutation? do
      unsafe(:dynamic_work)
    else
      :ok
    end
  end

  defp workflow_without_compensation(%Projection{} = projection) do
    if Enum.any?(Projection.planned_runnables(projection), &Compensation.runnable?/1) do
      unsafe(:compensation)
    else
      :ok
    end
  end

  defp durable_abort_reason(%Projection{} = projection) do
    case {projection.continuation_request, Projection.terminal_status(projection)} do
      {nil, terminal_status} when terminal_status not in [nil, :continued] ->
        {:ok, :predecessor_terminal}

      {nil, nil} ->
        if Projection.dynamic_work(projection) != [] or
             map_size(projection.graph.mutation_history) > 0 do
          {:ok, :predecessor_changed}
        else
          :not_abortable
        end

      _continuation_state ->
        :not_abortable
    end
  end

  defp linked_children_started(storage, %Projection{} = projection) do
    projection
    |> Projection.child_runs()
    |> Enum.reduce_while(:ok, fn child, :ok ->
      case Journal.load_thread(storage, {:run, Map.get(child, :child_run_id)}) do
        {:ok, _thread} -> {:cont, :ok}
        {:error, :not_found} -> {:halt, unsafe(:child_starting)}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp module_authored_workflow(storage, run_id) do
    case WorkflowDefinitionLoader.runtime_spec_run?(storage, run_id) do
      {:ok, false} -> :ok
      {:ok, true} -> unsafe(:runtime_spec)
      {:error, _reason} = error -> error
    end
  end

  defp persist_predecessor(
         :existing,
         _storage,
         _run_id,
         _queue,
         _dispatch_agent,
         workflow_agent,
         intent,
         _retries_left
       ) do
    {:ok, commit_result(workflow_agent, intent, false)}
  end

  defp persist_predecessor(
         :new,
         storage,
         run_id,
         queue,
         _dispatch_agent,
         workflow_agent,
         intent,
         retries_left
       ) do
    with {:ok, entries} <- continuation_entries(intent) do
      append_predecessor_entries(
        storage,
        run_id,
        queue,
        workflow_agent,
        intent,
        entries,
        retries_left
      )
    end
  end

  defp append_predecessor_entries(
         storage,
         run_id,
         queue,
         workflow_agent,
         intent,
         entries,
         retries_left
       ) do
    case Journal.append_entries(storage, entries,
           expected_rev: workflow_agent.state.thread_rev,
           telemetry_projection: workflow_agent.state.projection
         ) do
      {:ok, _thread} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id) do
          _checkpoint_result =
            WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: intent.occurred_at)

          {:ok, commit_result(workflow_agent, intent, true)}
        end

      {:error, :conflict} ->
        commit_predecessor(storage, run_id, queue, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp continuation_entries(intent) do
    request_attrs =
      intent
      |> ContinuationIntent.request()
      |> Map.put(:occurred_at, intent.occurred_at)

    terminal_attrs = %{
      run_id: intent.run_id,
      status: :continued,
      trace: intent.trace,
      occurred_at: intent.occurred_at
    }

    with {:ok, request_entry} <-
           DispatchProtocol.new_entry(:run_continuation_requested, request_attrs),
         {:ok, terminal_entry} <- DispatchProtocol.new_entry(:run_terminal, terminal_attrs) do
      {:ok, [request_entry, terminal_entry]}
    end
  end

  defp ensure_successor(storage, %ContinuationIntent{} = intent) do
    opts = continuation_start_options(storage, intent)

    case Starter.repair_existing_continuation_from_intent(
           intent.workflow,
           intent.trigger,
           intent.input,
           opts
         ) do
      {:error, :not_found} ->
        start_missing_successor(storage, intent, opts)

      result ->
        result
    end
  end

  defp start_missing_successor(storage, %ContinuationIntent{} = intent, opts) do
    with {:ok, target} <- ContinuationIntent.resolve_current_target(intent),
         {:ok, signal} <- continuation_start_signal(storage, intent) do
      Starter.start_continuation_from_intent(
        target.workflow,
        target.trigger,
        intent.input,
        Keyword.put(opts, :command_signal, signal)
      )
    end
  end

  defp continuation_start_options(storage, %ContinuationIntent{} = intent) do
    [
      journal_storage: storage,
      partition: Storage.partition(storage),
      queue: intent.queue,
      run_id: intent.successor_run_id,
      now: intent.occurred_at,
      continuation_origin: %{
        predecessor_run_id: intent.run_id,
        continuation_key: intent.continuation_key
      },
      continuation_definition_identity: %{
        definition_version: intent.definition_version,
        definition_fingerprint: intent.definition_fingerprint
      }
    ]
  end

  defp continuation_start_signal(storage, %ContinuationIntent{} = intent) do
    signal_id = "continuation:#{intent.successor_run_id}"

    Signal.start_run(intent.workflow, intent.trigger, intent.input,
      id: signal_id,
      trace: intent.trace,
      partition: Storage.partition(storage),
      occurred_at: intent.occurred_at,
      idempotency_key: signal_id
    )
  end

  defp acknowledge_repair(_storage, _intent, 0) do
    {:error, :conflict}
  end

  defp acknowledge_repair(storage, %ContinuationIntent{} = intent, retries_left) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, intent.queue) do
      case DispatchAgent.acknowledge_continuation_repair(
             storage,
             dispatch_agent,
             intent.run_id,
             now: intent.occurred_at
           ) do
        {:error, :conflict} ->
          acknowledge_repair(storage, intent, retries_left - 1)

        result ->
          result
      end
    end
  end

  defp commit_result(workflow_agent, intent, created?) do
    %{workflow_agent: workflow_agent, intent: intent, created?: created?}
  end

  defp unsafe(reason) do
    {:error, {:unsafe_continuation, reason}}
  end
end
