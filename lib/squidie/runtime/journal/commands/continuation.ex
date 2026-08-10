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
  alias Squidie.Runtime.Journal.EntryBuilder
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

  @doc false
  @spec validate_new_intent(
          Journal.storage_config(),
          Agent.t(),
          Agent.t(),
          ContinuationIntent.t()
        ) :: :ok | {:error, term()}
  def validate_new_intent(
        storage,
        %Agent{
          agent_module: DispatchAgent,
          state: %{partition: partition, queue: queue}
        } = dispatch_agent,
        %Agent{
          agent_module: WorkflowAgent,
          state: %{partition: partition}
        } = workflow_agent,
        %ContinuationIntent{queue: queue} = intent
      ) do
    with true <- Storage.partition(storage) == partition,
         {:ok, :new} <- predecessor_mode(workflow_agent, intent),
         :ok <- validate_static_boundary(storage, workflow_agent, intent, queue) do
      validate_mode(storage, dispatch_agent, workflow_agent, intent, queue, :new, nil)
    else
      false -> {:error, {:unsafe_continuation, :partition_mismatch}}
      {:ok, :existing} -> {:error, :conflicting_continuation}
      {:error, _reason} = error -> error
    end
  end

  def validate_new_intent(_storage, _dispatch_agent, _workflow_agent, _intent) do
    {:error, {:invalid_continuation, :invalid}}
  end

  @doc false
  @spec validate_native_intent(
          Journal.storage_config(),
          Agent.t(),
          Agent.t(),
          ContinuationIntent.t(),
          String.t()
        ) :: :ok | {:error, term()}
  def validate_native_intent(
        storage,
        %Agent{
          agent_module: DispatchAgent,
          state: %{partition: partition, queue: queue}
        } = dispatch_agent,
        %Agent{
          agent_module: WorkflowAgent,
          state: %{partition: partition}
        } = workflow_agent,
        %ContinuationIntent{queue: queue} = intent,
        source_runnable_key
      )
      when is_binary(source_runnable_key) and source_runnable_key != "" do
    with true <- Storage.partition(storage) == partition,
         {:ok, :new} <- predecessor_mode(workflow_agent, intent),
         :ok <- validate_native_emission_static_boundary(storage, workflow_agent, intent, queue),
         :ok <- ContinuationIntent.validate_current_target(intent),
         :ok <-
           validate_native_emission_workflow_boundary(
             storage,
             workflow_agent,
             source_runnable_key
           ) do
      validate_native_dispatch_boundary(
        dispatch_agent,
        intent.run_id,
        source_runnable_key,
        :claimed
      )
    else
      false -> {:error, {:unsafe_continuation, :partition_mismatch}}
      {:ok, :existing} -> {:error, :conflicting_continuation}
      {:error, _reason} = error -> error
    end
  end

  def validate_native_intent(
        _storage,
        _dispatch_agent,
        _workflow_agent,
        _intent,
        _source_runnable_key
      ) do
    {:error, {:invalid_continuation, :invalid}}
  end

  @doc false
  @spec consume_native_source(Journal.storage_config(), Agent.t(), Agent.t()) ::
          {:ok, Agent.t()} | {:error, term()}
  def consume_native_source(
        storage,
        %Agent{agent_module: DispatchAgent} = dispatch_agent,
        %Agent{
          agent_module: WorkflowAgent,
          state: %{run_id: run_id, projection: %Projection{} = projection}
        } = workflow_agent
      ) do
    with {:ok, fence} <- continuation_fence(dispatch_agent, run_id),
         source when is_map(source) <- native_source(fence) do
      if MapSet.member?(Projection.applied_runnable_keys(projection), source.runnable_key) do
        {:ok, workflow_agent}
      else
        append_native_source(storage, dispatch_agent, workflow_agent, fence, source)
      end
    else
      nil -> {:ok, workflow_agent}
      {:error, _reason} = error -> error
    end
  end

  def consume_native_source(_storage, _dispatch_agent, _workflow_agent) do
    {:error, {:invalid_continuation, :invalid}}
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
         native_source = native_source(fence),
         :ok <-
           validate_mode(
             storage,
             dispatch_agent,
             workflow_agent,
             intent,
             queue,
             mode,
             native_source
           ) do
      persist_predecessor(mode, %{
        storage: storage,
        run_id: run_id,
        queue: queue,
        dispatch_agent: dispatch_agent,
        workflow_agent: workflow_agent,
        intent: intent,
        native_source: native_source,
        retries_left: retries_left
      })
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

  defp validate_mode(
         _storage,
         _dispatch_agent,
         _workflow_agent,
         _intent,
         _queue,
         :existing,
         _native_source
       ) do
    :ok
  end

  defp validate_mode(
         storage,
         dispatch_agent,
         workflow_agent,
         %ContinuationIntent{} = intent,
         _queue,
         :new,
         %{runnable_key: source_runnable_key}
       )
       when is_binary(source_runnable_key) and source_runnable_key != "" do
    with :ok <- ContinuationIntent.validate_current_target(intent),
         :ok <- validate_native_workflow_boundary(storage, workflow_agent, source_runnable_key) do
      validate_native_dispatch_boundary(
        dispatch_agent,
        intent.run_id,
        source_runnable_key,
        :completed
      )
    end
  end

  defp validate_mode(storage, dispatch_agent, workflow_agent, intent, _queue, :new, nil) do
    with :ok <- ContinuationIntent.validate_current_target(intent),
         :ok <- validate_workflow_boundary(storage, workflow_agent) do
      validate_dispatch_boundary(dispatch_agent, intent.run_id)
    end
  end

  defp validate_native_workflow_boundary(storage, workflow_agent, source_runnable_key) do
    projection = workflow_agent.state.projection

    with :ok <- active_workflow(projection),
         :ok <- workflow_without_anomalies(projection),
         :ok <- workflow_without_manual_state(projection),
         :ok <- workflow_without_unsafe_dynamic_work(projection),
         :ok <- workflow_without_compensation(projection),
         :ok <- native_applied_plan(projection, source_runnable_key) do
      linked_children_started(storage, projection)
    end
  end

  defp validate_native_emission_workflow_boundary(
         storage,
         workflow_agent,
         source_runnable_key
       ) do
    storage
    |> validate_native_workflow_boundary(workflow_agent, source_runnable_key)
    |> normalize_native_emission_storage_result()
  end

  defp native_applied_plan(%Projection{} = projection, source_runnable_key) do
    planned_keys = MapSet.new(Projection.planned_runnable_keys(projection))
    applied_keys = Projection.applied_runnable_keys(projection)

    if MapSet.difference(planned_keys, applied_keys) == MapSet.new([source_runnable_key]) do
      :ok
    else
      unsafe(:unapplied_runnables)
    end
  end

  defp validate_native_dispatch_boundary(
         %Agent{agent_module: DispatchAgent, state: %{projection: dispatch_projection}},
         run_id,
         source_runnable_key,
         source_status
       ) do
    expected_reason = if source_status == :claimed, do: :claimed_attempt, else: :pending_result
    expected = [%{reason: expected_reason, runnable_key: source_runnable_key}]

    case DispatchProtocol.Projection.continuation_blockers(dispatch_projection, run_id) do
      ^expected -> :ok
      blockers -> unsafe({:dispatch_blockers, blockers})
    end
  end

  defp validate_static_boundary(storage, workflow_agent, intent, queue) do
    with :ok <- validate_fence_identity(workflow_agent, intent, queue),
         :ok <- single_queue_plan(workflow_agent.state.projection, queue) do
      module_authored_workflow(storage, intent.run_id)
    end
  end

  defp validate_native_emission_static_boundary(storage, workflow_agent, intent, queue) do
    storage
    |> validate_static_boundary(workflow_agent, intent, queue)
    |> normalize_native_emission_storage_result()
  end

  defp normalize_native_emission_storage_result(:ok) do
    :ok
  end

  defp normalize_native_emission_storage_result({:error, {:unsafe_continuation, _reason}} = error) do
    error
  end

  defp normalize_native_emission_storage_result({:error, reason}) do
    {:error, {:native_continuation_storage_failed, reason}}
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
         %{workflow_agent: workflow_agent, intent: intent}
       ) do
    {:ok, commit_result(workflow_agent, intent, false)}
  end

  defp persist_predecessor(
         :new,
         %{
           storage: storage,
           run_id: run_id,
           queue: queue,
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           intent: intent,
           native_source: native_source,
           retries_left: retries_left
         }
       ) do
    with {:ok, entries} <- continuation_entries(intent, dispatch_agent, native_source) do
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

  defp continuation_entries(%ContinuationIntent{} = intent, _dispatch_agent, nil) do
    continuation_entries(intent)
  end

  defp continuation_entries(
         %ContinuationIntent{} = intent,
         %Agent{agent_module: DispatchAgent, state: %{projection: dispatch_projection}},
         %{runnable_key: source_runnable_key, request_input: request_input}
       )
       when is_binary(source_runnable_key) do
    with {:ok, attempt} <- Map.fetch(dispatch_projection.attempts, source_runnable_key),
         :ok <- validate_native_completed_attempt(attempt, intent, request_input),
         {:ok, applied_entry} <-
           EntryBuilder.runnable_applied(
             attempt,
             attempt.result,
             nil,
             intent.occurred_at,
             attempt.execution_opts,
             attempt.completed_at
           ),
         {:ok, continuation_entries} <- continuation_entries(intent) do
      {:ok, [applied_entry | continuation_entries]}
    else
      :error -> {:error, {:invalid_continuation, :missing_source_attempt}}
      {:error, _reason} = error -> error
    end
  end

  defp append_native_source(storage, dispatch_agent, workflow_agent, fence, source) do
    with {:ok, intent} <- ContinuationIntent.from_fence(fence),
         {:ok, attempt} <-
           Map.fetch(dispatch_agent.state.projection.attempts, source.runnable_key),
         :ok <- validate_native_completed_attempt(attempt, intent, source.request_input),
         {:ok, applied_entry} <-
           EntryBuilder.runnable_applied(
             attempt,
             attempt.result,
             nil,
             intent.occurred_at,
             attempt.execution_opts,
             attempt.completed_at
           ),
         {:ok, _thread} <-
           Journal.append_entries(storage, [applied_entry],
             expected_rev: workflow_agent.state.thread_rev,
             telemetry_projection: workflow_agent.state.projection
           ) do
      WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)
    else
      :error -> {:error, {:invalid_continuation, :missing_source_attempt}}
      {:error, _reason} = error -> error
    end
  end

  defp validate_native_completed_attempt(attempt, intent, request_input) do
    directive = %{
      input: request_input,
      continuation_key: intent.continuation_key,
      definition: :current
    }

    cond do
      attempt.run_id != intent.run_id ->
        unsafe(:source_run_mismatch)

      attempt.status != :completed or not is_map(attempt.result) or
          not match?(%DateTime{}, attempt.completed_at) ->
        unsafe(:source_not_completed)

      not (is_list(attempt.execution_opts) and Keyword.keyword?(attempt.execution_opts)) ->
        unsafe(:source_directive_mismatch)

      Keyword.get(attempt.execution_opts, :continue_as_new) != directive ->
        unsafe(:source_directive_mismatch)

      true ->
        :ok
    end
  end

  defp native_source(%{source_runnable_key: runnable_key} = fence)
       when is_binary(runnable_key) and runnable_key != "" do
    %{runnable_key: runnable_key, request_input: Map.get(fence, :request_input, fence.input)}
  end

  defp native_source(_fence) do
    nil
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
