defmodule Squidie.Runtime.Journal.Commands.ContinueAsNew do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.ContinuationActivation
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.Journal.Commands.Continuation
  alias Squidie.Runtime.Journal.Commands.ContinuationRecovery
  alias Squidie.Runtime.Journal.ContinuationIntent
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.WorkflowDefinitionLoader
  alias Squidie.Runtime.Routing
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition

  @fence_retries 25

  @doc false
  @spec continue(term(), term()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  def continue(run_id, opts \\ []) do
    with :ok <- ContinuationActivation.ensure_enabled(),
         :ok <- Routing.public_continuation_options(opts),
         {:ok, run_id} <- Options.uuid_run_id(run_id),
         {:ok, input} <- input(opts),
         {:ok, continuation_key} <- continuation_key(opts),
         {:ok, :journal} <- Routing.runtime(opts),
         journal_opts = Routing.journal_continuation_options(opts),
         {:ok, storage} <- Routing.journal_storage(journal_opts),
         {:ok, queue} <- Options.queue_from_opts(journal_opts),
         {:ok, now} <- Options.now_from_opts(journal_opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         :ok <- validate_durable_queue(workflow_agent, queue),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
      continue_from_dispatch_state(
        storage,
        dispatch_agent,
        workflow_agent,
        run_id,
        input,
        continuation_key,
        queue,
        now
      )
    end
  end

  defp continue_from_dispatch_state(
         storage,
         dispatch_agent,
         workflow_agent,
         run_id,
         input,
         continuation_key,
         queue,
         now
       ) do
    case DispatchAgent.continuation_fence(dispatch_agent, run_id) do
      nil ->
        continue_with_new_fence(
          storage,
          workflow_agent,
          run_id,
          input,
          continuation_key,
          queue,
          now
        )

      fence ->
        continue_with_existing_fence(
          storage,
          dispatch_agent,
          fence,
          input,
          continuation_key,
          queue,
          now
        )
    end
  end

  defp continue_with_existing_fence(
         storage,
         dispatch_agent,
         fence,
         input,
         continuation_key,
         queue,
         now
       ) do
    with :ok <- matching_request(fence, input, continuation_key) do
      resolve_existing_fence(storage, dispatch_agent, fence, queue, now)
    end
  end

  defp resolve_existing_fence(storage, dispatch_agent, fence, queue, now) do
    cond do
      DispatchAgent.continuation_repair(dispatch_agent, fence.run_id) ->
        Inspection.snapshot(storage, fence.successor_run_id, queue: queue, now: now)

      abort = DispatchAgent.continuation_abort(dispatch_agent, fence.run_id) ->
        {:error, {:continuation_aborted, abort.abort_reason}}

      true ->
        case ContinuationRecovery.resolve_fenced_run(
               storage,
               fence.run_id,
               queue,
               now: now
             ) do
          {:ok, resolution} -> public_resolution(resolution)
          {:error, _reason} = error -> error
        end
    end
  end

  defp continue_with_new_fence(
         storage,
         workflow_agent,
         run_id,
         input,
         continuation_key,
         queue,
         now
       ) do
    with :ok <- module_authored_workflow(storage, run_id),
         {:ok, intent} <-
           ContinuationIntent.prepare_current(
             storage,
             workflow_agent,
             input,
             continuation_key,
             queue,
             now
           ),
         {:ok, _fence} <-
           persist_fence(storage, workflow_agent, intent, input, @fence_retries),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         fence when is_map(fence) <- DispatchAgent.continuation_fence(dispatch_agent, run_id) do
      continue_with_existing_fence(
        storage,
        dispatch_agent,
        fence,
        input,
        continuation_key,
        queue,
        now
      )
    else
      nil -> {:error, {:continuation_fence_not_found, run_id}}
      {:error, _reason} = error -> error
    end
  end

  defp persist_fence(_storage, _workflow_agent, _intent, _request_input, 0) do
    {:error, :conflict}
  end

  defp persist_fence(storage, workflow_agent, intent, request_input, retries_left) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, intent.queue),
         {:ok, workflow_agent} <- refresh_workflow_agent(storage, workflow_agent) do
      case persist_fence_attempt(
             storage,
             dispatch_agent,
             workflow_agent,
             intent,
             request_input
           ) do
        {:ok, update} ->
          {:ok, update.fence}

        {:error, :conflict} ->
          persist_fence(storage, workflow_agent, intent, request_input, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp persist_fence_attempt(
         storage,
         dispatch_agent,
         workflow_agent,
         intent,
         request_input
       ) do
    case DispatchAgent.continuation_fence(dispatch_agent, intent.run_id) do
      nil ->
        with :ok <-
               Continuation.validate_new_intent(
                 storage,
                 dispatch_agent,
                 workflow_agent,
                 intent
               ) do
          append_fence(storage, dispatch_agent, intent, request_input)
        end

      existing ->
        case append_fence(storage, dispatch_agent, intent, request_input) do
          {:error, {:continuation_already_aborted, run_id}} when run_id == intent.run_id ->
            {:ok, %{fence: existing}}

          result ->
            result
        end
    end
  end

  defp append_fence(storage, dispatch_agent, intent, request_input) do
    DispatchAgent.fence_run_for_continuation(
      storage,
      dispatch_agent,
      ContinuationIntent.fence_attrs(intent, request_input),
      now: intent.occurred_at
    )
  end

  defp refresh_workflow_agent(storage, %Agent{state: %{run_id: run_id}}) do
    WorkflowAgent.rebuild(storage, run_id)
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

  defp matching_request(fence, input, continuation_key) do
    if fence.continuation_key == continuation_key and matching_input?(fence, input) do
      :ok
    else
      {:error, :conflicting_continuation_fence}
    end
  end

  defp matching_input?(%{request_input: request_input}, input) do
    request_input == input
  end

  defp matching_input?(%{input: input}, input) do
    true
  end

  defp matching_input?(fence, input) do
    projection = %Projection{workflow: fence.workflow, trigger: fence.trigger}

    case current_target(projection, input) do
      {:ok, _definition, resolved_input} -> resolved_input == fence.input
      {:error, _reason} -> false
    end
  end

  defp validate_durable_queue(
         %Agent{state: %{projection: %Projection{} = projection}},
         queue
       ) do
    queues =
      projection
      |> Projection.planned_runnables()
      |> Enum.map(&Squidie.MapField.get(&1, :queue, "default"))
      |> Enum.uniq()

    if Enum.all?(queues, &(&1 == queue)) do
      :ok
    else
      {:error, {:unsafe_continuation, :queue_mismatch}}
    end
  end

  defp module_authored_workflow(storage, run_id) do
    case WorkflowDefinitionLoader.runtime_spec_run?(storage, run_id) do
      {:ok, false} -> :ok
      {:ok, true} -> {:error, {:unsafe_continuation, :runtime_spec}}
      {:error, _reason} = error -> error
    end
  end

  defp public_resolution({:repaired, repair}) do
    {:ok, repair.successor}
  end

  defp public_resolution({:aborted, update}) do
    {:error, {:continuation_aborted, update.abort.abort_reason}}
  end

  defp input(opts) when is_list(opts) do
    case Keyword.fetch(opts, :input) do
      {:ok, input} when is_map(input) -> {:ok, input}
      {:ok, _input} -> {:error, {:invalid_payload, :expected_map}}
      :error -> {:error, {:invalid_option, {:input, :missing}}}
    end
  end

  defp input(_opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp continuation_key(opts) when is_list(opts) do
    case Keyword.fetch(opts, :continuation_key) do
      {:ok, key} -> Options.thread_part(key, :continuation_key)
      :error -> {:error, {:invalid_option, {:continuation_key, :missing}}}
    end
  end

  defp continuation_key(_opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end
end
