defmodule Squidie.Runtime.Journal.Executor do
  @moduledoc """
  Executes one visible attempt from the journal-backed runtime queue.

  The runtime step boundary is the side-effect boundary for the journal-backed runtime. It
  claims a visible attempt with the dispatch agent, runs the declared workflow
  step once, records either a completed or failed attempt fact, and then applies
  any completed dispatch results back to the workflow journal.
  """

  alias Jido.Agent
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.BuiltInStep
  alias Squidie.Runtime.Deadline
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Compensation
  alias Squidie.Runtime.Journal.EntryBuilder
  alias Squidie.Runtime.Journal.Executor.ClaimContext
  alias Squidie.Runtime.Journal.Executor.RuntimeContext
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.Journal.WorkflowDefinitionLoader
  alias Squidie.Runtime.RetryPolicy
  alias Squidie.Runtime.StepInput
  alias Squidie.Runtime.Trace
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Step
  alias Squidie.Telemetry.CommitBuffer
  alias Squidie.Telemetry.Emitter
  alias Squidie.Telemetry.JournalEvents
  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.GuardrailRegistry

  @dispatch_append_retries 25
  @run_append_retries 25
  @minimum_heartbeat_interval_ms 50

  @type execute_error ::
          {:invalid_option,
           {:opts, term()}
           | {:runtime, term()}
           | {:journal_storage, nil}
           | {:queue, term()}
           | {:now, term()}
           | {:finished_at, term()}
           | {:owner_id, term()}
           | {:claim_id, term()}
           | {:claim_token, term()}
           | {:heartbeat_interval_ms, term()}
           | {:action_registry, term()}
           | {:test_after_claim, term()}
           | {:test_before_completion, term()}
           | {:test_after_transaction_step, term()}
           | {:test_after_transaction_completion, term()}
           | {:option, atom()}}
          | Definition.load_error()
          | {:unknown_step, atom()}
          | term()

  @type execute_result ::
          {:ok, Inspection.Snapshot.t()} | {:ok, :none} | {:error, execute_error()}

  @doc """
  Executes the next visible journal attempt, if one exists.

  Options:

  - `:runtime` must be `:journal`.
  - `:journal_storage` is the Jido storage adapter config.
  - `:queue` selects the dispatch queue and defaults to `"default"`.
  - `:owner_id` identifies the worker claiming the attempt.
  - `:claim_id` and `:claim_token` may be supplied by tests or host lease backends
    that need deterministic fencing values.
  - `:heartbeat_interval_ms` renews the claim lease while the executor owns a
    running attempt. The executor keeps claim tokens internal.
  - `:now` controls visibility, lease, and event timestamps.
  - `:finished_at` controls completion/failure timestamps for deterministic
    tests. Runtime callers normally omit it so the timestamp is captured after
    action execution.
  """
  @spec execute_next(keyword()) :: execute_result()
  def execute_next(opts) when is_list(opts) do
    with {:ok, opts} <- execute_options(opts),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, owner_id} <- owner_id(opts) do
      execute_with_span(storage, queue, now, owner_id, opts)
    end
  end

  def execute_next(_opts), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp execute_with_span(storage, queue, %DateTime{} = now, owner_id, opts) do
    Emitter.span(
      [:squidie, :runtime, :executor, :execute_next],
      %{queue: queue, partition: Storage.partition(storage)},
      fn -> execute_recovered(storage, queue, now, owner_id, opts) end
    )
  end

  defp execute_recovered(storage, queue, %DateTime{} = now, owner_id, opts) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, recovery_result} <-
           recover_pending_progressions(storage, dispatch_agent, queue, now) do
      execute_after_recovery(storage, queue, now, owner_id, opts, recovery_result)
    end
  end

  defp execute_after_recovery(_storage, _queue, _now, _owner_id, _opts, {:recovered, snapshot}) do
    {:ok, snapshot}
  end

  defp execute_after_recovery(storage, queue, %DateTime{} = now, owner_id, opts, :none) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, claim_result} <- claim_next(storage, dispatch_agent, owner_id, opts, now) do
      execute_claim_result(storage, queue, now, opts, claim_result)
    end
  end

  defp claim_next(storage, dispatch_agent, owner_id, opts, %DateTime{} = now) do
    claim_opts =
      opts
      |> Keyword.take([:claim_id, :claim_token, :lease_for])
      |> Keyword.put(:now, now)

    DispatchAgent.claim_next(storage, dispatch_agent, owner_id, claim_opts)
  end

  defp execute_claim_result(_storage, _queue, _claim_now, _opts, :none), do: {:ok, :none}

  defp execute_claim_result(storage, queue, %DateTime{} = claim_now, opts, %{
         agent: dispatch_agent,
         attempt: %ActionAttempt{} = attempt,
         claim_id: claim_id,
         claim_token: claim_token
       })
       when is_list(opts) do
    with :ok <- run_test_after_claim_hook(opts, attempt),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id) do
      claim = claim_context(dispatch_agent, workflow_agent, attempt, claim_id, claim_token)
      execute_claimed_attempt(storage, queue, claim_now, opts, claim)
    else
      {:error, reason} ->
        finished_at = lifecycle_time(opts, claim_now)

        fail_incompatible_attempt(
          storage,
          queue,
          finished_at,
          dispatch_agent,
          attempt,
          claim_id,
          claim_token,
          reason
        )
    end
  end

  defp execute_claimed_attempt(
         storage,
         queue,
         %DateTime{} = claim_now,
         opts,
         %{
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt
         } = claim
       )
       when is_list(opts) do
    if Projection.terminal?(workflow_agent.state.projection) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: claim_now)
    else
      run_active_claimed_attempt(storage, queue, claim_now, opts, claim)
    end
  end

  defp run_with_heartbeat(
         %{storage: storage, runtime: %{queue: queue}, opts: opts, claim: claim},
         fun
       )
       when is_function(fun, 1) do
    heartbeat_loop = start_heartbeat_loop(storage, queue, opts, claim)

    try do
      fun.(fn -> mark_heartbeat_finishing(heartbeat_loop) end)
    after
      stop_heartbeat_loop(heartbeat_loop)
    end
  end

  defp start_heartbeat_loop(storage, queue, opts, %{
         dispatch_agent: dispatch_agent,
         attempt: %ActionAttempt{} = attempt,
         claim_id: claim_id,
         claim_token: claim_token
       }) do
    case Keyword.get(opts, :heartbeat_interval_ms) do
      interval_ms when is_integer(interval_ms) and interval_ms > 0 ->
        stop_ref = make_ref()
        finish_ref = make_ref()
        lease_for = Keyword.get(opts, :lease_for, 300)
        parent_pid = self()

        state = %{
          stop_ref: stop_ref,
          finish_ref: finish_ref,
          parent_pid: parent_pid,
          phase: :running,
          storage: storage,
          queue: queue,
          dispatch_agent: dispatch_agent,
          runnable_key: attempt.runnable_key,
          claim_id: claim_id,
          claim_token: claim_token,
          lease_for: lease_for,
          interval_ms: interval_ms
        }

        pid =
          spawn_link(fn ->
            state
            |> Map.put(:owner_monitor_ref, Process.monitor(parent_pid))
            |> heartbeat_loop()
          end)

        %{pid: pid, monitor_ref: Process.monitor(pid), stop_ref: stop_ref, finish_ref: finish_ref}

      _disabled ->
        nil
    end
  end

  defp mark_heartbeat_finishing(nil), do: :ok

  defp mark_heartbeat_finishing(%{pid: pid, finish_ref: finish_ref}) do
    ack_ref = make_ref()
    send(pid, {finish_ref, :finishing, self(), ack_ref})

    receive do
      {^ack_ref, :ok} -> :ok
    after
      1_000 -> :ok
    end
  end

  defp stop_heartbeat_loop(nil), do: :ok

  defp stop_heartbeat_loop(%{pid: pid, monitor_ref: monitor_ref, stop_ref: stop_ref}) do
    send(pid, {stop_ref, :stop})

    receive do
      {:DOWN, ^monitor_ref, :process, ^pid, _reason} -> :ok
    after
      1_000 ->
        Process.unlink(pid)
        Process.exit(pid, :kill)
        Process.demonitor(monitor_ref, [:flush])
    end

    :ok
  end

  defp heartbeat_loop(
         %{stop_ref: stop_ref, finish_ref: finish_ref, interval_ms: interval_ms} = state
       ) do
    receive do
      {^stop_ref, :stop} ->
        :ok

      {^finish_ref, :finishing, caller, ack_ref} ->
        send(caller, {ack_ref, :ok})
        heartbeat_loop(%{state | phase: :finishing})

      {:DOWN, owner_monitor_ref, :process, parent_pid, _reason}
      when owner_monitor_ref == state.owner_monitor_ref and parent_pid == state.parent_pid ->
        :ok
    after
      interval_ms ->
        case heartbeat_claim(state) do
          {:ok, state} ->
            heartbeat_loop(state)

          {:lost, reason} ->
            Process.exit(state.parent_pid, :kill)
            exit({:squidie_heartbeat_lost, reason})
        end
    end
  end

  defp heartbeat_claim(state) do
    state
    |> do_heartbeat_claim()
    |> maybe_retry_conflicted_heartbeat()
  rescue
    _error in [
      ArgumentError,
      BadMapError,
      CaseClauseError,
      ErlangError,
      FunctionClauseError,
      KeyError,
      MatchError,
      RuntimeError,
      UndefinedFunctionError
    ] ->
      {:lost, :heartbeat_failed}
  catch
    _kind, _reason -> {:lost, :heartbeat_failed}
  end

  defp do_heartbeat_claim(
         %{
           storage: storage,
           dispatch_agent: dispatch_agent,
           runnable_key: runnable_key,
           claim_id: claim_id,
           claim_token: claim_token,
           lease_for: lease_for
         } = state
       ) do
    case DispatchAgent.heartbeat(storage, dispatch_agent, runnable_key, claim_id, claim_token,
           lease_for: lease_for,
           now: DateTime.utc_now()
         ) do
      {:ok, %{agent: heartbeat_agent}} -> {:ok, %{state | dispatch_agent: heartbeat_agent}}
      {:error, reason} -> {:error, reason, state}
    end
  end

  defp maybe_retry_conflicted_heartbeat({:error, :conflict, state}) do
    state
    |> rebuild_heartbeat_agent()
    |> do_heartbeat_claim()
    |> heartbeat_result()
  end

  defp maybe_retry_conflicted_heartbeat(other), do: heartbeat_result(other)

  defp heartbeat_result({:ok, state}), do: {:ok, state}

  defp heartbeat_result({:error, reason, %{phase: :finishing} = state})
       when reason in [:stale_claim, :terminal_run] do
    {:ok, state}
  end

  defp heartbeat_result({:error, reason, _state}), do: {:lost, reason}

  defp rebuild_heartbeat_agent(%{storage: storage, queue: queue} = state) do
    case DispatchAgent.rebuild(storage, queue) do
      {:ok, dispatch_agent} -> %{state | dispatch_agent: dispatch_agent}
      {:error, _reason} -> state
    end
  end

  defp run_active_claimed_attempt(
         storage,
         queue,
         %DateTime{} = claim_now,
         opts,
         %ClaimContext{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt,
           claim_id: claim_id,
           claim_token: claim_token
         } = claim
       )
       when is_list(opts) do
    case executable_step(storage, workflow_agent, attempt) do
      {:ok, workflow, definition, step_name, step} ->
        context = step_context(workflow_agent, attempt, workflow, step_name, step, claim_id, opts)
        finished_at = lifecycle_time(opts, claim_now)
        runtime = runtime_context(storage, queue, finished_at)

        execute_step_and_record(%{
          storage: storage,
          runtime: runtime,
          claim: claim,
          workflow: workflow,
          definition: definition,
          step_name: step_name,
          step: step,
          context: context,
          opts: opts
        })

      {:error, reason} ->
        finished_at = lifecycle_time(opts, claim_now)

        fail_incompatible_attempt(
          storage,
          queue,
          finished_at,
          dispatch_agent,
          attempt,
          claim_id,
          claim_token,
          reason
        )
    end
  end

  defp claim_context(dispatch_agent, workflow_agent, attempt, claim_id, claim_token) do
    ClaimContext.new(dispatch_agent, workflow_agent, attempt, claim_id, claim_token)
  end

  defp runtime_context(storage, queue, %DateTime{} = now) do
    RuntimeContext.new(storage, queue, now)
  end

  defp progression_context(
         execution_opts,
         queue,
         %DateTime{} = schedule_base_at,
         %DateTime{} = now
       ) do
    Map.new(
      execution_opts: execution_opts,
      queue: queue,
      schedule_base_at: schedule_base_at,
      now: now
    )
  end

  defp complete_attempt(
         %RuntimeContext{storage: storage, queue: queue, now: now},
         %ClaimContext{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt,
           claim_id: claim_id,
           claim_token: claim_token
         },
         definition,
         step_name,
         output,
         execution_opts,
         guardrails
       ) do
    runtime = runtime_context(storage, queue, now)

    claim = claim_context(dispatch_agent, workflow_agent, attempt, claim_id, claim_token)

    with {:ok, result} <- apply_step_output_mapping(definition, step_name, output),
         {:ok, %{agent: dispatch_agent, attempt: %ActionAttempt{} = completed_attempt}} <-
           complete_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             result,
             now: now,
             execution_opts: execution_opts,
             guardrails: guardrails
           ) do
      append_completed_attempt_progression(
        runtime,
        %{claim | dispatch_agent: dispatch_agent, attempt: completed_attempt},
        definition,
        step_name,
        result,
        execution_opts
      )
    else
      {:error, _reason} = error ->
        error
    end
  end

  defp append_completed_attempt_progression(
         %RuntimeContext{storage: storage, queue: queue, now: now} = runtime,
         %ClaimContext{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt
         } = claim,
         definition,
         step_name,
         result,
         execution_opts
       ) do
    case append_success_progression(
           workflow_agent,
           runtime,
           %{
             attempt: attempt,
             definition: definition,
             step_name: step_name,
             result: result,
             execution_opts: execution_opts
           }
         ) do
      {:ok, workflow_agent} ->
        with {:ok, _schedule_update} <-
               schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
          Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
        end

      {:error, reason} when is_tuple(reason) ->
        if StepInput.input_mapping_error?(reason) do
          fail_success_progression(runtime, claim, result, reason)
        else
          {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp fail_success_progression(
         %RuntimeContext{storage: storage, queue: queue, now: now},
         %ClaimContext{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt
         },
         result,
         reason
       ) do
    error = normalize_error(reason)

    with {:ok, workflow_agent} <-
           append_failed_success_progression(
             %{storage: storage, now: now},
             workflow_agent,
             attempt,
             result,
             error,
             @run_append_retries
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp fail_attempt(
         %RuntimeContext{storage: storage, queue: queue, now: now} = runtime,
         %ClaimContext{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt,
           claim_id: claim_id,
           claim_token: claim_token
         },
         workflow,
         definition,
         step_name,
         reason
       ) do
    error = normalize_error(reason)

    retry_opts =
      retry_options(workflow_agent, workflow, definition, step_name, attempt, error, now)

    with {:ok, _failed} <-
           fail_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             error,
             Keyword.put(retry_opts, :now, now)
           ),
         {:ok, workflow_agent} <-
           append_failure_progression(
             runtime,
             workflow_agent,
             attempt,
             definition,
             step_name,
             error,
             retry_opts
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp fail_incompatible_attempt(
         storage,
         queue,
         %DateTime{} = now,
         dispatch_agent,
         %ActionAttempt{} = attempt,
         claim_id,
         claim_token,
         reason
       ) do
    error =
      %{
        code: incompatible_error_code(reason),
        message: "journal attempt is incompatible with the current workflow definition",
        retryable?: false
      }
      |> Map.merge(incompatible_error_metadata(reason))
      |> normalize_error()

    with {:ok, _failed} <-
           fail_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             error,
             now: now
           ),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id),
         {:ok, _workflow_agent} <-
           append_run_entries(
             storage,
             workflow_agent,
             [run_terminal_entry!(workflow_agent, :failed, now)],
             @run_append_retries
           ) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp append_success_progression(
         workflow_agent,
         %{storage: _storage, queue: _queue, now: %DateTime{}} = runtime,
         %{attempt: %ActionAttempt{}} = success
       ) do
    success = Map.put_new(success, :schedule_base_at, runtime.now)
    append_success_progression(runtime, workflow_agent, success, @run_append_retries)
  end

  defp append_success_progression(
         runtime,
         workflow_agent,
         %{attempt: %ActionAttempt{} = attempt} = success,
         retries_left
       )
       when retries_left > 0 do
    if success_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      append_recomputed_success_progression(runtime, workflow_agent, success, retries_left)
    end
  end

  defp append_success_progression(_runtime, _workflow_agent, _success, 0),
    do: {:error, :conflict}

  defp append_recomputed_success_progression(
         %{queue: queue, now: now} = runtime,
         workflow_agent,
         %{
           attempt: %ActionAttempt{} = attempt,
           definition: definition,
           step_name: step_name,
           result: result,
           execution_opts: execution_opts
         } = success,
         retries_left
       ) do
    schedule_base_at = Map.get(success, :schedule_base_at, now)

    progression = progression_context(execution_opts, queue, schedule_base_at, now)

    with {:ok, transition, progression_entries} <-
           success_progression_entries(
             workflow_agent,
             attempt,
             definition,
             step_name,
             result,
             progression
           ) do
      entries = [
        runnable_applied_entry!(
          attempt,
          result,
          transition,
          now,
          execution_opts,
          schedule_base_at
        )
        | progression_entries
      ]

      append_success_entries(runtime, workflow_agent, success, entries, retries_left)
    end
  end

  defp append_success_entries(
         %{storage: storage} = runtime,
         workflow_agent,
         success,
         entries,
         retries_left
       ) do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_success_progression(runtime, workflow_agent, success, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp success_progression_recorded?(workflow_agent, %ActionAttempt{} = attempt) do
    MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) or
      workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled]
  end

  defp append_failed_success_progression(
         _runtime,
         workflow_agent,
         %ActionAttempt{},
         _result,
         _error,
         _retries_left
       )
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, workflow_agent}
  end

  defp append_failed_success_progression(
         %{storage: storage, now: now} = runtime,
         workflow_agent,
         %ActionAttempt{} = attempt,
         result,
         error,
         retries_left
       )
       when retries_left > 0 do
    entries =
      if MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) do
        [run_terminal_entry!(workflow_agent, :failed, now, error)]
      else
        [
          runnable_applied_entry!(attempt, result, now),
          run_terminal_entry!(workflow_agent, :failed, now, error)
        ]
      end

    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_failed_success_progression(
            runtime,
            workflow_agent,
            attempt,
            result,
            error,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failed_success_progression(
         _runtime,
         _workflow_agent,
         %ActionAttempt{},
         _result,
         _error,
         0
       ),
       do: {:error, :conflict}

  defp success_progression_entries(
         workflow_agent,
         attempt,
         definition,
         step_name,
         result,
         progression
       ) do
    cond do
      deferred_continuation_execution?(progression) ->
        with {:ok, progression_entries} <-
               deferred_continuation_progression_entries(
                 attempt,
                 definition,
                 step_name,
                 progression
               ) do
          {:ok, nil, progression_entries}
        end

      manual_intervention_execution?(definition, step_name, progression) ->
        with {:ok, progression_entries} <-
               manual_intervention_progression_entries(
                 attempt,
                 definition,
                 step_name,
                 result,
                 progression
               ) do
          {:ok, nil, progression_entries}
        end

      dynamic_attempt?(workflow_agent, attempt) ->
        with {:ok, progression_entries} <-
               dynamic_success_progression_entries(
                 workflow_agent,
                 attempt,
                 definition,
                 progression
               ) do
          {:ok, nil, progression_entries}
        end

      Definition.dependency_mode?(definition) ->
        with {:ok, progression_entries} <-
               dependency_success_progression_entries(
                 workflow_agent,
                 attempt,
                 definition,
                 step_name,
                 result,
                 progression
               ) do
          {:ok, nil, progression_entries}
        end

      true ->
        context = journal_context(workflow_agent, attempt, result)

        with {:ok, %{to: target} = transition} <-
               Definition.transition(definition, step_name, :ok, context),
             {:ok, progression_entries} <-
               success_target_progression_entries(
                 workflow_agent,
                 attempt,
                 definition,
                 target,
                 context,
                 progression
               ) do
          {:ok, Definition.serialize_transition_decision(transition), progression_entries}
        end
    end
  end

  defp deferred_continuation_execution?(%{execution_opts: execution_opts})
       when is_list(execution_opts) do
    Keyword.has_key?(execution_opts, :defer)
  end

  defp deferred_continuation_execution?(_progression), do: false

  defp deferred_continuation_progression_entries(
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         %{
           execution_opts: execution_opts,
           queue: queue,
           schedule_base_at: %DateTime{} = schedule_base_at,
           now: %DateTime{} = now
         }
       )
       when is_atom(step_name) do
    with {:ok, runnable} <-
           deferred_continuation_runnable(
             attempt,
             definition,
             step_name,
             execution_opts,
             queue,
             schedule_base_at
           ) do
      {:ok, [runnables_planned_entry!(attempt.run_id, [runnable], now)]}
    end
  end

  defp deferred_continuation_progression_entries(
         %ActionAttempt{},
         _definition,
         _step_name,
         _progression
       ) do
    {:error, {:unsupported_deferred_continuation, :dynamic_runnable}}
  end

  defp dynamic_success_progression_entries(workflow_agent, attempt, definition, progression) do
    case dynamic_planned_runnable(workflow_agent, attempt) do
      {:ok, runnable} ->
        if Compensation.runnable?(runnable) do
          compensation_success_progression_entries(
            workflow_agent,
            attempt,
            definition,
            runnable,
            progression
          )
        else
          {:ok, terminal_completion_entries(workflow_agent, attempt, progression.now)}
        end

      {:error, _reason} ->
        {:ok, terminal_completion_entries(workflow_agent, attempt, progression.now)}
    end
  end

  defp compensation_success_progression_entries(
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         runnable,
         %{queue: queue, now: now}
       ) do
    failure = Compensation.failure(runnable)
    failure_runnable_key = Compensation.failure_runnable_key(runnable)

    applied_compensation_keys =
      workflow_agent
      |> Compensation.applied_runnable_keys()
      |> MapSet.put(attempt.runnable_key)

    case Compensation.next_runnable(
           workflow_agent,
           definition,
           attempt.run_id,
           queue,
           now,
           failure,
           failure_runnable_key,
           applied_compensation_keys
         ) do
      {:ok, nil} ->
        {:ok, [run_terminal_entry!(workflow_agent, :failed, now, failure)]}

      {:ok, next_runnable} ->
        next_runnable = put_child_trace(next_runnable, attempt)
        {:ok, [runnables_planned_entry!(attempt.run_id, [next_runnable], now)]}
    end
  end

  defp manual_intervention_execution?(definition, step_name, %{execution_opts: execution_opts})
       when is_list(execution_opts) do
    with true <- Keyword.get(execution_opts, :pause, false),
         {:ok, %{module: module}} when module in [:pause, :approval] <-
           Definition.step(definition, step_name) do
      true
    else
      _not_manual -> false
    end
  end

  defp manual_intervention_execution?(_definition, _step_name, _progression), do: false

  defp manual_intervention_progression_entries(
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         result,
         %{now: now, schedule_base_at: paused_at}
       ) do
    with {:ok, step} <- Definition.step(definition, step_name) do
      case step do
        %{module: :pause} ->
          pause_progression_entries(attempt, definition, step_name, result, now, paused_at)

        %{module: :approval} ->
          approval_progression_entries(attempt, definition, step_name, now, paused_at)
      end
    end
  end

  defp pause_progression_entries(attempt, definition, step_name, result, now, paused_at) do
    with {:ok, target} <- Definition.transition_target(definition, step_name, :ok),
         {:ok, deadline} <- Deadline.from_definition(definition, step_name, paused_at) do
      {:ok,
       [
         manual_step_paused_entry!(
           attempt,
           step_name,
           :pause,
           %{output: result || %{}, target: serialize_manual_target(target)},
           now,
           paused_at,
           deadline
         )
       ]}
    end
  end

  defp approval_progression_entries(attempt, definition, step_name, now, paused_at) do
    with {:ok, targets} <- Definition.approval_transition_targets(definition, step_name),
         {:ok, deadline} <- Deadline.from_definition(definition, step_name, paused_at),
         {:ok, output_key} <- Definition.step_output_mapping(definition, step_name) do
      metadata =
        maybe_put(
          %{
            ok_target: serialize_manual_target(Map.fetch!(targets, :ok)),
            error_target: serialize_manual_target(Map.fetch!(targets, :error))
          },
          :output_key,
          serialize_output_key(output_key)
        )

      {:ok,
       [
         manual_step_paused_entry!(
           attempt,
           step_name,
           :approval,
           metadata,
           now,
           paused_at,
           deadline
         )
       ]}
    end
  end

  defp append_failure_progression(
         %RuntimeContext{storage: storage, queue: queue, now: now},
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         _error,
         retry_opts
       )
       when retry_opts != [] do
    if failed_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      retry_visible_at = Keyword.fetch!(retry_opts, :retry_visible_at)
      attempt_number = attempt.attempt_number + 1

      with {:ok, retry_runnable} <-
             retry_runnable_for_failure(
               definition,
               step_name,
               attempt,
               retry_opts,
               attempt_number,
               queue,
               retry_visible_at
             ) do
        append_failure_run_entries(
          storage,
          workflow_agent,
          attempt,
          [runnables_planned_entry!(attempt.run_id, [retry_runnable], now)],
          @run_append_retries
        )
      end
    end
  end

  defp append_failure_progression(
         %{storage: storage, now: now},
         workflow_agent,
         %ActionAttempt{},
         _definition,
         step_name,
         error,
         []
       )
       when not is_atom(step_name) do
    append_run_entries(
      storage,
      workflow_agent,
      [run_terminal_entry!(workflow_agent, :failed, now, error)],
      @run_append_retries
    )
  end

  defp append_failure_progression(
         %RuntimeContext{storage: storage, queue: queue, now: now},
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         error,
         []
       ) do
    case Definition.transition(
           definition,
           step_name,
           :error,
           journal_context(workflow_agent, attempt, %{})
         ) do
      {:ok, %{to: :complete} = transition} ->
        append_failure_run_entries(
          storage,
          workflow_agent,
          attempt,
          [
            runnable_applied_entry!(
              attempt,
              %{},
              Definition.serialize_transition_decision(transition),
              now
            ),
            run_terminal_entry!(workflow_agent, :completed, now)
          ],
          @run_append_retries
        )

      {:ok, %{to: next_step} = transition} when is_atom(next_step) ->
        append_failure_successor_progression(
          %RuntimeContext{storage: storage, queue: queue, now: now},
          workflow_agent,
          attempt,
          definition,
          transition,
          next_step
        )

      {:error, {:unknown_transition, _from_step, :error}} ->
        append_terminal_failure_or_compensation(
          %RuntimeContext{storage: storage, queue: queue, now: now},
          workflow_agent,
          attempt,
          definition,
          error
        )

      {:error, {:no_matching_transition, _from_step, :error}} ->
        append_terminal_failure_or_compensation(
          %RuntimeContext{storage: storage, queue: queue, now: now},
          workflow_agent,
          attempt,
          definition,
          error
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp append_terminal_failure_or_compensation(
         runtime,
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         error
       ) do
    append_terminal_failure_or_compensation(
      runtime,
      workflow_agent,
      attempt,
      definition,
      error,
      @run_append_retries
    )
  end

  defp append_terminal_failure_or_compensation(
         runtime,
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         error,
         retries_left
       ) do
    if failed_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      append_recomputed_terminal_failure_or_compensation(
        runtime,
        workflow_agent,
        attempt,
        definition,
        error,
        retries_left
      )
    end
  end

  defp append_recomputed_terminal_failure_or_compensation(
         %RuntimeContext{storage: storage, queue: queue, now: now} = runtime,
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         error,
         retries_left
       )
       when retries_left > 0 do
    entries =
      case Compensation.next_runnable(
             workflow_agent,
             definition,
             attempt.run_id,
             queue,
             now,
             error,
             attempt.runnable_key,
             MapSet.new()
           ) do
        {:ok, nil} ->
          [run_terminal_entry!(workflow_agent, :failed, now, error)]

        {:ok, runnable} ->
          runnable = put_child_trace(runnable, attempt)
          [runnables_planned_entry!(attempt.run_id, [runnable], now)]
      end

    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_terminal_failure_or_compensation(
            runtime,
            workflow_agent,
            attempt,
            definition,
            error,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_recomputed_terminal_failure_or_compensation(
         _runtime,
         _workflow_agent,
         %ActionAttempt{},
         _definition,
         _error,
         0
       ),
       do: {:error, :conflict}

  defp retry_runnable_for_failure(
         definition,
         step_name,
         %ActionAttempt{} = attempt,
         retry_opts,
         attempt_number,
         queue,
         retry_visible_at
       ) do
    retry_runnable_key = Keyword.fetch!(retry_opts, :retry_runnable_key)

    result =
      case Keyword.fetch(retry_opts, :retry_runnable) do
        {:ok, retry_runnable} ->
          {:ok, retry_runnable}

        :error ->
          EntryBuilder.retry_runnable(
            definition,
            step_name,
            attempt,
            retry_runnable_key,
            attempt_number,
            queue,
            retry_visible_at,
            Keyword.get(retry_opts, :retry_deadline)
          )
      end

    case result do
      {:ok, retry_runnable} ->
        {:ok, Map.put(retry_runnable, :trace, Keyword.get(retry_opts, :retry_trace))}

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failure_successor_progression(
         %RuntimeContext{storage: storage, queue: queue, now: now},
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         transition,
         next_step
       ) do
    with {:ok, runnable} <-
           successor_runnable(
             attempt,
             definition,
             next_step,
             journal_context(workflow_agent, attempt, %{}),
             queue,
             now
           ) do
      append_failure_run_entries(
        storage,
        workflow_agent,
        attempt,
        [
          runnable_applied_entry!(
            attempt,
            %{},
            Definition.serialize_transition_decision(transition),
            now
          ),
          runnables_planned_entry!(attempt.run_id, [runnable], now)
        ],
        @run_append_retries
      )
    end
  end

  defp append_failure_run_entries(
         storage,
         workflow_agent,
         %ActionAttempt{} = attempt,
         entries,
         retries_left
       ) do
    if failed_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      append_failure_run_entries_with_pending_progression(
        storage,
        workflow_agent,
        attempt,
        entries,
        retries_left
      )
    end
  end

  defp apply_step_output_mapping(definition, step_name, output)
       when is_atom(step_name) and is_map(output) do
    Definition.apply_output_mapping(definition, step_name, output)
  end

  defp apply_step_output_mapping(_definition, step_name, output)
       when is_binary(step_name) and is_map(output) do
    {:ok, output}
  end

  defp append_failure_run_entries_with_pending_progression(
         storage,
         workflow_agent,
         %ActionAttempt{} = attempt,
         entries,
         retries_left
       )
       when retries_left > 0 do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_failure_run_entries(storage, workflow_agent, attempt, entries, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failure_run_entries_with_pending_progression(
         _storage,
         _workflow_agent,
         %ActionAttempt{},
         _entries,
         0
       ),
       do: {:error, :conflict}

  defp success_target_progression_entries(
         workflow_agent,
         %ActionAttempt{} = attempt,
         _definition,
         :complete,
         _result,
         %{now: now}
       ) do
    {:ok, terminal_completion_entries(workflow_agent, attempt, now)}
  end

  defp success_target_progression_entries(
         _workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         next_step,
         result,
         %{
           execution_opts: execution_opts,
           queue: queue,
           schedule_base_at: %DateTime{} = schedule_base_at,
           now: %DateTime{} = now
         }
       )
       when is_atom(next_step) do
    visible_at = successor_visible_at(schedule_base_at, execution_opts)

    with {:ok, runnable} <-
           successor_runnable(attempt, definition, next_step, result, queue, visible_at) do
      {:ok, [runnables_planned_entry!(attempt.run_id, [runnable], now)]}
    end
  end

  defp terminal_completion_entries(workflow_agent, %ActionAttempt{} = attempt, %DateTime{} = now) do
    if planned_runnables_complete_after?(workflow_agent, attempt) do
      [run_terminal_entry!(workflow_agent, :completed, now)]
    else
      []
    end
  end

  defp planned_runnables_complete_after?(workflow_agent, %ActionAttempt{} = attempt) do
    planned_keys =
      workflow_agent
      |> WorkflowAgent.planned_runnables()
      |> latest_planned_runnable_keys()
      |> MapSet.new()

    applied_keys =
      workflow_agent
      |> WorkflowAgent.applied_runnable_keys()
      |> MapSet.put(attempt.runnable_key)

    MapSet.subset?(planned_keys, applied_keys)
  end

  defp latest_planned_runnable_keys(runnables) when is_list(runnables) do
    runnables
    |> Enum.reduce(%{}, &put_latest_planned_runnable/2)
    |> Enum.map(fn {_step, runnable} -> runnable.runnable_key end)
  end

  defp put_latest_planned_runnable(runnable, latest_by_step) do
    with step when is_binary(step) <- map_value(runnable, :step),
         key when is_binary(key) <- map_value(runnable, :runnable_key) do
      attempt_number =
        map_value(runnable, :attempt_number) || 1

      put_latest_planned_runnable(latest_by_step, step, key, attempt_number)
    else
      _missing -> latest_by_step
    end
  end

  defp put_latest_planned_runnable(latest_by_step, step, key, attempt_number) do
    current = Map.get(latest_by_step, step)

    if is_nil(current) or attempt_number >= current.attempt_number do
      Map.put(latest_by_step, step, %{
        attempt_number: attempt_number,
        runnable_key: key,
        step: step
      })
    else
      latest_by_step
    end
  end

  defp dependency_success_progression_entries(
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         result,
         %{now: now} = progression
       ) do
    step_statuses = dependency_step_statuses(workflow_agent, definition, step_name)

    case Definition.dependency_progress(definition, step_statuses) do
      :complete ->
        {:ok, terminal_completion_entries(workflow_agent, attempt, now)}

      {:dispatch, next_steps} ->
        context = dependency_context(workflow_agent, result)

        case dependency_success_runnables(
               workflow_agent,
               attempt,
               context,
               definition,
               next_steps,
               progression
             ) do
          {:ok, runnables} ->
            {:ok, [runnables_planned_entry!(attempt.run_id, runnables, now)]}

          {:error, _reason} = error ->
            error
        end

      {:wait, _phase_steps} ->
        {:ok, []}

      {:error, _reason} = error ->
        error
    end
  end

  defp dependency_success_runnables(
         workflow_agent,
         attempt,
         context,
         definition,
         next_steps,
         %{
           execution_opts: execution_opts,
           queue: queue,
           schedule_base_at: %DateTime{} = schedule_base_at
         }
       ) do
    base_visible_at = successor_visible_at(schedule_base_at, execution_opts)

    result =
      Enum.reduce_while(next_steps, {:ok, []}, fn next_step, {:ok, acc} ->
        case dependency_success_runnable(
               workflow_agent,
               attempt,
               context,
               definition,
               next_step,
               queue,
               base_visible_at
             ) do
          {:ok, runnable} -> {:cont, {:ok, [runnable | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, runnables} -> {:ok, Enum.reverse(runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp dependency_success_runnable(
         workflow_agent,
         attempt,
         context,
         definition,
         next_step,
         queue,
         %DateTime{} = base_visible_at
       ) do
    with {:ok, input} <- successor_input(context, definition, next_step) do
      visible_at =
        dependency_successor_visible_at(
          workflow_agent,
          definition,
          next_step,
          base_visible_at
        )

      definition
      |> journal_runnable(attempt.run_id, queue, next_step, input, 1, visible_at)
      |> put_child_trace_result(attempt)
    end
  end

  defp dependency_successor_visible_at(workflow_agent, definition, next_step, %DateTime{} = base) do
    workflow_agent
    |> completed_wait_dependency_visible_ats(definition, next_step)
    |> Enum.reduce(base, &max_datetime/2)
  end

  defp completed_wait_dependency_visible_ats(
         %{state: %{projection: %Projection{} = projection}},
         definition,
         next_step
       ) do
    definition
    |> dependency_steps(next_step)
    |> Enum.flat_map(&completed_wait_visible_at(projection, definition, &1))
  end

  defp dependency_steps(definition, next_step) do
    case Definition.step(definition, next_step) do
      {:ok, %{opts: opts}} ->
        opts
        |> Keyword.get(:after, [])
        |> List.wrap()

      {:error, _reason} ->
        []
    end
  end

  defp completed_wait_visible_at(%Projection{} = projection, definition, dependency_step) do
    with {:ok, %{module: :wait, opts: opts}} <- Definition.step(definition, dependency_step),
         {:ok, runnable_key} <-
           Projection.applied_runnable_key_for_step(
             projection,
             Definition.serialize_step(dependency_step)
           ),
         %DateTime{} = applied_at <- Projection.applied_at(projection, runnable_key) do
      execution_opts = wait_dependency_execution_opts(projection, runnable_key, opts)

      [successor_visible_at(applied_at, execution_opts)]
    else
      _not_a_completed_wait_dependency -> []
    end
  end

  defp wait_dependency_execution_opts(%Projection{} = projection, runnable_key, step_opts) do
    case Projection.applied_execution_opts(projection, runnable_key) do
      [] -> recovered_execution_opts(%{module: :wait, opts: step_opts})
      execution_opts -> execution_opts
    end
  end

  defp max_datetime(%DateTime{} = left, %DateTime{} = right) do
    case DateTime.compare(left, right) do
      :gt -> left
      _lte_or_eq -> right
    end
  end

  defp dependency_step_statuses(workflow_agent, definition, completed_step) do
    applied_keys = WorkflowAgent.applied_runnable_keys(workflow_agent)

    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> latest_planned_runnables_by_step()
    |> Enum.reduce(%{}, fn runnable, acc ->
      runnable_key = runnable.runnable_key
      step_name = Definition.deserialize_step(definition, runnable.step)

      cond do
        step_name == completed_step ->
          Map.put(acc, completed_step, :completed)

        MapSet.member?(applied_keys, runnable_key) ->
          Map.put(acc, step_name, :completed)

        true ->
          Map.put(acc, step_name, :pending)
      end
    end)
  end

  defp latest_planned_runnables_by_step(runnables) when is_list(runnables) do
    runnables
    |> Enum.reduce(%{}, &put_latest_planned_runnable/2)
    |> Map.values()
  end

  defp dependency_context(workflow_agent, current_result) do
    applied_results =
      applied_result_context(workflow_agent)

    applied_results
    |> Map.merge(current_result || %{})
    |> Map.merge(run_context(workflow_agent))
  end

  defp journal_context(workflow_agent, %ActionAttempt{input: input}, current_result) do
    workflow_agent
    |> applied_result_context()
    |> Map.merge(input || %{})
    |> Map.merge(current_result || %{})
    |> Map.merge(run_context(workflow_agent))
  end

  defp applied_result_context(workflow_agent) do
    workflow_agent.state.projection
    |> Map.get(:applied_results, %{})
    |> Enum.filter(fn {_key, value} -> is_map(value) end)
    |> Enum.map(fn {_key, value} -> value end)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp run_context(%Agent{state: %{projection: %Projection{context: context}}})
       when is_map(context) do
    context
  end

  defp run_context(_workflow_agent), do: %{}

  defp successor_runnable(
         %ActionAttempt{} = attempt,
         definition,
         next_step,
         context,
         queue,
         %DateTime{} = now
       ) do
    input =
      successor_input(context, definition, next_step)

    case input do
      {:ok, input} ->
        definition
        |> journal_runnable(attempt.run_id, queue, next_step, input, 1, now)
        |> put_child_trace_result(attempt)

      {:error, _reason} = error ->
        error
    end
  end

  defp deferred_continuation_runnable(
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         execution_opts,
         queue,
         %DateTime{} = schedule_base_at
       ) do
    with {:ok, recovery} <- replay_recovery_policy(definition, step_name) do
      runnable_key = "#{attempt.runnable_key}:deferred"

      runnable =
        Map.new(
          run_id: attempt.run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: attempt.attempt_number,
          queue: queue,
          step: Definition.serialize_step(step_name),
          input: attempt.input || %{},
          trace: child_trace(attempt),
          recovery: recovery,
          visible_at: successor_visible_at(schedule_base_at, execution_opts),
          deferred:
            deferred_continuation_metadata(
              attempt,
              Keyword.fetch!(execution_opts, :defer),
              schedule_base_at
            )
        )

      {:ok, maybe_put(runnable, :deadline, attempt.deadline)}
    end
  end

  defp deferred_continuation_metadata(%ActionAttempt{} = attempt, defer, %DateTime{} = now)
       when is_map(defer) do
    defer
    |> Map.put(:from_runnable_key, attempt.runnable_key)
    |> Map.put(:deferred_at, now)
  end

  defp successor_input(context, definition, next_step) do
    case Definition.step_input_mapping(definition, next_step) do
      {:ok, input_mapping} -> StepInput.apply_input_mapping(context, input_mapping)
      {:error, _reason} = error -> error
    end
  end

  defp put_child_trace_result({:ok, runnable}, %ActionAttempt{} = attempt) do
    {:ok, put_child_trace(runnable, attempt)}
  end

  defp put_child_trace_result({:error, _reason} = error, %ActionAttempt{}), do: error

  defp put_child_trace(runnable, %ActionAttempt{} = attempt) when is_map(runnable) do
    Map.put(runnable, :trace, child_trace(attempt))
  end

  defp child_trace(%ActionAttempt{trace: nil}), do: nil

  defp child_trace(%ActionAttempt{trace: trace, runnable_key: runnable_key}) do
    case Trace.child_of(trace, runnable_key) do
      {:ok, child_trace} -> child_trace
      {:error, _reason} -> nil
    end
  end

  defp successor_visible_at(%DateTime{} = now, execution_opts) when is_list(execution_opts) do
    case Keyword.get(execution_opts, :schedule_in) do
      seconds when is_integer(seconds) and seconds > 0 -> DateTime.add(now, seconds, :second)
      _immediate -> now
    end
  end

  defp append_run_entries(_storage, workflow_agent, _entries, _retries_left)
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, workflow_agent}
  end

  defp append_run_entries(storage, workflow_agent, entries, retries_left) when retries_left > 0 do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_run_entries(storage, workflow_agent, entries, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_run_entries(_storage, _workflow_agent, _entries, 0), do: {:error, :conflict}

  defp schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, %DateTime{} = now) do
    schedule_pending_dispatches(
      storage,
      workflow_agent,
      dispatch_agent,
      now,
      @dispatch_append_retries
    )
  end

  defp schedule_pending_dispatches(_storage, workflow_agent, dispatch_agent, _now, _retries_left)
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, %{agent: dispatch_agent, runnables: []}}
  end

  defp schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now, retries_left) do
    Squidie.Runtime.Journal.DispatchScheduler.schedule_pending_dispatches(
      storage,
      workflow_agent,
      dispatch_agent,
      now,
      retries_left
    )
  end

  defp complete_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         result,
         opts
       ) do
    complete_current_claim(
      storage,
      dispatch_agent,
      runnable_key,
      claim_id,
      claim_token,
      result,
      opts,
      @dispatch_append_retries
    )
  end

  defp complete_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         result,
         opts,
         retries_left
       )
       when retries_left > 0 do
    case DispatchAgent.complete(
           storage,
           dispatch_agent,
           runnable_key,
           claim_id,
           claim_token,
           result,
           opts
         ) do
      {:ok, _update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          complete_current_claim(
            storage,
            dispatch_agent,
            runnable_key,
            claim_id,
            claim_token,
            result,
            opts,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp complete_current_claim(_storage, _agent, _key, _claim_id, _claim_token, _result, _opts, 0),
    do: {:error, :conflict}

  defp fail_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         error,
         opts
       ) do
    fail_current_claim(
      storage,
      dispatch_agent,
      runnable_key,
      claim_id,
      claim_token,
      error,
      opts,
      @dispatch_append_retries
    )
  end

  defp fail_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         error,
         opts,
         retries_left
       )
       when retries_left > 0 do
    case DispatchAgent.fail(
           storage,
           dispatch_agent,
           runnable_key,
           claim_id,
           claim_token,
           error,
           opts
         ) do
      {:ok, _update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          fail_current_claim(
            storage,
            dispatch_agent,
            runnable_key,
            claim_id,
            claim_token,
            error,
            opts,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp fail_current_claim(_storage, _agent, _key, _claim_id, _claim_token, _error, _opts, 0),
    do: {:error, :conflict}

  defp retry_options(
         workflow_agent,
         workflow,
         definition,
         step_name,
         %ActionAttempt{} = attempt,
         error,
         %DateTime{} = now
       ) do
    retry_opts =
      cond do
        Map.get(error, :retryable?) == false ->
          []

        is_binary(step_name) ->
          dynamic_retry_options(workflow_agent, attempt, error, now)

        not is_atom(step_name) ->
          []

        true ->
          case RetryPolicy.resolve(workflow, step_name, attempt.attempt_number) do
            {:retry, next_attempt, delay_ms} ->
              retry_visible_at = DateTime.add(now, retry_delay_ms(error, delay_ms), :millisecond)

              deadline =
                optional_deadline_from_definition(definition, step_name, retry_visible_at)

              [
                retry_runnable_key: runnable_key(attempt.run_id, step_name, next_attempt),
                retry_visible_at: retry_visible_at,
                retry_deadline: deadline
              ]

            _no_retry ->
              []
          end
      end

    put_retry_trace(retry_opts, attempt)
  end

  defp put_retry_trace([], %ActionAttempt{}), do: []

  defp put_retry_trace(retry_opts, %ActionAttempt{} = attempt) do
    retry_trace = child_trace(attempt)
    retry_runnable = Keyword.get(retry_opts, :retry_runnable)

    retry_opts
    |> Keyword.put(:retry_trace, retry_trace)
    |> maybe_put_retry_runnable_trace(retry_runnable, retry_trace)
  end

  defp maybe_put_retry_runnable_trace(retry_opts, nil, _trace), do: retry_opts

  defp maybe_put_retry_runnable_trace(retry_opts, retry_runnable, trace)
       when is_map(retry_runnable) do
    Keyword.put(retry_opts, :retry_runnable, Map.put(retry_runnable, :trace, trace))
  end

  defp dynamic_retry_options(workflow_agent, %ActionAttempt{} = attempt, error, %DateTime{} = now) do
    with {:ok, runnable} <- dynamic_planned_runnable(workflow_agent, attempt),
         {:ok, max_attempts} <- dynamic_retry_max_attempts(runnable),
         true <- attempt.attempt_number < max_attempts do
      next_attempt = attempt.attempt_number + 1
      retry_visible_at = DateTime.add(now, retry_delay_ms(error, 0), :millisecond)
      retry_runnable_key = "#{attempt.run_id}:#{attempt.step}:#{next_attempt}"

      [
        retry_runnable_key: retry_runnable_key,
        retry_visible_at: retry_visible_at,
        retry_deadline: map_value(runnable, :deadline),
        retry_runnable:
          dynamic_retry_runnable(
            runnable,
            attempt,
            next_attempt,
            retry_runnable_key,
            retry_visible_at
          )
      ]
    else
      _no_retry -> []
    end
  end

  defp dynamic_retry_max_attempts(runnable) do
    dynamic_work = map_value(runnable, :dynamic_work, %{})
    retry = map_value(dynamic_work, :retry, %{})

    case map_value(retry, :max_attempts) do
      max_attempts when is_integer(max_attempts) and max_attempts > 0 -> {:ok, max_attempts}
      _missing -> :error
    end
  end

  defp dynamic_retry_runnable(
         runnable,
         attempt,
         attempt_number,
         retry_runnable_key,
         retry_visible_at
       ) do
    %{
      run_id: attempt.run_id,
      runnable_key: retry_runnable_key,
      idempotency_key: retry_runnable_key,
      attempt_number: attempt_number,
      queue: map_value(runnable, :queue),
      step: attempt.step,
      input: attempt.input || %{},
      recovery: map_value(runnable, :recovery),
      visible_at: retry_visible_at,
      deadline: map_value(runnable, :deadline),
      trace: child_trace(attempt),
      dynamic?: true,
      dynamic_work: map_value(runnable, :dynamic_work)
    }
  end

  defp execution_partition(opts) do
    case Options.storage_from_opts(opts) do
      {:ok, storage} -> Squidie.Runtime.Journal.Storage.partition(storage)
      {:error, _reason} -> nil
    end
  end

  defp retry_delay_ms(%{retry_after: retry_after}, _policy_delay_ms)
       when is_integer(retry_after) and retry_after >= 0 do
    retry_after
  end

  defp retry_delay_ms(_error, policy_delay_ms), do: policy_delay_ms

  defp runnable_applied_entry!(%ActionAttempt{} = attempt, result, %DateTime{} = now) do
    runnable_applied_entry!(attempt, result, nil, now, [], now)
  end

  defp runnable_applied_entry!(%ActionAttempt{} = attempt, result, transition, %DateTime{} = now) do
    runnable_applied_entry!(attempt, result, transition, now, [], now)
  end

  defp runnable_applied_entry!(
         %ActionAttempt{} = attempt,
         result,
         transition,
         %DateTime{} = now,
         execution_opts,
         %DateTime{} = applied_at
       )
       when is_list(execution_opts) do
    entry!(:runnable_applied, %{
      run_id: attempt.run_id,
      runnable_key: attempt.runnable_key,
      result: result,
      guardrails: attempt.guardrails,
      execution_opts: execution_opts,
      applied_at: applied_at,
      transition: transition,
      trace: attempt.trace,
      occurred_at: now
    })
  end

  defp manual_step_paused_entry!(
         %ActionAttempt{} = attempt,
         step_name,
         kind,
         metadata,
         %DateTime{} = now,
         %DateTime{} = paused_at,
         deadline
       )
       when is_atom(step_name) and is_atom(kind) and is_map(metadata) do
    entry!(:manual_step_paused, %{
      run_id: attempt.run_id,
      step: Definition.serialize_step(step_name),
      kind: Atom.to_string(kind),
      metadata: metadata,
      deadline: deadline,
      paused_at: paused_at,
      trace: attempt.trace,
      occurred_at: now
    })
  end

  defp runnables_planned_entry!(run_id, runnables, %DateTime{} = now) do
    Squidie.Runtime.Journal.EntryBuilder.runnables_planned!(run_id, runnables, now)
  end

  defp run_terminal_entry!(workflow_agent, status, %DateTime{} = now) do
    EntryBuilder.traced_run_terminal!(
      workflow_agent.state.run_id,
      status,
      Projection.trace(workflow_agent.state.projection),
      now
    )
  end

  defp run_terminal_entry!(workflow_agent, status, %DateTime{} = now, error)
       when is_map(error) do
    EntryBuilder.traced_run_terminal!(
      workflow_agent.state.run_id,
      status,
      Projection.trace(workflow_agent.state.projection),
      now,
      error
    )
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp journal_runnable(
         definition,
         run_id,
         queue,
         step_name,
         input,
         attempt_number,
         %DateTime{} = now
       ) do
    EntryBuilder.runnable(
      definition,
      run_id,
      queue,
      step_name,
      input,
      attempt_number,
      now
    )
  end

  defp runnable_key(run_id, step_name, attempt_number) do
    "#{run_id}:#{Definition.serialize_step(step_name)}:#{attempt_number}"
  end

  defp optional_deadline_from_definition(definition, step_name, %DateTime{} = started_at) do
    case Deadline.from_definition(definition, step_name, started_at) do
      {:ok, deadline} -> deadline
      {:error, _reason} -> nil
    end
  end

  defp replay_recovery_policy(definition, step_name) do
    with {:ok, recovery} <- Definition.step_recovery_policy(definition, step_name) do
      {:ok, Definition.serialize_recovery_policy(recovery)}
    end
  end

  defp recover_pending_progressions(storage, dispatch_agent, queue, %DateTime{} = now) do
    attempts = Map.values(dispatch_agent.state.projection.attempts)

    case recover_pending_dispatches(storage, dispatch_agent, queue, attempts, now) do
      {:ok, :none} ->
        cond do
          attempt = Enum.find(attempts, &recoverable_completed_attempt?(storage, &1)) ->
            recover_completed_progression(storage, dispatch_agent, queue, attempt, now)

          attempt = Enum.find(attempts, &recoverable_failed_attempt?(storage, &1)) ->
            recover_failed_progression(storage, dispatch_agent, queue, attempt, now)

          true ->
            {:ok, :none}
        end

      {:ok, {:recovered, %Inspection.Snapshot{}}} = recovered ->
        recovered

      {:error, _reason} = error ->
        error
    end
  end

  defp recover_pending_dispatches(storage, dispatch_agent, queue, attempts, %DateTime{} = now) do
    dispatch_agent
    |> DispatchAgent.run_ids()
    |> MapSet.union(attempt_run_ids(attempts))
    |> Enum.sort()
    |> Enum.reduce_while({:ok, :none}, fn run_id, {:ok, :none} ->
      case WorkflowAgent.rebuild(storage, run_id) do
        {:ok, workflow_agent} ->
          maybe_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, queue, now)

        {:error, _reason} ->
          {:cont, {:ok, :none}}
      end
    end)
  end

  defp attempt_run_ids(attempts) do
    attempts
    |> Enum.map(& &1.run_id)
    |> MapSet.new()
  end

  defp maybe_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, queue, now) do
    case pending_dispatches_for_queue(workflow_agent, dispatch_agent, queue) do
      [] ->
        {:cont, {:ok, :none}}

      [_runnable | _runnables] ->
        storage
        |> schedule_pending_dispatches(workflow_agent, dispatch_agent, now)
        |> pending_dispatch_recovery_result(storage, workflow_agent, queue, now)
    end
  end

  defp pending_dispatches_for_queue(workflow_agent, dispatch_agent, queue) do
    workflow_agent
    |> WorkflowAgent.pending_dispatches(dispatch_agent)
    |> Enum.filter(&(runnable_queue(&1) == queue))
  end

  defp runnable_queue(runnable) when is_map(runnable) do
    map_value(runnable, :queue)
  end

  defp pending_dispatch_recovery_result(
         {:ok, %{runnables: []}},
         _storage,
         _workflow_agent,
         _queue,
         _now
       ) do
    {:cont, {:ok, :none}}
  end

  defp pending_dispatch_recovery_result({:ok, %{}}, storage, workflow_agent, queue, now) do
    case Inspection.snapshot(storage, workflow_agent.state.run_id, queue: queue, now: now) do
      {:ok, %Inspection.Snapshot{} = snapshot} -> {:halt, {:ok, {:recovered, snapshot}}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp pending_dispatch_recovery_result(
         {:error, _reason} = error,
         _storage,
         _workflow_agent,
         _queue,
         _now
       ),
       do: {:halt, error}

  defp recover_completed_progression(
         storage,
         dispatch_agent,
         queue,
         %ActionAttempt{} = attempt,
         now
       ) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id) do
      recover_completed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now)
    end
  end

  defp recover_completed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now) do
    case executable_step(storage, workflow_agent, attempt) do
      {:ok, _workflow, definition, step_name, step} ->
        append_recovered_success(
          %RuntimeContext{storage: storage, queue: queue, now: now},
          dispatch_agent,
          workflow_agent,
          attempt,
          definition,
          step_name,
          step
        )

      {:error, reason} ->
        recover_incompatible_progression(storage, queue, workflow_agent, attempt, now, reason)
    end
  end

  defp append_recovered_success(
         %RuntimeContext{storage: storage, queue: queue, now: now} = runtime,
         dispatch_agent,
         workflow_agent,
         attempt,
         definition,
         step_name,
         step
       ) do
    with {:ok, workflow_agent} <-
           append_success_progression(
             workflow_agent,
             runtime,
             %{
               attempt: attempt,
               definition: definition,
               step_name: step_name,
               result: attempt.result || %{},
               execution_opts: recovered_attempt_execution_opts(attempt, step),
               schedule_base_at: attempt_completion_at(attempt, now)
             }
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    else
      {:error, reason} when is_tuple(reason) ->
        if StepInput.input_mapping_error?(reason) do
          recover_success_progression_failure(
            storage,
            queue,
            workflow_agent,
            attempt,
            now,
            reason
          )
        else
          {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp recovered_attempt_execution_opts(%ActionAttempt{execution_opts: execution_opts}, _step)
       when is_list(execution_opts) and execution_opts != [] do
    execution_opts
  end

  defp recovered_attempt_execution_opts(_attempt, step), do: recovered_execution_opts(step)

  defp recover_success_progression_failure(storage, queue, workflow_agent, attempt, now, reason) do
    error = normalize_error(reason)

    with {:ok, _workflow_agent} <-
           append_failed_success_progression(
             %{storage: storage, now: now},
             workflow_agent,
             attempt,
             attempt.result || %{},
             error,
             @run_append_retries
           ),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recover_failed_progression(storage, dispatch_agent, queue, %ActionAttempt{} = attempt, now) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id) do
      recover_failed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now)
    end
  end

  defp recover_failed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now) do
    case executable_step(storage, workflow_agent, attempt) do
      {:ok, _workflow, definition, step_name, _step} ->
        append_recovered_failure(
          storage,
          dispatch_agent,
          queue,
          workflow_agent,
          attempt,
          definition,
          step_name,
          now
        )

      {:error, reason} ->
        recover_incompatible_progression(storage, queue, workflow_agent, attempt, now, reason)
    end
  end

  defp append_recovered_failure(
         storage,
         dispatch_agent,
         queue,
         workflow_agent,
         attempt,
         definition,
         step_name,
         now
       ) do
    retry_opts = durable_retry_options(dispatch_agent, workflow_agent, attempt)
    runtime = runtime_context(storage, queue, now)

    with {:ok, workflow_agent} <-
           append_failure_progression(
             runtime,
             workflow_agent,
             attempt,
             definition,
             step_name,
             attempt.error || %{},
             retry_opts
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recover_incompatible_progression(
         storage,
         queue,
         workflow_agent,
         %ActionAttempt{} = attempt,
         %DateTime{} = now,
         _reason
       ) do
    with {:ok, _workflow_agent} <-
           append_run_entries(
             storage,
             workflow_agent,
             [run_terminal_entry!(workflow_agent, :failed, now)],
             @run_append_retries
           ),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recoverable_completed_attempt?(storage, %ActionAttempt{status: :completed} = attempt) do
    case WorkflowAgent.rebuild(storage, attempt.run_id) do
      {:ok, workflow_agent} ->
        not MapSet.member?(
          WorkflowAgent.applied_runnable_keys(workflow_agent),
          attempt.runnable_key
        ) and
          workflow_agent.state.projection.terminal_status not in [:completed, :failed, :cancelled]

      {:error, _reason} ->
        false
    end
  end

  defp recoverable_completed_attempt?(_storage, %ActionAttempt{}), do: false

  defp recoverable_failed_attempt?(storage, %ActionAttempt{status: :failed} = attempt) do
    case WorkflowAgent.rebuild(storage, attempt.run_id) do
      {:ok, workflow_agent} ->
        workflow_agent.state.projection.terminal_status not in [:completed, :failed, :cancelled] and
          not failed_progression_recorded?(workflow_agent, attempt)

      {:error, _reason} ->
        false
    end
  end

  defp recoverable_failed_attempt?(_storage, %ActionAttempt{}), do: false

  defp failed_progression_recorded?(workflow_agent, %ActionAttempt{} = attempt) do
    retry_key = runnable_key(attempt.run_id, attempt.step, attempt.attempt_number + 1)

    MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) or
      Enum.member?(WorkflowAgent.planned_runnable_keys(workflow_agent), retry_key) or
      Compensation.planned_for_failure?(workflow_agent, attempt.runnable_key) or
      workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled]
  end

  defp durable_retry_options(dispatch_agent, workflow_agent, %ActionAttempt{} = failed_attempt) do
    dispatch_agent.state.projection.attempts
    |> Enum.find(fn {_key, %ActionAttempt{} = attempt} ->
      attempt.run_id == failed_attempt.run_id and
        attempt.step == failed_attempt.step and
        attempt.attempt_number == failed_attempt.attempt_number + 1 and
        attempt.status in [:available, :retry_scheduled, :claimed, :completed, :failed]
    end)
    |> case do
      {_key, %ActionAttempt{} = retry_attempt} ->
        retry_opts = [
          retry_runnable_key: retry_attempt.runnable_key,
          retry_visible_at: retry_attempt.visible_at,
          retry_trace: retry_attempt.trace
        ]

        case dynamic_planned_runnable(workflow_agent, failed_attempt) do
          {:ok, runnable} ->
            retry_runnable =
              Map.put(
                dynamic_retry_runnable(
                  runnable,
                  failed_attempt,
                  retry_attempt.attempt_number,
                  retry_attempt.runnable_key,
                  retry_attempt.visible_at
                ),
                :trace,
                retry_attempt.trace
              )

            Keyword.put(retry_opts, :retry_runnable, retry_runnable)

          {:error, _reason} ->
            retry_opts
        end

      nil ->
        []
    end
  end

  defp executable_step(storage, workflow_agent, %ActionAttempt{} = attempt) do
    with {:ok, workflow, definition} <-
           WorkflowDefinitionLoader.load(storage, attempt.run_id, workflow_agent.state.workflow),
         step_name when is_atom(step_name) <-
           Definition.deserialize_step(definition, attempt.step),
         {:ok, step} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name, step}
    else
      step_name when is_binary(step_name) ->
        executable_dynamic_step(storage, workflow_agent, attempt, step_name)

      {:error, _reason} = error ->
        error
    end
  end

  defp executable_dynamic_step(storage, workflow_agent, %ActionAttempt{} = attempt, step_name) do
    with {:ok, workflow, definition} <-
           WorkflowDefinitionLoader.load(storage, attempt.run_id, workflow_agent.state.workflow),
         {:ok, runnable} <- dynamic_planned_runnable(workflow_agent, attempt),
         {:ok, module} <- dynamic_runnable_module(runnable) do
      {:ok, workflow, definition, step_name,
       %{
         module: module,
         opts: dynamic_runnable_opts(runnable),
         dynamic?: true,
         metadata: %{action: dynamic_runnable_action(runnable)}
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  defp dynamic_attempt?(workflow_agent, %ActionAttempt{} = attempt) do
    match?({:ok, _runnable}, dynamic_planned_runnable(workflow_agent, attempt))
  end

  defp dynamic_planned_runnable(workflow_agent, %ActionAttempt{} = attempt) do
    case Projection.planned_runnable(workflow_agent.state.projection, attempt.runnable_key) do
      {:ok, runnable} ->
        if dynamic_runnable?(runnable), do: {:ok, runnable}, else: {:error, :not_dynamic}

      :error ->
        {:error, :unknown_dynamic_runnable}
    end
  end

  defp dynamic_runnable?(runnable) when is_map(runnable) do
    Map.get(runnable, :dynamic?) == true or Map.get(runnable, "dynamic?") == true or
      is_map(Map.get(runnable, :dynamic_work)) or is_map(Map.get(runnable, "dynamic_work"))
  end

  defp dynamic_runnable_module(runnable) when is_map(runnable) do
    dynamic_work = map_value(runnable, :dynamic_work, %{})

    case map_value(dynamic_work, :module) do
      module when is_atom(module) -> {:ok, module}
      _missing -> {:error, {:invalid_dynamic_runnable, :missing_module}}
    end
  end

  defp dynamic_runnable_opts(runnable) when is_map(runnable) do
    dynamic_work = map_value(runnable, :dynamic_work, %{})

    case map_value(dynamic_work, :action_opts, []) do
      opts when is_list(opts) -> [action_opts: opts]
      _invalid -> []
    end
  end

  defp dynamic_runnable_action(runnable) when is_map(runnable) do
    runnable
    |> map_value(:dynamic_work, %{})
    |> map_value(:action)
  end

  defp map_value(map, key, default \\ nil), do: Squidie.MapField.get(map, key, default)

  defp step_context(
         workflow_agent,
         %ActionAttempt{} = attempt,
         workflow,
         step_name,
         step,
         claim_id,
         opts
       ) do
    %{
      run_id: attempt.run_id,
      partition: execution_partition(opts),
      workflow: workflow,
      step: step_name,
      step_opts: execution_step_opts(step, opts),
      attempt: attempt.attempt_number,
      runnable_key: attempt.runnable_key,
      idempotency_key: attempt.idempotency_key,
      claim_id: claim_id,
      trace: attempt.trace,
      state:
        workflow_agent
        |> applied_result_context()
        |> Map.merge(attempt.input)
        |> Map.merge(run_context(workflow_agent))
    }
  end

  defp execution_step_opts(step, opts) when is_map(step) and is_list(opts) do
    step_opts = Map.get(step, :opts, [])

    case execution_action_opts(step, opts) do
      {:ok, action_opts} when is_list(step_opts) ->
        Keyword.put(step_opts, :action_opts, action_opts)

      _missing_or_invalid ->
        step_opts
    end
  end

  defp execution_action_opts(step, opts) when is_map(step) and is_list(opts) do
    with {:ok, registry} <- Keyword.fetch(opts, :action_registry),
         {:ok, action} <- step_action_key(step),
         {:ok, action_opts} <- ActionRegistry.resolve_action_opts(action, registry) do
      {:ok, action_opts}
    else
      _missing_or_invalid -> :error
    end
  end

  defp step_action_key(step) when is_map(step) do
    action =
      case Map.get(step, :metadata) do
        %{action: action} -> action
        %{"action" => action} -> action
        _missing -> nil
      end

    case action do
      nil -> :error
      action -> {:ok, action}
    end
  end

  defp recovered_execution_opts(%{module: :wait, opts: opts}) when is_list(opts) do
    {:ok, _output, execution_opts} = BuiltInStep.execute_wait(opts)
    execution_opts
  end

  defp recovered_execution_opts(%{module: module}) when module in [:pause, :approval],
    do: [pause: true]

  defp recovered_execution_opts(_step), do: []

  defp attempt_completion_at(%ActionAttempt{} = attempt, %DateTime{} = fallback) do
    case Map.get(attempt, :completed_at) do
      %DateTime{} = completed_at -> completed_at
      _missing -> fallback
    end
  end

  defp execute_step_and_record(
         %{
           storage: %{adapter: Squidie.Runtime.Journal.Storage.Ecto, opts: storage_opts},
           step: %{opts: step_opts}
         } = execution
       )
       when is_list(storage_opts) and is_list(step_opts) do
    case Keyword.get(step_opts, :transaction) do
      :repo ->
        run_repo_transaction_attempt(Keyword.fetch!(storage_opts, :repo), execution)

      _other ->
        run_step_and_record(execution)
    end
  end

  defp execute_step_and_record(
         %{
           runtime: runtime,
           claim: claim,
           workflow: workflow,
           definition: definition,
           step_name: step_name,
           step: %{opts: step_opts}
         } = execution
       )
       when is_list(step_opts) do
    case Keyword.get(step_opts, :transaction) do
      :repo ->
        fail_attempt(
          runtime,
          claim,
          workflow,
          definition,
          step_name,
          repo_transaction_storage_error()
        )

      _other ->
        run_step_and_record(execution)
    end
  end

  defp run_step_and_record(
         %{
           claim: claim
         } = execution
       ) do
    run_with_heartbeat(execution, fn mark_finishing ->
      result =
        with {:ok, input_guardrails} <-
               evaluate_runtime_guardrails(execution, :input, claim.attempt.input),
             {:ok, action_guardrails} <-
               evaluate_runtime_guardrails(execution, :action, claim.attempt.input),
             {:ok, output, execution_opts} <- run_step_with_telemetry(execution),
             {:ok, output_guardrails} <- evaluate_runtime_guardrails(execution, :output, output) do
          {:ok, output, execution_opts,
           input_guardrails ++ action_guardrails ++ output_guardrails}
        end

      record_step_result(result, execution, mark_finishing)
    end)
  end

  defp record_step_result(
         {:ok, output, execution_opts, guardrails},
         %{runtime: runtime, claim: claim, definition: definition, step_name: step_name} =
           execution,
         mark_finishing
       ) do
    mark_finishing.()

    with :ok <- run_test_before_completion_hook(execution.opts, claim.attempt) do
      complete_attempt(runtime, claim, definition, step_name, output, execution_opts, guardrails)
    end
  end

  defp record_step_result(
         {:error, reason},
         %{
           runtime: runtime,
           claim: claim,
           workflow: workflow,
           definition: definition,
           step_name: step_name
         },
         mark_finishing
       ) do
    mark_finishing.()
    fail_attempt(runtime, claim, workflow, definition, step_name, reason)
  end

  defp run_repo_transaction_attempt(repo, execution) do
    buffer = CommitBuffer.new()
    {:ok, buffered_storage} = Storage.put_commit_buffer(execution.runtime.storage, buffer)
    transaction_execution = put_in(execution.runtime.storage, buffered_storage)

    try do
      result =
        repo.transaction(fn ->
          run_transactional_step_and_record(repo, transaction_execution)
        end)

      finish_repo_transaction_attempt(result, execution, buffer)
    catch
      kind, reason ->
        stacktrace = __STACKTRACE__
        JournalEvents.discard(buffer)
        :erlang.raise(kind, reason, stacktrace)
    end
  end

  defp finish_repo_transaction_attempt({:ok, snapshot}, _execution, buffer) do
    JournalEvents.flush(buffer)
    {:ok, snapshot}
  end

  defp finish_repo_transaction_attempt(
         {:error, {:squidie_step_error, reason}},
         execution,
         buffer
       ) do
    JournalEvents.discard(buffer)

    fail_attempt(
      execution.runtime,
      execution.claim,
      execution.workflow,
      execution.definition,
      execution.step_name,
      reason
    )
  end

  defp finish_repo_transaction_attempt(
         {:error, {:squidie_transaction_completion_failed, reason}},
         _execution,
         buffer
       ) do
    JournalEvents.discard(buffer)
    {:error, reason}
  end

  defp finish_repo_transaction_attempt({:error, reason}, _execution, buffer) do
    JournalEvents.discard(buffer)
    {:error, reason}
  end

  defp run_transactional_step_and_record(repo, execution) do
    run_with_heartbeat(execution, fn mark_finishing ->
      result =
        with {:ok, input_guardrails} <-
               evaluate_runtime_guardrails(execution, :input, execution.claim.attempt.input),
             {:ok, action_guardrails} <-
               evaluate_runtime_guardrails(execution, :action, execution.claim.attempt.input),
             {:ok, output, execution_opts} <-
               run_transactional_step_with_telemetry(execution),
             {:ok, output_guardrails} <- evaluate_runtime_guardrails(execution, :output, output) do
          {:ok, output, execution_opts,
           input_guardrails ++ action_guardrails ++ output_guardrails}
        end

      record_transactional_step_result(result, repo, execution, mark_finishing)
    end)
  end

  defp record_transactional_step_result(
         {:ok, output, execution_opts, guardrails},
         repo,
         execution,
         mark_finishing
       ) do
    mark_finishing.()

    with :ok <- run_test_before_completion_hook(execution.opts, execution.claim.attempt) do
      complete_transactional_attempt(repo, execution, output, execution_opts, guardrails)
    end
  end

  defp record_transactional_step_result({:error, reason}, repo, _execution, mark_finishing) do
    mark_finishing.()
    repo.rollback({:squidie_step_error, reason})
  end

  defp complete_transactional_attempt(repo, execution, output, execution_opts, guardrails) do
    with :ok <- run_test_after_transaction_step_hook(execution.opts, execution.claim.attempt),
         {:ok, snapshot} <-
           complete_attempt(
             execution.runtime,
             execution.claim,
             execution.definition,
             execution.step_name,
             output,
             execution_opts,
             guardrails
           ),
         :ok <-
           run_test_after_transaction_completion_hook(
             execution.opts,
             execution.claim.attempt
           ) do
      snapshot
    else
      {:error, reason} ->
        repo.rollback({:squidie_transaction_completion_failed, reason})
    end
  end

  defp run_transactional_step(%{module: module}, input, context) when is_atom(module) do
    if Step.native_step?(module) do
      run_native_step(module, input, context)
    else
      {:error,
       %{
         message: "repo transaction steps must use a native Squidie step",
         retryable?: false
       }}
    end
  end

  defp run_step_with_telemetry(execution) do
    step_span(execution, fn ->
      run_step(execution.step, execution.claim.attempt.input, execution.context)
    end)
  end

  defp run_transactional_step_with_telemetry(execution) do
    step_span(execution, fn ->
      run_transactional_step(
        execution.step,
        execution.claim.attempt.input,
        execution.context
      )
    end)
  end

  defp step_span(execution, operation) when is_function(operation, 0) do
    attempt = execution.claim.attempt

    Emitter.span(
      [:squidie, :runtime, :step, :execute],
      %{
        workflow: Definition.serialize_workflow(execution.workflow),
        step: Definition.serialize_step(execution.step_name),
        partition: Storage.partition(execution.runtime.storage),
        run_id: attempt.run_id,
        runnable_key: attempt.runnable_key,
        attempt_number: attempt.attempt_number,
        trace: attempt.trace
      },
      operation
    )
  end

  defp repo_transaction_storage_error do
    %{
      message: "repo transaction steps require Ecto journal storage",
      retryable?: false,
      code: "unsupported_repo_transaction_storage"
    }
  end

  defp run_step(%{module: :wait, opts: opts}, _input, _context) when is_list(opts) do
    BuiltInStep.execute_wait(opts)
  end

  defp run_step(%{module: :log, opts: opts}, _input, _context) when is_list(opts) do
    {:ok, output, _execution_opts} = BuiltInStep.execute_log(opts)
    {:ok, output, []}
  end

  defp run_step(%{module: :pause}, _input, _context) do
    {:ok, %{}, [pause: true]}
  end

  defp run_step(%{module: :approval}, _input, _context) do
    {:ok, %{}, [pause: true]}
  end

  defp run_step(%{module: module}, input, context) when is_atom(module) do
    if Step.native_step?(module) do
      run_native_step(module, input, context)
    else
      run_action_step(module, input, context)
    end
  end

  defp evaluate_runtime_guardrails(
         %{
           definition: definition,
           step_name: step_name,
           claim: %{attempt: %ActionAttempt{} = attempt},
           opts: opts
         },
         placement,
         value
       )
       when is_atom(step_name) and placement in [:input, :action, :output] and is_map(value) do
    with {:ok, step} <- Definition.step(definition, step_name),
         {:ok, registry} <- runtime_guardrail_registry(step, opts) do
      index = step_index(definition, step_name)

      case GuardrailRegistry.evaluate_step(step, index, placement, value, registry, %{
             phase: :execution,
             run_id: attempt.run_id,
             runnable_key: attempt.runnable_key,
             attempt: attempt.attempt_number
           }) do
        {:ok, decisions} ->
          {:ok, Enum.map(decisions, &GuardrailRegistry.public_decision/1)}

        {:error, _error, decisions} ->
          {:error, GuardrailRegistry.runtime_error(step_name, failed_decision(decisions))}
      end
    else
      {:error, :missing_guardrail_registry} ->
        {:error,
         %{
           code: "guardrail_registry_required",
           message: "guardrail registry is required when a step references guardrails",
           retryable?: false,
           guardrail_registry?: false
         }}

      {:error, _reason} ->
        {:ok, []}
    end
  end

  defp evaluate_runtime_guardrails(_execution, _placement, _value), do: {:ok, []}

  defp runtime_guardrail_registry(step, opts) do
    case {GuardrailRegistry.public_step_guardrails(step),
          Keyword.fetch(opts, :guardrail_registry)} do
      {[], _registry} -> {:error, :no_guardrails}
      {_guardrails, {:ok, registry}} -> {:ok, registry}
      {_guardrails, :error} -> {:error, :missing_guardrail_registry}
    end
  end

  defp failed_decision(decisions) do
    Enum.find(decisions, &(Map.get(&1, :status) == :failed)) ||
      %GuardrailRegistry.Decision{
        key: "unknown",
        placement: :action,
        policy: :route_error,
        status: :failed
      }
  end

  defp step_index(definition, step_name) when is_map(definition) do
    definition
    |> Map.get(:steps, [])
    |> Enum.find_index(&(map_value(&1, :name) == step_name))
    |> case do
      nil -> 0
      index -> index
    end
  end

  defp serialize_manual_target(:complete), do: "__complete__"
  defp serialize_manual_target(target) when is_atom(target), do: Definition.serialize_step(target)

  defp serialize_output_key(nil), do: nil
  defp serialize_output_key(output_key) when is_atom(output_key), do: Atom.to_string(output_key)

  defp run_action_step(action, input, context) do
    {action, input} = action_input(action, input)

    result =
      :erlang.apply(Jido.Exec, :run, [
        action,
        input,
        context,
        [max_retries: 0, log_level: :emergency, telemetry: :silent]
      ])

    case result do
      {:ok, output} when is_map(output) -> {:ok, output, []}
      {:ok, output, extras} when is_map(output) and is_list(extras) -> {:ok, output, []}
      {:ok, output, _extras} when is_map(output) -> {:ok, output, []}
      {:error, reason} -> {:error, reason}
      other -> unexpected_exec_result(other)
    end
  end

  defp run_native_step(module, input, context) when is_map(context) do
    Step.Action.run(%{step: module, input: input}, context)
  end

  defp unexpected_exec_result(result) do
    {:error,
     %{
       message: "unexpected Jido.Exec.run result",
       retryable?: false,
       result: inspect(result)
     }}
  end

  defp action_input(action, input) do
    if Squidie.Step.native_step?(action) do
      {Squidie.Step.Action, %{step: action, input: input}}
    else
      {action, input}
    end
  end

  defp normalize_error(%{__struct__: _struct, message: message, details: details})
       when is_binary(message) and is_map(details) do
    details
    |> Map.put(:message, message)
    |> redact_error()
  end

  defp normalize_error({:missing_input_path, _details} = reason) do
    StepInput.input_mapping_error_to_map(reason)
  end

  defp normalize_error(%{__struct__: _struct, message: message}) when is_binary(message) do
    redact_error(%{message: message})
  end

  defp normalize_error(reason) when is_map(reason), do: redact_error(reason)
  defp normalize_error(reason) when is_binary(reason), do: redact_error(%{message: reason})
  defp normalize_error(_reason), do: %{message: "step execution failed"}

  defp redact_error(error) when is_map(error) do
    %{}
    |> maybe_put_safe(:code, safe_error_code(Map.get(error, :code)))
    |> maybe_put_safe(:exception, safe_exception_name(Map.get(error, :exception)))
    |> maybe_put_safe_map(:origin, safe_exception_origin(Map.get(error, :origin)))
    |> maybe_put_safe(:retryable?, Map.get(error, :retryable?))
    |> maybe_put_safe(:retry_after, Map.get(error, :retry_after))
    |> maybe_put_safe_map(:guardrail, Map.get(error, :guardrail))
    |> maybe_put_safe(
      :persisted_definition_version,
      Map.get(error, :persisted_definition_version)
    )
    |> maybe_put_safe(
      :persisted_definition_fingerprint,
      Map.get(error, :persisted_definition_fingerprint)
    )
    |> maybe_put_safe(:current_definition_version, Map.get(error, :current_definition_version))
    |> maybe_put_safe(
      :current_definition_fingerprint,
      Map.get(error, :current_definition_fingerprint)
    )
    |> Map.put(:message, safe_error_message(Map.get(error, :message)))
  end

  defp maybe_put_safe(acc, key, value)
       when is_binary(value) or is_boolean(value) or is_integer(value) do
    Map.put(acc, key, value)
  end

  defp maybe_put_safe(acc, _key, _value), do: acc

  defp maybe_put_safe_map(acc, key, value) when is_map(value), do: Map.put(acc, key, value)
  defp maybe_put_safe_map(acc, _key, _value), do: acc

  defp maybe_put(acc, _key, nil), do: acc
  defp maybe_put(acc, key, value), do: Map.put(acc, key, value)

  defp incompatible_error_code(%{code: code}) when is_binary(code), do: code
  defp incompatible_error_code(_reason), do: "incompatible_journal_attempt"

  defp incompatible_error_metadata(reason) when is_map(reason) do
    Map.take(reason, [
      :persisted_definition_version,
      :persisted_definition_fingerprint,
      :current_definition_version,
      :current_definition_fingerprint
    ])
  end

  defp incompatible_error_metadata(_reason), do: %{}

  defp safe_error_code(code) when is_binary(code) do
    if Regex.match?(~r/^[a-z][a-z0-9_]{0,63}$/, code) do
      code
    else
      "step_error"
    end
  end

  defp safe_error_code(_code), do: nil

  defp safe_exception_name(exception) when is_binary(exception) do
    if Regex.match?(~r/^(?:Elixir\.)?[A-Z][A-Za-z0-9_]*(?:\.[A-Z][A-Za-z0-9_]*)*$/, exception) do
      exception
    end
  end

  defp safe_exception_name(_exception), do: nil

  defp safe_exception_origin(origin) when is_map(origin) do
    module = Map.get(origin, :module)
    function = Map.get(origin, :function)
    arity = Map.get(origin, :arity)

    with true <-
           is_binary(module) and is_binary(function) and is_integer(arity) and arity >= 0 and
             arity <= 255,
         true <-
           Regex.match?(
             ~r/^(?:(?:Elixir\.)?[A-Z][A-Za-z0-9_]*(?:\.[A-Z][A-Za-z0-9_]*)*|:[a-z][a-z0-9_]*)$/,
             module
           ),
         true <- Regex.match?(~r/^[a-zA-Z_][a-zA-Z0-9_?!]*$/, function) do
      maybe_put_safe_origin_line(
        Map.take(origin, [:module, :function, :arity]),
        Map.get(origin, :line)
      )
    else
      _invalid -> nil
    end
  end

  defp safe_exception_origin(_origin), do: nil

  defp maybe_put_safe_origin_line(origin, line)
       when is_integer(line) and line > 0 and line <= 10_000_000 do
    Map.put(origin, :line, line)
  end

  defp maybe_put_safe_origin_line(origin, _line), do: origin

  defp safe_error_message(message)
       when message in [
              "gateway timeout",
              "journal attempt is incompatible with the current workflow definition",
              "step execution failed"
            ] do
    message
  end

  defp safe_error_message("step " <> rest = message) do
    if String.contains?(rest, " guardrail ") do
      message
    else
      "step execution failed"
    end
  end

  defp safe_error_message(_message), do: "step execution failed"

  defp execute_options(opts) when is_list(opts) do
    with :ok <- validate_keyword_options(opts),
         :ok <- validate_supported_options(opts),
         :ok <- validate_finished_at_option(opts),
         :ok <- validate_heartbeat_interval_option(opts),
         :ok <- validate_test_hook_option(opts, :test_after_claim),
         :ok <- validate_test_hook_option(opts, :test_before_completion),
         :ok <- validate_test_hook_option(opts, :test_after_transaction_step),
         :ok <- validate_test_hook_option(opts, :test_after_transaction_completion),
         :ok <- validate_runtime_option(opts) do
      {:ok, opts}
    end
  end

  defp validate_keyword_options(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp validate_supported_options(opts) do
    case Enum.find(Keyword.keys(opts), &(&1 not in supported_options())) do
      nil -> :ok
      unsupported -> {:error, {:invalid_option, {:option, unsupported}}}
    end
  end

  defp validate_finished_at_option(opts) do
    if Keyword.has_key?(opts, :finished_at) and not match?(%DateTime{}, opts[:finished_at]) do
      invalid_option(:finished_at)
    else
      :ok
    end
  end

  defp validate_heartbeat_interval_option(opts) do
    interval_ms = Keyword.get(opts, :heartbeat_interval_ms)

    if interval_ms != nil and
         not (is_integer(interval_ms) and interval_ms >= @minimum_heartbeat_interval_ms) do
      invalid_option(:heartbeat_interval_ms)
    else
      :ok
    end
  end

  defp validate_test_hook_option(opts, option) do
    if Keyword.has_key?(opts, option) and not is_function(opts[option], 1) do
      invalid_option(option)
    else
      :ok
    end
  end

  defp validate_runtime_option(opts) do
    if Keyword.get(opts, :runtime) == :journal, do: :ok, else: invalid_option(:runtime)
  end

  defp supported_options do
    [
      :runtime,
      :journal_storage,
      :queue,
      :partition,
      :owner_id,
      :claim_id,
      :claim_token,
      :lease_for,
      :heartbeat_interval_ms,
      :action_registry,
      :guardrail_registry,
      :now,
      :finished_at,
      :test_after_claim,
      :test_before_completion,
      :test_after_transaction_step,
      :test_after_transaction_completion
    ]
  end

  defp run_test_after_claim_hook(opts, %ActionAttempt{} = attempt) do
    case Keyword.get(opts, :test_after_claim) do
      nil -> :ok
      hook when is_function(hook, 1) -> hook.(attempt)
    end
  end

  defp run_test_before_completion_hook(opts, %ActionAttempt{} = attempt) do
    case Keyword.get(opts, :test_before_completion) do
      nil ->
        :ok

      hook when is_function(hook, 1) ->
        case hook.(attempt) do
          :ok -> :ok
          {:error, reason} -> {:error, {:test_before_completion, reason}}
          other -> {:error, {:test_before_completion, other}}
        end
    end
  end

  defp run_test_after_transaction_step_hook(opts, %ActionAttempt{} = attempt) do
    case Keyword.get(opts, :test_after_transaction_step) do
      nil ->
        :ok

      hook when is_function(hook, 1) ->
        case hook.(attempt) do
          :ok -> :ok
          {:error, reason} -> {:error, {:test_after_transaction_step, reason}}
          other -> {:error, {:test_after_transaction_step, other}}
        end
    end
  end

  defp run_test_after_transaction_completion_hook(opts, %ActionAttempt{} = attempt) do
    case Keyword.get(opts, :test_after_transaction_completion) do
      nil ->
        :ok

      hook when is_function(hook, 1) ->
        case hook.(attempt) do
          :ok -> :ok
          {:error, reason} -> {:error, {:test_after_transaction_completion, reason}}
          other -> {:error, {:test_after_transaction_completion, other}}
        end
    end
  end

  defp journal_storage(opts) do
    Options.storage_from_opts(opts)
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp now(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> invalid_option(:now)
    end
  end

  defp lifecycle_time(opts, %DateTime{} = claim_now) do
    cond do
      Keyword.has_key?(opts, :finished_at) ->
        Keyword.fetch!(opts, :finished_at)

      Keyword.has_key?(opts, :now) ->
        claim_now

      true ->
        DateTime.utc_now()
    end
  end

  defp owner_id(opts) do
    case Keyword.get(opts, :owner_id, "squidie") do
      owner_id when is_binary(owner_id) and owner_id != "" -> {:ok, owner_id}
      _owner_id -> invalid_option(:owner_id)
    end
  end

  defp invalid_option(field), do: {:error, {:invalid_option, {field, :invalid}}}
end
