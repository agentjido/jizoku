# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.DispatchAgent do
  @moduledoc """
  Jido-native dispatch coordination state for one durable dispatch queue.

  The agent rebuilds from dispatch-thread journal entries and performs durable
  claim appends so the runtime can coordinate leases, retries, and workflow
  wakeups from durable facts instead of in-memory state.
  """

  use Jido.Agent,
    name: "squidie_dispatch_agent",
    description: "Rebuildable dispatch coordination state for one Squidie queue.",
    default_plugins: false

  alias Jido.Agent
  alias Squidie.Runtime.DispatchAgent.State
  alias Squidie.Runtime.DispatchNotifier
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Projection
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Checkpoint
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.Partition

  @default_lease_seconds 300
  @continuation_fence_fields [
    :run_id,
    :successor_run_id,
    :continuation_key,
    :workflow,
    :trigger,
    :input,
    :definition,
    :definition_version,
    :definition_fingerprint,
    :trace
  ]

  @type queue :: String.t()
  @type claim :: %{
          required(:agent) => Agent.t(),
          required(:attempt) => ActionAttempt.t(),
          required(:claim_id) => String.t(),
          required(:claim_token) => String.t(),
          required(:lease_until) => DateTime.t()
        }
  @type lifecycle_update :: %{
          required(:agent) => Agent.t(),
          required(:attempt) => ActionAttempt.t(),
          optional(:lease_until) => DateTime.t()
        }
  @type schedule_update :: %{
          required(:agent) => Agent.t(),
          required(:runnables) => [map()]
        }
  @type queue_update :: %{
          required(:agent) => Agent.t(),
          required(:queued?) => boolean()
        }
  @type continuation_fence_update :: %{
          required(:agent) => Agent.t(),
          required(:fence) => map(),
          required(:created?) => boolean()
        }
  @type continuation_repair_update :: %{
          required(:agent) => Agent.t(),
          required(:repair) => map(),
          required(:created?) => boolean()
        }
  @type continuation_abort_update :: %{
          required(:agent) => Agent.t(),
          required(:abort) => map(),
          required(:created?) => boolean()
        }
  @type storage_config :: Journal.storage_config()

  @doc """
  Rebuilds a dispatch agent for one queue from the durable dispatch thread.
  """
  @spec rebuild(storage_config(), queue() | atom()) :: {:ok, Agent.t()} | {:error, term()}
  def rebuild(storage, queue) do
    queue = normalize_queue(queue)
    partition = Storage.partition(storage)

    with {:ok, loaded_thread} <- load_dispatch_thread(storage, queue),
         {:ok, projection} <- current_projection(storage, loaded_thread) do
      {:ok,
       new(
         id: agent_id(queue, partition),
         state: dispatch_state(partition, queue, projection, loaded_thread.rev)
       )}
    end
  end

  @doc """
  Returns the stable Jido agent id for a dispatch queue.
  """
  @spec agent_id(queue() | atom()) :: String.t()
  def agent_id(queue), do: "squidie.dispatch.#{normalize_queue(queue)}"

  @doc false
  @spec agent_id(queue() | atom(), String.t() | nil) :: String.t()
  def agent_id(queue, nil), do: agent_id(queue)

  def agent_id(queue, partition) when is_binary(partition) do
    queue = normalize_queue(queue)
    "squidie.dispatch.partition.#{Partition.identity([partition, queue])}"
  end

  @doc """
  Stores the current dispatch projection as a checkpoint for faster rebuilds.
  """
  @spec put_checkpoint(storage_config(), Agent.t(), keyword()) :: :ok | {:error, term()}
  def put_checkpoint(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{queue: queue, projection: projection, thread_rev: thread_rev}
        } = agent,
        opts \\ []
      )
      when is_binary(queue) and is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    with :ok <- validate_agent_partition(storage, agent) do
      Journal.put_checkpoint(storage, {:dispatch, queue}, projection, thread_rev, opts)
    end
  end

  @doc """
  Lists attempts whose visibility window has opened and can be claimed.
  """
  @spec visible_attempts(Agent.t(), DateTime.t()) :: [
          Squidie.Runtime.DispatchProtocol.ActionAttempt.t()
        ]
  def visible_attempts(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        %DateTime{} = at
      ) do
    Projection.visible_attempts(projection, at)
  end

  @doc """
  Lists claimed attempts whose leases have expired by the given time.
  """
  @spec expired_claims(Agent.t(), DateTime.t()) :: [
          Squidie.Runtime.DispatchProtocol.ActionAttempt.t()
        ]
  def expired_claims(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        %DateTime{} = at
      ) do
    Projection.expired_claims(projection, at)
  end

  @doc """
  Lists completed dispatch attempts waiting for workflow application.
  """
  @spec completed_results(Agent.t()) :: [Squidie.Runtime.DispatchProtocol.ActionAttempt.t()]
  def completed_results(%Agent{agent_module: __MODULE__, state: %State{projection: projection}}) do
    Projection.completed_results(projection)
  end

  @doc false
  @spec results_ready_to_apply(Agent.t()) :: [
          Squidie.Runtime.DispatchProtocol.ActionAttempt.t()
        ]
  def results_ready_to_apply(%Agent{
        agent_module: __MODULE__,
        state: %State{projection: projection}
      }) do
    Projection.results_ready_to_apply(projection)
  end

  @doc """
  Returns every runnable key already known by the dispatch projection.
  """
  @spec runnable_keys(Agent.t()) :: MapSet.t(String.t())
  def runnable_keys(%Agent{agent_module: __MODULE__, state: %State{projection: projection}}) do
    Projection.attempt_runnable_keys(projection)
  end

  @doc """
  Returns every run id known by the dispatch projection.
  """
  @spec run_ids(Agent.t()) :: MapSet.t(String.t())
  def run_ids(%Agent{agent_module: __MODULE__, state: %State{projection: projection}}) do
    Projection.run_ids(projection)
  end

  @doc false
  @spec continuation_fence(Agent.t(), String.t()) :: map() | nil
  def continuation_fence(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        run_id
      )
      when is_binary(run_id) do
    Projection.continuation_fence(projection, run_id)
  end

  @doc false
  @spec active_continuation_fence(Agent.t(), String.t()) :: map() | nil
  def active_continuation_fence(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        run_id
      )
      when is_binary(run_id) do
    Projection.active_continuation_fence(projection, run_id)
  end

  @doc false
  @spec continuation_repair(Agent.t(), String.t()) :: map() | nil
  def continuation_repair(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        run_id
      )
      when is_binary(run_id) do
    Projection.continuation_repair(projection, run_id)
  end

  @doc false
  @spec continuation_abort(Agent.t(), String.t()) :: map() | nil
  def continuation_abort(
        %Agent{agent_module: __MODULE__, state: %State{projection: projection}},
        run_id
      )
      when is_binary(run_id) do
    Projection.continuation_abort(projection, run_id)
  end

  @doc false
  @spec continuation_abort_update(Agent.t(), map(), boolean()) :: continuation_abort_update()
  def continuation_abort_update(%Agent{agent_module: __MODULE__} = agent, abort, created?)
      when is_map(abort) and is_boolean(created?) do
    %{agent: agent, abort: abort, created?: created?}
  end

  @doc false
  @spec continuation_fences(Agent.t()) :: [map()]
  def continuation_fences(%Agent{agent_module: __MODULE__, state: %State{projection: projection}}) do
    projection.continuation_fences
    |> Map.values()
    |> Enum.sort_by(& &1.run_id)
  end

  @doc false
  @spec pending_continuation_fences(Agent.t()) :: [map()]
  def pending_continuation_fences(%Agent{
        agent_module: __MODULE__,
        state: %State{projection: projection}
      }) do
    Projection.pending_continuation_fences(projection)
  end

  @doc false
  @spec fence_run_for_continuation(storage_config(), Agent.t(), map(), keyword()) ::
          {:ok, continuation_fence_update()} | {:error, term()}
  def fence_run_for_continuation(storage, agent, fence, opts \\ [])

  def fence_run_for_continuation(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        fence,
        opts
      )
      when is_binary(queue) and is_map(fence) and is_integer(thread_rev) and thread_rev >= 0 and
             is_list(opts) do
    with :ok <- validate_agent_partition(storage, agent),
         {:ok, fence_entry} <- continuation_fence_entry(fence, queue, opts),
         :ok <- validate_continuation_fence_entry(fence_entry),
         {:ok, mode} <- continuation_fence_mode(projection, fence_entry.data) do
      persist_continuation_fence(
        mode,
        storage,
        agent,
        projection,
        thread_rev,
        fence_entry
      )
    end
  end

  def fence_run_for_continuation(_storage, _agent, _fence, _opts) do
    {:error, {:invalid_continuation_fence, :invalid}}
  end

  @doc false
  @spec acknowledge_continuation_repair(
          storage_config(),
          Agent.t(),
          String.t(),
          keyword()
        ) :: {:ok, continuation_repair_update()} | {:error, term()}
  def acknowledge_continuation_repair(storage, agent, run_id, opts \\ [])

  def acknowledge_continuation_repair(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        run_id,
        opts
      )
      when is_binary(queue) and is_binary(run_id) and run_id != "" and
             is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    with :ok <- validate_agent_partition(storage, agent),
         {:ok, repair_entry} <- continuation_repair_entry(projection, run_id, queue, opts),
         {:ok, mode} <- continuation_repair_mode(projection, run_id) do
      persist_continuation_repair(
        mode,
        storage,
        agent,
        projection,
        thread_rev,
        repair_entry
      )
    end
  end

  def acknowledge_continuation_repair(_storage, _agent, _run_id, _opts) do
    {:error, {:invalid_continuation_repair, :invalid}}
  end

  @doc false
  @spec abort_continuation_fence(
          storage_config(),
          Agent.t(),
          String.t(),
          Projection.continuation_abort_reason(),
          keyword()
        ) :: {:ok, continuation_abort_update()} | {:error, term()}
  def abort_continuation_fence(storage, agent, run_id, abort_reason, opts \\ [])

  def abort_continuation_fence(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        run_id,
        abort_reason,
        opts
      )
      when is_binary(queue) and is_binary(run_id) and run_id != "" and
             is_integer(thread_rev) and thread_rev >= 0 and is_atom(abort_reason) and
             is_list(opts) do
    with :ok <- validate_agent_partition(storage, agent),
         true <- Projection.valid_continuation_abort_reason?(abort_reason),
         {:ok, mode} <- continuation_abort_mode(projection, run_id),
         {:ok, abort_entry} <-
           continuation_abort_entry(projection, run_id, queue, abort_reason, opts) do
      persist_continuation_abort(
        mode,
        storage,
        agent,
        projection,
        thread_rev,
        abort_entry
      )
    else
      false -> {:error, {:invalid_continuation_abort, :invalid}}
      {:error, _reason} = error -> error
    end
  end

  def abort_continuation_fence(_storage, _agent, _run_id, _abort_reason, _opts) do
    {:error, {:invalid_continuation_abort, :invalid}}
  end

  @doc """
  Records that a run belongs to this dispatch queue before runnable attempts are
  scheduled.

  This queue marker lets recovery discover a started run even if the process
  crashes after the run thread is committed and before the first
  `:attempt_scheduled` entry is written.
  """
  @spec ensure_run_queued(storage_config(), Agent.t(), String.t(), keyword()) ::
          {:ok, queue_update()} | {:error, term()}
  def ensure_run_queued(storage, agent, run_id, opts \\ [])

  def ensure_run_queued(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        run_id,
        opts
      )
      when is_binary(queue) and is_binary(run_id) and is_integer(thread_rev) and
             thread_rev >= 0 and is_list(opts) do
    ensure_run_queued_after_fence(
      storage,
      agent,
      projection,
      thread_rev,
      queue,
      run_id,
      opts
    )
  end

  @doc """
  Appends durable scheduled attempts for planned runnables that are not already
  present in the dispatch-agent projection.

  The append uses the dispatch thread's current revision as the optimistic fence.
  Duplicate callers with stale dispatch projections therefore fail at the journal
  boundary, while callers that already see the scheduled attempts return
  idempotently without writing.
  """
  @spec schedule_attempts(storage_config(), Agent.t(), String.t(), [map()], keyword()) ::
          {:ok, schedule_update()} | {:error, term()}
  def schedule_attempts(storage, agent, run_id, runnables, opts \\ [])

  def schedule_attempts(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        run_id,
        runnables,
        opts
      )
      when is_binary(queue) and is_binary(run_id) and is_list(runnables) and
             is_integer(thread_rev) and thread_rev >= 0 and is_list(opts) do
    with {:ok, now} <- lifecycle_now(opts),
         {:ok, notifier, notifier_opts} <- notifier_options(opts),
         {:ok, entries, scheduled_runnables} <-
           schedule_entries(projection, queue, run_id, runnables, now),
         :ok <- ensure_new_schedules_not_continuation_fenced(projection, run_id, entries) do
      wakeup = %{run_id: run_id, notifier: notifier, notifier_opts: notifier_opts, now: now}

      persist_dispatch_entries(
        storage,
        agent,
        projection,
        thread_rev,
        entries,
        scheduled_runnables,
        wakeup
      )
    end
  end

  @doc """
  Claims the next visible or expired attempt for a dispatch queue agent.

  The claim is persisted as an `:attempt_claimed` journal entry with the
  agent's current dispatch-thread revision as `:expected_rev`. Concurrent
  claimers therefore race at the journal boundary and receive `{:error,
  :conflict}` when their projection is stale.

  The returned claim contains the raw `claim_token` for the worker process, but
  the durable journal stores only its hash. If the append succeeds, the returned
  `:attempt` reflects the post-claim projection state.
  """
  @spec claim_next(storage_config(), Agent.t(), String.t(), keyword()) ::
          {:ok, claim()} | {:ok, :none} | {:error, term()}
  def claim_next(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        owner_id,
        opts \\ []
      )
      when is_binary(queue) and is_integer(thread_rev) and thread_rev >= 0 and
             is_binary(owner_id) and is_list(opts) do
    with {:ok, claim_options} <- claim_options(opts) do
      claim_attempt(storage, agent, queue, projection, thread_rev, owner_id, claim_options)
    end
  end

  @doc """
  Extends the lease for a currently claimed attempt.

  The heartbeat is rejected before writing when the claim token is stale, the
  claim has expired, or the dispatch-agent projection is not currently claimed.
  """
  @spec heartbeat(storage_config(), Agent.t(), String.t(), String.t(), String.t(), keyword()) ::
          {:ok, lifecycle_update()} | {:error, term()}
  def heartbeat(storage, agent, runnable_key, claim_id, claim_token, opts \\ [])

  def heartbeat(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        runnable_key,
        claim_id,
        claim_token,
        opts
      )
      when is_binary(queue) and is_binary(runnable_key) and is_binary(claim_id) and
             is_binary(claim_token) and is_integer(thread_rev) and thread_rev >= 0 and
             is_list(opts) do
    with {:ok, heartbeat_options} <- heartbeat_options(opts),
         {:ok, attempt} <-
           current_claim(projection, runnable_key, claim_id, claim_token, heartbeat_options.now),
         :ok <- ensure_run_not_continuation_fenced(projection, attempt.run_id),
         :ok <- active_run(storage, attempt.run_id),
         lease_until = DateTime.add(heartbeat_options.now, heartbeat_options.lease_for, :second),
         {:ok, heartbeat_entry} <-
           DispatchProtocol.new_entry(:attempt_heartbeat, %{
             run_id: attempt.run_id,
             runnable_key: attempt.runnable_key,
             claim_id: claim_id,
             claim_token_hash: claim_token_hash(claim_token),
             queue: queue,
             trace: attempt.trace,
             lease_until: lease_until,
             occurred_at: heartbeat_options.now
           }),
         {:ok, heartbeat_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, heartbeat_entry) do
      {:ok,
       lifecycle_update(
         heartbeat_agent,
         claimed_attempt!(heartbeat_agent, runnable_key),
         lease_until
       )}
    end
  end

  @doc """
  Records a durable successful result for a currently claimed attempt.
  """
  @spec complete(
          storage_config(),
          Agent.t(),
          String.t(),
          String.t(),
          String.t(),
          map(),
          keyword()
        ) ::
          {:ok, lifecycle_update()} | {:error, term()}
  def complete(storage, agent, runnable_key, claim_id, claim_token, result, opts \\ [])

  def complete(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        runnable_key,
        claim_id,
        claim_token,
        result,
        opts
      )
      when is_binary(queue) and is_binary(runnable_key) and is_binary(claim_id) and
             is_binary(claim_token) and is_map(result) and is_integer(thread_rev) and
             thread_rev >= 0 and is_list(opts) do
    with {:ok, now} <- lifecycle_now(opts),
         {:ok, completion_target} <-
           completion_target(projection, runnable_key, claim_id, claim_token, result, now) do
      complete_target(completion_target, %{
        storage: storage,
        agent: agent,
        projection: projection,
        thread_rev: thread_rev,
        queue: queue,
        claim_id: claim_id,
        claim_token: claim_token,
        result: result,
        execution_opts: Keyword.get(opts, :execution_opts, []),
        guardrails: Keyword.get(opts, :guardrails, []),
        now: now
      })
    end
  end

  @doc """
  Records a durable failure for a currently claimed attempt.

  `:retry_runnable_key` and `:retry_visible_at` may be provided together to make
  a retry attempt visible through the dispatch projection after the given time.
  """
  @spec fail(
          storage_config(),
          Agent.t(),
          String.t(),
          String.t(),
          String.t(),
          map(),
          keyword()
        ) ::
          {:ok, lifecycle_update()} | {:error, term()}
  def fail(storage, agent, runnable_key, claim_id, claim_token, error, opts \\ [])

  def fail(
        storage,
        %Agent{
          agent_module: __MODULE__,
          state: %State{
            queue: queue,
            projection: %Projection{} = projection,
            thread_rev: thread_rev
          }
        } = agent,
        runnable_key,
        claim_id,
        claim_token,
        error,
        opts
      )
      when is_binary(queue) and is_binary(runnable_key) and is_binary(claim_id) and
             is_binary(claim_token) and is_map(error) and is_integer(thread_rev) and
             thread_rev >= 0 and is_list(opts) do
    with {:ok, now} <- lifecycle_now(opts),
         {:ok, retry_attrs} <- retry_attrs(opts),
         {:ok, attempt} <- current_claim(projection, runnable_key, claim_id, claim_token, now),
         :ok <- ensure_run_not_continuation_fenced(projection, attempt.run_id),
         :ok <- active_run(storage, attempt.run_id),
         {:ok, failed_entry} <-
           DispatchProtocol.new_entry(
             :attempt_failed,
             Map.merge(
               %{
                 run_id: attempt.run_id,
                 runnable_key: attempt.runnable_key,
                 claim_id: claim_id,
                 claim_token_hash: claim_token_hash(claim_token),
                 queue: queue,
                 trace: attempt.trace,
                 error: error,
                 occurred_at: now
               },
               retry_attrs
             )
           ),
         {:ok, failed_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, failed_entry) do
      {:ok, lifecycle_update(failed_agent, claimed_attempt!(failed_agent, runnable_key))}
    end
  end

  defp continuation_fence_entry(fence, queue, opts) do
    if Enum.all?(Map.keys(fence), &(&1 in @continuation_fence_fields)) do
      with {:ok, now} <- lifecycle_now(opts) do
        fence
        |> Map.merge(%{queue: queue, occurred_at: now})
        |> then(&DispatchProtocol.new_entry(:run_continuation_fenced, &1))
        |> normalize_continuation_fence_entry_result()
      end
    else
      {:error, {:invalid_continuation_fence, :invalid}}
    end
  end

  defp continuation_repair_entry(%Projection{} = projection, run_id, queue, opts) do
    case Projection.continuation_fence(projection, run_id) do
      %{queue: ^queue} = fence ->
        with {:ok, now} <- lifecycle_now(opts) do
          fence
          |> Map.put(:occurred_at, now)
          |> then(&DispatchProtocol.new_entry(:run_continuation_repaired, &1))
          |> normalize_continuation_repair_entry_result()
        end

      nil ->
        {:error, {:continuation_fence_not_found, run_id}}

      _wrong_queue ->
        {:error, {:invalid_continuation_repair, :wrong_queue}}
    end
  end

  defp continuation_abort_entry(
         %Projection{} = projection,
         run_id,
         queue,
         abort_reason,
         opts
       ) do
    case Projection.continuation_fence(projection, run_id) do
      %{queue: ^queue} = fence ->
        with {:ok, now} <- lifecycle_now(opts),
             {:ok, entry} <-
               fence
               |> Map.merge(%{abort_reason: abort_reason, occurred_at: now})
               |> then(&DispatchProtocol.new_entry(:run_continuation_aborted, &1)),
             true <- Projection.valid_continuation_abort?(entry.data) do
          {:ok, entry}
        else
          {:error, {:invalid_option, :now}} = error -> error
          _invalid -> {:error, {:invalid_continuation_abort, :invalid}}
        end

      nil ->
        {:error, {:continuation_fence_not_found, run_id}}

      _wrong_queue ->
        {:error, {:invalid_continuation_abort, :wrong_queue}}
    end
  end

  defp normalize_continuation_repair_entry_result({:ok, entry}) do
    {:ok, entry}
  end

  defp normalize_continuation_repair_entry_result({:error, _reason}) do
    {:error, {:invalid_continuation_repair, :invalid}}
  end

  defp normalize_continuation_fence_entry_result({:ok, entry}) do
    {:ok, entry}
  end

  defp normalize_continuation_fence_entry_result({:error, _reason}) do
    {:error, {:invalid_continuation_fence, :invalid}}
  end

  defp ensure_run_queued_after_fence(
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         queue,
         run_id,
         opts
       ) do
    if MapSet.member?(Projection.run_ids(projection), run_id) do
      {:ok, %{agent: agent, queued?: false}}
    else
      with :ok <- ensure_run_not_continuation_fenced(projection, run_id),
           {:ok, now} <- lifecycle_now(opts),
           {:ok, queued_entry} <-
             DispatchProtocol.new_entry(:run_queued, %{
               run_id: run_id,
               queue: queue,
               occurred_at: now
             }),
           {:ok, queued_agent} <-
             persist_dispatch_entry(storage, agent, projection, thread_rev, queued_entry) do
        {:ok, %{agent: queued_agent, queued?: true}}
      end
    end
  end

  defp validate_continuation_fence_entry(%{data: data}) do
    if Projection.valid_continuation_fence?(data) do
      :ok
    else
      {:error, {:invalid_continuation_fence, :invalid}}
    end
  end

  defp continuation_fence_mode(%Projection{} = projection, fence) do
    case Projection.continuation_fence(projection, fence.run_id) do
      nil ->
        case Projection.continuation_blockers(projection, fence.run_id) do
          [] -> {:ok, :new}
          blockers -> {:error, {:unsafe_continuation, blockers}}
        end

      existing ->
        if Projection.same_continuation_fence?(existing, fence) do
          existing_continuation_fence_mode(projection, fence.run_id, existing)
        else
          {:error, :conflicting_continuation_fence}
        end
    end
  end

  defp existing_continuation_fence_mode(projection, run_id, existing) do
    case Projection.continuation_abort(projection, run_id) do
      %{} -> {:error, {:continuation_already_aborted, run_id}}
      nil -> {:ok, {:existing, existing}}
    end
  end

  defp continuation_abort_mode(%Projection{} = projection, run_id) do
    case {
      Projection.continuation_abort(projection, run_id),
      Projection.continuation_repair(projection, run_id),
      Projection.active_continuation_fence(projection, run_id)
    } do
      {%{} = abort, _repair, _fence} ->
        {:ok, {:existing, abort}}

      {nil, %{}, _fence} ->
        {:error, {:continuation_already_repaired, run_id}}

      {nil, nil, %{} = _fence} ->
        {:ok, :new}

      {nil, nil, nil} ->
        {:error, {:continuation_fence_not_found, run_id}}
    end
  end

  defp persist_continuation_abort(
         {:existing, existing},
         _storage,
         %Agent{} = agent,
         %Projection{},
         _thread_rev,
         _entry
       ) do
    {:ok, continuation_abort_update(agent, existing, false)}
  end

  defp persist_continuation_abort(
         :new,
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entry
       ) do
    with {:ok, aborted_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, entry),
         %{} = abort <-
           Projection.continuation_abort(aborted_agent.state.projection, entry.data.run_id) do
      {:ok, continuation_abort_update(aborted_agent, abort, true)}
    else
      nil -> {:error, {:invalid_continuation_abort, :not_retained}}
      {:error, _reason} = error -> error
    end
  end

  defp continuation_repair_mode(%Projection{} = projection, run_id) do
    case {
      Projection.continuation_abort(projection, run_id),
      Projection.continuation_repair(projection, run_id)
    } do
      {%{}, _repair} -> {:error, {:continuation_already_aborted, run_id}}
      {nil, nil} -> {:ok, :new}
      {nil, existing} -> {:ok, {:existing, existing}}
    end
  end

  defp persist_continuation_fence(
         {:existing, existing},
         _storage,
         %Agent{} = agent,
         %Projection{},
         _thread_rev,
         _entry
       ) do
    {:ok, %{agent: agent, fence: existing, created?: false}}
  end

  defp persist_continuation_fence(
         :new,
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entry
       ) do
    with :ok <- continuation_active_run(storage, entry.data.run_id),
         {:ok, fenced_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, entry) do
      {:ok,
       %{
         agent: fenced_agent,
         fence: Projection.continuation_fence(fenced_agent.state.projection, entry.data.run_id),
         created?: true
       }}
    end
  end

  defp persist_continuation_repair(
         {:existing, existing},
         _storage,
         %Agent{} = agent,
         %Projection{},
         _thread_rev,
         _entry
       ) do
    {:ok, %{agent: agent, repair: existing, created?: false}}
  end

  defp persist_continuation_repair(
         :new,
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entry
       ) do
    with {:ok, repaired_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, entry) do
      {:ok,
       %{
         agent: repaired_agent,
         repair:
           Projection.continuation_repair(
             repaired_agent.state.projection,
             entry.data.run_id
           ),
         created?: true
       }}
    end
  end

  defp continuation_active_run(storage, run_id) do
    case active_run(storage, run_id) do
      {:error, :terminal_run} -> {:error, {:unsafe_continuation, [%{reason: :terminal_run}]}}
      result -> result
    end
  end

  defp ensure_run_not_continuation_fenced(%Projection{} = projection, run_id) do
    if Projection.active_continuation_fence(projection, run_id) do
      {:error, :continuation_fenced}
    else
      :ok
    end
  end

  defp ensure_new_schedules_not_continuation_fenced(_projection, _run_id, []) do
    :ok
  end

  defp ensure_new_schedules_not_continuation_fenced(projection, run_id, _entries) do
    ensure_run_not_continuation_fenced(projection, run_id)
  end

  defp claim_attempt(storage, agent, queue, projection, thread_rev, owner_id, claim_options) do
    projection
    |> next_claimable_attempt(claim_options.now)
    |> persist_claimable_attempt(
      storage,
      agent,
      queue,
      projection,
      thread_rev,
      owner_id,
      claim_options
    )
  end

  defp persist_claimable_attempt(
         nil,
         _storage,
         _agent,
         _queue,
         _projection,
         _thread_rev,
         _owner,
         _opts
       ),
       do: {:ok, :none}

  defp persist_claimable_attempt(
         attempt,
         storage,
         agent,
         queue,
         projection,
         thread_rev,
         owner_id,
         opts
       ) do
    case run_status(storage, attempt.run_id) do
      :active ->
        persist_claim(storage, agent, queue, projection, thread_rev, attempt, owner_id, opts)

      :terminal ->
        {:ok, :none}

      {:error, _reason} = error ->
        error
    end
  end

  defp complete_target({:completed, %ActionAttempt{} = attempt}, %{agent: agent}) do
    {:ok, lifecycle_update(agent, attempt)}
  end

  defp complete_target(
         {:claimed, %ActionAttempt{} = attempt},
         %{
           storage: storage,
           agent: agent,
           projection: projection,
           thread_rev: thread_rev,
           queue: queue,
           claim_id: claim_id,
           claim_token: claim_token,
           result: result,
           execution_opts: execution_opts,
           guardrails: guardrails,
           now: now
         }
       ) do
    with :ok <- ensure_run_not_continuation_fenced(projection, attempt.run_id),
         :ok <- active_run(storage, attempt.run_id),
         {:ok, completed_entry} <-
           DispatchProtocol.new_entry(:attempt_completed, %{
             run_id: attempt.run_id,
             runnable_key: attempt.runnable_key,
             claim_id: claim_id,
             claim_token_hash: claim_token_hash(claim_token),
             queue: queue,
             trace: attempt.trace,
             result: result,
             guardrails: guardrails,
             execution_opts: execution_opts,
             occurred_at: now
           }),
         {:ok, completed_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, completed_entry) do
      {:ok,
       lifecycle_update(completed_agent, claimed_attempt!(completed_agent, attempt.runnable_key))}
    end
  end

  defp load_dispatch_thread(storage, queue) do
    case Journal.load_thread(storage, {:dispatch, queue}) do
      {:ok, loaded_thread} ->
        {:ok, loaded_thread}

      {:error, :not_found} ->
        {:ok,
         %{
           thread: {:dispatch, queue},
           thread_id: Journal.thread_id({:dispatch, queue}, Storage.partition(storage)),
           rev: 0,
           entries: []
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp current_projection(storage, %{thread: thread, rev: rev, entries: entries}) do
    with {:ok, projection} <- projection_from_checkpoint(storage, thread, rev, entries),
         {:ok, run_overlay_entries} <- load_run_overlay_entries(storage, entries) do
      {:ok, Projection.replay(projection, run_overlay_entries)}
    end
  end

  defp projection_from_checkpoint(storage, thread, rev, entries) do
    case Journal.fetch_checkpoint(storage, thread) do
      {:ok, %Checkpoint{thread_rev: checkpoint_rev, projection: %Projection{} = projection}}
      when is_integer(checkpoint_rev) and checkpoint_rev >= 0 and checkpoint_rev <= rev ->
        if Projection.checkpoint_compatible?(projection) do
          {:ok, Projection.replay(projection, Enum.drop(entries, checkpoint_rev))}
        else
          {:ok, Projection.rebuild(entries)}
        end

      {:error, :not_found} ->
        {:ok, Projection.rebuild(entries)}

      {:error, _reason} = error ->
        error

      _future_or_invalid_checkpoint ->
        {:ok, Projection.rebuild(entries)}
    end
  end

  defp load_run_overlay_entries(storage, entries) do
    entries
    |> entry_run_ids()
    |> Enum.reduce_while({:ok, []}, fn run_id, {:ok, overlay_entry_chunks} ->
      case Journal.load_thread(storage, {:run, run_id}) do
        {:ok, %{entries: run_entries}} ->
          overlay_entries =
            Enum.filter(run_entries, &(&1.type in [:runnable_applied, :run_terminal]))

          {:cont, {:ok, [overlay_entries | overlay_entry_chunks]}}

        {:error, :not_found} ->
          {:cont, {:ok, overlay_entry_chunks}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, overlay_entry_chunks} ->
        overlay_entries =
          overlay_entry_chunks
          |> Enum.reverse()
          |> Enum.concat()

        {:ok, overlay_entries}

      {:error, _reason} = error ->
        error
    end
  end

  defp entry_run_ids(entries) do
    entries
    |> Enum.map(&Map.get(&1.data, :run_id))
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp schedule_entries(%Projection{} = projection, queue, run_id, runnables, %DateTime{} = now) do
    known_keys = Projection.attempt_runnable_keys(projection)

    runnables
    |> Enum.reject(fn runnable -> MapSet.member?(known_keys, runnable_key(runnable)) end)
    |> Enum.reduce_while({:ok, [], []}, fn runnable, {:ok, entries, scheduled_runnables} ->
      case schedule_entry(queue, run_id, runnable, now) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries], [runnable | scheduled_runnables]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries, scheduled_runnables} ->
        {:ok, Enum.reverse(entries), Enum.reverse(scheduled_runnables)}

      {:error, _reason} = error ->
        error
    end
  end

  defp schedule_entry(queue, run_id, runnable, %DateTime{} = now) when is_map(runnable) do
    runnable_run_id = runnable_value(runnable, :run_id) || run_id
    runnable_key = runnable_key(runnable)
    runnable_queue = runnable_value(runnable, :queue) || queue

    cond do
      runnable_run_id != run_id ->
        {:error, {:wrong_run, runnable_key}}

      normalize_queue(runnable_queue) != queue ->
        {:error, {:wrong_queue, runnable_key}}

      true ->
        DispatchProtocol.new_entry(:attempt_scheduled, %{
          run_id: run_id,
          workflow: runnable_value(runnable, :workflow),
          runnable_key: runnable_key,
          idempotency_key: runnable_value(runnable, :idempotency_key),
          attempt_number: runnable_value(runnable, :attempt_number),
          queue: queue,
          step: runnable_value(runnable, :step),
          input: runnable_value(runnable, :input),
          trace: runnable_value(runnable, :trace),
          visible_at: runnable_value(runnable, :visible_at),
          deadline: runnable_value(runnable, :deadline),
          occurred_at: now
        })
    end
  end

  defp schedule_entry(_queue, _run_id, runnable, %DateTime{}) do
    {:error, {:invalid_runnable, runnable}}
  end

  defp persist_dispatch_entries(
         _storage,
         %Agent{} = agent,
         %Projection{},
         _thread_rev,
         [],
         [],
         _wakeup
       ) do
    {:ok, schedule_update(agent, [])}
  end

  defp persist_dispatch_entries(
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entries,
         scheduled_runnables,
         %{
           run_id: run_id,
           notifier: notifier,
           notifier_opts: notifier_opts,
           now: %DateTime{} = now
         }
       ) do
    with :ok <- validate_agent_partition(storage, agent),
         {:ok, thread} <-
           Journal.append_entries(storage, entries,
             expected_rev: thread_rev,
             telemetry_projection: projection
           ) do
      scheduled_agent = apply_dispatch_entries(agent, projection, entries, thread.rev)

      emit_live_wakeups(
        storage,
        scheduled_agent,
        run_id,
        scheduled_runnables,
        notifier,
        notifier_opts,
        now
      )
    end
  end

  defp emit_live_wakeups(
         storage,
         %Agent{} = agent,
         run_id,
         scheduled_runnables,
         notifier,
         notifier_opts,
         %DateTime{} = now
       )
       when is_binary(run_id) and is_atom(notifier) and not is_nil(notifier) do
    result =
      Enum.reduce_while(scheduled_runnables, {:ok, agent, []}, fn runnable,
                                                                  {:ok, current_agent,
                                                                   notified_runnables} ->
        case notify_scheduled_attempt(
               storage,
               current_agent,
               run_id,
               runnable,
               notifier,
               notifier_opts,
               now
             ) do
          {:ok, next_agent} ->
            {:cont, {:ok, next_agent, [runnable | notified_runnables]}}

          {:ignored, current_agent} ->
            {:cont, {:ok, current_agent, notified_runnables}}
        end
      end)

    case result do
      {:ok, notified_agent, _notified_runnables} ->
        {:ok, schedule_update(notified_agent, scheduled_runnables)}
    end
  end

  defp emit_live_wakeups(
         _storage,
         %Agent{} = agent,
         _run_id,
         scheduled_runnables,
         nil,
         _notifier_opts,
         %DateTime{}
       ) do
    {:ok, schedule_update(agent, scheduled_runnables)}
  end

  defp notify_scheduled_attempt(
         storage,
         %Agent{} = agent,
         run_id,
         runnable,
         notifier,
         notifier_opts,
         %DateTime{} = now
       )
       when is_binary(run_id) do
    attempt = wakeup_attempt(agent.state.partition, agent.state.queue, run_id, runnable)

    case DispatchNotifier.notify_attempt_scheduled(notifier, attempt, notifier_opts) do
      :ok ->
        append_live_wakeup(storage, agent, attempt, now)

      {:error, _reason} ->
        {:ignored, agent}
    end
  end

  defp append_live_wakeup(storage, %Agent{} = agent, attempt, %DateTime{} = now) do
    with {:ok, wakeup_entry} <-
           DispatchProtocol.new_entry(:live_wakeup_emitted, %{
             run_id: Map.fetch!(attempt, :run_id),
             runnable_key: Map.fetch!(attempt, :runnable_key),
             queue: Map.fetch!(attempt, :queue),
             occurred_at: now
           }),
         {:ok, next_agent} <-
           persist_dispatch_entry(
             storage,
             agent,
             agent.state.projection,
             agent.state.thread_rev,
             wakeup_entry
           ) do
      {:ok, next_agent}
    else
      {:error, _reason} -> {:ignored, agent}
    end
  end

  defp wakeup_attempt(partition, queue, run_id, runnable) do
    maybe_put_partition(
      %{
        run_id: runnable_value(runnable, :run_id) || run_id,
        runnable_key: runnable_key(runnable),
        queue: runnable_value(runnable, :queue) || queue,
        visible_at: runnable_value(runnable, :visible_at)
      },
      partition
    )
  end

  defp maybe_put_partition(attempt, nil), do: attempt
  defp maybe_put_partition(attempt, partition), do: Map.put(attempt, :partition, partition)

  defp persist_claim(
         storage,
         %Agent{} = agent,
         queue,
         %Projection{} = projection,
         thread_rev,
         %ActionAttempt{} = attempt,
         owner_id,
         claim_options
       ) do
    claim_id = Map.fetch!(claim_options, :claim_id)
    claim_token = Map.fetch!(claim_options, :claim_token)
    now = Map.fetch!(claim_options, :now)
    lease_until = DateTime.add(now, Map.fetch!(claim_options, :lease_for), :second)

    attrs = %{
      run_id: attempt.run_id,
      runnable_key: attempt.runnable_key,
      claim_id: claim_id,
      claim_token_hash: claim_token_hash(claim_token),
      owner_id: owner_id,
      queue: queue,
      trace: attempt.trace,
      lease_until: lease_until,
      occurred_at: now
    }

    with {:ok, claim_entry} <- DispatchProtocol.new_entry(:attempt_claimed, attrs),
         {:ok, claimed_agent} <-
           persist_dispatch_entry(storage, agent, projection, thread_rev, claim_entry) do
      claimed_attempt = claimed_attempt!(claimed_agent, attempt.runnable_key)

      {:ok, claim_result(claimed_agent, claimed_attempt, claim_id, claim_token, lease_until)}
    end
  end

  defp persist_dispatch_entry(
         storage,
         %Agent{} = agent,
         %Projection{} = projection,
         thread_rev,
         entry
       ) do
    with :ok <- validate_agent_partition(storage, agent),
         {:ok, thread} <-
           Journal.append_entries(storage, [entry],
             expected_rev: thread_rev,
             telemetry_projection: projection
           ) do
      {:ok, apply_dispatch_entry(agent, projection, entry, thread.rev)}
    end
  end

  defp validate_agent_partition(storage, %Agent{state: state}) do
    case {Storage.partition(storage), Map.get(state, :partition)} do
      {partition, partition} -> :ok
      {_storage_partition, _agent_partition} -> {:error, {:partition_mismatch, :dispatch_agent}}
    end
  end

  defp apply_dispatch_entry(%Agent{} = agent, %Projection{} = projection, entry, thread_rev) do
    apply_dispatch_entries(agent, projection, [entry], thread_rev)
  end

  defp apply_dispatch_entries(%Agent{} = agent, %Projection{} = projection, entries, thread_rev) do
    %Agent{
      agent
      | state:
          dispatch_state(
            agent.state.partition,
            agent.state.queue,
            Projection.replay(projection, entries),
            thread_rev
          )
    }
  end

  defp dispatch_state(partition, queue, %Projection{} = projection, thread_rev) do
    State.new(partition, queue, projection, thread_rev)
  end

  defp lifecycle_update(%Agent{} = agent, %ActionAttempt{} = attempt) do
    Map.new(agent: agent, attempt: attempt)
  end

  defp lifecycle_update(%Agent{} = agent, %ActionAttempt{} = attempt, %DateTime{} = lease_until) do
    Map.new(agent: agent, attempt: attempt, lease_until: lease_until)
  end

  defp schedule_update(%Agent{} = agent, runnables) when is_list(runnables) do
    Map.new(agent: agent, runnables: runnables)
  end

  defp claim_result(
         %Agent{} = agent,
         %ActionAttempt{} = attempt,
         claim_id,
         claim_token,
         %DateTime{} = lease_until
       ) do
    Map.new(
      agent: agent,
      attempt: attempt,
      claim_id: claim_id,
      claim_token: claim_token,
      lease_until: lease_until
    )
  end

  defp runnable_key(runnable) when is_map(runnable) do
    runnable_value(runnable, :runnable_key)
  end

  defp runnable_key(_runnable), do: nil

  defp runnable_value(runnable, key) when is_map(runnable) and is_atom(key) do
    Map.get(runnable, key) || Map.get(runnable, Atom.to_string(key))
  end

  defp claimed_attempt!(
         %Agent{state: %State{projection: %Projection{} = projection}},
         runnable_key
       ) do
    Map.fetch!(projection.attempts, runnable_key)
  end

  defp run_status(storage, run_id) do
    case Journal.load_thread(storage, {:run, run_id}) do
      {:ok, %{entries: entries}} ->
        if Enum.any?(entries, &(&1.type == :run_terminal)) do
          :terminal
        else
          :active
        end

      {:error, :not_found} ->
        :active

      {:error, _reason} = error ->
        error
    end
  end

  defp active_run(storage, run_id) do
    case run_status(storage, run_id) do
      :active -> :ok
      :terminal -> {:error, :terminal_run}
      {:error, _reason} = error -> error
    end
  end

  defp claim_options(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    lease_for = Keyword.get(opts, :lease_for, @default_lease_seconds)
    claim_id = Keyword.get_lazy(opts, :claim_id, fn -> random_token(16) end)
    claim_token = Keyword.get_lazy(opts, :claim_token, fn -> random_token(32) end)

    cond do
      not match?(%DateTime{}, now) ->
        {:error, {:invalid_option, :now}}

      not (is_integer(lease_for) and lease_for > 0) ->
        {:error, {:invalid_option, :lease_for}}

      not is_binary(claim_id) or claim_id == "" ->
        {:error, {:invalid_option, :claim_id}}

      not is_binary(claim_token) or claim_token == "" ->
        {:error, {:invalid_option, :claim_token}}

      true ->
        {:ok, %{now: now, lease_for: lease_for, claim_id: claim_id, claim_token: claim_token}}
    end
  end

  defp heartbeat_options(opts) do
    with {:ok, now} <- lifecycle_now(opts) do
      lease_for = Keyword.get(opts, :lease_for, @default_lease_seconds)

      if is_integer(lease_for) and lease_for > 0 do
        {:ok, %{now: now, lease_for: lease_for}}
      else
        {:error, {:invalid_option, :lease_for}}
      end
    end
  end

  defp lifecycle_now(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now())

    if match?(%DateTime{}, now) do
      {:ok, now}
    else
      {:error, {:invalid_option, :now}}
    end
  end

  defp notifier_options(opts) do
    notifier = Keyword.get(opts, :notifier)
    notifier_opts = Keyword.get(opts, :notifier_opts, [])

    cond do
      is_nil(notifier) ->
        {:ok, nil, notifier_opts}

      not is_atom(notifier) ->
        {:error, {:invalid_option, :notifier}}

      not is_list(notifier_opts) ->
        {:error, {:invalid_option, :notifier_opts}}

      true ->
        {:ok, notifier, notifier_opts}
    end
  end

  defp retry_attrs(opts) do
    case {Keyword.fetch(opts, :retry_runnable_key), Keyword.fetch(opts, :retry_visible_at)} do
      {:error, :error} ->
        {:ok, %{}}

      {{:ok, retry_runnable_key}, {:ok, %DateTime{} = retry_visible_at}}
      when is_binary(retry_runnable_key) ->
        attrs =
          maybe_put_retry_trace(
            %{
              retry_runnable_key: retry_runnable_key,
              retry_visible_at: retry_visible_at
            },
            Keyword.get(opts, :retry_trace)
          )

        case Keyword.fetch(opts, :retry_deadline) do
          {:ok, deadline} when is_map(deadline) ->
            {:ok, Map.put(attrs, :retry_deadline, deadline)}

          {:ok, nil} ->
            {:ok, attrs}

          :error ->
            {:ok, attrs}

          {:ok, _invalid} ->
            {:error, {:invalid_option, :retry}}
        end

      _invalid ->
        {:error, {:invalid_option, :retry}}
    end
  end

  defp maybe_put_retry_trace(attrs, trace) when is_map(trace),
    do: Map.put(attrs, :retry_trace, trace)

  defp maybe_put_retry_trace(attrs, _trace), do: attrs

  defp current_claim(
         %Projection{} = projection,
         runnable_key,
         claim_id,
         claim_token,
         %DateTime{} = now
       ) do
    case Map.fetch(projection.attempts, runnable_key) do
      {:ok, %ActionAttempt{status: :claimed} = attempt} ->
        validate_current_claim(attempt, claim_id, claim_token, now)

      {:ok, %ActionAttempt{}} ->
        {:error, :stale_claim}

      :error ->
        {:error, :unknown_runnable_intent}
    end
  end

  defp completion_target(
         %Projection{} = projection,
         runnable_key,
         claim_id,
         claim_token,
         result,
         %DateTime{} = now
       ) do
    case Map.fetch(projection.attempts, runnable_key) do
      {:ok, %ActionAttempt{status: :claimed} = attempt} ->
        with {:ok, current_attempt} <- validate_current_claim(attempt, claim_id, claim_token, now) do
          {:ok, {:claimed, current_attempt}}
        end

      {:ok, %ActionAttempt{status: :completed, result: ^result} = attempt} ->
        if matching_claim_token?(attempt, claim_id, claim_token) do
          {:ok, {:completed, attempt}}
        else
          {:error, :stale_claim}
        end

      {:ok, %ActionAttempt{status: :completed} = attempt} ->
        if matching_claim_token?(attempt, claim_id, claim_token) do
          {:error, :conflicting_completion}
        else
          {:error, :stale_claim}
        end

      {:ok, %ActionAttempt{}} ->
        {:error, :stale_claim}

      :error ->
        {:error, :unknown_runnable_intent}
    end
  end

  defp validate_current_claim(%ActionAttempt{} = attempt, claim_id, claim_token, now) do
    cond do
      not matching_claim_token?(attempt, claim_id, claim_token) ->
        {:error, :stale_claim}

      expired_claim?(attempt, now) ->
        {:error, :expired_claim}

      true ->
        {:ok, attempt}
    end
  end

  defp matching_claim_token?(%ActionAttempt{} = attempt, claim_id, claim_token) do
    attempt.claim_id == claim_id and attempt.claim_token_hash == claim_token_hash(claim_token)
  end

  defp next_claimable_attempt(%Projection{} = projection, %DateTime{} = at) do
    projection
    |> claimable_attempts(at)
    |> Enum.sort_by(&claim_priority/1)
    |> List.first()
  end

  defp claimable_attempts(%Projection{} = projection, %DateTime{} = at) do
    Projection.visible_attempts(projection, at) ++ Projection.expired_claims(projection, at)
  end

  defp claim_priority(%ActionAttempt{} = attempt) do
    {DateTime.to_unix(attempt.visible_at, :microsecond), attempt.run_id, attempt.attempt_number,
     attempt.runnable_key}
  end

  defp claim_token_hash(token) do
    Base.encode16(:crypto.hash(:sha256, token), case: :lower)
  end

  defp expired_claim?(%ActionAttempt{lease_until: %DateTime{} = lease_until}, %DateTime{} = at) do
    not after?(lease_until, at)
  end

  defp expired_claim?(%ActionAttempt{}, _at), do: false

  defp after?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :gt
  end

  defp random_token(byte_count) do
    byte_count
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp normalize_queue(nil), do: "default"
  defp normalize_queue(queue) when is_binary(queue), do: queue
  defp normalize_queue(queue), do: to_string(queue)
end
