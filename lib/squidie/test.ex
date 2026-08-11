defmodule Squidie.Test do
  @moduledoc """
  Deterministic helpers for exercising Squidie workflows in tests.

  Each runtime owns isolated in-memory journal state and executes through the
  same public start, execution, and inspection paths used by a host. Execution
  is bounded by default, and a quiescent nonterminal run is reported as
  `{:blocked, snapshot}` without wall-clock polling.

  Stop runtimes explicitly with `stop_runtime/1`. Their storage process also
  monitors the process that created them, so ExUnit process exit cleans up
  abandoned runtimes.
  """

  import Kernel, except: [inspect: 2]

  alias Squidie.ReadModel.Explanation.Diagnostic
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.ReadModel.Timeline
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Test.Runtime
  alias Squidie.Test.Storage
  alias Squidie.Workflow.Definition

  @default_max_steps 100
  @control_options [:idempotency_key, :metadata]
  @runtime_options [:max_steps, :now, :partition, :queue, :workflow]
  @execution_options [:max_steps]
  @terminal_statuses [:cancelled, :completed, :continued, :failed]
  @time_units [:second, :millisecond, :microsecond]

  @type execution_result ::
          {:blocked, Snapshot.t()}
          | {:cancelled | :completed | :continued | :failed, Snapshot.t()}
          | {:error, term()}
  @type execute_until_result :: {:reached, Snapshot.t()} | execution_result()
  @type snapshot_predicate :: (Snapshot.t() -> boolean())
  @type time_unit :: :second | :millisecond | :microsecond

  @doc """
  Starts an isolated in-memory workflow runtime owned by the calling process.

  `:workflow` is required. `:queue`, `:partition`, `:now`, and `:max_steps`
  configure every helper call made through the returned runtime.
  """
  @spec start_runtime(keyword()) :: {:ok, Runtime.t()} | {:error, term()}
  def start_runtime(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @runtime_options),
         {:ok, workflow} <- workflow(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, partition} <- Options.partition_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, max_steps} <- max_steps(opts, @default_max_steps),
         {:ok, storage_server} <- Storage.start_link(self(), now) do
      storage = {Storage, server: storage_server}

      {:ok,
       %Runtime{
         id: Ecto.UUID.generate(),
         owner: self(),
         workflow: workflow,
         storage: storage,
         storage_server: storage_server,
         queue: queue,
         partition: partition,
         max_steps: max_steps
       }}
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  def start_runtime(_opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Stops a test runtime and discards all of its in-memory state.
  """
  @spec stop_runtime(Runtime.t()) :: :ok
  def stop_runtime(%Runtime{storage_server: storage_server}) do
    Storage.stop(storage_server)
  end

  @doc """
  Returns the runtime's current virtual time.
  """
  @spec now(Runtime.t()) :: {:ok, DateTime.t()} | {:error, :runtime_stopped}
  def now(%Runtime{storage_server: storage_server}) do
    Storage.now(storage_server)
  end

  @doc """
  Advances the runtime's virtual time without sleeping.

  Only the process that created the runtime may advance its clock. Supported
  units are `:second`, `:millisecond`, and `:microsecond`.
  """
  @spec advance_time(Runtime.t(), non_neg_integer(), time_unit()) ::
          {:ok, DateTime.t()}
          | {:error,
             :runtime_owner_required
             | :runtime_busy
             | :runtime_stopped
             | {:invalid_option, {:amount | :unit, :invalid}}}
  def advance_time(runtime, amount, unit \\ :millisecond)

  def advance_time(%Runtime{owner: owner}, _amount, _unit) when owner != self() do
    {:error, :runtime_owner_required}
  end

  def advance_time(%Runtime{}, amount, _unit) when not is_integer(amount) or amount < 0 do
    {:error, {:invalid_option, {:amount, :invalid}}}
  end

  def advance_time(%Runtime{}, _amount, unit) when unit not in @time_units do
    {:error, {:invalid_option, {:unit, :invalid}}}
  end

  def advance_time(%Runtime{storage_server: storage_server}, amount, unit) do
    Storage.advance_time(storage_server, amount, unit)
  end

  @doc """
  Deletes every projection checkpoint in the isolated runtime.

  Journal threads remain unchanged. The next inspection or execution rebuilds
  projections through the normal journal replay path. Only the runtime owner
  may delete checkpoints, and deletion fails while execution is active.
  """
  @spec delete_checkpoints(Runtime.t()) ::
          :ok | {:error, :runtime_busy | :runtime_owner_required | :runtime_stopped}
  def delete_checkpoints(%Runtime{storage_server: storage_server}) do
    Storage.delete_checkpoints(storage_server)
  end

  @doc """
  Starts the runtime's workflow through `Squidie.start/3`.

  Each runtime owns one root run. Create another runtime when a test needs an
  unrelated run so bounded execution cannot claim work outside the target's
  workflow tree. The process that created the runtime must start its root;
  another process receives `{:error, :runtime_owner_required}`.
  """
  @spec start(Runtime.t(), map()) :: {:ok, Snapshot.t()} | {:error, term()}
  def start(%Runtime{owner: owner} = runtime, payload) when is_map(payload) and owner == self() do
    with :ok <- Storage.reserve_start(runtime.storage_server) do
      start_root(runtime, payload)
    end
  end

  def start(%Runtime{}, payload) when is_map(payload) do
    {:error, :runtime_owner_required}
  end

  def start(%Runtime{}, _payload) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @doc """
  Inspects a run through the runtime's isolated journal.
  """
  @spec inspect(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def inspect(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Squidie.inspect_run(run_id(run), opts)
    end
  end

  @doc """
  Returns the run's chronological, redaction-safe operator timeline.
  """
  @spec timeline(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Timeline.t()} | {:error, term()}
  def timeline(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Squidie.inspect_run_timeline(run_id(run), opts)
    end
  end

  @doc """
  Returns the run's structured operator explanation.
  """
  @spec explain(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Diagnostic.t()} | {:error, term()}
  def explain(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Squidie.explain_run(run_id(run), opts)
    end
  end

  @doc """
  Cancels the runtime's root run through `Squidie.cancel/2`.

  The optional `:idempotency_key` and `:metadata` values are forwarded to the
  durable command signal. Runtime routing and occurrence time always come from
  the isolated test runtime.
  """
  @spec cancel(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def cancel(runtime, run, opts \\ []) do
    run_control(runtime, run, opts, &Squidie.cancel/2)
  end

  @doc """
  Resumes the runtime's paused root run through `Squidie.resume/3`.
  """
  @spec resume(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def resume(runtime, run) do
    resume(runtime, run, %{}, [])
  end

  @doc """
  Resumes a paused run with either command options or manual action attributes.
  """
  @spec resume(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def resume(runtime, run, opts) when is_list(opts) do
    resume(runtime, run, %{}, opts)
  end

  @spec resume(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def resume(runtime, run, attrs) when is_map(attrs) do
    resume(runtime, run, attrs, [])
  end

  def resume(%Runtime{}, _run, _attrs) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Resumes a paused run with manual action attributes and command options.
  """
  @spec resume(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def resume(%Runtime{} = runtime, run, attrs, opts) when is_map(attrs) do
    run_control(runtime, run, opts, fn run_id, control_opts ->
      Squidie.resume(run_id, attrs, control_opts)
    end)
  end

  def resume(%Runtime{}, _run, _attrs, _opts) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Approves the runtime's paused approval step through `Squidie.approve/3`.
  """
  @spec approve(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def approve(runtime, run, attrs, opts \\ [])

  def approve(%Runtime{} = runtime, run, attrs, opts) when is_map(attrs) do
    run_control(runtime, run, opts, fn run_id, control_opts ->
      Squidie.approve(run_id, attrs, control_opts)
    end)
  end

  def approve(%Runtime{}, _run, _attrs, _opts) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Rejects the runtime's paused approval step through `Squidie.reject/3`.
  """
  @spec reject(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def reject(runtime, run, attrs, opts \\ [])

  def reject(%Runtime{} = runtime, run, attrs, opts) when is_map(attrs) do
    run_control(runtime, run, opts, fn run_id, control_opts ->
      Squidie.reject(run_id, attrs, control_opts)
    end)
  end

  def reject(%Runtime{}, _run, _attrs, _opts) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Executes until a durable snapshot satisfies `predicate`.

  The predicate runs after each inspection and before terminal, blocked, or
  execution-limit classification. A match returns `{:reached, snapshot}`;
  otherwise the helper preserves the same results as `execute_until_blocked/3`.
  """
  @spec execute_until(
          Runtime.t(),
          Ecto.UUID.t() | Snapshot.t(),
          snapshot_predicate(),
          keyword()
        ) :: execute_until_result()
  def execute_until(runtime, run, predicate, opts \\ [])

  def execute_until(%Runtime{} = runtime, run, predicate, opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- validate_predicate(predicate),
         :ok <- reject_unknown_options(opts, @execution_options),
         {:ok, limit} <- max_steps(opts, runtime.max_steps),
         {:ok, target_run_id} <- target_run_id(runtime, run) do
      execute_with_lease(runtime, target_run_id, limit, predicate)
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  def execute_until(%Runtime{}, _run, _predicate, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Executes until the target run is terminal or no work is currently eligible.

  Returns a terminal-status tuple or `{:blocked, snapshot}`. The optional
  `:max_steps` bound defaults to the runtime's configured bound.
  """
  @spec execute_until_blocked(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), keyword()) ::
          execution_result()
  def execute_until_blocked(runtime, run, opts \\ [])

  def execute_until_blocked(%Runtime{} = runtime, run, opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @execution_options),
         {:ok, limit} <- max_steps(opts, runtime.max_steps),
         {:ok, target_run_id} <- target_run_id(runtime, run) do
      execute_with_lease(runtime, target_run_id, limit, &never_reached?/1)
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  def execute_until_blocked(%Runtime{}, _run, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Drains eligible work for the target run using the same bounded semantics as
  `execute_until_blocked/3`.
  """
  @spec drain(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), keyword()) :: execution_result()
  def drain(runtime, run, opts \\ []) do
    execute_until_blocked(runtime, run, opts)
  end

  defp execute(runtime, run_id, remaining, limit, now, predicate) do
    with {:ok, snapshot} <- inspect_at(runtime, run_id, now) do
      cond do
        predicate.(snapshot) ->
          {:reached, snapshot}

        snapshot.status in @terminal_statuses ->
          {snapshot.status, snapshot}

        remaining == 0 ->
          {:error,
           {:execution_limit_reached, %{limit: limit, run_id: run_id, snapshot: snapshot}}}

        true ->
          execute_next(runtime, run_id, remaining, limit, now, predicate)
      end
    end
  end

  defp execute_with_lease(runtime, run_id, limit, predicate) do
    with_execution_lease(runtime, fn now ->
      execute(runtime, run_id, limit, limit, now, predicate)
    end)
  end

  defp with_execution_lease(runtime, operation) do
    case Storage.begin_execution(runtime.storage_server) do
      {:ok, now, lease_ref} ->
        try do
          operation.(now)
        after
          _result = Storage.end_execution(runtime.storage_server, lease_ref)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp finish_start({:ok, %Snapshot{run_id: run_id}} = result, runtime) do
    with :ok <- Storage.commit_start(runtime.storage_server, run_id) do
      result
    end
  end

  defp finish_start(
         {:error, {:journal_start_committed, run_id, _reason}} = error,
         runtime
       ) do
    with :ok <- Storage.commit_start(runtime.storage_server, run_id) do
      error
    end
  end

  defp finish_start({:error, _reason} = error, runtime) do
    _result = Storage.release_start(runtime.storage_server)
    error
  end

  defp start_root(runtime, payload) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      runtime.workflow
      |> Squidie.start(payload, opts)
      |> finish_start(runtime)
    end
  catch
    kind, reason ->
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp execute_next(runtime, run_id, remaining, limit, now, predicate) do
    opts =
      runtime
      |> runtime_options(now)
      |> Keyword.put(:owner_id, runtime.id)

    execute_next_result(
      Squidie.execute_next(opts),
      runtime,
      run_id,
      remaining,
      limit,
      now,
      predicate
    )
  end

  defp execute_next_result(result, runtime, run_id, remaining, limit, now, predicate) do
    case result do
      {:ok, :none} ->
        with {:ok, snapshot} <- inspect_at(runtime, run_id, now) do
          classify_final_snapshot(snapshot, predicate)
        end

      {:ok, _snapshot} ->
        execute(runtime, run_id, remaining - 1, limit, now, predicate)

      {:error, _reason} = error ->
        error
    end
  end

  defp inspect_at(runtime, run, now) do
    Squidie.inspect_run(run_id(run), runtime_options(runtime, now))
  end

  defp common_runtime_options(%Runtime{} = runtime) do
    with {:ok, now} <- now(runtime) do
      {:ok, runtime_options(runtime, now)}
    end
  end

  defp classify_final_snapshot(snapshot, predicate) do
    cond do
      predicate.(snapshot) -> {:reached, snapshot}
      snapshot.status in @terminal_statuses -> {snapshot.status, snapshot}
      true -> {:blocked, snapshot}
    end
  end

  defp runtime_options(%Runtime{} = runtime, %DateTime{} = now) do
    [
      runtime: :journal,
      journal_storage: runtime.storage,
      queue: runtime.queue,
      partition: runtime.partition,
      now: now
    ]
  end

  defp workflow(opts) do
    case Keyword.get(opts, :workflow) do
      workflow when is_atom(workflow) ->
        case Definition.load(workflow) do
          {:ok, _definition} -> {:ok, workflow}
          {:error, _reason} -> {:error, {:invalid_option, {:workflow, :invalid}}}
        end

      _invalid ->
        {:error, {:invalid_option, {:workflow, :invalid}}}
    end
  end

  defp max_steps(opts, default) do
    case Keyword.get(opts, :max_steps, default) do
      max_steps when is_integer(max_steps) and max_steps > 0 -> {:ok, max_steps}
      _invalid -> {:error, {:invalid_option, {:max_steps, :invalid}}}
    end
  end

  defp never_reached?(_snapshot) do
    false
  end

  defp reject_unknown_options(opts, allowed) do
    case Keyword.keys(opts) -- allowed do
      [] -> :ok
      [option | _rest] -> {:error, {:invalid_option, {:option, option}}}
    end
  end

  defp run_control(%Runtime{} = runtime, run, opts, operation) do
    with :ok <- validate_control_options(opts),
         {:ok, target_run_id} <- target_run_id(runtime, run) do
      with_execution_lease(runtime, fn now ->
        operation.(target_run_id, control_runtime_options(runtime, now, opts))
      end)
    end
  end

  defp control_runtime_options(runtime, now, command_opts) do
    Keyword.merge(runtime_options(runtime, now), command_opts)
  end

  defp target_run_id(runtime, run) do
    target_run_id = run_id(run)

    case Storage.root_run_id(runtime.storage_server) do
      {:ok, ^target_run_id} -> {:ok, target_run_id}
      {:ok, _other_run_id} -> {:error, :run_outside_runtime}
      {:error, _reason} = error -> error
    end
  end

  defp validate_predicate(predicate) when is_function(predicate, 1) do
    :ok
  end

  defp validate_predicate(_predicate) do
    {:error, {:invalid_option, {:predicate, :invalid}}}
  end

  defp validate_control_options(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @control_options) do
      :ok
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  defp validate_control_options(_opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp run_id(%Snapshot{run_id: run_id}) do
    run_id
  end

  defp run_id(run_id) do
    run_id
  end
end
