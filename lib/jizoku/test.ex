defmodule Jizoku.Test do
  @moduledoc """
  Deterministic helpers for exercising Jizoku workflows in tests.

  Each runtime owns isolated in-memory journal state and executes through the
  same public start, execution, and inspection paths used by a host. Execution
  is bounded by default, and a quiescent nonterminal run is reported as
  `{:blocked, snapshot}` without wall-clock polling.

  Stop runtimes explicitly with `stop_runtime/1`. Their storage process also
  monitors the process that created them, so ExUnit process exit cleans up
  abandoned runtimes.
  """

  import Kernel, except: [inspect: 2]

  alias Jizoku.ReadModel.Explanation.Diagnostic
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.ReadModel.Timeline
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.ScheduleIdentity
  alias Jizoku.Runtime.Signal
  alias Jizoku.Test.Assertions
  alias Jizoku.Test.GoldenHistory
  alias Jizoku.Test.Invariants
  alias Jizoku.Test.Runtime
  alias Jizoku.Test.Storage
  alias Jizoku.Test.StubAction
  alias Jizoku.Workflow.Definition

  @default_max_steps 100
  @control_options [:idempotency_key, :metadata]
  @cron_options [:idempotency_key, :metadata]
  @runtime_options [
    :action_registry,
    :action_stubs,
    :guardrail_registry,
    :jido_dispatch_routes,
    :max_steps,
    :now,
    :partition,
    :queue,
    :workflow
  ]
  @execution_options [:max_steps]
  @assertion_options [:diagnostics, :max_steps]
  @terminal_statuses [:cancelled, :completed, :continued, :failed]
  @time_units [:second, :millisecond, :microsecond]

  @type execution_result ::
          {:blocked, Snapshot.t()}
          | {:cancelled | :completed | :continued | :failed, Snapshot.t()}
          | {:error, term()}
  @type execute_until_result :: {:reached, Snapshot.t()} | execution_result()
  @type snapshot_predicate :: (Snapshot.t() -> boolean())
  @type append_target :: :run | :dispatch
  @type assertion_diagnostics :: :none | :timeline
  @type invariant_violation_code ::
          :duplicate_runnable_key
          | :malformed_runnable_key
          | :pending_and_applied
          | :pending_in_multiple_views
          | :projection_anomaly
          | :terminal_state_incoherent
          | :unknown_runnable
  @type invariant_violation :: %{
          required(:code) => invariant_violation_code(),
          required(:details) => map()
        }
  @type invariant_report :: %{
          required(:version) => pos_integer(),
          required(:run_id) => String.t(),
          required(:partition) => String.t() | nil,
          required(:queue) => String.t(),
          required(:thread_revisions) => %{run: non_neg_integer(), dispatch: non_neg_integer()},
          required(:violations) => [invariant_violation()]
        }
  @type golden_history_event :: %{
          required(:type) => atom(),
          required(:offset_us) => integer() | nil,
          required(:run) => String.t() | :malformed,
          optional(:step) => String.t(),
          optional(:runnable) => String.t() | :malformed,
          optional(:status) => atom(),
          optional(:details) => map()
        }
  @type golden_history :: %{
          required(:schema_version) => pos_integer(),
          required(:workflow) => String.t() | nil,
          required(:queue) => String.t(),
          required(:partition) => String.t() | nil,
          required(:status) => atom(),
          required(:terminal_status) => atom() | nil,
          required(:events) => [golden_history_event()]
        }
  @type time_unit :: :second | :millisecond | :microsecond

  @doc """
  Starts an isolated in-memory workflow runtime owned by the calling process.

  `:workflow` is required and may be a compiled workflow module or a
  runtime-authored workflow spec. Runtime specs may use a host-owned
  `:action_registry`, deterministic `:action_stubs`, and a
  `:guardrail_registry`. `:queue`, `:partition`, `:now`, and `:max_steps`
  configure every helper call made through the returned runtime.
  """
  @spec start_runtime(keyword()) :: {:ok, Runtime.t()} | {:error, term()}
  def start_runtime(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @runtime_options),
         {:ok, workflow} <- workflow(opts),
         {:ok, action_registry} <- action_registry(opts),
         {:ok, action_stubs} <- action_stubs(opts),
         :ok <- validate_action_stub_workflow(workflow, action_stubs),
         :ok <- validate_action_stub_keys(action_registry, action_stubs),
         {:ok, guardrail_registry} <- guardrail_registry(opts),
         {:ok, jido_dispatch_routes} <-
           Jizoku.Runtime.Jido.OutboxDelivery.routes(Keyword.get(opts, :jido_dispatch_routes)),
         :ok <-
           validate_test_workflow(
             workflow,
             validation_action_registry(action_registry, action_stubs),
             guardrail_registry
           ),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, partition} <- Options.partition_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, max_steps} <- max_steps(opts, @default_max_steps),
         {:ok, storage_server} <- Storage.start_link(self(), now, action_stubs) do
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
         max_steps: max_steps,
         action_registry: action_registry,
         action_stub_keys: Map.keys(action_stubs),
         guardrail_registry: guardrail_registry,
         jido_dispatch_routes: jido_dispatch_routes
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
  Restarts the isolated runtime while preserving its durable state and clock.

  The returned runtime has a fresh worker identity and storage process. The old
  runtime handle is stopped. Restart is owner-only and fails while a live
  execution or start reservation is active. Armed deterministic append faults
  remain armed across the restart.
  """
  @spec restart_runtime(Runtime.t()) ::
          {:ok, Runtime.t()}
          | {:error, :runtime_busy | :runtime_owner_required | :runtime_stopped | term()}
  def restart_runtime(%Runtime{} = runtime) do
    with {:ok, storage_server} <- Storage.restart(runtime.storage_server) do
      {:ok,
       %Runtime{
         runtime
         | id: Ecto.UUID.generate(),
           storage: {Storage, server: storage_server},
           storage_server: storage_server
       }}
    end
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
  Injects one expected-revision conflict at the next matching journal append.

  The target is the runtime's root run thread or configured dispatch thread.
  The conflict is consumed only by that exact partitioned thread, and the
  runtime owner must inject it while no execution or control helper is active.
  """
  @spec inject_append_conflict(Runtime.t(), append_target()) ::
          :ok
          | {:error,
             :append_conflict_already_armed
             | :run_not_started
             | :runtime_busy
             | :runtime_owner_required
             | :runtime_stopped
             | {:invalid_option, {:target, :invalid}}}
  def inject_append_conflict(%Runtime{}, target) when target not in [:run, :dispatch] do
    {:error, {:invalid_option, {:target, :invalid}}}
  end

  def inject_append_conflict(%Runtime{owner: owner}, _target) when owner != self() do
    {:error, :runtime_owner_required}
  end

  def inject_append_conflict(%Runtime{} = runtime, target) do
    with {:ok, root_run_id} <- Storage.root_run_id(runtime.storage_server) do
      thread_id = append_target_thread_id(runtime, root_run_id, target)
      Storage.inject_append_conflict(runtime.storage_server, thread_id)
    end
  end

  @doc """
  Returns calls recorded by a configured deterministic action stub.

  Calls are ordered by execution and include the application input plus durable
  run, runnable, step, and attempt identity. This is explicit test data and is
  not a redacted diagnostic surface.
  """
  @spec stub_calls(Runtime.t(), Jizoku.Workflow.ActionRegistry.action_key()) ::
          {:ok, [map()]}
          | {:error, :run_not_started | :runtime_stopped | :unknown_action_stub}
  def stub_calls(%Runtime{storage_server: storage_server}, action_key) do
    Storage.action_stub_calls(storage_server, action_key)
  end

  @doc """
  Starts the runtime's workflow through `Jizoku.start/3`.

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
  Starts the runtime's workflow through a declared cron trigger.

  Schedule receipt time, partition, queue, and storage come from the isolated
  runtime. Callers may provide signal `:metadata` or an explicit
  `:idempotency_key`; scheduler identity may also be carried in `input` through
  `signal_id` or a complete `intended_window`.

  Each test runtime still owns one root run, regardless of whether it was
  started manually or through cron.
  """
  @spec start_cron(Runtime.t(), atom() | String.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def start_cron(runtime, trigger, input, opts \\ [])

  def start_cron(%Runtime{}, _trigger, input, _opts) when not is_map(input) do
    {:error, {:invalid_payload, :expected_map}}
  end

  def start_cron(%Runtime{}, _trigger, _input, opts) when not is_list(opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  def start_cron(%Runtime{owner: owner}, _trigger, _input, _opts) when owner != self() do
    {:error, :runtime_owner_required}
  end

  def start_cron(%Runtime{workflow: workflow}, _trigger, _input, _opts)
      when not is_atom(workflow) do
    {:error, {:invalid_option, {:workflow, :cron_requires_module}}}
  end

  def start_cron(%Runtime{} = runtime, trigger, input, opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @cron_options),
         {:ok, runtime_opts} <- common_runtime_options(runtime),
         {:ok, signal} <-
           Signal.start_cron(
             runtime.workflow,
             trigger,
             input,
             cron_signal_options(runtime, runtime_opts, opts)
           ) do
      start_cron_signal(runtime, signal, runtime_opts)
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Inspects a run through the runtime's isolated journal.
  """
  @spec inspect(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def inspect(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Jizoku.inspect_run(run_id(run), opts)
    end
  end

  @doc """
  Returns the run's chronological, redaction-safe operator timeline.
  """
  @spec timeline(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Timeline.t()} | {:error, term()}
  def timeline(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Jizoku.inspect_run_timeline(run_id(run), opts)
    end
  end

  @doc """
  Returns a versioned, redacted golden history for compatibility assertions.

  Generated run and runnable identifiers are replaced with encounter-order
  aliases, timestamps become offsets from run start, and event details use a
  strict structural allowlist.
  """
  @spec golden_history(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, golden_history()} | {:error, term()}
  def golden_history(%Runtime{} = runtime, run) do
    with {:ok, timeline} <- timeline(runtime, run) do
      {:ok, GoldenHistory.from_timeline(timeline)}
    end
  end

  @doc """
  Drains a run and returns its snapshot when `expected_status` matches.

  A mismatch raises `ExUnit.AssertionError`. Failures are concise by default;
  pass `diagnostics: :timeline` to include the versioned, redacted golden
  history in ExUnit output. `:max_steps` has the same meaning as in `drain/3`.
  """
  @spec assert_status(
          Runtime.t(),
          Ecto.UUID.t() | Snapshot.t(),
          atom(),
          keyword()
        ) :: Snapshot.t() | no_return()
  def assert_status(runtime, run, expected_status, opts \\ [])

  def assert_status(%Runtime{} = runtime, run, expected_status, opts)
      when is_atom(expected_status) and is_list(opts) do
    case assertion_options(runtime, opts) do
      {:ok, diagnostics, drain_opts} ->
        result = drain(runtime, run, drain_opts)
        snapshot = assertion_snapshot(result)

        if match?(%Snapshot{status: ^expected_status}, snapshot) do
          snapshot
        else
          golden = assertion_golden(snapshot, diagnostics)
          Assertions.raise_status_failure(expected_status, result, snapshot, golden)
        end

      {:error, reason} ->
        raise ArgumentError, message: assertion_argument_error(reason)
    end
  end

  def assert_status(%Runtime{}, _run, expected_status, _opts) when not is_atom(expected_status) do
    raise ArgumentError, message: "expected status must be an atom"
  end

  def assert_status(%Runtime{}, _run, _expected_status, _opts) do
    raise ArgumentError, message: "assertion options must be a keyword list"
  end

  @doc """
  Returns the run's structured operator explanation.
  """
  @spec explain(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Diagnostic.t()} | {:error, term()}
  def explain(%Runtime{} = runtime, run) do
    with {:ok, opts} <- common_runtime_options(runtime) do
      Jizoku.explain_run(run_id(run), opts)
    end
  end

  @doc """
  Checks universal workflow invariants against one durable inspection snapshot.

  A successful check returns the inspected snapshot. Failures contain
  redaction-safe structural metadata such as stable identifiers, counts,
  classifications, and violation codes; workflow inputs, outputs, context, and
  raw errors are excluded.
  """
  @spec check_invariants(Runtime.t(), Ecto.UUID.t() | Snapshot.t()) ::
          {:ok, Snapshot.t()}
          | {:error, {:invariant_violations, invariant_report()}}
          | {:error, term()}
  def check_invariants(%Runtime{} = runtime, run) do
    case inspect(runtime, run) do
      {:ok, snapshot} -> check_snapshot_invariants(snapshot)
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Cancels the runtime's root run through `Jizoku.cancel/2`.

  The optional `:idempotency_key` and `:metadata` values are forwarded to the
  durable command signal. Runtime routing and occurrence time always come from
  the isolated test runtime.
  """
  @spec cancel(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def cancel(runtime, run, opts \\ []) do
    run_control(runtime, run, opts, &Jizoku.cancel/2)
  end

  @doc """
  Resumes the runtime's paused root run through `Jizoku.resume/3`.
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
      Jizoku.resume(run_id, attrs, control_opts)
    end)
  end

  def resume(%Runtime{}, _run, _attrs, _opts) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Approves the runtime's paused approval step through `Jizoku.approve/3`.
  """
  @spec approve(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def approve(runtime, run, attrs, opts \\ [])

  def approve(%Runtime{} = runtime, run, attrs, opts) when is_map(attrs) do
    run_control(runtime, run, opts, fn run_id, control_opts ->
      Jizoku.approve(run_id, attrs, control_opts)
    end)
  end

  def approve(%Runtime{}, _run, _attrs, _opts) do
    {:error, {:invalid_attributes, :expected_map}}
  end

  @doc """
  Rejects the runtime's paused approval step through `Jizoku.reject/3`.
  """
  @spec reject(Runtime.t(), Ecto.UUID.t() | Snapshot.t(), map(), keyword()) ::
          {:ok, Snapshot.t()} | {:error, term()}
  def reject(runtime, run, attrs, opts \\ [])

  def reject(%Runtime{} = runtime, run, attrs, opts) when is_map(attrs) do
    run_control(runtime, run, opts, fn run_id, control_opts ->
      Jizoku.reject(run_id, attrs, control_opts)
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
      result =
        case runtime.workflow do
          workflow when is_atom(workflow) -> Jizoku.start(workflow, payload, opts)
          spec when is_map(spec) -> Jizoku.start_spec(spec, payload, opts)
        end

      finish_start(result, runtime)
    end
  catch
    kind, reason ->
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp start_cron_root(runtime, signal, runtime_opts) do
    signal
    |> Jizoku.apply_signal(runtime_opts)
    |> finish_start(runtime)
  catch
    kind, reason ->
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp start_cron_signal(runtime, signal, runtime_opts) do
    case Storage.root_run_id(runtime.storage_server) do
      {:error, :run_not_started} ->
        with :ok <- Storage.reserve_start(runtime.storage_server) do
          start_cron_root(runtime, signal, runtime_opts)
        end

      {:ok, root_run_id} ->
        if matching_cron_run_id?(signal, root_run_id) do
          Jizoku.apply_signal(signal, runtime_opts)
        else
          {:error, :runtime_already_started}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp matching_cron_run_id?(
         %Signal{
           idempotency_key: idempotency_key,
           payload: %{workflow: workflow, trigger: trigger}
         },
         root_run_id
       )
       when is_binary(idempotency_key) and is_binary(workflow) and is_binary(trigger) do
    case ScheduleIdentity.run_id(workflow, trigger, idempotency_key) do
      {:ok, ^root_run_id} -> true
      {:ok, _other_run_id} -> false
      {:error, _reason} -> false
    end
  end

  defp matching_cron_run_id?(%Signal{}, _root_run_id) do
    false
  end

  defp cron_signal_options(runtime, runtime_opts, opts) do
    opts
    |> Keyword.put(:occurred_at, Keyword.fetch!(runtime_opts, :now))
    |> Keyword.put(:partition, runtime.partition)
  end

  defp execute_next(runtime, run_id, remaining, limit, now, predicate) do
    opts =
      runtime
      |> runtime_options(now)
      |> Keyword.put(:owner_id, runtime.id)

    execute_next_result(
      Jizoku.execute_next(opts),
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
    Jizoku.inspect_run(run_id(run), runtime_options(runtime, now))
  end

  defp common_runtime_options(%Runtime{} = runtime) do
    with {:ok, now} <- now(runtime) do
      {:ok, runtime_options(runtime, now)}
    end
  end

  defp check_snapshot_invariants(%Snapshot{} = snapshot) do
    case Invariants.check(snapshot) do
      :ok -> {:ok, snapshot}
      {:error, {:invariant_violations, _report}} = error -> error
    end
  end

  defp assertion_options(runtime, opts) do
    with true <- Keyword.keyword?(opts),
         :ok <- reject_unknown_options(opts, @assertion_options),
         {:ok, diagnostics} <- assertion_diagnostics(opts),
         {:ok, _max_steps} <- max_steps(opts, runtime.max_steps) do
      {:ok, diagnostics, Keyword.take(opts, @execution_options)}
    else
      false -> {:error, {:invalid_option, {:opts, :invalid}}}
      {:error, _reason} = error -> error
    end
  end

  defp assertion_diagnostics(opts) do
    case Keyword.get(opts, :diagnostics, :none) do
      diagnostics when diagnostics in [:none, :timeline] -> {:ok, diagnostics}
      _invalid -> {:error, {:invalid_option, {:diagnostics, :invalid}}}
    end
  end

  defp assertion_snapshot({_outcome, %Snapshot{} = snapshot}) do
    snapshot
  end

  defp assertion_snapshot(
         {:error, {:execution_limit_reached, %{snapshot: %Snapshot{} = snapshot}}}
       ) do
    snapshot
  end

  defp assertion_snapshot(_result) do
    nil
  end

  defp assertion_golden(_snapshot, :none) do
    nil
  end

  defp assertion_golden(%Snapshot{} = snapshot, :timeline) do
    {:ok, timeline} = Inspection.timeline(snapshot)
    GoldenHistory.from_timeline(timeline)
  end

  defp assertion_golden(nil, :timeline) do
    :unavailable
  end

  defp assertion_argument_error({:invalid_option, {:diagnostics, :invalid}}) do
    "diagnostics must be :none or :timeline"
  end

  defp assertion_argument_error({:invalid_option, {:max_steps, :invalid}}) do
    "max_steps must be a positive integer"
  end

  defp assertion_argument_error({:invalid_option, {:option, option}}) do
    "unsupported assertion option: #{inspect(option)}"
  end

  defp assertion_argument_error(_reason) do
    "assertion options must be a keyword list"
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
    |> maybe_put_action_registry(runtime)
    |> maybe_put_guardrail_registry(runtime)
    |> maybe_put_jido_dispatch_routes(runtime)
  end

  defp maybe_put_jido_dispatch_routes(opts, %Runtime{jido_dispatch_routes: nil}) do
    opts
  end

  defp maybe_put_jido_dispatch_routes(opts, %Runtime{jido_dispatch_routes: routes}) do
    Keyword.put(opts, :jido_dispatch_routes, routes)
  end

  defp append_target_thread_id(runtime, root_run_id, :run) do
    Journal.thread_id({:run, root_run_id}, runtime.partition)
  end

  defp append_target_thread_id(runtime, _root_run_id, :dispatch) do
    Journal.thread_id({:dispatch, runtime.queue}, runtime.partition)
  end

  defp workflow(opts) do
    case Keyword.get(opts, :workflow) do
      workflow when is_atom(workflow) ->
        case Definition.load(workflow) do
          {:ok, _definition} -> {:ok, workflow}
          {:error, _reason} -> {:error, {:invalid_option, {:workflow, :invalid}}}
        end

      workflow when is_map(workflow) ->
        {:ok, workflow}

      _invalid ->
        {:error, {:invalid_option, {:workflow, :invalid}}}
    end
  end

  defp action_registry(opts) do
    case Keyword.get(opts, :action_registry, %{}) do
      registry when is_map(registry) -> validate_action_registry(registry)
      registry when is_list(registry) -> normalize_keyword_action_registry(registry)
      _invalid -> {:error, {:invalid_option, {:action_registry, :invalid}}}
    end
  end

  defp normalize_keyword_action_registry(registry) do
    if Keyword.keyword?(registry) and unique_registry_keys?(registry) do
      registry
      |> Map.new()
      |> validate_action_registry()
    else
      {:error, {:invalid_option, {:action_registry, :invalid}}}
    end
  end

  defp unique_registry_keys?(registry) do
    keys = Keyword.keys(registry)
    length(keys) == MapSet.size(MapSet.new(keys))
  end

  defp validate_action_registry(registry) do
    if Enum.all?(Map.keys(registry), &valid_action_stub_key?/1) do
      {:ok, registry}
    else
      {:error, {:invalid_option, {:action_registry, :invalid}}}
    end
  end

  defp action_stubs(opts) do
    case Keyword.get(opts, :action_stubs, %{}) do
      stubs when is_map(stubs) -> validate_action_stubs(stubs)
      _invalid -> {:error, {:invalid_option, {:action_stubs, :invalid}}}
    end
  end

  defp validate_action_stubs(stubs) do
    if Enum.all?(stubs, fn {key, results} ->
         valid_action_stub_key?(key) and is_list(results) and results != []
       end) do
      {:ok, stubs}
    else
      {:error, {:invalid_option, {:action_stubs, :invalid}}}
    end
  end

  defp valid_action_stub_key?(key) when is_atom(key) do
    true
  end

  defp valid_action_stub_key?(key) when is_binary(key) do
    String.trim(key) != ""
  end

  defp valid_action_stub_key?(_key) do
    false
  end

  defp validate_action_stub_workflow(workflow, action_stubs)
       when is_atom(workflow) and map_size(action_stubs) > 0 do
    {:error, {:invalid_option, {:action_stubs, :requires_runtime_spec}}}
  end

  defp validate_action_stub_workflow(_workflow, _action_stubs) do
    :ok
  end

  defp validate_action_stub_keys(action_registry, action_stubs) do
    registry_keys = Map.keys(action_registry)

    if Enum.any?(Map.keys(action_stubs), &equivalent_registry_key?(&1, registry_keys)) do
      {:error, {:invalid_option, {:action_stubs, :duplicate_action_key}}}
    else
      :ok
    end
  end

  defp equivalent_registry_key?(key, keys) do
    Enum.any?(keys, &(to_string(&1) == to_string(key)))
  end

  defp guardrail_registry(opts) do
    case Keyword.fetch(opts, :guardrail_registry) do
      {:ok, registry} when is_map(registry) or is_list(registry) -> {:ok, registry}
      {:ok, _invalid} -> {:error, {:invalid_option, {:guardrail_registry, :invalid}}}
      :error -> {:ok, nil}
    end
  end

  defp validate_test_workflow(workflow, action_registry, guardrail_registry)
       when is_map(workflow) do
    opts =
      maybe_put_option(
        [action_registry: action_registry],
        :guardrail_registry,
        guardrail_registry
      )

    Jizoku.Workflow.validate_spec(workflow, opts)
  end

  defp validate_test_workflow(_workflow, _action_registry, _guardrail_registry) do
    :ok
  end

  defp validation_action_registry(action_registry, action_stubs) do
    Map.merge(action_registry, stub_registry(Map.keys(action_stubs), nil, nil))
  end

  defp maybe_put_action_registry(opts, %Runtime{} = runtime) do
    registry =
      Map.merge(
        runtime.action_registry,
        stub_registry(
          runtime.action_stub_keys,
          runtime.storage_server,
          runtime.test_action_stub_after_consume
        )
      )

    if map_size(registry) == 0 do
      opts
    else
      Keyword.put(opts, :action_registry, registry)
    end
  end

  defp maybe_put_guardrail_registry(opts, %Runtime{guardrail_registry: nil}) do
    opts
  end

  defp maybe_put_guardrail_registry(opts, %Runtime{guardrail_registry: registry}) do
    Keyword.put(opts, :guardrail_registry, registry)
  end

  defp stub_registry(action_keys, storage_server, after_consume) do
    Map.new(action_keys, fn action_key ->
      action_opts =
        maybe_put_option(
          maybe_put_option(
            [action_key: action_key],
            :storage_server,
            storage_server
          ),
          :after_consume,
          after_consume
        )

      {action_key, [module: StubAction, action_opts: action_opts]}
    end)
  end

  defp maybe_put_option(opts, _key, nil) do
    opts
  end

  defp maybe_put_option(opts, key, value) do
    Keyword.put(opts, key, value)
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
