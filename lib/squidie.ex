# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie do
  @moduledoc """
  Public entrypoint for the Squidie runtime.

  The API exposed here stays focused on declarative workflow operations. Host
  applications start, inspect, and later control runs through this surface
  without needing to work directly with persistence internals.
  """

  alias Squidie.Config
  alias Squidie.ReadModel.Inspection
  alias Squidie.ReadModel.Listing
  alias Squidie.ReadModel.Timeline
  alias Squidie.Runs.DynamicWorkPreview
  alias Squidie.Runs.GraphInspection
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal.Cancellation
  alias Squidie.Runtime.Journal.ChildStarter
  alias Squidie.Runtime.Journal.DynamicWork
  alias Squidie.Runtime.Journal.EntryBuilder
  alias Squidie.Runtime.Journal.Executor
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.Replay
  alias Squidie.Runtime.Journal.SignalInterpreter
  alias Squidie.Runtime.Journal.Starter
  alias Squidie.Runtime.Journal.WorkflowDefinitionLoader
  alias Squidie.Runtime.ScheduleIdentity
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Workflow.ActionRegistry
  alias Squidie.Workflow.SpecPreview

  @read_models [:read_model]
  @runtimes [:journal]
  @dispatch_schedule_retries 25
  @projection_snapshot_options [:queue, :now]
  @projection_list_options [:queue, :now]
  @journal_start_options [:runtime, :journal_storage, :queue, :now, :run_id]
  @journal_spec_start_options [
    :runtime,
    :journal_storage,
    :queue,
    :now,
    :run_id,
    :action_registry
  ]
  @journal_child_start_options [
    :runtime,
    :journal_storage,
    :queue,
    :now,
    :child_key,
    :metadata
  ]
  @journal_control_options [:runtime, :journal_storage, :queue, :now]
  @journal_execute_options [
    :runtime,
    :journal_storage,
    :queue,
    :owner_id,
    :lease_for,
    :heartbeat_interval_ms,
    :now,
    :action_registry
  ]
  @journal_dynamic_work_options [
    :runtime,
    :read_model,
    :journal_storage,
    :queue,
    :now,
    :repo,
    :action_registry
  ]

  @typedoc """
  Structured validation errors returned by the public read-model APIs.
  """
  @type read_option_error ::
          {:invalid_option,
           {:opts, term()}
           | {:option, term()}
           | {:read_model, term()}
           | {:journal_storage, nil}
           | {:run_id, term()}}

  @typedoc """
  Structured validation errors returned by the public start APIs.
  """
  @type start_option_error ::
          {:invalid_option,
           {:opts, term()}
           | {:runtime, term()}
           | {:journal_storage, nil}
           | {:queue, term()}
           | {:now, term()}
           | {:run_id, term()}
           | {:schedule_idempotency_key, term()}}

  @doc """
  Loads Squidie configuration from the application environment with optional
  runtime overrides.
  """
  @spec config(keyword()) :: {:ok, Config.t()} | {:error, Config.config_error()}
  defdelegate config(overrides \\ []), to: Config, as: :load

  @doc """
  Loads Squidie configuration and raises if required keys are missing.
  """
  @spec config!(keyword()) :: Config.t()
  defdelegate config!(overrides \\ []), to: Config, as: :load!

  @doc """
  Starts a new workflow run through the workflow's default trigger.

  Public manual starts load journal runtime defaults from `config :squidie`.
  When `journal_storage:` is not passed explicitly, Squidie infers Ecto-backed
  journal storage from the configured `:repo`, so host apps must either set
  `config :squidie, repo: MyApp.Repo` globally or pass an explicit
  `journal_storage:` override from their runtime boundary.
  """
  @spec start(module(), map()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start(workflow, payload) when is_map(payload), do: start(workflow, payload, [])

  @doc """
  Starts a new workflow run through the workflow's default trigger with runtime
  overrides, or through a named trigger without runtime overrides.
  """
  @spec start(module(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom()}}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start(workflow, payload, overrides) when is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_default_run_with_runtime(:journal, workflow, payload, overrides)
    end
  end

  def start(_workflow, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @spec start(module(), atom(), map()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start(workflow, trigger_name, payload)
      when is_atom(trigger_name) and is_map(payload),
      do: start(workflow, trigger_name, payload, [])

  @doc """
  Starts a named trigger while applying runtime configuration overrides.

  Overrides are intended for host-app test and integration boundaries. Runtime
  context injection is kept out of this public API so scheduled starts and other
  internal callers can keep their idempotency metadata isolated.

  The Jido journal runtime is the default and infers Ecto-backed journal storage
  from the configured repo. Pass `journal_storage:` only when a host, test, or
  integration boundary needs a non-default storage adapter. Journal execution
  supports normal action steps, immediate built-in `:log` steps, built-in
  `:wait` steps in transition and dependency workflows, and manual `:pause` or
  `:approval` boundaries.

  Public manual starts still resolve that default storage through
  `config :squidie`, so host apps must keep `:repo` configured globally unless
  they pass an explicit `journal_storage:` override.
  """
  @spec start(module(), atom(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom()}}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start(workflow, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_triggered_run_with_runtime(:journal, workflow, trigger_name, payload, overrides)
    end
  end

  @doc """
  Starts a runtime-authored workflow spec through its default trigger.

  Runtime-authored specs should be validated with a host-owned action registry
  before activation. Passing `:action_registry` lets Squidie resolve stable
  action keys to approved executable modules at the start boundary.
  """
  @spec start_spec(Squidie.Workflow.Spec.t() | map(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_spec(spec, payload, overrides \\ [])

  def start_spec(spec, payload, overrides) when is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_spec_run_with_runtime(:journal, spec, nil, payload, overrides)
    end
  end

  def start_spec(_spec, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @doc """
  Starts a runtime-authored workflow spec through a named trigger.
  """
  @spec start_spec(Squidie.Workflow.Spec.t() | map(), atom(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_spec(spec, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_spec_run_with_runtime(:journal, spec, trigger_name, payload, overrides)
    end
  end

  def start_spec(_spec, trigger_name, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_trigger, trigger_name}}
  end

  @doc """
  Previews a runtime-authored workflow spec with a sample payload.

  Preview execution resolves runtime-authored action keys through the host-owned
  action registry and calls only actions that explicitly opt into dry-run
  behavior. It does not create a run, append journal state, schedule dispatch
  attempts, or call durable action `run/2` callbacks.
  """
  @spec preview_spec(Squidie.Workflow.Spec.t() | map(), map(), keyword()) ::
          {:ok, Squidie.Runs.SpecPreview.t()} | {:error, term()}
  def preview_spec(spec, payload, overrides \\ [])

  def preview_spec(spec, payload, overrides) when is_map(payload) and is_list(overrides) do
    SpecPreview.preview(spec, nil, payload, overrides)
  end

  def preview_spec(_spec, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @doc """
  Previews a runtime-authored workflow spec through a named trigger.
  """
  @spec preview_spec(Squidie.Workflow.Spec.t() | map(), atom(), map(), keyword()) ::
          {:ok, Squidie.Runs.SpecPreview.t()} | {:error, term()}
  def preview_spec(spec, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    SpecPreview.preview(spec, trigger_name, payload, overrides)
  end

  def preview_spec(_spec, trigger_name, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_trigger, trigger_name}}
  end

  @doc false
  @spec start_run_with_initial_context(module(), atom(), map(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, Squidie.ReadModel.Inspection.Snapshot.t()}}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run_with_initial_context(workflow, trigger_name, payload, initial_context, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_map(initial_context) and
             is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_initial_context_run_with_runtime(
        :journal,
        workflow,
        trigger_name,
        payload,
        initial_context,
        overrides
      )
    end
  end

  @doc """
  Starts a child workflow run from a native step context.

  Child starts are deterministic for the parent run, parent step, child
  workflow, child trigger, and required `:child_key`. Duplicate calls with the
  same key return the existing child run and do not duplicate parent lineage.
  """
  @spec start_child_run(Squidie.Step.Context.t(), module(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom() | term()}}
          | {:error, ChildStarter.start_error()}
  def start_child_run(parent_context, child_workflow, payload, overrides \\ [])

  def start_child_run(parent_context, child_workflow, payload, overrides)
      when is_map(payload) and is_list(overrides) do
    with :ok <- public_child_start_options(overrides),
         {:ok, :journal} <- runtime(overrides),
         {:ok, definition} <- Squidie.Workflow.Definition.load(child_workflow) do
      child_trigger = Squidie.Workflow.Definition.default_trigger(definition)

      ChildStarter.start_child_run(
        parent_context,
        child_workflow,
        child_trigger,
        payload,
        journal_child_start_options(overrides)
      )
    end
  end

  def start_child_run(_parent_context, _child_workflow, _payload, overrides)
      when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  def start_child_run(_parent_context, _child_workflow, _payload, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @spec start_child_run(Squidie.Step.Context.t(), module(), atom(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom() | term()}}
          | {:error, {:invalid_trigger, :expected_atom}}
          | {:error, ChildStarter.start_error()}
  def start_child_run(parent_context, child_workflow, child_trigger, payload, overrides)
      when is_atom(child_trigger) and is_map(payload) and is_list(overrides) do
    with :ok <- public_child_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      ChildStarter.start_child_run(
        parent_context,
        child_workflow,
        child_trigger,
        payload,
        journal_child_start_options(overrides)
      )
    end
  end

  def start_child_run(_parent_context, _child_workflow, child_trigger, payload, overrides)
      when not is_atom(child_trigger) and is_map(payload) and is_list(overrides) do
    {:error, {:invalid_trigger, :expected_atom}}
  end

  def start_child_run(_parent_context, _child_workflow, _child_trigger, _payload, overrides)
      when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  def start_child_run(_parent_context, _child_workflow, _child_trigger, _payload, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Fetches one workflow run by id.

  The selected read model comes from host configuration unless overridden. The
  read model rebuilds a projection-backed snapshot from durable journal entries.
  """
  @spec inspect_run(String.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides) do
      inspect_projected_run(run_id, overrides)
    end
  end

  @doc """
  Fetches one workflow run as graph-oriented inspection data.

  The graph projection preserves `inspect_run/2` as the factual run snapshot and
  derives nodes and edges from that same durable state. The selected read model
  comes from host configuration unless overridden.
  """
  @spec inspect_run_graph(String.t(), keyword()) ::
          {:ok, GraphInspection.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run_graph(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides),
         {:ok, inspection} <- inspect_graph_source(run_id, :read_model, overrides) do
      {:ok, graph_inspection(inspection, :read_model, overrides)}
    end
  end

  @doc """
  Fetches one workflow run as chronological operator timeline events.

  The timeline projection is derived from the same durable read-model snapshot
  as `inspect_run/2`. Events carry stable ordering fields and redaction-safe
  details so clients do not need to parse raw journal entries.
  """
  @spec inspect_run_timeline(String.t(), keyword()) ::
          {:ok, Timeline.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run_timeline(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides),
         {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides) do
      Inspection.timeline(snapshot)
    end
  end

  @doc """
  Validates bounded dynamic work and returns the graph it would produce.

  This is the read-only companion to `record_dynamic_work/3`. It applies the
  same validation and normalization rules, but does not append a journal fact.
  Use it for UI previews, visual editor validation, and host-side dry runs
  before recording dynamic work durably. Pass `:action_registry` to require
  every dynamic node action key to be host-allowlisted before previewing.
  """
  @spec preview_dynamic_work(String.t(), map() | keyword(), keyword()) ::
          {:ok, DynamicWorkPreview.t()}
          | {:error,
             :not_found
             | Config.config_error()
             | read_option_error()
             | DynamicWork.dynamic_work_error()
             | term()}
  def preview_dynamic_work(run_id, attrs, overrides \\ [])

  def preview_dynamic_work(run_id, attrs, overrides) when is_list(overrides) do
    with :ok <- public_dynamic_work_options(overrides),
         {:ok, :journal} <- runtime(overrides),
         {:ok, :read_model} <- read_model(overrides),
         {:ok, run_id} <- Options.thread_part(run_id, :run_id),
         {:ok, now} <- dynamic_work_time(overrides),
         {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides),
         {:ok, context} <- dynamic_work_context(snapshot, overrides),
         {:ok, preview} <- DynamicWork.preview(run_id, attrs, now, context) do
      {:ok, dynamic_work_preview(snapshot, preview, overrides)}
    end
  end

  def preview_dynamic_work(_run_id, _attrs, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Records bounded dynamic work for one workflow run.

  This API records inspection-only dynamic structure. It does not make dynamic
  nodes executable, schedule dispatch attempts, or alter terminal-state
  decisions. Use it when host code or a runtime step has already made a bounded
  fanout or planning decision and dashboards need durable, validated metadata
  about that decision.

  The attribute payload must include:

    * `:dynamic_key` - a stable non-empty string for this dynamic work group.
    * `:origin` - a map with `:runnable_key`, `:step`, and positive `:attempt`
      matching a planned runnable in the active run.
    * `:nodes` - one or more dynamic node maps with unique non-empty `:id`
      values that do not collide with declared workflow steps or previously
      recorded dynamic nodes.

  Optional `:edges`, `:metadata`, `:reason`, and `:status` values are normalized
  and redacted like other journal metadata. Pass `:action_registry` to require
  every dynamic node action key to be host-allowlisted before recording. Exact
  duplicate records are idempotent while the run remains active. Terminal runs,
  stale workflow definitions, invalid origins, node collisions, unknown options,
  and write conflicts return structured `{:error, reason}` tuples.
  """
  @spec record_dynamic_work(String.t(), map() | keyword(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | Config.config_error()
             | read_option_error()
             | DynamicWork.dynamic_work_error()
             | term()}
  def record_dynamic_work(run_id, attrs, overrides \\ [])

  def record_dynamic_work(run_id, attrs, overrides) when is_list(overrides) do
    with :ok <- public_dynamic_work_options(overrides),
         {:ok, :journal} <- runtime(overrides),
         {:ok, :read_model} <- read_model(overrides),
         {:ok, storage} <- journal_storage(overrides),
         {:ok, run_id} <- Options.thread_part(run_id, :run_id),
         {:ok, now} <- dynamic_work_time(overrides),
         {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides),
         {:ok, context} <- dynamic_work_context(snapshot, overrides),
         {:ok, intent} <-
           DynamicWork.new_entry(run_id, attrs, now, context) do
      record_dynamic_work_intent(storage, run_id, overrides, snapshot, intent)
    end
  end

  def record_dynamic_work(_run_id, _attrs, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Records bounded dynamic work and schedules each dynamic node as executable work.

  This is the executable counterpart to `record_dynamic_work/3`. The dynamic
  work fact and the planned runnable intents are appended to the run thread in
  one optimistic write, then missing dispatch attempts are scheduled from that
  durable plan. Pass `:action_registry`; every dynamic node must include an
  approved `:action` key and may include an `:input` map for the scheduled
  attempt.
  """
  @spec schedule_dynamic_work(String.t(), map() | keyword(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | Config.config_error()
             | read_option_error()
             | DynamicWork.dynamic_work_error()
             | term()}
  def schedule_dynamic_work(run_id, attrs, overrides \\ [])

  def schedule_dynamic_work(run_id, attrs, overrides) when is_list(overrides) do
    with :ok <- public_dynamic_work_options(overrides),
         {:ok, :journal} <- runtime(overrides),
         {:ok, :read_model} <- read_model(overrides),
         {:ok, storage} <- journal_storage(overrides),
         {:ok, queue} <- dynamic_work_queue(overrides),
         {:ok, registry} <- dynamic_work_action_registry(overrides),
         {:ok, run_id} <- Options.thread_part(run_id, :run_id),
         {:ok, now} <- dynamic_work_time(overrides),
         {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides),
         {:ok, context} <- dynamic_work_context(snapshot, overrides),
         {:ok, intent} <-
           DynamicWork.new_entry(
             run_id,
             put_new_dynamic_work_status(attrs, :scheduled),
             now,
             context
           ),
         :ok <- dynamic_work_origin_applied(intent, snapshot),
         {:ok, runnables} <- dynamic_work_runnables(run_id, intent, queue, now, registry) do
      schedule_dynamic_work_intent(storage, run_id, overrides, snapshot, intent, runnables, now)
    end
  end

  def schedule_dynamic_work(_run_id, _attrs, _overrides) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc """
  Explains the current runtime state of one workflow run.

  The result is structured diagnostic data for host apps, CLIs, and dashboards.
  Use `inspect_run/2` for the factual run snapshot and `explain_run/2` when an
  operator-facing surface needs the reason, evidence, and valid next actions for
  the run's current state.

  The selected read model comes from host configuration unless overridden. The
  read model derives diagnostics from durable journal projections.
  """
  @spec explain_run(String.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Explanation.Diagnostic.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Squidie.ReadModel.Explanation.explanation_error()}
  def explain_run(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides) do
      explain_projected_run(run_id, overrides)
    end
  end

  @doc """
  Executes the next visible workflow attempt through the selected runtime.

  The default journal runtime claims one visible Jido journal-backed attempt,
  runs its declared step, and appends durable attempt completion or failure
  facts. Pass `journal_storage:` only when overriding the inferred Ecto storage
  boundary.
  """
  @spec execute_next(keyword()) :: Executor.execute_result()
  def execute_next(overrides \\ [])

  def execute_next(overrides) when is_list(overrides) do
    with :ok <- public_execute_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      Executor.execute_next(journal_execute_options(overrides))
    end
  end

  def execute_next(overrides), do: Executor.execute_next(overrides)

  defp public_execute_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        :ok

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in public_execute_option_keys())) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  defp public_execute_option_keys do
    [
      :runtime,
      :journal_storage,
      :queue,
      :owner_id,
      :lease_for,
      :heartbeat_interval_ms,
      :now,
      :action_registry
    ]
  end

  defp public_child_start_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in @journal_child_start_options)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  defp public_dynamic_work_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in @journal_dynamic_work_options)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  @doc """
  Lists workflow runs with optional filters.

  Journal-backed runtime calls return redacted listing summaries. Use
  `inspect_run/2` or `inspect_run_graph/2` with a run id when callers need
  detailed runtime state for one run.
  """
  @spec list_runs([Listing.list_filter()], keyword()) ::
          {:ok, [Listing.Summary.t()]}
          | {:error, Config.config_error() | Listing.list_error()}
  def list_runs(filters \\ [], overrides \\ []) do
    with {:ok, :journal} <- runtime(overrides) do
      list_runs_with_runtime(:journal, filters, overrides)
    end
  end

  @doc """
  Requests cancellation for an eligible workflow run.
  """
  @spec cancel(Ecto.UUID.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | Cancellation.cancel_error()}
  def cancel(run_id, overrides \\ []) do
    with {:ok, :journal} <- runtime(overrides) do
      cancel_run_with_runtime(:journal, run_id, overrides)
    end
  end

  @doc """
  Applies a Squidie-native runtime command signal.

  Use the named helpers such as `start/3`, `cancel/2`, `resume/3`,
  `approve/3`, `reject/3`, and `replay/2` for ordinary application calls. Use
  `apply_signal/2` when an agent, router, webhook, scheduler, or Jido interop
  boundary has already normalized a request into a
  `Squidie.Runtime.Signal` envelope.

  The envelope API applies the same runtime commands for starts, cron starts,
  replays, cancellation, and manual decisions while preserving signal metadata,
  occurrence time, and idempotency keys in the journal command history.
  """
  @spec apply_signal(Signal.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error() | term()}
  def apply_signal(signal, overrides \\ [])

  def apply_signal(%Signal{} = signal, overrides) when is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides) do
      SignalInterpreter.apply(signal, journal_control_options(overrides))
    end
  end

  def apply_signal(%Signal{}, _overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}
  def apply_signal(_signal, _overrides), do: {:error, :invalid_signal}

  @doc """
  Resumes a run that is intentionally paused for manual intervention.
  """
  @spec resume(Ecto.UUID.t()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def resume(run_id), do: resume(run_id, %{}, [])

  @doc """
  Resumes a paused run with either configuration overrides or manual action
  attributes.
  """
  @spec resume(Ecto.UUID.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def resume(run_id, overrides) when is_list(overrides), do: resume(run_id, %{}, overrides)

  @spec resume(Ecto.UUID.t(), map()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def resume(run_id, attrs) when is_map(attrs), do: resume(run_id, attrs, [])

  @doc """
  Resumes a paused run with manual action attributes and configuration
  overrides.
  """
  @spec resume(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def resume(run_id, attrs, overrides) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides),
         {:ok, signal} <- control_signal(:resume_run, run_id, attrs, overrides) do
      SignalInterpreter.apply(signal, journal_control_options(overrides))
    else
      {:error, {:invalid_signal, reason}} -> {:error, public_signal_error(reason)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Approves a paused approval step and resumes the run through its success path.
  """
  @spec approve(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def approve(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides),
         {:ok, signal} <- control_signal(:approve_run, run_id, attrs, overrides) do
      SignalInterpreter.apply(signal, journal_control_options(overrides))
    else
      {:error, {:invalid_signal, reason}} -> {:error, public_signal_error(reason)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Rejects a paused approval step and resumes the run through its rejection path.
  """
  @spec reject(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def reject(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides),
         {:ok, signal} <- control_signal(:reject_run, run_id, attrs, overrides) do
      SignalInterpreter.apply(signal, journal_control_options(overrides))
    else
      {:error, {:invalid_signal, reason}} -> {:error, public_signal_error(reason)}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Creates a new run from a prior run and links it to the original run.

  Replays are blocked by default once the source run completed an irreversible
  or non-compensatable step. Pass `allow_irreversible: true` only after an
  operator has reviewed the side effect and accepted re-execution.
  """
  @spec replay(Ecto.UUID.t(), keyword()) ::
          {:ok, Squidie.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | Replay.replay_error()}
          | {:error, {:dispatch_failed, term()}}
  def replay(run_id, overrides \\ []) do
    {replay_opts, config_overrides} = Keyword.split(overrides, [:allow_irreversible])

    with {:ok, :journal} <- runtime(config_overrides),
         {:ok, run} <- replay_run_with_runtime(:journal, run_id, replay_opts, config_overrides) do
      {:ok, run}
    else
      {:error, reason} = error when reason in [:not_found, :invalid_run_id] ->
        error

      {:error, {:unsafe_replay, _details} = reason} ->
        {:error, reason}

      {:error, {:invalid_run, _changeset} = reason} ->
        {:error, reason}

      {:error, {:invalid_option, _details} = reason} ->
        {:error, reason}

      {:error, {:incompatible_workflow_definition, _operation} = reason} ->
        {:error, reason}

      {:error, {:incompatible_workflow_definition, _operation, _metadata} = reason} ->
        {:error, reason}

      {:error, {:invalid_replay_source, _details} = reason} ->
        {:error, reason}

      {:error, %_struct{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end

  defp replay_run_with_runtime(:journal, run_id, replay_opts, config_overrides) do
    Replay.replay(run_id, replay_opts, journal_control_options(config_overrides))
  end

  defp start_default_run_with_runtime(:journal, workflow, payload, overrides) do
    Starter.start_run(workflow, nil, payload, journal_start_options(overrides))
  end

  defp start_triggered_run_with_runtime(:journal, workflow, trigger_name, payload, overrides) do
    Starter.start_run(workflow, trigger_name, payload, journal_start_options(overrides))
  end

  defp start_spec_run_with_runtime(:journal, spec, trigger_name, payload, overrides) do
    Starter.start_spec_run(spec, trigger_name, payload, journal_spec_start_options(overrides))
  end

  defp start_initial_context_run_with_runtime(
         :journal,
         workflow,
         trigger_name,
         payload,
         initial_context,
         overrides
       ) do
    with {:ok, opts} <-
           journal_initial_context_start_options(
             workflow,
             trigger_name,
             initial_context,
             overrides
           ) do
      Starter.start_run(workflow, trigger_name, payload, opts)
    end
  end

  defp journal_child_start_options(overrides) do
    configured_journal_options(overrides, @journal_child_start_options)
  end

  defp list_runs_with_runtime(:journal, filters, overrides) do
    with {:ok, storage} <- journal_storage(overrides) do
      Listing.list(storage, filters, journal_list_options(overrides))
    end
  end

  defp cancel_run_with_runtime(:journal, run_id, overrides) do
    case control_signal(:cancel_run, run_id, overrides) do
      {:ok, signal} ->
        SignalInterpreter.apply(signal, journal_control_options(overrides))

      {:error, {:invalid_signal, reason}} ->
        {:error, public_signal_error(reason)}

      {:error, _reason} = error ->
        error
    end
  end

  defp control_signal(:cancel_run, run_id, overrides) do
    with {:ok, signal_opts} <- control_signal_options(overrides) do
      run_id
      |> Signal.cancel_run(signal_opts)
      |> normalize_control_signal_result()
    end
  end

  defp control_signal(type, run_id, attrs, overrides) do
    with {:ok, signal_opts} <- control_signal_options(overrides) do
      Signal
      |> apply(type, [run_id, attrs, signal_opts])
      |> normalize_control_signal_result()
    end
  end

  defp control_signal_options(overrides) do
    with {:ok, opts} <- control_signal_occurred_at(overrides),
         {:ok, opts} <- control_signal_metadata(overrides, opts) do
      control_signal_idempotency_key(overrides, opts)
    end
  end

  defp control_signal_occurred_at(overrides) do
    case Keyword.fetch(overrides, :now) do
      {:ok, %DateTime{} = now} -> {:ok, [occurred_at: now]}
      {:ok, _invalid} -> {:error, {:invalid_option, {:now, :invalid}}}
      :error -> {:ok, []}
    end
  end

  defp control_signal_metadata(overrides, opts) do
    case Keyword.fetch(overrides, :metadata) do
      {:ok, metadata} when is_map(metadata) -> {:ok, Keyword.put(opts, :metadata, metadata)}
      {:ok, _invalid} -> {:error, {:invalid_option, {:metadata, :invalid}}}
      :error -> {:ok, opts}
    end
  end

  defp control_signal_idempotency_key(overrides, opts) do
    case Keyword.fetch(overrides, :idempotency_key) do
      {:ok, idempotency_key} when is_binary(idempotency_key) and idempotency_key != "" ->
        {:ok, Keyword.put(opts, :idempotency_key, idempotency_key)}

      {:ok, _invalid} ->
        {:error, {:invalid_option, {:idempotency_key, :invalid}}}

      :error ->
        {:ok, opts}
    end
  end

  defp normalize_control_signal_result({:ok, %Signal{}} = result), do: result

  defp normalize_control_signal_result({:error, {:invalid_signal, _reason}} = error), do: error

  defp normalize_control_signal_result({:error, reason}), do: {:error, {:invalid_signal, reason}}

  defp public_signal_error({:run_id, :invalid}), do: :invalid_run_id

  defp public_signal_error({:occurred_at, :expected_datetime}),
    do: {:invalid_option, {:now, :invalid}}

  defp public_signal_error(reason), do: {:invalid_signal, reason}

  defp journal_initial_context_start_options(workflow, trigger_name, initial_context, overrides) do
    opts =
      overrides
      |> journal_start_options()
      |> Keyword.put(:initial_context, initial_context)

    with {:ok, idempotency_key} <- schedule_idempotency_key(initial_context) do
      case idempotency_key do
        nil ->
          {:ok, opts}

        idempotency_key ->
          opts =
            opts
            |> Keyword.put(:run_id, schedule_run_id(workflow, trigger_name, idempotency_key))
            |> Keyword.put(:duplicate_schedule_start, true)

          {:ok, opts}
      end
    end
  end

  defp schedule_idempotency_key(context) when is_map(context) do
    context
    |> Squidie.Runtime.ScheduleContext.get()
    |> Squidie.Runtime.ScheduleContext.value(:idempotency_key)
    |> validate_schedule_idempotency_key()
  end

  defp validate_schedule_idempotency_key(nil), do: {:ok, nil}

  defp validate_schedule_idempotency_key(key) when is_binary(key), do: {:ok, key}

  defp validate_schedule_idempotency_key(_key) do
    {:error, {:invalid_option, {:schedule_idempotency_key, :invalid}}}
  end

  defp schedule_run_id(workflow, trigger_name, idempotency_key) do
    workflow_name = Squidie.Workflow.Definition.serialize_workflow(workflow)
    trigger = Squidie.Workflow.Definition.serialize_trigger(trigger_name)

    {:ok, run_id} = ScheduleIdentity.run_id(workflow_name, trigger, idempotency_key)
    run_id
  end

  defp reject_public_start_options(overrides) do
    cond do
      Keyword.has_key?(overrides, :context) ->
        {:error, {:invalid_option, :context}}

      Keyword.has_key?(overrides, :initial_context) ->
        {:error, {:invalid_option, :initial_context}}

      true ->
        :ok
    end
  end

  defp dynamic_work_time(overrides) do
    case Keyword.get(overrides, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp dynamic_work_queue(overrides) do
    overrides
    |> projected_snapshot_options()
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp dynamic_work_action_registry(overrides) do
    case Keyword.fetch(overrides, :action_registry) do
      {:ok, registry} -> {:ok, registry}
      :error -> {:error, {:invalid_dynamic_work, {:action_registry, :required}}}
    end
  end

  defp put_new_dynamic_work_status(attrs, status) when is_map(attrs) do
    Map.put_new(attrs, :status, status)
  end

  defp put_new_dynamic_work_status(attrs, status) when is_list(attrs) do
    if Keyword.keyword?(attrs), do: Keyword.put_new(attrs, :status, status), else: attrs
  end

  defp put_new_dynamic_work_status(attrs, _status), do: attrs

  defp record_dynamic_work_intent(
         _storage,
         _run_id,
         _overrides,
         %Inspection.Snapshot{} = snapshot,
         :duplicate
       ) do
    {:ok, snapshot}
  end

  defp record_dynamic_work_intent(
         storage,
         run_id,
         overrides,
         %Inspection.Snapshot{} = snapshot,
         entry
       ) do
    case Squidie.Runtime.Journal.append_entries(storage, [entry],
           expected_rev: snapshot.thread_revisions.run
         ) do
      {:ok, _thread} ->
        inspect_projected_run(run_id, overrides)

      {:error, :conflict} ->
        resolve_dynamic_work_conflict(run_id, overrides, entry)

      {:error, _reason} = error ->
        error
    end
  end

  defp schedule_dynamic_work_intent(
         storage,
         run_id,
         overrides,
         %Inspection.Snapshot{},
         :duplicate,
         _runnables,
         %DateTime{} = now
       ) do
    schedule_pending_dynamic_dispatches(storage, run_id, overrides, now)
  end

  defp schedule_dynamic_work_intent(
         storage,
         run_id,
         overrides,
         %Inspection.Snapshot{} = snapshot,
         entry,
         runnables,
         %DateTime{} = now
       ) do
    with {:ok, planned_entry} <- dynamic_work_runnables_planned_entry(run_id, runnables, now),
         {:ok, queue} <- dynamic_work_queue(overrides),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, %{agent: dispatch_agent}} <-
           ensure_dynamic_work_run_queued(storage, dispatch_agent, run_id, now),
         {:ok, _thread} <-
           Squidie.Runtime.Journal.append_entries(storage, [entry, planned_entry],
             expected_rev: snapshot.thread_revisions.run
           ),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, _scheduled} <-
           WorkflowAgent.schedule_pending_dispatches(storage, workflow_agent, dispatch_agent,
             now: now
           ) do
      inspect_projected_run(run_id, overrides)
    else
      {:error, :conflict} ->
        resolve_scheduled_dynamic_work_conflict(storage, run_id, overrides, entry, now)

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve_scheduled_dynamic_work_conflict(
         storage,
         run_id,
         overrides,
         entry,
         %DateTime{} = now
       ) do
    with {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides),
         {:ok, context} <- dynamic_work_context(snapshot, overrides) do
      case DynamicWork.new_entry(run_id, entry.data, entry.occurred_at, context) do
        {:ok, :duplicate} ->
          schedule_pending_dynamic_dispatches(storage, run_id, overrides, now)

        {:error, {:invalid_dynamic_work, {:run, :terminal}}} = error ->
          error

        {:ok, _entry} ->
          {:error, :conflict}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp schedule_pending_dynamic_dispatches(storage, run_id, overrides, %DateTime{} = now) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, queue} <- dynamic_work_queue(overrides),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, %{agent: dispatch_agent}} <-
           ensure_dynamic_work_run_queued(storage, dispatch_agent, run_id, now),
         {:ok, _scheduled} <-
           WorkflowAgent.schedule_pending_dispatches(storage, workflow_agent, dispatch_agent,
             now: now
           ) do
      inspect_projected_run(run_id, overrides)
    end
  end

  defp ensure_dynamic_work_run_queued(storage, dispatch_agent, run_id, %DateTime{} = now) do
    ensure_dynamic_work_run_queued(
      storage,
      dispatch_agent,
      run_id,
      now,
      @dispatch_schedule_retries
    )
  end

  defp ensure_dynamic_work_run_queued(_storage, _dispatch_agent, _run_id, %DateTime{}, 0) do
    {:error, :conflict}
  end

  defp ensure_dynamic_work_run_queued(storage, dispatch_agent, run_id, %DateTime{} = now, retries) do
    case DispatchAgent.ensure_run_queued(storage, dispatch_agent, run_id, now: now) do
      {:ok, _queued} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, queue} <- Options.queue(dispatch_agent.state.queue),
             {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
          ensure_dynamic_work_run_queued(storage, dispatch_agent, run_id, now, retries - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp dynamic_work_runnables_planned_entry(run_id, runnables, %DateTime{} = now) do
    EntryBuilder.runnables_planned(run_id, runnables, now)
  end

  defp dynamic_work_runnables(_run_id, :duplicate, _queue, _now, _registry), do: {:ok, []}

  defp dynamic_work_runnables(run_id, %DispatchProtocol.Entry{} = entry, queue, now, registry) do
    entry.data.nodes
    |> Enum.reduce_while({:ok, []}, fn node, {:ok, runnables} ->
      case dynamic_work_runnable(run_id, entry.data, node, queue, now, registry) do
        {:ok, runnable} -> {:cont, {:ok, [runnable | runnables]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, runnables} -> {:ok, Enum.reverse(runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp dynamic_work_runnable(run_id, dynamic_work, node, queue, %DateTime{} = now, registry) do
    with {:ok, module} <- ActionRegistry.resolve_action(Map.get(node, :action), registry),
         {:ok, action_opts} <-
           ActionRegistry.resolve_action_opts(Map.get(node, :action), registry),
         :ok <- validate_dynamic_action_input(module, Map.get(node, :input, %{}), action_opts) do
      node_id = Map.fetch!(node, :id)
      runnable_key = Enum.join([run_id, node_id, 1], ":")

      {:ok,
       %{
         run_id: run_id,
         runnable_key: runnable_key,
         idempotency_key: runnable_key,
         attempt_number: 1,
         queue: queue,
         step: node_id,
         input: Map.get(node, :input, %{}),
         visible_at: now,
         recovery: dynamic_work_recovery(Map.get(node, :action)),
         dynamic?: true,
         dynamic_work: %{
           dynamic_key: dynamic_work.dynamic_key,
           action: Map.get(node, :action),
           module: module,
           action_opts: persisted_action_opts(module, action_opts),
           retry: Map.get(node, :retry),
           origin: dynamic_work.origin
         }
       }}
    else
      {:error, reason} ->
        {:error, {:invalid_dynamic_work, {:nodes, {:action, reason}}}}
    end
  end

  defp validate_dynamic_action_input(module, input, action_opts) do
    if {:validate_action_input, 2} in module.__info__(:functions) do
      case module.validate_action_input(input || %{}, action_opts) do
        :ok -> :ok
        {:error, error} -> {:error, {:action_input, Map.get(error, :validation_errors, %{})}}
      end
    else
      :ok
    end
  end

  defp persisted_action_opts(module, action_opts)
       when is_atom(module) and is_list(action_opts) do
    if function_exported?(module, :persisted_action_opts, 1) do
      module.persisted_action_opts(action_opts)
    else
      action_opts
    end
  end

  defp persisted_action_opts(_module, _action_opts), do: []

  defp dynamic_work_recovery(action) do
    %{
      irreversible?: true,
      compensatable?: false,
      replay: :manual_review_required,
      recovery: :manual_intervention,
      dynamic?: true,
      action: action
    }
  end

  defp dynamic_work_origin_applied(:duplicate, _snapshot), do: :ok

  defp dynamic_work_origin_applied(%DispatchProtocol.Entry{} = entry, snapshot) do
    origin_key = entry.data.origin.runnable_key

    if origin_key in snapshot.applied_runnable_keys do
      :ok
    else
      {:error, {:invalid_dynamic_work, {:origin, :unapplied_runnable}}}
    end
  end

  defp dynamic_work_context(%Inspection.Snapshot{terminal?: true} = snapshot, overrides) do
    maybe_put_dynamic_work_action_registry(
      {:ok,
       %{
         terminal?: snapshot.terminal?,
         planned_runnables: snapshot.planned_runnables,
         dynamic_work: snapshot.dynamic_work,
         definition: nil
       }},
      overrides
    )
  end

  defp dynamic_work_context(%Inspection.Snapshot{} = snapshot, overrides) do
    with {:ok, definition} <- dynamic_work_definition(snapshot, overrides) do
      maybe_put_dynamic_work_action_registry(
        {:ok,
         %{
           terminal?: snapshot.terminal?,
           planned_runnables: snapshot.planned_runnables,
           dynamic_work: snapshot.dynamic_work,
           definition: definition
         }},
        overrides
      )
    end
  end

  defp maybe_put_dynamic_work_action_registry({:ok, context}, overrides) do
    case Keyword.fetch(overrides, :action_registry) do
      {:ok, registry} -> {:ok, Map.put(context, :action_registry, registry)}
      :error -> {:ok, context}
    end
  end

  defp dynamic_work_preview(%Inspection.Snapshot{} = snapshot, preview, overrides)
       when is_map(preview) do
    dynamic_work = Map.fetch!(preview, :dynamic_work)
    duplicate? = Map.fetch!(preview, :duplicate?)

    preview_snapshot =
      if duplicate? do
        snapshot
      else
        %Inspection.Snapshot{
          snapshot
          | dynamic_work: preview_dynamic_work_items(snapshot.dynamic_work, dynamic_work)
        }
      end

    DynamicWorkPreview.new(
      snapshot.run_id,
      dynamic_work,
      duplicate?,
      graph_inspection(preview_snapshot, :read_model, overrides)
    )
  end

  defp preview_dynamic_work_items(dynamic_work, preview) when is_list(dynamic_work) do
    Enum.sort_by(
      [preview | dynamic_work],
      &{Map.get(&1, :dynamic_key), Map.get(&1, :recorded_at)}
    )
  end

  defp resolve_dynamic_work_conflict(run_id, overrides, entry) do
    with {:ok, %Inspection.Snapshot{} = snapshot} <- inspect_projected_run(run_id, overrides),
         {:ok, context} <- dynamic_work_context(snapshot, overrides) do
      case DynamicWork.new_entry(run_id, entry.data, entry.occurred_at, context) do
        {:ok, :duplicate} -> {:ok, snapshot}
        {:error, {:invalid_dynamic_work, {:run, :terminal}}} = error -> error
        {:error, _reason} -> {:error, :conflict}
        {:ok, _entry} -> {:error, :conflict}
      end
    end
  end

  defp inspect_projected_run(run_id, overrides) when is_binary(run_id) do
    with {:ok, storage} <- journal_storage(overrides) do
      Inspection.snapshot(storage, run_id, projected_snapshot_options(overrides))
    end
  end

  defp inspect_projected_run(_run_id, _overrides) do
    {:error, {:invalid_option, {:run_id, :invalid}}}
  end

  defp inspect_graph_source(run_id, :read_model, overrides) do
    inspect_projected_run(run_id, overrides)
  end

  defp graph_inspection(%Inspection.Snapshot{} = snapshot, read_model, overrides) do
    GraphInspection.from_snapshot(
      snapshot,
      graph_inspection_options(snapshot, read_model, overrides)
    )
  end

  defp graph_inspection_options(%Inspection.Snapshot{} = snapshot, read_model, overrides) do
    [
      source: read_model,
      include_details: Keyword.get(overrides, :include_history, false),
      definition: graph_definition(snapshot, overrides)
    ]
  end

  defp graph_definition(%Inspection.Snapshot{run_id: run_id, workflow: workflow}, overrides)
       when is_binary(run_id) and is_binary(workflow) do
    with {:ok, storage} <- journal_storage(overrides),
         {:ok, _workflow, definition} <- WorkflowDefinitionLoader.load(storage, run_id, workflow) do
      definition
    else
      {:error, _reason} -> nil
    end
  end

  defp graph_definition(%Inspection.Snapshot{}, _overrides), do: nil

  defp dynamic_work_definition(
         %Inspection.Snapshot{run_id: run_id, workflow: workflow},
         overrides
       )
       when is_binary(run_id) and is_binary(workflow) do
    with {:ok, storage} <- journal_storage(overrides),
         {:ok, _workflow, definition} <- WorkflowDefinitionLoader.load(storage, run_id, workflow) do
      {:ok, definition}
    else
      {:error, reason} -> {:error, {:invalid_dynamic_work, {:definition, reason}}}
    end
  end

  defp dynamic_work_definition(%Inspection.Snapshot{}, _overrides) do
    {:error, {:invalid_dynamic_work, {:definition, :missing}}}
  end

  defp explain_projected_run(run_id, overrides) do
    with {:ok, storage} <- journal_storage(overrides) do
      Squidie.ReadModel.Explanation.explain(
        storage,
        run_id,
        projected_snapshot_options(overrides)
      )
    end
  end

  defp read_model(overrides) when is_list(overrides) do
    with :ok <- validate_keyword_options(overrides) do
      configured_read_model(overrides)
    end
  end

  defp read_model(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp runtime(overrides) when is_list(overrides) do
    with :ok <- validate_keyword_options(overrides) do
      configured_runtime(overrides)
    end
  end

  defp runtime(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp validate_keyword_options(overrides) do
    if Keyword.keyword?(overrides) do
      :ok
    else
      {:error, {:invalid_option, {:opts, :invalid}}}
    end
  end

  defp configured_read_model(overrides) do
    case Keyword.fetch(overrides, :read_model) do
      {:ok, read_model} when read_model in @read_models ->
        {:ok, read_model}

      {:ok, _read_model} ->
        {:error, {:invalid_option, {:read_model, :invalid}}}

      :error ->
        load_configured_read_model(overrides)
    end
  end

  defp load_configured_read_model(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{read_model: read_model}} -> {:ok, read_model}
      {:error, _reason} = error -> error
    end
  end

  defp configured_runtime(overrides) do
    case Keyword.fetch(overrides, :runtime) do
      {:ok, runtime} when runtime in @runtimes ->
        {:ok, runtime}

      {:ok, _runtime} ->
        {:error, {:invalid_option, {:runtime, :invalid}}}

      :error ->
        load_configured_runtime(overrides)
    end
  end

  defp load_configured_runtime(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{runtime: runtime}} -> {:ok, runtime}
      {:error, _reason} = error -> error
    end
  end

  defp journal_storage(overrides) do
    case Keyword.fetch(overrides, :journal_storage) do
      {:ok, storage} ->
        Options.storage(storage)

      :error ->
        case Config.load(config_routing_overrides(overrides)) do
          {:ok, %Config{} = config} -> Options.storage(config.journal_storage)
          {:error, {:missing_config, [:journal_storage]}} -> Options.storage(nil)
          {:error, _reason} = error -> error
        end
    end
  end

  defp projected_snapshot_options(overrides) do
    configured_journal_options(overrides, @projection_snapshot_options)
  end

  defp journal_start_options(overrides) do
    configured_journal_options(overrides, @journal_start_options)
  end

  defp journal_spec_start_options(overrides) do
    configured_journal_options(overrides, @journal_spec_start_options)
  end

  defp journal_control_options(overrides) do
    configured_journal_options(overrides, @journal_control_options)
  end

  defp journal_execute_options(overrides) do
    configured_journal_options(overrides, @journal_execute_options)
  end

  defp journal_list_options(overrides) do
    configured_journal_options(overrides, @projection_list_options)
  end

  defp configured_journal_options(overrides, keys) do
    case load_config_for_journal_options(overrides) do
      {:ok, %Config{} = config} ->
        [
          runtime: :journal,
          journal_storage: config.journal_storage,
          queue: config.queue
        ]
        |> Keyword.merge(Keyword.take(overrides, keys))
        |> Keyword.take(keys)

      {:error, _reason} ->
        Keyword.take(overrides, keys)
    end
  end

  defp load_config_for_journal_options(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{} = config} ->
        {:ok, config}

      {:error, _reason} ->
        overrides
        |> Keyword.delete(:journal_storage)
        |> config_routing_overrides()
        |> Config.load()
    end
  end

  defp config_routing_overrides(overrides) do
    Keyword.take(overrides, [
      :repo,
      :runtime,
      :read_model,
      :journal_storage
    ])
  end
end
