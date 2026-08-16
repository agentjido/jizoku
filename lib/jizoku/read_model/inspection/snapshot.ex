# credo:disable-for-this-file Credo.Check.Warning.StructFieldAmount
defmodule Jizoku.ReadModel.Inspection.Snapshot do
  @moduledoc """
  Projection-backed inspection snapshot for one Jido-native workflow run.

  This struct is a compact read model built from the workflow and dispatch
  durable journals so callers can inspect a stable public shape without parsing
  raw journal entries.

  Terminal runs keep both `terminal?` and `terminal_status` so operator-facing
  surfaces can suppress recovery actions while still distinguishing completed,
  failed, and cancelled histories.

  Future-visible attempts are kept separate from currently visible attempts.
  This lets operator-facing surfaces explain delayed retry or deferred dispatch
  state without treating the run as idle or recoverable.
  """

  @type reason ::
          :terminal
          | :completed_result_pending_apply
          | :planned_dispatch_pending_schedule
          | :expired_claim
          | :attempt_claimed
          | :attempt_visible
          | :deferred_continuation
          | :attempt_scheduled_for_later
          | :manual_intervention_required
          | :run_started
          | :idle
          | :waiting_for_dispatch

  @type attempt :: %{
          required(:runnable_key) => String.t(),
          required(:status) => atom(),
          required(:attempt_number) => pos_integer(),
          required(:step) => String.t(),
          required(:input) => map(),
          optional(:scheduled_at) => DateTime.t(),
          required(:visible_at) => DateTime.t(),
          required(:idempotency_key) => String.t(),
          optional(:claim_id) => String.t(),
          optional(:owner_id) => String.t(),
          optional(:lease_until) => DateTime.t(),
          optional(:claimed_at) => DateTime.t(),
          optional(:result) => map(),
          optional(:completed_at) => DateTime.t(),
          optional(:transition) => map(),
          optional(:error) => map(),
          optional(:recovery) => map(),
          optional(:deferred) => map(),
          required(:wakeup_emitted?) => boolean(),
          required(:applied?) => boolean()
        }

  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          workflow: String.t() | nil,
          trigger: String.t() | nil,
          input: map() | nil,
          started_at: DateTime.t() | nil,
          context: map(),
          definition_version: String.t() | nil,
          definition_migrations: [map()],
          continuation: %{
            required(:continued_from) => map() | nil,
            required(:continued_to) => map() | nil
          },
          history: Jizoku.ReadModel.HistoryPolicy.summary(),
          parent_run: map() | nil,
          child_runs: [map()],
          dynamic_work: [map()],
          graph_version: non_neg_integer(),
          graph_provenance: map(),
          active_node_ids: [String.t()],
          active_edge_ids: [String.t()],
          ready_node_ids: [String.t()],
          blocked_node_ids: [String.t()],
          tombstoned_node_ids: [String.t()],
          tombstoned_edge_ids: [String.t()],
          mutation_history: [map()],
          reconciliation_status: :not_required | :required | :completed | :unknown,
          guardrails: [map()],
          replayed_from_run_id: String.t() | nil,
          queue: String.t(),
          status: atom(),
          reason: reason(),
          terminal?: boolean(),
          terminal_status: atom() | nil,
          terminal_at: DateTime.t() | nil,
          terminal_error: map() | nil,
          deadline: map() | nil,
          manual_state: map() | nil,
          command_history: [map()],
          jido_signals: %{
            required(:pending_count) => non_neg_integer(),
            required(:delivered_count) => non_neg_integer(),
            required(:items) => [map()]
          },
          thread_revisions: %{run: non_neg_integer(), dispatch: non_neg_integer()},
          planned_runnables: [map()],
          planned_runnable_keys: [String.t()],
          applied_runnable_keys: [String.t()],
          applied_at: %{optional(String.t()) => DateTime.t()},
          pending_dispatches: [map()],
          pending_results: [attempt()],
          visible_attempts: [attempt()],
          scheduled_attempts: [attempt()],
          next_visible_at: DateTime.t() | nil,
          expired_claims: [attempt()],
          attempts: [attempt()],
          anomalies: [map()]
        }

  @enforce_keys [
    :run_id,
    :workflow,
    :queue,
    :status,
    :reason,
    :terminal?,
    :terminal_status,
    :thread_revisions
  ]

  defstruct [
    :run_id,
    :partition,
    :workflow,
    :trigger,
    :input,
    :started_at,
    :definition_version,
    :parent_run,
    :replayed_from_run_id,
    :queue,
    :status,
    :reason,
    :terminal?,
    :terminal_status,
    :terminal_at,
    :terminal_error,
    :deadline,
    :thread_revisions,
    continuation: %{continued_from: nil, continued_to: nil},
    definition_migrations: [],
    history: %{
      thread_revision: 0,
      level: :normal,
      warning_threshold: 5_000,
      critical_threshold: 20_000
    },
    command_history: [],
    jido_signals: %{pending_count: 0, delivered_count: 0, items: []},
    manual_state: nil,
    child_runs: [],
    dynamic_work: [],
    graph_version: 0,
    graph_provenance: %{nodes: [], edges: []},
    active_node_ids: [],
    active_edge_ids: [],
    ready_node_ids: [],
    blocked_node_ids: [],
    tombstoned_node_ids: [],
    tombstoned_edge_ids: [],
    mutation_history: [],
    reconciliation_status: :not_required,
    guardrails: [],
    planned_runnables: [],
    planned_runnable_keys: [],
    applied_runnable_keys: [],
    applied_at: %{},
    pending_dispatches: [],
    pending_results: [],
    visible_attempts: [],
    scheduled_attempts: [],
    next_visible_at: nil,
    expired_claims: [],
    attempts: [],
    anomalies: [],
    context: %{}
  ]
end
