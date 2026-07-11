# Observability

Squidie is observable through durable runtime state first. Host applications
inspect the journal-backed read models, graph output, explanation diagnostics,
and their own worker logs or metrics.

Squidie does not currently expose a public `:telemetry` event contract under
the `[:squidie, ...]` prefix. Treat telemetry event names and metric labels
as host-app concerns until a dedicated runtime telemetry API exists.

## Runtime State Surfaces

Use these public APIs as the stable observability boundary:

- `Squidie.list_runs/2` - redacted run index rows for dashboards and queue
  views.
- `Squidie.inspect_run/2` - one run's durable state, including attempts,
  visible work, scheduled work, expired claims, manual state, context, and
  anomalies.
- `Squidie.inspect_run_graph/2` - graph-oriented node and edge state for UI
  builders.
- `Squidie.inspect_run_timeline/2` - chronological operator events for trace
  views without parsing raw journal entries.
- `Squidie.explain_run/2` - operator-facing reason, details, evidence, and
  next actions.

`list_runs/2` intentionally stays narrow. It exposes lookup and status fields
without attempt inputs, outputs, errors, claim metadata, or idempotency keys.
Use `inspect_run/2` only after selecting a specific run and applying the host
app's authorization rules.

For read-only fleet and queue summaries outside a dashboard, use
`mix squidie.status`; use `mix squidie.doctor` for configuration, claim,
pending-fact, manual-action, anomaly, and schema diagnostics. See
[Operational CLI Diagnostics](operations.md#operational-cli-diagnostics).

## Redaction And Field Selection

Treat Squidie observability data as three tiers:

| Tier | Examples | Suggested use |
| --- | --- | --- |
| Index-safe | `run_id`, workflow, queue, status, terminal status, indexed time | Run lists, dashboards, queue counters. |
| Operator detail | reason, visible/scheduled attempt counts, next visibility time, manual step, anomaly count | Support views and incident pages after authorization. |
| Sensitive detail | run input, durable context, attempt input/output/error, idempotency keys, claim IDs, owner IDs, manual metadata | Privileged audit views only, with host redaction. |

`inspect_run/2` and `inspect_run_graph/2` can expose host-domain data because
step inputs, outputs, errors, manual metadata, and durable context come from the
embedding application. Squidie cannot know which fields are customer data,
provider responses, tokens, or internal notes. Apply an allow-list at the HTTP,
LiveView, CLI, or API boundary instead of serializing the full snapshot by
default.

`Squidie.ReadModel.Visibility.redact/2` and
`Squidie.ReadModel.Visibility.redact/3` provide the built-in projection
helper for that boundary. For comprehensive documentation on actor visibility
and redaction patterns, see the [Actor Visibility Guide](./actor_visibility.md). The helper accepts an existing listing summary,
inspection snapshot, graph inspection, timeline, or explanation diagnostic plus
a host-owned actor. The two-arity form defaults to `:external`; the three-arity
form accepts a host policy. Policies may return `:external`, `:operator`, or
`:auditor`; auditor views preserve the full read model, while external and
operator views keep high-level runtime status and current/manual task shape
without payloads, command history, claim metadata, or attempt results.
The helper also applies conservative nested redaction to JSON-ready maps, which
is useful after calling `Squidie.Runs.GraphInspection.to_map/1`.

```elixir
defmodule MyApp.SquidieVisibility do
  def visibility_scope(%{role: :auditor}, _view), do: :auditor
  def visibility_scope(%{role: :support}, _view), do: :operator
  def visibility_scope(_actor, _view), do: :external
end

{:ok, snapshot} = Squidie.inspect_run(run_id, include_history: true)

{:ok, visible_snapshot} =
  Squidie.ReadModel.Visibility.redact(
    snapshot,
    current_actor,
    MyApp.SquidieVisibility
  )
```

For example, an operator summary can keep runtime state while dropping step
payloads:

```elixir
def operator_summary(snapshot) do
  manual_state = snapshot.manual_state || %{}

  %{
    run_id: snapshot.run_id,
    workflow: snapshot.workflow,
    queue: snapshot.queue,
    status: snapshot.status,
    reason: snapshot.reason,
    visible_attempt_count: length(snapshot.visible_attempts),
    scheduled_attempt_count: length(snapshot.scheduled_attempts),
    next_visible_at: snapshot.next_visible_at,
    manual_step: Map.get(manual_state, :step) || Map.get(manual_state, "step"),
    anomaly_count: length(snapshot.anomalies)
  }
end
```

For graph views, prefer `inspect_run_graph/2` without `include_history: true`
unless the viewer needs input, output, error, manual-state, or attempt detail.
When history is enabled, redact each node's `input`, `output`, `error`,
`manual_state`, and `attempts` fields before exposing the payload outside a
trusted operator surface.

Use the same rule for metrics and logs: record counts, statuses, queues,
workflow names, and reason categories. Avoid user-provided payload fields,
provider responses, idempotency keys, claim identifiers, and raw errors as
labels or log fields.

## What To Measure

The read model gives host apps enough durable state to derive useful operational
signals:

| Signal | Source | Why it matters |
| --- | --- | --- |
| Run counts by workflow, queue, and status | `list_runs/2` | Tracks volume, completion rate, and backlog shape. |
| Visible attempt depth | `inspect_run/2.visible_attempts` | Shows work that workers can claim now. |
| Scheduled attempt depth and next wakeup | `scheduled_attempts`, `next_visible_at` | Shows delayed retries, waits, and future-visible work. |
| Claimed or expired attempts | `attempts`, `expired_claims` | Identifies workers that are busy, stalled, or recoverable. |
| Pending dispatch/results | `pending_dispatches`, `pending_results` | Detects journal facts that need runtime reconciliation. |
| Manual intervention count | `manual_state` and status `:paused` | Drives approval queues and operator SLAs. |
| Deadline health | `deadline`, attempt `deadline`, node `deadline` | Shows on-time, due-soon, overdue, and escalated workflow work without exposing payloads. |
| Terminal outcomes | `terminal?`, `terminal_status` | Tracks completed, failed, cancelled, and replayed work. |
| Runtime anomalies | `anomalies` | Surfaces inconsistent or malformed durable facts. |

For dashboards, start with `list_runs/2`, then inspect selected runs with
history only when the caller needs detailed attempts or audit evidence.
Deadline alerting belongs at the host boundary: use Squidie's deadline state
as durable evidence, then route notifications or operator actions through the
host application's policy and authorization layer.

## Operator Explanations

`explain_run/2` is the highest-signal surface for support tooling. It condenses
the inspection snapshot into:

- `reason` - the runtime state category, such as `:attempt_visible`,
  `:attempt_scheduled_for_later`, `:manual_intervention_required`,
  `:expired_claim`, or `:terminal`.
- `summary` and `details` - a short explanation plus structured state.
- `next_actions` - safe host/operator actions, such as waiting for a worker,
  resolving a manual step, recovering an expired claim, or inspecting a
  terminal run.
- `evidence` - thread revisions, attempt counts, planned/applied runnable keys,
  manual state, command history, duplicate command evidence, next visibility
  time, and anomalies.

When command receipt facts are present, `details.latest_command` identifies the
latest runtime command that led to the current state. `evidence.command_history`
keeps the redacted command audit trail, `evidence.command_counts` summarizes
command types, and `evidence.duplicate_commands` makes at-least-once command
delivery visible without exposing raw Jido internals.

Use this for incident pages, CLI output, and support views where raw journal
facts would be too noisy.

## Graph Output

`inspect_run_graph/2` presents the same durable state as workflow nodes and
edges. It is useful when a host UI needs to show:

- current nodes
- completed, pending, retrying, failed, skipped, and paused nodes
- selected transition edges
- dependency edges and pending joins
- manual-state detail when history is included

For JSON or LiveView boundaries, call `Squidie.Runs.GraphInspection.to_map/1`
after applying the host app's authorization and redaction policy. See
[Graph inspection contract](graph_inspection.md) for the stable map shape.

## Logs

Squidie emits application logs only for explicit built-in `:log` workflow
steps. It does not currently attach automatic logger metadata such as `run_id`,
`workflow`, `step`, or `attempt` to every runtime log.

If a host app needs correlated logs, wrap worker execution and host boundaries
with its own logger metadata:

```elixir
Logger.metadata(queue: queue, worker: worker_id)
Squidie.execute_next(queue: queue, owner_id: worker_id)
```

For step-specific external calls, prefer logging at the host boundary or inside
native `Squidie.Step` modules, and avoid logging secrets, claim tokens,
payloads, or raw provider responses.

## Host Telemetry

Host applications can still emit their own telemetry around Squidie calls:

```elixir
:telemetry.span(
  [:my_app, :squidie, :execute_next],
  %{queue: queue, worker: worker_id},
  fn ->
    result = Squidie.execute_next(queue: queue, owner_id: worker_id)
    {result, %{result: elem(result, 0)}}
  end
)
```

Keep host telemetry labels low-cardinality. Good labels include queue, workflow,
status, and result category. Avoid `run_id`, claim tokens, idempotency keys,
raw errors, or user-provided payload fields as metric labels.

## Related Reading

- [Getting started](getting_started.md) shows the inspection and explanation
  APIs in a small runnable workflow.
- [Graph inspection contract](graph_inspection.md) documents the node and edge
  payload for host UIs.
- [Host app integration](host_app_integration.md) shows where host apps wrap
  worker loops, inspection, and manual-control APIs.
- [Operations](operations.md) covers production concerns such as retries,
  waits, cancellation, and cron activation.
