# Observability

Squidie is observable through durable runtime state first. Host applications
inspect the journal-backed read models, graph output, and explanation
diagnostics for authoritative state. Squidie also emits a stable public
`:telemetry` contract under `[:squidie, :runtime, ...]` for live measurements.

Telemetry is a best-effort operational signal, not a replacement for the
journal. Squidie installs no reporter, exporter, logger integration, dashboard,
or alert policy; the host application owns those choices.

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

All of these surfaces expose the selected `partition`. Treat
`{partition, run_id}` as the minimum run identity in dashboards, links, caches,
logs, and operator commands because the same run UUID may exist in multiple
partitions. Fleet queries are partition-local and do not search other
partitions. Host authorization remains mandatory; the partition is a storage
namespace, not an access-control decision.

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

## Runtime Telemetry

`Squidie.Telemetry.events/0` returns every public event. There are three span
boundaries:

| Operation | Event prefix |
| --- | --- |
| Runtime command application | `[:squidie, :runtime, :command, :apply]` |
| Worker execution poll | `[:squidie, :runtime, :executor, :execute_next]` |
| Actual step invocation | `[:squidie, :runtime, :step, :execute]` |

Each prefix emits `:start` followed by either `:stop` or `:exception`:

- `:start` measurements are `system_time` and `monotonic_time`.
- `:stop` measurements are `duration` and `monotonic_time`.
- `:exception` measurements are `duration` and `monotonic_time`.
- `outcome` is `:unknown` on start, `:ok` for an ordinary result, `:error`
  when the operation returns `{:error, reason}`, and `:exception` for a raise,
  throw, or exit.

Raw error reasons, exceptions, and stacktraces are never included in span
metadata. Step spans cover only the action invocation; durable completion,
failure, result application, and successor planning happen outside that span
but inside the executor span.

The runtime also emits these committed lifecycle point events:

| Area | Events |
| --- | --- |
| Commands and runs | `:command, :received`; `:run, :started`; `:run, :terminal` |
| Runnables | `:runnable, :planned`; `:runnable, :applied` |
| Attempts | `:attempt, :scheduled`; `:retry_scheduled`; `:claimed`; `:heartbeat`; `:completed`; `:failed` |
| Control and branching | `:manual, :paused`; `:manual, :resolved`; `:child, :started`; `:dynamic_work, :recorded` |

All point names start with `[:squidie, :runtime]`. Point measurements are
`%{count: 1, system_time: integer}`. One `:runnable, :planned` event is emitted
for each runnable in a committed planning fact. A failed attempt that durably
schedules a retry emits both `:attempt, :failed` and
`:attempt, :retry_scheduled` in journal order.

### Metadata And Trace Correlation

Metadata is event-specific and keys may be absent when the value is not
available. The complete allowlist is:

- bounded dimensions: `queue`, `workflow`, `step`, `outcome`, `status`,
  `command_type`, `retry_state`, `partition`, `attempt_number`, `action`, and
  `kind`
- correlation fields: `run_id`, `signal_id`, `runnable_key`, `trace_id`,
  `span_id`, `parent_span_id`, `causation_id`, `child_run_id`, and
  `dynamic_key`

Metadata values are atoms, integers, or valid non-empty strings of at most 255
bytes. Squidie drops every non-allowlisted or invalid value. In particular,
events never publish workflow payloads, step inputs or results, raw errors,
arbitrary command/manual metadata, actors, comments, claim or owner values,
idempotency keys, credentials, or trace state.

Runtime commands carry an optional W3C-compatible trace. Squidie creates a root
trace at the command boundary when one is missing, preserves it on the run, and
persists child spans for runnable lineage. Trace IDs are 32 lowercase hexadecimal
characters and span IDs are 16; all-zero identifiers are rejected. A runnable
keeps the same durable span across schedule, claim, heartbeat, completion or
failure, and result application, even when different workers execute it.
Successors, retries, deferred continuations, compensation, child work, and
dynamic work receive persisted child spans. Replay starts a fresh command
lineage rather than inheriting the source run's trace.

`Squidie.Runtime.Signal.JidoAdapter` preserves the Jido/CloudEvents envelope ID
and carries trace correlation through the Jido `"correlation"` extension. It
does not rely on process-dictionary trace state.

### Metrics And Cardinality

`Squidie.Telemetry.metrics/0` returns reporter-neutral `Telemetry.Metrics`
definitions for span durations and exceptions plus command, run, runnable, and
selected attempt counters. The defaults use bounded tags such as `workflow`,
`step`, `queue`, `status`, `command_type`, and `outcome`. Correlation fields
such as `run_id`, `signal_id`, `runnable_key`, and trace IDs are never built-in
metric tags.

Partition can be high-cardinality in multi-tenant systems, so it is excluded
from the defaults. Use `Squidie.Telemetry.partition_metrics/0` only when the
host has reviewed and accepted that cardinality. High-volume heartbeat,
claimed-attempt, manual, child, and dynamic-work events remain available for
custom metrics but are not in the recommended default set.

A host metrics module can expose the definitions to its selected reporter:

```elixir
def metrics do
  application_metrics() ++ Squidie.Telemetry.metrics()
end
```

The concrete reporter module and its supervision/configuration remain
host-owned.

### Delivery And Transaction Boundaries

Lifecycle point events are emitted only after the storage adapter reports a
successful journal append. Conflicts, stale claims, semantic duplicate no-ops,
checkpoint writes, projection rebuilds, and replaying source history do not
emit lifecycle points. Append batches preserve event order.

For a Squidie-owned `transaction: :repo` step, completion-related event intents
are buffered until the outer Ecto transaction commits. A rollback, returned
transaction error, raise, throw, or exit discards them. On success, buffered
points flush before the enclosing executor `:stop` event. Heartbeats use their
own storage path and are not held behind the step transaction.

These guarantees do not extend through an arbitrary caller-owned outer
`Repo.transaction/1` that Squidie does not control. Telemetry handlers are also
best-effort: handler failures do not change runtime results, and a VM crash
after commit but before emission can lose an event. Squidie does not guarantee
exactly-once telemetry delivery; use journal-backed read models for durable
reconciliation or add a host-owned durable outbox when that guarantee is
required.

### Host Telemetry

Host applications may add their own spans around Squidie calls when they need
application-specific dimensions:

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
