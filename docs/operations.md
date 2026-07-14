# Operations Guide

This guide covers the operational boundaries Squidie expects host
applications to own.

## Runtime Guarantees

Squidie currently guarantees:

- durable run, step, attempt, dispatch, and manual-control facts in the configured journal storage
- durable queued and scheduled workflow intent through journal dispatch facts
- workflow-level retry, replay, inspection, and cancellation on top of that durable state

Squidie does not currently claim:

- exactly-once external side effects
- replacing every backend-specific worker lease or heartbeat system
- dynamic cron registration after boot

## Idempotent Step Design

Any step that talks to an external system should be idempotent at that
boundary.

Recommended patterns:

- include an application-owned idempotency key in the external request
- persist enough domain state to detect duplicate delivery
- treat remote `409` or duplicate acknowledgements as success when appropriate
- for payment providers, pass a stable key derived from the workflow run, step,
  and domain operation rather than generating a fresh key on each attempt

Avoid:

- steps that produce irreversible side effects without a duplicate strategy
- relying on "this step should only run once" as the safety model

## Worker Sizing

Squidie workers pull visible journal attempts through `Squidie.execute_next/1`,
so worker sizing stays a host-app decision.

Recommended starting point:

- dedicate a small supervised worker pool to Squidie execution
- isolate higher-cost workflow traffic from unrelated app jobs
- size concurrency conservatively, then increase based on visible attempt depth

If workflows perform mostly I/O:

- a moderate queue limit is usually fine

If workflows call slow external systems:

- keep limits lower
- prefer backoff and queue isolation over large worker counts

## Retries And Backoff

Workflow-step retries are owned by Squidie, not by the host job backend's
retry counter.

Jido action retries are also disabled at the Squidie runtime boundary so one
workflow attempt maps to one persisted step attempt.

Recommended practice:

- declare retries only on steps that own recoverable work
- prefer bounded exponential backoff
- surface structured errors from steps so retry behavior is understandable in inspection
- mark non-compensatable external side effects with `irreversible: true` or
  `compensatable: false` so replay requires explicit operator approval
- mark error transitions with `recovery: :compensation` or `recovery: :undo`
  when the operational response differs

Example:

```elixir
step :check_gateway_status, MyApp.Steps.CheckGatewayStatus,
  retry: [max_attempts: 5, backoff: [type: :exponential, min: 1_000, max: 30_000]]
```

## Compensation Versus Undo

Compensation is a forward recovery action that reconciles partial work. Undo is
a reversal of local work the application still controls. Keep those paths
explicit in the workflow so operators can tell whether a failure was reconciled
or reversed:

```elixir
transition(:capture_payment, on: :error, to: :issue_credit, recovery: :compensation)
transition(:reserve_inventory, on: :error, to: :release_inventory, recovery: :undo)
```

When Squidie routes through one of these transitions, inspection history
shows the failed step's `recovery.failure` decision and emits either
`:compensation_routed` or `:undo_routed` in `audit_events`.

## Local Transaction Boundaries

`transaction: :repo` gives one custom step a same-process host repo transaction.
It is useful for local database groups that should commit or roll back together
before Squidie advances the durable workflow:

```elixir
step :post_local_ledger_entries, MyApp.Steps.PostLocalLedgerEntries,
  transaction: :repo
```

Operational boundary:

- the action callback runs in the worker process inside `config.repo.transaction/1`
- `{:ok, output}` commits the host repo transaction, then Squidie persists
  the step result and dispatches successors in its normal durable transaction
- `{:error, reason}` rolls back the host repo transaction, then Squidie
  persists the failed step and applies retry or failure routing
- a crash after local commit but before Squidie persists progress can still
  be redelivered, so local transaction groups should use natural keys,
  uniqueness, or other idempotency guards when duplicate local writes matter
- this option does not cover external APIs, downstream steps, runtime dispatch, or
  saga compensation callbacks

Keep this option for small local write groups. If a boundary crosses services,
queues, or later workflow steps, model recovery explicitly with retries,
compensation callbacks, or `:error` transitions.

## Replay After Irreversible Side Effects

Replay starts a new run from the original payload. That is useful for
recoverable workflows, but it can repeat external effects that have already
happened.

When a completed source run contains a step marked `irreversible: true` or
`compensatable: false`, Squidie blocks replay by default:

```elixir
{:error, {:unsafe_replay, details}} = Squidie.replay(run_id)
```

Operator tooling should show `details.steps` and require a deliberate decision
before retrying. If the operator accepts the risk, pass the explicit override:

```elixir
Squidie.replay(run_id, allow_irreversible: true)
```

Use this path only after checking the external system or domain records. The
marker changes Squidie recovery semantics; it does not make a payment,
message, shipment, or webhook idempotent.

## Backend-Owned Leases And Fencing

Lease-capable delivery backends should own queue delivery, claim expiry,
heartbeats, retry timing, and worker recovery. Squidie keeps the
workflow-facing facts durable: runnable identity, attempt history,
workflow-state mutation fences, completion, failure, cancellation, and
inspection.

Recommended lease settings:

- choose a lease timeout longer than the expected gap between heartbeats plus
  normal scheduler and database latency
- heartbeat often enough that one missed heartbeat does not expire healthy work
- use stable run, step, attempt, and domain-operation identifiers as backend
  work item keys or lineage metadata

For a concrete backend-neutral shape with Bedrock as the recommended lease
backend, see the Bedrock setup section in `docs/host_app_integration.md`.

Completion, failure, pause, and approval progression must be applied only by
the current attempt owner. If an expired attempt is reclaimed and a newer
attempt takes over, a stale worker is rejected before it can mutate step
history or run state. This protects Squidie's durable state from stale
workers, but it does not make external side effects exactly once. External API
calls still need idempotency keys or domain-level duplicate detection.

## Long Waits

Built-in `:wait` steps are non-blocking because they append delayed journal
runnable intent instead of sleeping inside a worker.

Still, long waits have real operational cost:

- more scheduled jobs
- longer-lived run records
- more delayed work to reason about during incidents

Recommended practice:

- keep `:wait` for workflow-scale delays, not arbitrary timers everywhere
- prefer application scheduling or cron triggers when the delay is really about when the workflow should start
- avoid extremely large waits unless the workflow truly needs to remain in-flight

## Cron Activation

Cron triggers are declared in the workflow but activated by the host app through
its scheduler.

Current boundary:

- activation is static at boot
- the host scheduler owns recurring scheduling
- Squidie turns the delivered cron payload into a normal journal-backed run
  start

Recommended practice:

- treat cron workflows as deploy-time configuration
- review cron registrations alongside the host app's scheduler setup
- keep payload defaults complete so cron runs do not rely on manual input

## Operational CLI Diagnostics

Squidie includes two read-only Mix tasks for host-worker rollout checks and
incident triage:

```sh
mix squidie.status
mix squidie.doctor
```

`mix squidie.status` summarizes run counts by workflow, queue, and status, plus
visible, scheduled, and claimed attempt depth; the next scheduled visibility
time; pending dispatches and results; and manual-intervention counts.
The report includes the configured partition, and every collected run and queue
row is scoped to that partition.

`mix squidie.doctor` checks configuration, expired claims, overdue attempts,
terminal runs with pending facts, manual-action queues, projection anomalies,
and the installed Squidie database schema. Every finding includes a direct next
action. Neither task schedules work, applies results, reclaims claims, runs
migrations, or rewrites journal history.

Both tasks support machine-readable output:

```sh
mix squidie.status --json
mix squidie.doctor --json
```

The JSON contract is versioned with `schema_version` and excludes workflow
payloads, attempt inputs and results, claim tokens, owner metadata, and command
history. Both reports include the configured `partition` so operators can keep
otherwise identical run and queue identities distinct. Use an explicit schema
gate in CI after host migrations:

```sh
mix squidie.doctor --json --fail-on-drift
```

The gate exits nonzero only when required schema objects are missing or
incompatible. Custom non-Ecto journal storage reports the SQL schema check as
not applicable. Catalog permission or connection failures are reported as
unavailable rather than mislabeled as drift.

These commands provide fleet and queue summaries. Use `Squidie.list_runs/2`
for host-owned run indexes and `Squidie.inspect_run/2` or
`Squidie.explain_run/2` for one run's authorized detail. Web dashboards remain
host-owned. Effective heartbeat settings also remain part of host worker calls
to `Squidie.execute_next/1`; doctor cannot infer arbitrary worker options from
static application configuration.

## Observability

At minimum, production deployments should capture:

- run counts by workflow, queue, and status
- visible-attempt depth and journal worker throughput
- scheduled attempts, expired claims, and manual intervention queues
- terminal outcomes, anomalies, and operator explanations
- Squidie's public runtime telemetry for committed lifecycle counts and
  command, executor, and step spans
- host-owned reporters, exporters, dashboards, alerts, and structured logs
  around worker and domain boundaries

Recommended reading:

- [Observability](observability.md)
- [Architecture](architecture.md)
