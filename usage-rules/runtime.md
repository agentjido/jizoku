# Squidie Runtime Usage Rules

## Journal Runtime

- Treat the Jido journal runtime as the only execution path.
- Treat journal entries as the durable source of truth.
- Treat checkpoints as rebuild accelerators, not authority.
- Preserve ordered per-thread appends and optimistic conflict detection.
- Rebuild workflow and dispatch projections from persisted facts after
  conflicts, restarts, or checkpoint loss.
- Keep `claim_id` and claim-token fences on heartbeat, completion, and failure.
- Store only claim token hashes in durable entries.
- Apply completed dispatch results to the run thread only after completion is
  durable in the dispatch thread.
- Preserve terminal-run fencing: later claims, completions, manual actions, or
  wakeups for a terminal run must not mutate terminal state.
- Preserve continue-as-new ordering: complete and fence the native source in
  one dispatch append, then apply the source, record continuation intent, and
  terminalize the predecessor in one run-thread append before exposing the
  deterministic successor.
- Treat continuation activation as a host-level all-workers readiness gate.
  Never accept request-level activation overrides, and never gate recovery of
  a fence that is already durable.
- Preserve child-run lineage as durable journal facts. Child starts must be
  idempotent for the parent run, parent step, child workflow, child trigger, and
  `child_key`.
- Preserve dynamic-work records as validated journal facts. `record_dynamic_work/3`
  remains inspection-only; `schedule_dynamic_work/3` must append the
  dynamic-work fact and planned runnable intents together before dispatch
  scheduling.
- Reject missing, unknown, disabled, or incompatible dynamic node action keys
  before scheduling executable dynamic work. `schedule_dynamic_work/3` requires
  the host-owned `:action_registry`.
- Require the dynamic-work origin runnable to be applied before scheduling
  executable dynamic nodes.
- Treat dynamic edges as inspection metadata until dependency-ordered dynamic
  scheduling is explicitly implemented.
- Treat scheduled dynamic nodes as replay-unsafe by default unless a future
  API persists a stronger host-owned recovery policy.
- Treat `:dynamic_graph_mutated` plus its planned runnables as one run-thread
  commit. Dispatch follows that commit and is repaired idempotently through
  `reconcile_dynamic_graph/2` after partial failure.
- Fence graph mutations with their semantic graph version, keep mutation IDs
  idempotent, and never schedule blocked or tombstoned nodes.

## Execution

- Execute visible work through `Squidie.execute_next/1`.
- Use a stable `owner_id` for workers when possible.
- Use `heartbeat_interval_ms` on `Squidie.execute_next/1` for long-running
  steps that may exceed the journal claim lease window. Keep intervals at or
  above the runtime minimum and large enough to avoid unnecessary journal write
  volume.
- Keep internal execution controls private; public callers must not pass claim
  tokens or private runner options.
- Keep external backend leases separate from journal claim leases; Bedrock,
  Oban, or another host scheduler must renew its own delivery lease if needed.
- Retry scheduling must be durable journal intent with a future `visible_at`.
- Built-in `:wait` must create delayed journal intent instead of sleeping in a
  worker.
- Deferred continuation must persist a same-step planned runnable with deferred
  metadata and future `visible_at`; it must not be represented as failure or
  consume retry budget.
- Cancellation, replay, pause, approval, rejection, and unblock behavior must
  append durable facts before exposing success.
- Starting a child run must append parent lineage and start the child as one
  repairable journal operation; stale parent contexts and terminal parent runs
  must be rejected at the boundary.
- Previewing or recording dynamic work must not schedule dispatch attempts,
  change dependency readiness, or mutate terminal-state decisions.
- Scheduling dynamic work must use durable planned runnable intents and the
  normal `Squidie.execute_next/1` claim, completion, failure, and application
  path.
- Dynamic node retry must be persisted in the planned runnable metadata; do not
  recover retry behavior from current host code alone.
- Terminal runs must reject new dynamic-work previews, records, and schedules.
- After a continuation predecessor is terminal, ordinary scheduling, claims,
  completion, failure, heartbeat, manual control, and wakeups must remain
  fenced. Exact retries must repair or return the same successor.

## Runtime Command Signals

- Treat `Squidie.Runtime.Signal` as the Squidie-native command envelope for
  runtime control.
- Public control APIs and `Squidie.apply_signal/2` must apply signals through
  the journal runtime, including starts, cron starts, replays, cancellation, and
  manual controls.
- Pass recognized command `Jido.Signal` envelopes from agents, routers, or
  other Jido primitives directly to `Squidie.apply_signal/2`. Use
  `Squidie.Runtime.Signal.JidoAdapter` for outbound conversion. Route arbitrary
  domain signals only through a host-owned `Squidie.Jido.SignalResolver` that
  returns a closed start or run-control command.
- Preserve runtime routing authority outside resolvers. Resolver output cannot
  select storage, queues, dispatch adapters, or modules derived from signal
  strings.
- Fence each domain signal by partition plus CloudEvents source and ID before
  applying its resolved command. Exact retries must reuse the persisted
  resolver decision and queue without invoking current resolver code; changed
  envelopes with the same identity fail closed.
- Preserve `:run_signal_received` command history for applied commands.
- Preserve the signal ID and normalized trace through the Jido adapter and
  durable command receipt. Preserve an external Jido source as audit
  provenance. Create a command root trace only when one is absent.
- Keep one durable span per runnable across schedule, claim, heartbeat,
  completion/failure, and application. Persist child spans for new work and
  give replay a fresh command lineage.
- Reusing an idempotency key means duplicate delivery. A different idempotency
  key is a different command and must not be silently collapsed.

## Telemetry

- Emit lifecycle point events only from successful journal appends, in append
  order. Rebuilds, checkpoints, conflicts, stale operations, and duplicate
  no-ops must not emit lifecycle points.
- Buffer completion event intents inside Squidie-owned Ecto step transactions;
  flush after commit and discard on rollback or non-local exit.
- Keep telemetry metadata on the public allowlist. Never emit payloads,
  results, raw errors, arbitrary metadata, claim/owner values, idempotency keys,
  credentials, or trace state.
- Keep correlation identifiers out of built-in metric tags. Partition is an
  explicit metric opt-in because it may be high-cardinality.
- Treat telemetry as best-effort. Journal facts remain authoritative and
  handler failures must not change runtime results.

## Storage

- Keep `Squidie.Runtime.Journal.Storage` as the Squidie-owned storage
  boundary.
- Treat `:partition` as part of every durable workflow identity. Scope run,
  dispatch, workflow-index, global-catalog, and checkpoint keys together; never
  search or fall back to another partition.
- Preserve the exact legacy storage namespace when `:partition` is omitted.
- Propagate the selected partition through signals, cron payloads, native step
  contexts, child runs, recovery agents, and read models. Child runs inherit the
  parent partition and reject an explicit mismatch.
- Validate partitions at trusted host boundaries. A partition is a namespace,
  not an authorization or row-level security policy.
- Default to Ecto/Postgres-backed Jido storage for documented host setup.
- Keep the boundary database-agnostic, but require production adapters to
  provide ordered appends, conflict detection, deterministic replay, durable
  checkpoint reads, and trusted configuration.
- Never derive `journal_storage` from request input.
