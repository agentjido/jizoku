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

## Runtime Command Signals

- Treat `Squidie.Runtime.Signal` as the Squidie-native command envelope for
  runtime control.
- Public control APIs and `Squidie.apply_signal/2` must apply signals through
  the journal runtime, including starts, cron starts, replays, cancellation, and
  manual controls.
- Use `Squidie.Runtime.Signal.JidoAdapter` only at the Jido interop boundary
  for agents, routers, or other Jido primitives. Do not leak raw `Jido.Signal`
  into normal workflow authoring.
- Preserve `:run_signal_received` command history for applied commands.
- Reusing an idempotency key means duplicate delivery. A different idempotency
  key is a different command and must not be silently collapsed.

## Storage

- Keep `Squidie.Runtime.Journal.Storage` as the Squidie-owned storage
  boundary.
- Default to Ecto/Postgres-backed Jido storage for documented host setup.
- Keep the boundary database-agnostic, but require production adapters to
  provide ordered appends, conflict detection, deterministic replay, durable
  checkpoint reads, and trusted configuration.
- Never derive `journal_storage` from request input.
