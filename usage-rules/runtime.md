# Squid Mesh Runtime Usage Rules

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
- Preserve dynamic-work records as validated, inspection-only journal facts
  until executable dynamic graph expansion is explicitly supported.
- When `:action_registry` is supplied for dynamic work, reject missing, unknown,
  disabled, or incompatible dynamic node action keys before previewing or
  appending.

## Execution

- Execute visible work through `SquidMesh.execute_next/1`.
- Use a stable `owner_id` for workers when possible.
- Keep internal execution controls private; public callers must not pass claim
  tokens or private runner options.
- Retry scheduling must be durable journal intent with a future `visible_at`.
- Built-in `:wait` must create delayed journal intent instead of sleeping in a
  worker.
- Cancellation, replay, pause, approval, rejection, and unblock behavior must
  append durable facts before exposing success.
- Starting a child run must append parent lineage and start the child as one
  repairable journal operation; stale parent contexts and terminal parent runs
  must be rejected at the boundary.
- Previewing or recording dynamic work must not schedule dispatch attempts,
  change dependency readiness, or mutate terminal-state decisions.
- Terminal runs must reject new dynamic-work previews and records.

## Runtime Command Signals

- Treat `SquidMesh.Runtime.Signal` as the Squid Mesh-native command envelope for
  runtime control.
- Public control APIs and `SquidMesh.apply_signal/2` must apply signals through
  the journal runtime, including starts, cron starts, replays, cancellation, and
  manual controls.
- Use `SquidMesh.Runtime.Signal.JidoAdapter` only at the Jido interop boundary
  for agents, routers, or other Jido primitives. Do not leak raw `Jido.Signal`
  into normal workflow authoring.
- Preserve `:run_signal_received` command history for applied commands.
- Reusing an idempotency key means duplicate delivery. A different idempotency
  key is a different command and must not be silently collapsed.

## Storage

- Keep `SquidMesh.Runtime.Journal.Storage` as the Squid Mesh-owned storage
  boundary.
- Default to Ecto/Postgres-backed Jido storage for documented host setup.
- Keep the boundary database-agnostic, but require production adapters to
  provide ordered appends, conflict detection, deterministic replay, durable
  checkpoint reads, and trusted configuration.
- Never derive `journal_storage` from request input.
