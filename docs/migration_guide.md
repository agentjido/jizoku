# Migrating from Squidie

Jizoku is the new name and package identity for the runtime previously released
as Squidie. This is an explicit breaking upgrade, not an automatic package
replacement. Applications may remain pinned to the old package and release
tags until their operators choose a migration window.

## Rename map

| Previous | New |
| --- | --- |
| Hex dependency `:squidie` | Hex dependency `:jizoku` |
| Module namespace `Squidie` | Module namespace `Jizoku` |
| OTP/config application `:squidie` | OTP/config application `:jizoku` |
| `mix squidie.install` | `mix jizoku.install` |
| `mix squidie.status` | `mix jizoku.status` |
| `mix squidie.doctor` | `mix jizoku.doctor` |
| `[:squidie, :runtime, ...]` telemetry | `[:jizoku, :runtime, ...]` telemetry |
| `squidie.runtime.*` Jido command types | `jizoku.runtime.*` Jido command types |
| `squidie_journal_*` tables | `jizoku_journal_*` tables |

Update workflow modules, native steps, runtime calls, configuration, telemetry
handlers, command producers, worker names, Mix tasks, and operational tooling as
one application release.

## Cutover boundary

Jizoku does not read Squidie journal entries or checkpoints. Before deploying
Jizoku:

1. Stop new starts, cron activation, inbound signals, and delivery producers.
2. While the previous release is still running, use `mix squidie.status` and its
   normal operational inspection to verify the old runtime has no claimed
   attempts, pending dispatch, future retries, paused approvals, continuations,
   or undelivered Jido signals. Drain, cancel, or explicitly archive every
   remaining nonterminal run.
3. Stop every Squidie worker and backend consumer.
4. Back up the database and export any history that must remain operator-visible.
5. Run `mix jizoku.install` and migrate the new `jizoku_journal_*` schema.
6. Deploy Jizoku and begin only new Jizoku runs.

Do not run Squidie and Jizoku workers together against one environment. Leave
the old tables untouched through the rollback and audit-retention window.

## Rollback

Stop Jizoku and restore the previous application release against the untouched
Squidie tables. Do not point old workers at Jizoku tables or attempt to replay
Jizoku data through the previous runtime.

Dropping or externally archiving the old tables is a separate destructive
operation that should happen only after the rollback and retention periods end.
