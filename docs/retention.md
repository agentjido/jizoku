# Archive and Retention Operations

Retention has two distinct phases. Archive is a reversible journal state that
hides a terminal run from default listings while preserving direct inspection.
Deletion is an irreversible, explicitly confirmed Ecto operation that removes
the selected run history and its secondary references.

Jizoku does not choose a legal or regulatory retention period. The host owns
the schedule, authorization, partition selection, export requirements, backup
policy, and review of every deletion plan.

## Safety Boundary

Only archived terminal runs that are fully reconciled are eligible. Active
claims, visible or pending dispatch work, unresolved results, anomalies,
incomplete child or continuation lineage, and unarchived runs are blocked. A
trusted host policy can add a legal-hold or export block; it cannot waive an
intrinsic runtime check:

```elixir
defmodule MyApp.RetentionPolicy do
  @behaviour Jizoku.Retention.Policy

  def evaluate(snapshot, _context, opts) do
    if MyApp.Holds.blocked?(snapshot.run_id, opts) do
      {:block, :legal_hold}
    else
      :allow
    end
  end
end

config :jizoku,
  retention_policy: {MyApp.RetentionPolicy, export_required: true}
```

Keep policy results bounded and non-sensitive. Complete any required export
before returning `:allow`; Jizoku does not export workflow payloads.

The selected partition is part of the operation identity. Omission selects the
legacy namespace. Run preview, backfill, apply, and receipt lookup separately
for every host-owned partition; never derive partition selection from an
unauthorized request.

## Prepare Existing Ecto Installations

The retention migration adds an ownership column and a payload-free receipt
table. New journal entries populate ownership automatically. Rows written
before the migration must be backfilled before deletion can safely edit shared
catalog, workflow-index, and dispatch threads.

Inspect one partition without changing it:

```sh
mix jizoku.retention.backfill --partition tenant_acme
```

Apply one bounded batch and repeat until `complete=true`:

```sh
mix jizoku.retention.backfill --partition tenant_acme --batch-size 500 --apply
```

Each batch safely decodes the versioned journal envelope, locks only the chosen
legacy rows, and updates ownership atomically. A malformed row rolls back the
whole batch. The operation is resumable and idempotent because already owned
rows are not selected again.

Applications can inspect adapter support before exposing an operator action:

```elixir
{:ok, capabilities} = Jizoku.retention_capabilities()

capabilities.archive?
capabilities.preview?
capabilities.transactional_apply?
capabilities.ownership_backfill?
```

The Ecto adapter supports both operations. Other adapters can still support
archive and read-only preview but reject apply and ownership backfill until
Jizoku provides an implementation with equivalent durability guarantees.

## Archive and Preview

Archive a terminal run with a bounded, non-sensitive reason:

```elixir
{:ok, archived} =
  Jizoku.archive_run(run_id, reason: "policy_window_elapsed")
```

Default listings omit the run. Explicit archived listing and direct
inspection remain available, and `Jizoku.unarchive_run/2` reverses the state.

Preview is read-only, deterministic for the same durable revisions and
timestamp, and returns exact run IDs, source revisions, affected thread,
checkpoint, index, catalog, and search identities, estimated entry counts,
blocked reasons, expiry, and a confirmation token:

```elixir
{:ok, plan} =
  Jizoku.preview_retention(
    terminal_before: ~U[2026-01-01 00:00:00Z],
    statuses: [:completed, :cancelled],
    limit: 100
  )
```

Do not edit or reconstruct a plan. Review the exact candidates, blocks,
partition, counts, and expiry before confirmation. The token binds every plan
field and cannot authorize a different candidate set.

The operator command is also preview-only by default:

```sh
mix jizoku.retention \
  --partition tenant_acme \
  --terminal-before 2026-01-01T00:00:00Z \
  --statuses completed,cancelled \
  --limit 100
```

For machine-readable review, add `--json`. Output contains retention evidence
and identifiers, never workflow input, context, result, error, or archive
reason payloads. Treat run identifiers and operational metadata according to
the host's privacy classification.

## Confirm and Apply

Apply requires the exact `created_at` and token printed by preview:

```sh
mix jizoku.retention \
  --partition tenant_acme \
  --terminal-before 2026-01-01T00:00:00Z \
  --statuses completed,cancelled \
  --limit 100 \
  --created-at 2026-08-16T23:00:00Z \
  --apply \
  --confirmation PLAN_TOKEN
```

The command reconstructs the exact preview, then apply checks expiry against
the current time. It locks run identities and source threads, validates every
revision, reruns lifecycle and host-policy checks, validates ownership counts,
and performs deletion plus receipt insertion in one database transaction.

Within that transaction Jizoku removes candidate-owned facts from shared
catalog, workflow-index, and dispatch threads; advances their monotonic
revisions while recording intentional retention gaps; removes affected
checkpoints, run threads, and search rows; and inserts one minimal receipt per
run. Normal listings cannot observe a partially deleted run because commit is
atomic. Unaffected shared-thread entries retain their original sequence and
remain replayable.

Receipts contain partition, run ID, plan digest, workflow and queue identity,
terminal status, deletion counts, and deletion time. They do not contain
workflow payloads, results, errors, archive reasons, or host-policy data. A
receipt also fences reuse of the deleted run identity.

## Backup, Restore, and Failure Recovery

Take and verify a database backup that covers all Jizoku journal, checkpoint,
search, and receipt tables before the first production apply. Keep the backup
for the host's approved recovery window. A restore must be a consistent
database restore, including receipts; selectively restoring deleted run rows
without the matching shared-thread state can create invalid histories.

Use these responses during an operation:

- Ownership backfill required: finish the bounded backfill for that partition,
  preview again, and apply the new plan.
- Expired or stale plan: do not retry with edited evidence. Generate and review
  a fresh preview.
- Candidate blocked after preview: resolve the claim, reconciliation, lineage,
  archive, export, or legal-hold condition, then preview again.
- Database or receipt error before commit: the transaction rolls back all
  deletion changes. Correct the cause and retry the exact unexpired plan or
  generate a new preview.
- Lost response after commit: retry the exact plan and token. Jizoku returns the
  existing aggregate receipt with `idempotent?: true` and does not delete again.
- Malformed legacy ownership row: stop deletion for the partition, preserve the
  row and backup, repair the underlying journal data through an approved
  recovery procedure, then resume the bounded backfill.

Retention removes Jizoku's durable workflow state. It cannot retract external
side effects, downstream events, logs, traces, exports, or copies stored by host
systems. Apply the host's privacy and deletion procedures to those systems
separately.

## Executable Host Examples

`MinimalHostApp.WorkflowRuns` and `BedrockMinimalHostApp.WorkflowRuns` wrap
archive, preview, and confirmed apply at a host-owned boundary. Their tests
create an archived terminal run, review its exact plan, apply deletion, verify
the payload-free receipt, and prove the run is no longer inspectable. The
minimal host smoke task exercises the same lifecycle as part of its full
application path.
