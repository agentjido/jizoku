# Supported Baseline

This page defines the currently supported baseline for Squidie.

## Supported Baseline

| Component | Supported baseline |
| --- | --- |
| Elixir | `1.19.5-otp-28` |
| Erlang/OTP | `28.4.1` |
| Postgres | `15+` |
| Jido | `2.0+` |
| Telemetry | `1.3+` |
| Telemetry.Metrics | `1.1+` |

## What Supported Means

For the current release line, "supported" means:

- the combination is exercised in CI or repeatable local verification
- the documentation and example harnesses target that baseline
- bug reports on that baseline are in scope for active support

## Host App Expectations

Supported host apps are expected to provide:

- an Ecto `Repo`
- Postgres for durable state
- a supervised worker that calls `Squidie.execute_next/1`
- a scheduler that can deliver cron payloads to `Squidie.Runtime.Runner.perform/2`, if the app uses cron triggers
- step modules that conform to the current Squidie action contract
- a host-selected telemetry reporter or exporter only when runtime metrics or
  traces should leave the BEAM

## Storage Compatibility

The currently supported bundled production relational storage path is
`Squidie.Runtime.Journal.Storage.Ecto` with a Postgres-compatible Ecto repo.
Other durable stores may be valid when they are exposed through a journal
storage adapter that provides the same ordered append, optimistic conflict,
checkpoint, rebuild, and error-shape guarantees. See [Storage
strategy](storage_strategy.md).

## Dynamic Graph Compatibility

Legacy `preview_dynamic_work/3`, `record_dynamic_work/3`, and
`schedule_dynamic_work/3` contracts keep eager scheduling and inspection-only
edge semantics. Versioned dependency ordering is opt-in through
`preview_graph_mutation/3` and `apply_graph_mutation/3`. Inspection normalizes
legacy atom- or string-keyed graph records and drops malformed entries without
exposing their untrusted fields.

## Version Evaluation Policy

Before a new version is called supported, the team should:

1. Run the root test suite.
2. Run the example host app smoke path.
3. Run the restart resilience and soak/load verifications in the example app, including paused-run unblock after restart.
4. Review docs and configuration snippets for version-specific drift.

Until that work is done, newer versions may still work, but they should be
treated as unverified rather than supported.
