# Squidie Testing Usage Rules

## Test Scope

- Add regression tests for each meaningful runtime failure mode found during
  review.
- Prefer behavior-focused assertions over implementation details.
- Test public APIs when the change affects host-facing behavior.
- Test pure protocol and projection modules directly when changing journal
  semantics.
- Keep one reason to fail per test.

## Runtime Cases

- Cover retries, replay, cancellation, pause/resume, approval/rejection, waits,
  dependency/fan-out behavior, conditional routing, stale workflow definitions,
  duplicate delivery, stale claims, and terminal-state ordering when relevant.
- For stateful or concurrency-sensitive changes, prove stale reads are either
  eliminated, revalidated under the same lock, or safe by idempotent design.
- For telemetry or audit-history changes, test terminal-event symmetry and
  ordering relative to durable commits.

## Example Apps

- Use `examples/minimal_host_app` for embedded runtime smoke tests.
- Use `examples/bedrock_minimal_host_app` for Bedrock queue, lease, heartbeat,
  retry, dead-letter, and cron delivery coverage.
- Reset example app databases when persisted test rows can affect later smoke
  runs.

## Verification

- Run `mix format` before handoff.
- Run focused tests first, then `mix precommit` before finishing a code slice.
- Run `MIX_ENV=test mix coveralls` when tests or coverage-sensitive runtime
  paths change; coverage gates live in `coveralls.json` and `codecov.yml`.
- For runtime, workflow, persistence, jobs, state-machine, or public API
  changes, run an end-to-end smoke path in the relevant example app.
