# Repository Guidelines

## Project Structure & Module Organization

Jizoku is an Elixir library for durable workflow execution. Core source
lives in `lib/jizoku`, with Mix tasks under `lib/mix`. Database schema and
migrations live in `priv/repo`. Tests mirror the source layout under
`test/jizoku`, with shared helpers in `test/support`. Project docs are in
`docs`, and `examples/minimal_host_app` is the executable host-app harness used
for integration and smoke testing.

Package-style usage rules for AI agents live in `usage-rules.md` and
`usage-rules/*.md`. Read them before changing runtime behavior, host-app setup,
workflow authoring docs, tests, or dashboard/tooling APIs.

## Build, Test, and Development Commands

- `mix deps.get` installs root project dependencies.
- `mix compile --warnings-as-errors` compiles the library and fails on warnings.
- `mix test` runs the ExUnit suite.
- `mix format --check-formatted` verifies formatting.
- `mix precommit` runs the full root verification gate.
- `MIX_ENV=test mix coveralls` runs the local coverage gate.
- `cd examples/minimal_host_app && MIX_ENV=test mix example.smoke` runs the
  example host-app smoke path.

For a full local pass before handoff, run `mix precommit` and the smoke path
when runtime, workflow, persistence, or public API behavior changed.

## Coding Style & Naming Conventions

Use `mix format`; `.formatter.exs` defines the project DSL calls that should be
written without parentheses, such as `workflow`, `step`, `transition`, and
`payload`. Keep modules small and named by intent. Prefer explicit structs for
domain data and maps for transport boundaries. Avoid `String.to_atom/1` on
external input, list index access, hidden global state, and swallowing errors.

## Testing Guidelines

Tests use ExUnit. Name test files with `_test.exs` and place them near the
behavior they verify, for example `test/jizoku/runtime/step_worker_test.exs`.
Add regression coverage with behavior-focused assertions, especially for retries,
pause/resume, cancellation, dependency ordering, stale workflow definitions,
input/output mapping, persistence, and error routing.

Use `examples/minimal_host_app` for embedded runtime smoke tests. Use
`examples/bedrock_minimal_host_app` for Bedrock queue, lease, heartbeat, retry,
dead-letter, and cron delivery coverage.

## Commit & Pull Request Guidelines

Follow Conventional Commits, for example `feat: add native step contract` or
`fix: align planner with workflow mappings`. Keep each commit focused on one
intent. Pull requests should fill the repository template, describe the final net
change, explain why it is needed, reference related issues when applicable, and
include the verification commands run.

## Security & Configuration Tips

Do not commit secrets, local machine paths, hostnames, or user-specific config.
Review dependency and lockfile changes carefully because Jizoku is embedded
inside host applications.

## Jizoku Runtime Rules

- Treat the Jido journal runtime as the only execution path.
- Execute visible work through `Jizoku.execute_next/1`.
- Keep workflow modules backend-neutral; Bedrock, Oban, or another backend
  belongs behind host adapter modules.
- Cron activation may use `Jizoku.Executor.Payload.cron/3` delivered through
  `Jizoku.Runtime.Runner.perform/2`; step and compensation payloads are not
  part of the current runner boundary.
- Preserve public tooling surfaces needed by dashboards and visual editors:
  `Jizoku.list_runs/2`, `Jizoku.inspect_run/2`,
  `Jizoku.inspect_run_graph/2`, `Jizoku.explain_run/2`, and normalized
  workflow specs.
