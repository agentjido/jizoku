# Jizoku Development Guide

## Intent

Jizoku is an Elixir library for durable workflow execution. Keep changes small
and keep the workflow runtime stable.

Read `usage-rules.md` and the files in `usage-rules/` before you change runtime
behavior, host setup, workflow authoring, tests, or tooling APIs.

## Runtime Baseline

- Elixir `~> 1.18`
- Erlang/OTP versions in the CI matrix
- Postgres for the root tests and example applications

## Canonical Commands

- `mix deps.get`
- `mix compile --warnings-as-errors`
- `mix test`
- `mix quality`
- `mix precommit`
- `MIX_ENV=test mix coveralls`
- `cd examples/minimal_host_app && MIX_ENV=test mix example.smoke`

## Architecture And Scope

- Keep core source in `lib/jizoku` and Mix tasks in `lib/mix`.
- Keep database migrations in `priv/repo`.
- Keep shared test support in `test/support`.
- Keep example setup in `examples/`.
- Treat the Jido journal runtime as the only execution path.
- Execute visible work through `Jizoku.execute_next/1`.
- Keep workflow modules independent from a delivery backend.
- Deliver cron activation with `Jizoku.Executor.Payload.cron/3` through
  `Jizoku.Runtime.Runner.perform/2`. Step and compensation payloads are not in
  the current runner boundary.
- Preserve `Jizoku.list_runs/2`, `Jizoku.inspect_run/2`,
  `Jizoku.inspect_run_graph/2`, `Jizoku.explain_run/2`, and normalized workflow
  specifications for dashboards and visual editors.
- Keep the established `Jizoku` namespace until an approved breaking release.
- Keep `mix jizoku.install` until a separate change adds and tests an Igniter installer.

## Standards And Conventions

- Use Conventional Commits.
- Use `mix format`.
- Install Git hooks only with `mix install_hooks` from the primary checkout.
- Prefer explicit structs for domain data and maps at transport boundaries.
- Do not use `String.to_atom/1` on external input.
- Do not add machine paths, credentials, or deployment assumptions.

## Testing And QA

- Put tests near the behavior that they verify.
- Add regression tests for retries, pause and resume, cancellation, dependency
  order, stale workflow definitions, mappings, persistence, and error routing.
- Use `examples/minimal_host_app` for embedded runtime tests.
- Use `examples/bedrock_minimal_host_app` for Bedrock delivery tests.
- Run `mix precommit` and the applicable example tests before handoff.

## Release Hygiene And References

- Do not edit `CHANGELOG.md` in a normal pull request.
- Use the repository release workflow. Use its dry-run mode before a release.
- Preserve the public tooling APIs used by dashboards and visual editors.
- Follow the Jido package quality standards at
  <https://jido.run/docs/contributors/package-quality-standards>.
