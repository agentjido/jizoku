# Squidie Documentation

Squidie is an embedded durable workflow runtime for Elixir applications. The
docs are organized by how readers usually arrive: first learning the model,
then installing it in a host app, then authoring workflows, operating them, and
finally reading internals when contributing to the runtime.

## Start Here

New to Squidie:

1. [Getting started](getting_started.md) - learn the model, install the
   runtime, start a run, drain work, inspect state, and add reliability.
2. [Getting started Livebook](getting_started.livemd) - run a small workflow
   interactively and inspect the output.
3. [Reference workflows](reference_workflows.md) - see realistic approval,
   recovery, dependency, saga, and scheduled workflows in the example host app.
4. [Positioning](positioning.md) - understand where Squidie sits relative to
   Jido, Runic, Spark, job queues, Reactor, and workflow services.

## Learn By Doing

Livebooks are best for concepts that benefit from running code and inspecting
the resulting workflow state.

- [Getting started Livebook](getting_started.livemd) - first workflow, visible
  attempts, scheduled wakeups, graph inspection, explanation output, and manual
  approval.
- [Workflow authoring Livebook](workflow_authoring.livemd) - DSL structure,
  normalized workflow specs, dependency joins, input mappings, execution, and
  graph output.

## Guides

Use these when building or embedding Squidie in an application.

- [Host app integration](host_app_integration.md) - installation,
  configuration, worker loops, cron payloads, Phoenix/OTP host shapes, and
  optional Bedrock-backed leases.
- [Workflow authoring](workflow_authoring.md) - DSL syntax, payloads, triggers,
  steps, transitions, retries, waits, dependencies, mapping, compensation, and
  current boundaries.
- [Operations](operations.md) - operational CLI diagnostics, retries,
  idempotency, replay, local transactions, leases, waits, cron activation, and
  production concerns.
- [Observability](observability.md) - durable read-model surfaces,
  field-selection and redaction guidance, operator explanations, graph output,
  host-owned telemetry, and logs.
- [Actor Visibility](actor_visibility.md) - actor-scoped visibility patterns,
  redaction policies, multi-tenant access control, and security best practices.

## Reference

Use these as stable contracts when implementing host integrations or tooling.

- [Graph inspection contract](graph_inspection.md) - node and edge map shapes
  for dashboards and visual workflow tools.
- [Storage strategy](storage_strategy.md) - journal storage adapter guarantees,
  the current Postgres-compatible Ecto path, and future backend storage
  expectations.
- [Reference workflows](reference_workflows.md) - executable product examples
  backed by the minimal host app.
- [Tool adapters](tool_adapters.md) - normalized result and error shape for
  external tool wrappers.
- [Quality gates](quality_gates.md) - structural quality checks, baseline
  rollout policy, and local commands.
- [Supported baseline](compatibility.md) - supported Elixir, OTP, Jido, Ecto,
  Postgres, and adapter expectations.
- [Production readiness](production_readiness.md) - current readiness bar and
  verification entry points.

## Example Apps

Use the example apps when you want an executable host boundary rather than a
small notebook.

- [Minimal host app](../examples/minimal_host_app/README.md) - standalone
  recovery, approvals, cron, local transactions, replay, smoke, resilience, and
  soak coverage.
- [Bedrock minimal host app](../examples/bedrock_minimal_host_app/README.md) -
  Bedrock-backed delivery, leases, delayed visibility, retry requeue,
  dead-letter handling, and cron payload mapping.

## Internals

These pages are for contributors, adapter authors, and advanced users who need
to reason about runtime durability.

- [Architecture](architecture.md) - high-level components, responsibilities,
  execution flow, and recovery boundary.
- [Jido runtime architecture](jido_runtime_architecture.md) - journal runtime,
  agents, projections, dispatch, leases, failure handling, and Jido primitive
  boundaries.
- [Storage strategy](storage_strategy.md) - storage-adapter boundary,
  production guarantees, and Bedrock storage direction.
- [Durable dispatch protocol](durable_dispatch_protocol.md) - journal threads,
  commit order, claims, leases, heartbeats, retries, manual boundaries, and
  terminal fencing.

## AI Agent Usage Rules

Package-style rules help coding agents use and modify Squidie without
guessing the runtime boundaries.

- [Usage rules](../usage-rules.md)
- [Runtime rules](../usage-rules/runtime.md)
- [Host app rules](../usage-rules/host-apps.md)
- [Workflow authoring rules](../usage-rules/workflow-authoring.md)
- [Testing rules](../usage-rules/testing.md)
- [Documentation rules](../usage-rules/documentation.md)
- [Tooling rules](../usage-rules/tooling.md)
