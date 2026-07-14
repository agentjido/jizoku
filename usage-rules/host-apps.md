# Squidie Host App Usage Rules

## Configuration

- Configure Squidie with the host repo and queue:

  ```elixir
  config :squidie,
    repo: MyApp.Repo,
    partition: "tenant_acme",
    queue: "default"
  ```

- Omit `:partition` for the exact legacy namespace. When it is configured,
  workers, cron delivery, runtime commands, and inspection use that partition
  by default; explicit overrides must come from trusted host routing.
- Do not derive `:partition` directly from an unauthenticated request or treat
  partition isolation as host authorization.

- Do not configure `:executor` for step execution.
- Use explicit `journal_storage` only when replacing the default inferred Ecto
  storage boundary.

## Worker Loop

- Start one or more supervised workers that call `Squidie.execute_next/1`.
- Back off briefly when `execute_next/1` returns `{:ok, :none}`.
- Add metrics, capacity limits, and shutdown behavior around the public call
  rather than inside workflow modules.
- Keep workers generic. They should not encode workflow-specific business
  decisions.

## Cron

- Declare cron triggers in workflow modules.
- Keep recurring scheduling in the host app.
- Deliver cron activations with `Squidie.Executor.Payload.cron/3` and
  `Squidie.Runtime.Runner.perform/2`.
- Include `signal_id` or a complete `intended_window` for idempotent cron
  triggers.
- Preserve the active `:partition` in durable cron payloads.
- Do not deliver step or compensation payloads through `Runner.perform/2`.

## Runtime Commands

- Host API and operator boundaries may build `Squidie.Runtime.Signal` values
  and pass them to `Squidie.apply_signal/2`.
- Attach host-owned metadata and idempotency keys for externally delivered
  commands so duplicate delivery and operator audit history are explicit.
- Preserve the active partition when constructing or adapting signals; a
  signal whose explicit partition conflicts with runtime options is rejected.
- Assert `command_history` in integration tests for cancel, resume, approval,
  rejection, replay, and scheduler-driven starts.
- Convert to raw `Jido.Signal` only through `Squidie.Runtime.Signal.JidoAdapter`
  when integrating with Jido agents, signal routers, or other Jido primitives.

## Read-Model Visibility

- Authorize run listing, inspection, graph, and explanation calls at the host
  boundary.
- Use `Squidie.ReadModel.Visibility.redact/2` for default external-safe views
  or `Squidie.ReadModel.Visibility.redact/3` with a host policy when exposing
  read-model data to actor-scoped UI, API, or CLI surfaces.
- Treat `:auditor` visibility as privileged; it preserves full snapshots and
  diagnostics.
- Use `:external` or `:operator` visibility for surfaces that need status,
  current/manual task state, and safe next actions without payloads, command
  history, claim metadata, attempt inputs/results/errors, or manual metadata.
- Keep durable history immutable. Visibility policy derives read-side
  projections only and must not be treated as deletion or retention policy.

## Observability

- Use `Squidie.Telemetry.metrics/0` for the recommended metric definitions;
  use `partition_metrics/0` only after reviewing partition cardinality.
- Keep telemetry reporters, exporters, dashboards, alerts, sampling, and
  structured logger integration in the host app.
- Use trace and run identifiers for diagnostic correlation, not metric labels.
- Reconcile durable operational truth through the read model. Runtime telemetry
  is best-effort and is not an outbox or exactly-once delivery channel.

## Bedrock

- Use Bedrock when the host needs durable backend delivery, delayed visibility,
  lease ownership, heartbeats, retry requeue, dead-letter behavior, or
  distributed worker recovery.
- Keep Bedrock code in adapter modules.
- Do not let workflow modules depend on Bedrock APIs.
- Use `examples/bedrock_minimal_host_app` as the reference integration shape.
