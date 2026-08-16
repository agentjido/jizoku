# Jizoku Host App Usage Rules

## Configuration

- Configure Jizoku with the host repo and queue:

  ```elixir
  config :jizoku,
    repo: MyApp.Repo,
    partition: "tenant_acme",
    queue: "default"
  ```

- Omit `:partition` for the exact legacy namespace. When it is configured,
  workers, cron delivery, runtime commands, and inspection use that partition
  by default; explicit overrides must come from trusted host routing.
- Do not derive `:partition` directly from an unauthenticated request or treat
  partition isolation as host authorization.
- Register retained workflow implementations through host-owned
  `:workflow_versions`, keyed by the stable current workflow module and declared
  version. Keep historical modules deployed while non-terminal runs reference
  them; version labels never override exact fingerprint fencing.
- Migrate definitions only with a host-owned `Jizoku.Workflow.Migration`
  contract at a quiescent manual pause. Keep callbacks deterministic and free
  of side effects; use a new explicit contract for rollback.
- Use `inspect_run/2`, `inspect_run_graph/2`, and `explain_run/2` to expose the
  persisted definition version, fingerprint, resolution state, and migration
  evidence. When resolution fails, restore the exact historical definition and
  verify checked-in history fixtures before replaying terminal work.
- Treat `Jizoku.migrate_run/2` as an authorized operator boundary. Do not accept
  migration modules, version labels, storage, queues, or partitions directly
  from untrusted input.

- Do not configure `:executor` for step execution.
- Use explicit `journal_storage` only when replacing the default inferred Ecto
  storage boundary.

## Worker Loop

- Start one or more supervised workers that call `Jizoku.execute_next/1`.
- Back off briefly when `execute_next/1` returns `{:ok, :none}`.
- Add metrics, capacity limits, and shutdown behavior around the public call
  rather than inside workflow modules.
- Keep workers generic. They should not encode workflow-specific business
  decisions.

## Cron

- Declare cron triggers in workflow modules.
- Keep recurring scheduling in the host app.
- Deliver cron activations with `Jizoku.Executor.Payload.cron/3` and
  `Jizoku.Runtime.Runner.perform/2`.
- Include `signal_id` or a complete `intended_window` for idempotent cron
  triggers.
- Preserve the active `:partition` in durable cron payloads.
- Do not deliver step or compensation payloads through `Runner.perform/2`.

## Runtime Commands

- Host API and operator boundaries may build `Jizoku.Runtime.Signal` values
  and pass them to `Jizoku.apply_signal/2`.
- Attach host-owned metadata and idempotency keys for externally delivered
  commands so duplicate delivery and operator audit history are explicit.
- Preserve the active partition when constructing or adapting signals; a
  signal whose explicit partition conflicts with runtime options is rejected.
- Assert `command_history` in integration tests for cancel, resume, approval,
  rejection, replay, and scheduler-driven starts.
- Convert outbound commands to raw `Jido.Signal` only through
  `Jizoku.Runtime.Signal.JidoAdapter`. Pass recognized inbound Jido command
  envelopes directly to `Jizoku.apply_signal/2`. Route domain signals through
  an allowlisted `Jizoku.Jido.SignalResolver`; keep workflow modules and
  lifecycle command selection in trusted host code.
- Do not let resolvers accept storage, queue, runtime, dispatch, or module names
  from signal payloads. Jizoku keeps those choices at the host call boundary.
- Treat CloudEvents source and ID as one identity. Reusing that pair is an exact
  delivery retry and reuses the first durable resolver decision; send a new ID
  for a new domain event.
- Authorize inbound Jido commands before calling Jizoku. Their CloudEvents
  source is persisted as audit provenance and is not an authorization grant.

## External Events

- Authenticate and authorize callbacks before calling `Jizoku.signal_run/4`.
  Verify provider signatures against the untouched request body, then decode
  and validate the payload.
- Map provider callbacks to allowlisted internal event names. Do not accept
  runtime, storage, queue, partition, module, or internal event choices from
  callback input.
- Use a stable provider event ID as the Jizoku idempotency key. Exact retries
  are safe; conflicting reuse fails closed.
- Resolve the run and correlation through host-owned domain data. Treat a run ID
  or correlation supplied by an unauthenticated callback as untrusted.
- Do not log signature secrets, raw callback bodies, or event payloads. Use
  external or operator visibility for safe wait status and identity summaries.

## Read-Model Visibility

- Authorize run listing, inspection, graph, and explanation calls at the host
  boundary.
- Use `Jizoku.ReadModel.Visibility.redact/2` for default external-safe views
  or `Jizoku.ReadModel.Visibility.redact/3` with a host policy when exposing
  read-model data to actor-scoped UI, API, or CLI surfaces.
- Treat `:auditor` visibility as privileged; it preserves full snapshots and
  diagnostics.
- Use `:external` or `:operator` visibility for surfaces that need status,
  current/manual task state, and safe next actions without payloads, command
  history, claim metadata, attempt inputs/results/errors, or manual metadata.
- Keep durable history immutable. Visibility policy derives read-side
  projections only and must not be treated as deletion or retention policy.

## Observability

- Use `Jizoku.Telemetry.metrics/0` for the recommended metric definitions;
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
