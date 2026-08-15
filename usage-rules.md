# Jizoku Usage Rules

Use these rules when building host apps with Jizoku or changing Jizoku
itself.

## Core Model

- Jizoku is an embedded durable workflow runtime for Elixir applications.
- Workflow authors define compiled Elixir workflow modules with triggers,
  payload contracts, steps, transitions, waits, approvals, retries, and
  recovery routes.
- The Jido-native journal runtime is the source of truth for run, dispatch,
  attempt, manual-control, and terminal facts.
- Host workers provide execution capacity by calling `Jizoku.execute_next/1`.
- Runtime command, run, runnable, and attempt trace lineage is durable journal
  data; public telemetry is a best-effort projection of committed lifecycle
  facts and execution spans.
- Optional schedulers can deliver cron payloads through
  `Jizoku.Runtime.Runner.perform/2`.
- Optional backend adapters, such as the Bedrock example, can own durable
  delivery and lease mechanics without changing workflow modules.

## Rules To Follow

- Prefer `use Jizoku.Step` for custom workflow steps.
- Use raw `Jido.Action` modules only for explicit interop.
- Schedule raw `Jido.Instruction` values only through
  `Jizoku.schedule_dynamic_work/3`, with an explicit applied runnable origin
  and a host-owned action registry.
- Return `{:ok, output}` or `{:ok, output, []}` from raw `Jido.Action`
  modules. Jizoku rejects non-empty or malformed action extras as an explicit
  action failure; it never silently discards Jido directives.
- Pass recognized Jizoku command `Jido.Signal` envelopes directly to
  `Jizoku.apply_signal/2`. Do not manually call the adapter first. Route
  arbitrary domain signals only through a host-owned
  `Jizoku.Jido.SignalResolver` that returns closed lifecycle commands.
- Resolver start commands must name compiled workflow modules in host code.
  Never derive modules, runtime options, queues, storage, or dispatch adapters
  from signal strings.
- Treat the Jido envelope source as durable provenance, not authorization.
  Authenticate and authorize inbound signals before applying them.
- Keep workflow definitions backend-neutral.
- Keep delivery and job boundaries thin; call host-owned modules that wrap
  Jizoku public APIs.
- Pass the same trusted `:partition` through every start, worker, cron, signal,
  control, replay, and read-model boundary when a host isolates workflow state
  by tenant or domain. Omitting it selects the legacy unpartitioned namespace.
- Treat partitions as storage routing, not authorization. Authorize the caller
  before selecting a partition or returning its run data.
- Use `Jizoku.list_runs/2` for index views and
  `Jizoku.inspect_run/2`, `Jizoku.inspect_run_graph/2`,
  `Jizoku.inspect_run_timeline/2`, or `Jizoku.explain_run/2` for details.
- Use continue-as-new to bound recurring workflow history. Keep successor input
  explicit, choose a stable continuation key, and use
  `Jizoku.inspect_continuation_chain/2` with a bounded `:max_hops` when more
  than the immediate lineage edge is required.
- Use `Jizoku.record_dynamic_work/3` when host/runtime code needs to persist
  bounded, inspection-only dynamic work metadata for a run.
- Use `Jizoku.preview_dynamic_work/3` when dashboards or visual editors need
  to validate candidate dynamic work and inspect the graph overlay before
  appending.
- Use `Jizoku.preview_graph_mutation/3` before proposing dependency-ordered
  graph changes. Commit with `Jizoku.apply_graph_mutation/3`, and call
  `Jizoku.reconcile_dynamic_graph/2` when the report or inspection state says
  reconciliation is required.
- Use `Jizoku.preview_spec/3` or `Jizoku.preview_spec/4` when visual editors
  need execution-style node output for a runtime-authored draft. Pass a
  host-owned `:action_registry`; preview calls only registry entries that opt
  into `dry_run` behavior and does not append durable runtime state.
- Use `:guardrail_registry` with runtime-authored specs that declare step
  `opts[:guardrails]`; guardrail keys are host-owned validator contracts and
  decisions are exposed through previews, inspection, and explanations.
- Add idempotency keys or domain duplicate detection to side-effecting steps.
- Use `Jizoku.Telemetry.metrics/0` for the default bounded-cardinality metric
  set. Keep reporters, exporters, dashboards, alerts, and logging host-owned.
- Treat external exactly-once behavior as out of scope for Jizoku.

## Rules To Avoid

- Do not configure `:executor` for step execution.
- Do not use or document `:runtime_tables`.
- Do not deliver step or compensation payloads through
  `Jizoku.Runtime.Runner.perform/2`.
- Do not append `:dynamic_work_recorded` journal entries directly from host app
  code; use the public recording API so validation stays centralized.
- Do not make workflow modules depend on Bedrock, Oban, or another backend's
  APIs.
- Do not use `String.to_atom/1` on external input or persisted untrusted data.

## Topic Rules

- [Runtime rules](usage-rules/runtime.md)
- [Host app rules](usage-rules/host-apps.md)
- [Workflow authoring rules](usage-rules/workflow-authoring.md)
- [Testing rules](usage-rules/testing.md)
- [Documentation rules](usage-rules/documentation.md)
- [Tooling and dashboard rules](usage-rules/tooling.md)
