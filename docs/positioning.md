# Positioning

Squidie is an embedded durable workflow runtime for Elixir applications.
It is for teams that want business workflows to live inside their existing
Phoenix or OTP app, share that app's repo and deployment model, and still have
durable run history, retries, approvals, replay, cancellation, and operator
inspection.

The public authoring surface is intentionally Squidie-native: workflow
modules, triggers, payload contracts, steps, transitions, dependency edges, and
retry policy. Jido, Runic, and Spark are important foundations, but workflow
authors should not have to think in raw agent, planner, or storage primitives
for the common path.

## Product Lane

Squidie sits between a job backend and a standalone workflow service.

- It is more than a job queue: Squidie owns workflow structure, step state,
  attempts, retries, waits, approvals, replay policy, and inspection.
- It is less than a separate workflow platform: Squidie runs inside the host
  app and keeps durable journal execution inside the host repo. Host workers
  call `Squidie.execute_next/1`, and host schedulers may deliver cron
  activations through backend-neutral payloads.
- Jido, Runic, and Spark are foundation layers in the current architecture.
- Reactor, Ash Reactor, Sage, and FlowStone solve adjacent workflow and
  orchestration problems at different abstraction layers.
- Squidie is not a generic replacement for all of them; it targets durable,
  inspectable workflow runs inside Phoenix and OTP applications.

That boundary is deliberate. The host application keeps its domain contexts,
database, supervision tree, observability stack, and job infrastructure. Squid
Mesh adds the durable workflow contract above those pieces.

## Runtime Shape

The current runtime shape is:

1. Spark defines the authoring DSL and normalized workflow spec.
2. Runic plans dependency readiness and runnable workflow work.
3. Squidie records durable workflow and dispatch facts.
4. Jido provides the runtime foundation for actions, signals, agents, thread
   journals, checkpoints, storage, and supervised execution.
5. Optional backend adapters remain responsible for concrete delivery mechanics
   such as queues, delayed visibility, redelivery, leases, and worker
   infrastructure. Bedrock is the recommended reference backend for
   backend-owned leases today, while Squidie keeps the core dispatch
   contract backend-neutral.

Host applications get the journal runtime, projection read model, and inferred
Ecto storage by default. Storage and queue remain explicit boundary options
when a host needs a non-default journal setup.

## Status Terms

Status labels describe current adoption readiness, not roadmap intent.

- Supported: available in the journal-backed runtime, covered by repository docs
  and tests, and stable enough to treat as part of the current product surface.
- Supported, evolving: available for real use today, but the surrounding API,
  examples, or documentation may still tighten through the `0.1.x` release
  line. Use these capabilities when their notes match your workflow, but expect
  integration details to be less settled than plain supported rows.
- Out of scope: intentionally not part of Squidie's product surface, even if
  a host application may integrate a separate tool for that concern.

## Capability Map

| Capability | Status | Notes |
| --- | --- | --- |
| Workflow DSL and normalized spec | Supported, evolving | Workflow definitions cover triggers, payload contracts, steps, transitions, retries, dependency edges, and formatter support. Trigger, payload, step, and transition declarations are Spark-backed metadata. |
| Native step contract | Supported | `Squidie.Step` is the preferred authoring path. Raw `Jido.Action` modules remain an explicit interop path. |
| Durable run history | Supported | Run, dispatch, attempt, terminal, manual-control, replay, and catalog facts are persisted in the configured Jido journal storage. |
| Cron payload boundary | Supported | Host schedulers may enqueue `Squidie.Executor.Payload.cron/3` maps and deliver them to `Squidie.Runtime.Runner.perform/2`; step execution is claimed through `Squidie.execute_next/1`. |
| Human approval workflows | Supported | Pause and approval flows are durable for transition-based workflows. |
| Replay | Supported, evolving | Journal replay starts a fresh Jido-native run from durable source-run metadata and preserves irreversible-step safety gates. |
| Inspection and explanation | Supported, evolving | Journal-backed inspection and explanation are the default and infer Ecto storage from the configured repo; [#163](https://github.com/dark-trench/squidie/issues/163) delivered the durable projection foundation. |
| Durable dispatch protocol | Supported, evolving | The pure protocol, projection, and `Jido.Storage` journal boundary define runnable intent, claims, leases, heartbeats, completion, failure, retries, terminal-run fencing, and checkpoint pointers for the journal runtime. |
| Jido.Storage-backed core | Supported, evolving | Start, cron start, execution, replay, cancellation, inspection, explanation, pause, and approval controls run through the Jido journal runtime by default. |
| Jido-native runtime agents | Supported, evolving | Workflow and dispatch agents rebuild from durable journals and checkpoints; [#164](https://github.com/dark-trench/squidie/issues/164) covers the completed agent foundation. |
| Bedrock lease backend | Supported, evolving | The Bedrock example app demonstrates durable delivery, delayed visibility, leases, heartbeats, retries, and dead-letter behavior through Squidie's backend-neutral lease boundary. |
| Scheduled-start metadata | Supported, evolving | Intended schedule windows are stored in durable run context for journal cron starts and exposed to steps through `context.state.schedule`. |
| Conditional and deferred continuation | Supported, evolving | Durable planner facts and deferred wakeups are tracked in [#140](https://github.com/dark-trench/squidie/issues/140). |
| Fan-out and fan-in contract | Supported, evolving | Runic-backed dependency ordering and join semantics are defined for the current static workflow graph; [#142](https://github.com/dark-trench/squidie/issues/142) captured the closed design clarification. |
| Runtime-authored workflow specs | Supported, evolving | `start_spec/3` and `start_spec/4` can start validated UI-authored or DB-authored specs through the journal runtime. The resolved definition is persisted with the run for execution and graph inspection; replay support is not yet available for these runs. |
| Safe action registry | Supported, evolving | `validate_spec/2`, `resolve_spec_actions/2`, and `start_spec/3`/`4` let host apps allowlist runtime-authored action keys before activation. |
| UI graph serialization | Supported, evolving | `Squidie.Runs.GraphInspection.to_map/1` exposes stable node, edge, status, output, and selected-edge data for dashboards and CLIs. `Squidie.Workflow.EditorSpec` supports JSON-safe spec round trips and draft graph previews for visual editors. |
| Actor-scoped read views | Supported, evolving | `Squidie.ReadModel.Visibility.redact/2` and `/3` let host apps derive external, operator, or auditor views from existing listing, inspection, graph, and explanation read models without mutating durable history. |
| Dynamic child runs | Supported, evolving | Native steps can start idempotent child workflow runs with durable parent lineage through `Squidie.start_child_run/4` and `Squidie.start_child_run/5`; visual-editor dynamic expansion is separate from the current child-run API. |
| Oban-specific core | Out of scope | Host apps may choose Oban behind the delivery boundary, but Squidie core is not Oban-centric. |
| Exactly-once external side effects | Out of scope | Squidie can provide durable workflow state and fencing semantics, but external systems still require idempotency. |
| Bundled workflow dashboard | Out of scope | Squidie exposes inspection data; host apps own their operator UI. |

## Why Squidie Exists Above Jido

[Jido](https://hex.pm/packages/jido) is the runtime foundation: agents,
actions, signals, directives, supervision, persistence primitives, and
operational runtime structure. Squidie uses that foundation, but it is not
trying to make users write Jido-native workflow applications by hand.

Squidie adds the workflow product layer:

- workflow definitions and validation
- trigger and payload contracts
- step input and output mapping
- retry, replay, cancellation, approval, and failure-routing policy
- durable dispatch semantics
- workflow inspection and explanation projections
- host-app integration around an existing Ecto repo and worker supervision
- a backend-neutral dispatch contract that can use Bedrock or another durable
  backend for lease ownership without making workflow definitions depend on
  backend-specific concepts

Use Jido directly when the main abstraction is an autonomous or supervised
agent. Use Squidie when the main abstraction is a durable workflow run that
operators need to inspect, resume, retry, replay, or cancel.

## Reactor And Ash Reactor

[Reactor](https://hex.pm/packages/reactor) is the closest adjacent project at
the orchestration layer. It is a strong fit for in-process dependency graphs,
concurrent step execution, saga compensation, and undo behavior. Teams that need
a short-lived saga or dependency-resolving operation inside regular Elixir code
should evaluate Reactor directly.

[Ash Reactor](https://hexdocs.pm/ash/reactor.html) is Reactor's Ash integration
layer. It is especially compelling for Ash-first applications because workflow
steps can call Ash resources and actions with Ash actor, context, and domain
semantics. In an Ash application, Ash Reactor is often the most natural way to
orchestrate Ash actions.

Squidie intentionally sits one layer farther out. It treats the durable
workflow run as the product surface: persisted run, step, attempt, and audit
history; approvals and manual unblocking; replay and cancellation policy;
operator inspection; explanation; and backend-owned recovery. Reactor and Ash
Reactor are useful comparison points for orchestration semantics, while Squid
Mesh focuses on long-running, inspectable, host-app workflow state.

## Foundation Layers

| Project | Primary fit | Relationship to Squidie |
| --- | --- | --- |
| [Jido](https://hex.pm/packages/jido) | OTP-native agents, actions, signals, directives, and supervised autonomous systems. | Runtime foundation and interop layer. Squidie keeps raw Jido primitives out of the common workflow authoring path. |
| [Runic](https://hex.pm/packages/runic) | Data-driven workflow graphs, dependency planning, and runnable extraction. | Planner foundation. Squidie maps declared workflow structure and readiness into durable runnable intent. |
| [Spark](https://hex.pm/packages/spark) | Declarative Elixir DSL and extension framework. | Authoring foundation. Squidie uses Spark to define the workflow DSL and normalized workflow spec. |
| Durable backend adapters | Concrete delivery mechanics such as queues, scheduled work, leases, redelivery, and worker infrastructure. | Delivery foundation. Squidie keeps this boundary backend-neutral and recommends Bedrock as the reference lease backend today. |

For application teams, these foundations are implementation boundaries rather
than prerequisites for the happy path. Use Squidie APIs and workflow modules
first. Reach for Jido details only when replacing storage or contributing to the
runtime. Reach for Bedrock or another lease-capable backend only when a simple
host worker loop does not provide enough delivery and worker-ownership
semantics.

## Adjacent Choices

| Project | Primary fit | Relationship to Squidie |
| --- | --- | --- |
| [Reactor](https://hex.pm/packages/reactor) | Concurrent dependency-resolving saga orchestration for Elixir applications, with compensation and undo semantics. | Closest adjacent orchestrator. Squidie emphasizes durable host-app workflow state, operator inspection, approvals, replay, cancellation, and backend-owned recovery. |
| [Ash Reactor](https://hexdocs.pm/ash/reactor.html) | Ash-native orchestration over resources and actions, built on Reactor. | Strong fit for Ash applications that want saga orchestration inside the Ash domain model. Squidie targets broader Phoenix/OTP workflow runs with durable history, HITL, replay, cancellation, and recovery. |
| [Sage](https://hex.pm/packages/sage) | Dependency-free saga composition with transaction and compensation callbacks. | Good fit for local saga execution. Squidie targets longer-lived inspectable workflow runs with persisted step and attempt history. |
| [FlowStone](https://hex.pm/packages/flowstone) | Asset-first data orchestration and dependency-aware pipelines. | Adjacent data-pipeline tool. Squidie focuses on application workflows, approvals, recovery policy, dispatch semantics, and inspection. |

## How To Choose

Choose Squidie when:

- workflow state belongs inside an existing Phoenix or OTP application;
- runs must survive restarts, deploys, retries, and worker redelivery;
- operators need to know why work is waiting, retrying, paused, failed,
  cancelled, or complete;
- approvals, manual review, replay, cancellation, and recovery policy are part
  of the business process;
- workflow authors should use domain-level workflow concepts instead of raw
  process, job, agent, or planner primitives.

Choose another layer when:

- a short-lived in-memory saga is enough;
- the main abstraction is a long-running autonomous agent;
- the main abstraction is an asset graph or data materialization pipeline;
- the app only needs a job queue, scheduler, or background worker backend;
- a separate workflow service is a better operational boundary than embedding
  workflow state in the host application.

## Adoption Boundary

Supported rows are available in the journal-backed runtime. Supported, evolving
rows are also usable now, with API polish, examples, or documentation still
settling through the `0.1.x` release line.

The configured journal runtime provides the workflow DSL, persisted run and
dispatch facts, journal dispatch claims, retries, approvals, pause/resume
controls, replay, cancellation, projection-backed inspection, and UI-friendly
graph output.

Runtime-authored workflow activation for UI-authored or DB-authored workflow
systems is now available through `Squidie.start_spec/3` and
`Squidie.start_spec/4`, with the safe action registry as the allowlist
boundary for executable actions. Full replay support for runtime-authored specs
is not yet available. Visual editor spec round trips are available today through
`Squidie.Workflow.EditorSpec`, and inspected run graph serialization is
available through `Squidie.Runs.GraphInspection.to_map/1`.

The durable dispatch protocol defines the vocabulary for runnable intent, claim
fencing, leases, heartbeats, retries, terminal-run behavior, and parent-child
lineage. Workflow and dispatch agents rebuild that state from durable journals.
Runtime-safe child workflow starts are exposed through `start_child_run/4` and
`start_child_run/5`; visual-editor dynamic expansion remains a separate
surface.

Oban can still be a practical scheduler or job backend in a host application.
It is an implementation detail, not the core Squidie runtime model.
