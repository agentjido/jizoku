# Jido Runtime Architecture

The Jido-native runtime keeps workflow truth in durable journal facts while
host-owned workers provide execution capacity. Runtime processes can crash,
restart, and rebuild their projections from storage without becoming the source
of truth.

Squidie's runtime shape is:

- workflow authors keep using the Squidie DSL for business workflows
- custom step modules run through Jido action contracts
- runtime coordination rebuilds from Jido-backed journals
- workflow runs and dispatch queues are represented by Jido agents
- step execution is pulled through `Squidie.execute_next/1`
- optional cron payload delivery remains backend-neutral through
  `Squidie.Runtime.Runner.perform/2`

The core boundaries are stable:

- journal entries are authoritative lifecycle facts
- checkpoints are rebuildable projection caches
- host workers call `Squidie.execute_next/1` to claim visible work
- host backends own delivery, leases, redelivery, and worker placement
- inspection reads projections and never mutates workflow state

## System Overview

```mermaid
flowchart TB
    subgraph HostApp[Host application]
        Workflow[Workflow modules]
        Repo[Host Repo]
        Backend[Optional lease backend]
        Workers[Worker processes]
    end

    subgraph Core[Squidie core]
        API[Public API]
        Planner[Workflow planner]
        Policies[Retry, cancellation, replay, mapping]
        Inspector[Inspection and explanation]
    end

    subgraph JidoRuntime[Jido-native runtime]
        Journal[Runtime journal]
        WorkflowAgent[WorkflowAgent per run]
        DispatchAgent[DispatchAgent per queue]
        Recovery[AgentRecovery]
    end

    subgraph Jido[Jido]
        Storage[Jido.Storage]
        Actions[Jido.Action step execution]
        AgentModel[Jido.Agent state model]
    end

    Workflow --> API
    API --> Planner
    Planner --> Journal
    Journal --> Storage
    Journal --> WorkflowAgent
    Journal --> DispatchAgent
    WorkflowAgent --> DispatchAgent
    DispatchAgent --> Backend
    Backend --> Workers
    Workers --> Actions
    Actions --> DispatchAgent
    DispatchAgent --> WorkflowAgent
    WorkflowAgent --> Policies
    Policies --> Inspector
    Repo -. default Ecto storage .- Journal
    AgentModel -. agent process shape .- WorkflowAgent
    AgentModel -. agent process shape .- DispatchAgent
```

The key design point is that the journal, not a worker process, becomes the
authority for workflow intent and dispatch lifecycle. Processes are allowed to
crash and restart because their projections can be rebuilt from durable facts.

## Runtime Flow

| Component | Owns | Does not own |
| --- | --- | --- |
| Workflow DSL | Business triggers, payload contracts, step graph, retry declarations | Queue leases, worker lifecycle, storage adapter details |
| Squidie core | Validation, planning, replay/cancellation semantics, inspection model | Host scheduling infrastructure or external side-effect idempotency |
| Runtime journal | Append-only facts, thread revisions, checkpoints through `Jido.Storage` | Business decisions hidden outside entries |
| `WorkflowAgent` | Per-run coordination projection, planned runnables, applied results, manual state, terminal state | Executing step code directly |
| `DispatchAgent` | Queue projection, visible attempts, claims, leases, heartbeats, completions, failures | Choosing the workflow graph |
| Optional lease backend | Waking workers and integrating durable delivery, claim, heartbeat, retry, and recovery mechanics | Rewriting Squidie workflow semantics |
| Jido actions | Step callback contract and action execution boundary | Whole-workflow orchestration |
| Host app | Domain code, repo, deployment, external APIs, permissions | Squidie runtime invariants |

```mermaid
flowchart LR
    Author[Workflow author] --> DSL[Squidie DSL]
    DSL --> Plan[Planner and runtime journal]
    Plan --> RunAgent[WorkflowAgent]
    RunAgent --> DispatchAgent[DispatchAgent]
    DispatchAgent --> Backend[Optional lease backend]
    Backend --> Worker[Worker loop]
    Worker --> Action[Jido action or built-in step]
    Action --> DispatchAgent
    DispatchAgent --> RunAgent
    RunAgent --> Inspect[Projection-backed inspection]
    Inspect --> Editor[SquidSonar or visual editor]
```

The runtime is intentionally asymmetric:

- authoring stays declarative
- durable facts stay in the journal
- execution stays in worker processes and Jido actions
- inspection stays read-only and projection-backed

## Jido Primitive Boundary

Squidie uses Jido as an internal runtime foundation while keeping the public
workflow API focused on Squidie concepts. Runtime contributors should know
which Jido primitives sit behind that boundary:

| Jido primitive | Squidie use |
| --- | --- |
| `Jido.Agent` | Rebuildable workflow and dispatch coordination state |
| `Jido.Action` | Step execution interop, including raw Jido action modules and the native `Squidie.Step` adapter |
| `Jido.Storage` | Journal and checkpoint persistence boundary |
| `Jido.Thread` / `Jido.Thread.Entry` | Durable journal facts for run, dispatch, index, and catalog threads |
| `Jido.Exec` | Action execution inside the journal executor |
| `Jido.Signal` | Interop envelope for Squidie runtime command signals |

Support code also uses lower-level primitives such as
`Jido.Thread.EntryNormalizer` and validates built-in storage adapters like
`Jido.Storage.File` and `Jido.Storage.Redis`. Workflow authors do not need to
use those primitives directly; public callers stay on Squidie APIs and host
apps adapt to Jido only at explicit runtime integration boundaries.

Runtime command signals use `Squidie.Runtime.Signal` as the stable contract.
`Squidie.Runtime.Signal.JidoAdapter` converts between Squidie signal structs
and `Jido.Signal` envelopes for advanced integration. Command receipt and
inspection details are covered in the next section.

## Runtime Command Signals

`Squidie.Runtime.Signal` is the Squidie-native command envelope for
runtime requests. These structs sit above backend primitives:
`Squidie.Runtime.Signal.JidoAdapter` can translate them into `Jido.Signal`
envelopes at the boundary when agents, signal routers, or other Jido primitives
need to exchange runtime commands or events. Workflow authors and host apps
should not need to construct raw Jido signals for normal workflow control.

| Command type | Stable payload shape | Identity and idempotency |
| --- | --- | --- |
| `:start_run` | `%{workflow, trigger, input}` | optional caller-supplied idempotency key |
| `:start_cron` | `%{workflow, trigger, input}` | scheduler `signal_id` or complete `intended_window` derives the idempotency key |
| `:approve_run` | `%{run_id, attributes}` | `run_id` is a validated UUID |
| `:reject_run` | `%{run_id, attributes}` | `run_id` is a validated UUID |
| `:resume_run` | `%{run_id, attributes}` | `run_id` is a validated UUID |
| `:cancel_run` | `%{run_id}` | `run_id` is a validated UUID |
| `:replay_run` | `%{run_id, allow_irreversible}` | `run_id` is a validated UUID and irreversible replay stays explicit |

All command signals carry a generated or caller-supplied `id`, plus `metadata`,
`occurred_at`, an optional `idempotency_key`, and an optional durable `trace`.
The journal command boundary creates a W3C-compatible root trace when one is
missing. Runtime code should adapt these product-level signals at the Jido
boundary instead of leaking backend signal shapes into public APIs. The signal
path is first-class inside Squidie; raw `Jido.Signal` is the interop envelope,
not a replacement for the Squidie signal taxonomy.

The Jido adapter preserves the outer envelope ID exactly and stores trace
correlation in the `"correlation"` extension. The runtime preserves that trace
on the run and persists child spans with runnable and attempt facts. Because correlation is
durable, a worker handoff, restart, conflict rebuild, or checkpoint loss does
not depend on process-local trace state. Replay deliberately starts a fresh
command lineage; `replayed_from_run_id` remains the source-run relationship.

Public workflow-control functions normalize caller input into these signals and
then hand them to the journal signal interpreter. `Squidie.apply_signal/2`
uses the same path for starts, cron starts, replays, cancellation, and manual
decisions. That keeps public callers on Squidie concepts while host apps that
already normalize commands at their own boundary can pass a
`Squidie.Runtime.Signal` directly. The named helpers remain the ergonomic API
for ordinary application code; `apply_signal/2` is the envelope API for agents,
routers, schedulers, webhooks, and Jido interop boundaries.

Workflow definitions are authored with the Squidie DSL. Runtime signals
start, replay, cancel, or resolve runs of those definitions.

When a command reaches the journal runtime, Squidie records a
`:run_signal_received` fact in the run thread before the command's lifecycle
facts. Starts, cron starts, manual approvals, rejections, resumes,
cancellations, and replays all use that audit shape. The fact stores the signal
type, run id when available, payload, actor, comment, metadata, idempotency key,
and occurrence time. Metadata is redacted for common sensitive keys before it is
persisted.

The command receipt and command application facts are appended together with one
thread revision fence. That keeps the journal as the source of truth and avoids a
crash window where inspection could see a command receipt without the matching
workflow-state change. Duplicate commands keep their existing semantics only
when the same idempotency key is reused; a different key is a distinct command.

Inspection exposes the projected command receipts through
`Snapshot.command_history`, ordered by receipt time. This is the lightweight
operator-facing command audit surface; `include_history: true` still controls
the detailed step and manual audit events.

The Jido adapter uses CloudEvents-compatible envelopes with source
`/squidie/runtime/commands`, type names such as
`squidie.runtime.command.start_run`, and content type
`application/vnd.squidie.runtime-signal+json`. The envelope `data` holds the
Squidie command type, payload, metadata, occurrence timestamp, and
idempotency key. `from_jido/1` accepts only the known Squidie source and
command types, and maps serialized string command names through an explicit
whitelist rather than creating atoms from input.

## Trace And Telemetry Flow

The runtime exposes live telemetry without changing journal authority:

```mermaid
flowchart LR
    Signal[Command signal and trace] --> Command[Command apply span]
    Command --> Journal[Committed journal facts]
    Journal --> Points[Lifecycle point events]
    Journal --> Executor[Executor span]
    Executor --> Step[Step invocation span]
    Step --> Journal
```

Command, executor, and actual step invocation boundaries emit symmetric
`:start`, `:stop`, or `:exception` span events. Successfully appended lifecycle
facts emit point events in append order. For Squidie-owned Ecto step
transactions, completion points are buffered until commit and discarded on
rollback. They flush before the executor stop event.

Telemetry is sanitized, bounded, and best-effort. It excludes payloads,
results, raw errors, claim tokens, owner values, idempotency keys, arbitrary
metadata, and trace state. Handlers, metrics reporters, exporters, dashboards,
and alerting remain host-owned. See [Observability](observability.md) for the
event catalog, metadata contract, metrics, and delivery limits.

## Runtime Capability Matrix

```mermaid
flowchart LR
    Start[Start workflow run] --> RunThread[Append run_started]
    RunThread --> Plan[Append planned runnables]
    Plan --> DispatchThread[Append attempt_scheduled]
    DispatchThread --> Claim[Worker claims attempt]
    Claim --> Heartbeat[Heartbeat extends lease]
    Heartbeat --> Execute[Run Jido action]
    Execute --> Complete[Append attempt_completed or attempt_failed]
    Complete --> Apply[Append runnable_applied]
    Apply --> Decide{More runnable work?}
    Decide -- yes --> Plan
    Decide -- no --> Terminal[Append run_terminal]
```

The journal-backed runtime uses two different kinds of durable state:

- journal entries: the source of truth for lifecycle facts
- checkpoints: cached projections that speed up rebuilds

Checkpoints are always disposable. If a checkpoint is missing or stale, the
agent can replay entries from the thread and reconstruct the same projection.

```mermaid
flowchart TD
    RunJournal[Run thread entries] --> RunProjection[WorkflowAgent projection]
    DispatchJournal[Dispatch thread entries] --> DispatchProjection[DispatchAgent projection]
    RunProjection --> ListRuns[list_runs]
    RunProjection --> InspectRun[inspect_run]
    RunProjection --> ExplainRun[explain_run]
    DispatchProjection --> ExplainRun
    RunProjection --> Editor[SquidSonar / visual editor]
    DispatchProjection --> Editor
```

This is the contract visual tooling should target. The editor reads from
projections, not from worker processes or live queue internals.

## Execution Ordering

The runtime is careful about which durable fact is written before the next
effect becomes visible. The ordering below is the core safety model for normal
step execution, retry, and successor dispatch.

```mermaid
sequenceDiagram
    participant API as Public API
    participant Run as Run thread
    participant Dispatch as Dispatch thread
    participant Worker as Worker loop
    participant Step as Step module

    API->>Run: append run_started
    Run->>Run: append runnable_planned
    Run->>Dispatch: append attempt_scheduled
    Worker->>Dispatch: append attempt_claimed
    Dispatch-->>Worker: claim fence
    Worker->>Step: execute input
    Step-->>Worker: ok or error
    Worker->>Dispatch: append attempt_completed or attempt_failed
    Dispatch->>Run: append runnable_applied
    Run->>Run: append successor plan or terminal fact
    Run->>Dispatch: append successor attempt if needed
```

The worker never becomes the authority for workflow progress. It only holds a
claim fence long enough to execute a visible attempt and report the result.

## Journal Threads

```mermaid
erDiagram
    RUN_THREAD ||--o{ RUN_ENTRY : contains
    DISPATCH_THREAD ||--o{ DISPATCH_ENTRY : contains
    RUN_INDEX_THREAD ||--o{ INDEX_ENTRY : contains
    RUN_CATALOG_THREAD ||--o{ CATALOG_ENTRY : contains
    RUN_ENTRY {
        string type
        string run_id
        string runnable_key
        string step
        datetime occurred_at
    }
    DISPATCH_ENTRY {
        string type
        string queue
        string runnable_key
        string claim_id
        datetime lease_until
    }
    INDEX_ENTRY {
        string type
        string workflow
        string queue
        string run_id
        datetime occurred_at
    }
    CATALOG_ENTRY {
        string type
        string workflow
        string queue
        string run_id
        datetime occurred_at
    }
```

| Thread | Example Jido thread id | Purpose |
| --- | --- | --- |
| Run thread | `squidie:run:<run-id>` | Workflow lifecycle facts for one run |
| Dispatch thread | `squidie:dispatch:<queue>` | Queue-visible attempts, claims, heartbeats, retries, completions, and failures |
| Run index thread | `squidie:run_index:<workflow>` | Rebuildable lookup facts for host-facing run discovery |
| Run catalog thread | `squidie:run_catalog:all` | Global lookup facts for all-run discovery |

Each append uses the current thread revision as an optimistic fence. A stale
caller that tries to append based on an old projection receives a conflict
instead of silently overwriting runtime state.

## Projection Rebuild Map

The read model is intentionally rebuildable from journal threads. Checkpoints
only shorten replay; deleting them must not change the resulting state.

```mermaid
flowchart TD
    subgraph Storage[Jido storage]
        RunThread[Run thread entries]
        DispatchThread[Dispatch thread entries]
        IndexThread[Run index entries]
        CatalogThread[Run catalog entries]
        Checkpoints[Projection checkpoints]
    end

    RunThread --> WorkflowAgent[WorkflowAgent projection]
    DispatchThread --> DispatchAgent[DispatchAgent projection]
    IndexThread --> RunIndex[Workflow run index]
    CatalogThread --> RunCatalog[Global run catalog]
    Checkpoints -. accelerate replay .-> WorkflowAgent
    Checkpoints -. accelerate replay .-> DispatchAgent

    WorkflowAgent --> Inspection[inspect_run snapshot]
    DispatchAgent --> Inspection
    RunIndex --> Listing[list_runs by workflow]
    RunCatalog --> Listing
    Inspection --> Explanation[explain_run details]
    Inspection --> Graph[inspect_run_graph nodes and edges]
```

This is the boundary SquidSonar and visual editors should depend on: listing
comes from catalog or index projections, while run detail and graph views come
from inspection projections.

## Agents

Squidie uses Jido agents as rebuildable runtime coordinators, not as a new
business workflow authoring surface.

```mermaid
flowchart TD
    subgraph RunA[Workflow run A]
        WA[WorkflowAgent]
        RT[run thread]
        WA --- RT
    end

    subgraph Queue[Dispatch queue]
        DA[DispatchAgent]
        DT[dispatch thread]
        DA --- DT
    end

    WA -- planned runnables --> DA
    DA -- completed attempts --> WA
```

| Agent | Cardinality | Rebuilds from | Main questions it answers |
| --- | --- | --- | --- |
| `WorkflowAgent` | One per workflow run | Run thread and checkpoint | What is planned, applied, waiting, terminal, or recoverable for this run? |
| `DispatchAgent` | One per queue | Dispatch thread and checkpoint | Which attempts are visible, claimed, expired, completed, failed, or retryable? |
| Future step agent | Optional, per long-running step or sub-agent | A step-owned thread or parent run thread | What state belongs inside one long-running autonomous step? |

The phrase "a workflow run is coordinated by an agent" is useful with one
important nuance: the workflow definition remains declarative data, and each
step remains a business action or built-in step.

## Heartbeats And Leases

Heartbeats belong to dispatch claims. They are not a second workflow state
machine and they do not make external side effects exactly-once.

```mermaid
sequenceDiagram
    participant Worker
    participant DispatchAgent
    participant Journal
    participant WorkflowAgent

    Worker->>DispatchAgent: claim_next(owner_id)
    DispatchAgent->>Journal: append attempt_claimed(expected_rev)
    Journal-->>DispatchAgent: new thread revision
    DispatchAgent-->>Worker: claim_id + raw claim_token + lease_until

    loop while work is still active
        Worker->>DispatchAgent: heartbeat(runnable_key, claim_id, token)
        DispatchAgent->>Journal: append attempt_heartbeat(expected_rev)
        Journal-->>DispatchAgent: extended lease_until
    end

    Worker->>DispatchAgent: complete/fail with claim fence
    DispatchAgent->>Journal: append attempt_completed or attempt_failed
    DispatchAgent->>WorkflowAgent: wake/apply completed result
    WorkflowAgent->>Journal: append runnable_applied
```

Heartbeat rules:

| Rule | Reason |
| --- | --- |
| A heartbeat must include the current `claim_id` and raw claim token | Prevents an old worker from extending a replacement worker's lease |
| The journal stores only `claim_token_hash` | Keeps the durable audit trail useful without storing bearer tokens |
| Heartbeats extend `lease_until` only before the current lease expires | Makes expired work recoverable without active takeover |
| Completion and failure use the same claim fence | Prevents stale workers from reporting final results after losing the lease |
| Expired claims remain visible to projection rebuilds | Allows recovery after worker death or node restart |

For long-running steps, this heartbeat path lets the runtime distinguish "still
alive" from "needs recovery". A lease-capable backend should own the concrete
lease mechanics when a host needs backend-owned worker fencing, with Squidie
translating the resulting lifecycle facts into its dispatch projection.

## Recovery Flow

```mermaid
stateDiagram-v2
    [*] --> RebuildWorkflowAgent
    RebuildWorkflowAgent --> RebuildDispatchAgent
    RebuildDispatchAgent --> ScheduleMissingDispatches
    ScheduleMissingDispatches --> ApplyCompletedResults
    ApplyCompletedResults --> Ready
    Ready --> [*]

    ScheduleMissingDispatches --> Conflict: stale dispatch revision
    ApplyCompletedResults --> Conflict: stale run revision
    Conflict --> RebuildWorkflowAgent
```

`Squidie.Runtime.AgentRecovery` drains two restart-safe windows in order:

1. Planned-but-unscheduled runnables are written to the dispatch thread.
2. Completed-but-unapplied dispatch results are written back to the run thread.

This ordering matters. A restarted node should first make all durable workflow
intent visible to dispatch before applying finished work back to the workflow
projection.

## Failure Handling Matrix

| Failure | Durable evidence | Recovery behavior |
| --- | --- | --- |
| Crash after planning but before scheduling dispatch | Run thread has planned runnable; dispatch thread lacks attempt | `WorkflowAgent.schedule_pending_dispatches/4` appends missing attempts |
| Worker dies mid-step | Dispatch thread has claimed attempt; heartbeat stops and lease expires | Attempt becomes claimable again after expiry |
| Duplicate worker delivery | Dispatch projection already has active or terminal attempt state | Duplicate claim or completion is rejected, ignored, or reported as anomaly |
| Completion wakeup is lost | Dispatch thread has completed attempt; run thread lacks `runnable_applied` | `WorkflowAgent.apply_pending_results/4` appends the missing application |
| Run reaches terminal state while dispatch work exists | Run thread has `run_terminal` | Rebuilt dispatch views exclude terminal-run attempts from redelivery |
| Stale projection writes | Append uses old thread revision | `Jido.Storage` returns conflict; caller rebuilds |

## Where Backend Leases Fit

Backend leases are optional runtime infrastructure. They are not the place
where Squidie workflow semantics move.

```mermaid
flowchart LR
    DispatchAgent --> Adapter[Lease adapter]
    Adapter --> Backend[Durable queue or lease backend]
    Backend --> Worker[Worker process]
    Worker --> Adapter
    Adapter --> DispatchAgent
```

| Squidie concept | Backend-facing concept |
| --- | --- |
| Runnable intent | Durable work item, job, or intent |
| `runnable_key` | Backend key, idempotency key, or lineage metadata |
| Claim and heartbeat | Backend lease lifecycle |
| Completion or failure | Intent result translated back to dispatch facts |
| Retry visibility | Durable rescheduling or delayed visibility |

This keeps setup friction low for most users while preserving an escape hatch:
basic hosts can use a simple `execute_next/1` worker loop, while advanced hosts
can connect a backend lease adapter when they need stronger distributed worker
ownership. Bedrock is the recommended reference backend today because the
example app exercises queueing, delayed visibility, claims, heartbeats,
completion, retry, and dead-letter behavior without coupling workflow modules
to Bedrock APIs.

## AI-Backed Steps

In the journal-backed runtime, the workflow run is coordinated by a `WorkflowAgent`.
That means Squidie does not need a separate step kind just because a step
implementation uses an LLM, calls tools, or delegates some local decision-making
to Jido.

AI-backed work should usually be modeled as an ordinary step:

```elixir
step :triage_ticket, MyApp.Steps.TriageTicket,
  input: [:ticket],
  output: :triage,
  retry: [max_attempts: 2]
```

That keeps the important contract visible:

- the workflow owns lifecycle, retries, replay, cancellation, and audit history
- the step owns its input/output contract and side-effect safety
- model calls and tool calls stay inside the step boundary
- inspection can explain the workflow without inventing a second workflow
  primitive

The closed `agent_step/3` issue
[#138](https://github.com/dark-trench/squidie/issues/138) explored an
explicit metadata marker for agentic steps. With the workflow run itself now
coordinated by a Jido agent, that separate DSL construct is not currently part
of the core runtime surface.

A new construct would only be worth adding later if it has different lifecycle
semantics from a normal step. Examples might include a child journal, independent
checkpointing, or a bounded sub-agent whose internal state must survive
pause/resume, retry, replay, and deploys.

That possible shape would look like this:

```mermaid
flowchart TD
    ParentRun[Parent WorkflowAgent] --> AgentStep[Agent-backed step]
    AgentStep --> StepThread[Step agent journal]
    AgentStep --> ToolCalls[Tools or external services]
    AgentStep --> Human[Human approval or input]
    StepThread --> Result[Step result]
    Result --> ParentRun
```

Design questions before adding such a construct:

| Question | Direction |
| --- | --- |
| Does this need a child journal, or is a normal step enough? | Prefer a normal step unless separate durable state is required |
| How much child state should appear in `Squidie.explain_run/2`? | Surface high-signal checkpoints and links, not every internal token |
| How are permissions applied inside child work? | Host app policy should remain the trust boundary |
| Can child work be replayed safely? | Require explicit replay contracts and side-effect idempotency |
| Can child work outlive its parent run? | Default no; terminal parent runs should fence child work |

## Runtime Shape

| Area | Current path | Notes |
| --- | --- | --- |
| Workflow authoring | Squidie DSL | Workflow authors do not need to write Jido agents directly |
| Step execution | `Squidie.Step` and `Jido.Action` interop | Workers claim visible attempts with `Squidie.execute_next/1`; both paths receive safe attempt metadata in context |
| Durable run state | Jido-backed run threads plus projections | The default Ecto adapter stores threads, entries, and checkpoints in the host repo |
| Dispatch | Dispatch agent plus journal attempts | Live wakeups go through `Squidie.Runtime.DispatchNotifier`; backend-owned leases can be layered through `Squidie.Executor.Leases` |
| Long-running recovery | Lease heartbeat, expired claim recovery, journal rebuild | Timeout-based step reclaim is not part of the public config |
| Inspection | Projection-backed snapshots and explanations | Inspection rebuilds from journal facts |
| Storage | `Jido.Storage` adapters | Postgres-compatible Ecto storage is the default supported path |

## Runtime Feature Map

Projection-backed inspection rebuilds workflow and dispatch agent projections
into a read-only view of pending dispatches, unapplied results, scheduled
attempts, visible attempts, expired claims, manual pause or approval state,
terminal state, and projection anomalies. Run-index projections rebuild
workflow-scoped run lookup state from durable index entries, while the global
run-catalog projection rebuilds all-run lookup state without scanning adapter
internals. Both facts retain the queue each run was dispatched through and keep
malformed or conflicting facts visible as anomalies. The projected explanation
layer derives deterministic reason-specific details and next actions from the
inspection snapshot. The public `Squidie.inspect_run/2`,
`Squidie.list_runs/2`, and `Squidie.explain_run/2` APIs expose this read
model by default and infer Ecto storage from the configured repo. Host apps can
still pass explicit `journal_storage:` or `queue:` overrides when a test or
integration boundary needs a non-default journal boundary. Public start,
listing, execution, inspection, explanation, and manual-control APIs pick up
the configured defaults without repeating journal options at every call site.

The journal start path appends run, run-index, and run-catalog facts to
`Jido.Storage`, rebuilds the workflow and dispatch agents, schedules the initial
dispatch attempts from the journal, and returns the projection-backed inspection
snapshot. Journal execution currently supports normal action steps, immediate
built-in `:log` steps, built-in `:wait` steps in transition and dependency
workflows, and manual `:pause` or `:approval` boundaries. Manual boundaries
persist intervention state: `resume/3` resumes `:pause` steps, while
`approve/3` and `reject/3` resolve `:approval` decisions through the
configured journal runtime.

| Feature | Issue | Runtime dependency |
| --- | --- | --- |
| Projection-backed inspection and explanation hardening | No active issue | Additional coverage for ambiguous attempt states and operator-facing edge cases |
| Conditional paths and deferred continuation | [#140](https://github.com/dark-trench/squidie/issues/140) | Durable planner facts and wakeup metadata |
| Dynamic child runs | [#141](https://github.com/dark-trench/squidie/issues/141) | Stable parent runnable keys, idempotent child keys, inspectable parent-child lineage |
| Advanced reference workflows | [#109](https://github.com/dark-trench/squidie/issues/109) | Implemented target features only, without Oban-specific assumptions |
| Child-agent step lifecycle | No active core issue | Only relevant if normal steps are insufficient because child journal semantics are required |

## Reading Order

After this overview, read:

1. [Architecture](architecture.md) for the current component list.
2. [Durable dispatch protocol](durable_dispatch_protocol.md) for exact journal
   entry semantics.
3. [Operations guide](operations.md) for current production boundaries.
4. [Workflow authoring](workflow_authoring.md) for the DSL that remains stable
   while backend execution choices evolve.
