<div align="left">

# SquidMesh

<p>
  <a href="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml">
    <img alt="CI" src="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/dark-trench/squid_mesh">
    <img alt="Codecov" src="https://codecov.io/gh/dark-trench/squid_mesh/branch/main/graph/badge.svg" />
  </a>
  <a href="https://hex.pm/packages/squid_mesh">
    <img alt="Hex.pm" src="https://img.shields.io/hexpm/v/squid_mesh" />
  </a>
  <a href="https://hexdocs.pm/squid_mesh">
    <img alt="HexDocs" src="https://img.shields.io/badge/docs-hexdocs-purple" />
  </a>
  <a href="https://github.com/dark-trench/squid_mesh/blob/main/LICENSE">
    <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
  </a>
</p>

</div>

---

Squid Mesh is an embedded durable workflow runtime for Elixir applications. Workflows are declared as Elixir modules through a DSL, persisted through Jido journals, and executed by host-owned workers calling `SquidMesh.execute_next/1`.

The runtime stores workflow state, step attempts, retries, approvals, transitions, audit events, and recovery history in the host application's database through `Jido.Storage` and the default Ecto adapter. Squid Mesh does not run as a separate service, broker, or orchestration cluster. The host application retains its existing supervision tree, deployment model, repository, schedulers, and queue backend.

Storage portability is defined by the journal storage adapter contract, not arbitrary database compatibility. The production relational implementation uses a Postgres-compatible Ecto adapter. See the [storage strategy](docs/storage_strategy.md) for adapter guarantees.

Squid Mesh manages workflow progression, transition routing, retry semantics, pause and approval handling, replay and recovery policy, durable execution history, and graph inspection. Queue delivery, worker supervision, and backend leasing remain host-owned concerns.

The runtime builds on [Jido](https://github.com/agentjido/jido) for actions, execution, and journaling; [Runic](https://github.com/dark-trench/runic) for workflow planning; and [Spark](https://github.com/ash-project/spark) for the DSL authoring surface.

> **Warning**
> Squid Mesh is in early development. The runtime is suitable for evaluation, local development, and integration work, but is not production-ready. See [Production Readiness](docs/production_readiness.md) for the current status and remaining work.

## Start Here

The fastest way to start is the guided Livebook. It demonstrates creating a workflow, starting a journal-backed run, executing work with `SquidMesh.execute_next/1`, and inspecting the durable result.

[![Run in Livebook](https://livebook.dev/badge/v1/pink.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fdark-trench%2Fsquid_mesh%2Fblob%2Fmain%2Fdocs%2Fgetting_started.livemd)

| Goal | Resource |
| --- | --- |
| Run a guided interactive example | [Getting Started Livebook](docs/getting_started.livemd) |
| Integrate Squid Mesh into an existing application | [Getting Started guide](docs/getting_started.md) |
| Review a complete working example | [Minimal host app](examples/minimal_host_app/README.md) |

The written guide covers installation, workflow creation, journal execution, run inspection, retries, manual gates, cron triggers, and Bedrock-backed leases.

## Jido Primitive Boundary

Squid Mesh uses Jido as an internal runtime foundation while keeping the public workflow API focused on Squid Mesh concepts. The runtime uses these Jido primitives:

| Jido primitive | Squid Mesh use |
| --- | --- |
| `Jido.Agent` | Rebuildable workflow and dispatch coordination state |
| `Jido.Action` | Step execution interop, including raw Jido action modules and the native `SquidMesh.Step` adapter |
| `Jido.Storage` | Journal and checkpoint persistence boundary |
| `Jido.Thread` / `Jido.Thread.Entry` | Durable journal facts for run, dispatch, index, and catalog threads |
| `Jido.Exec` | Action execution inside the journal executor |
| `Jido.Signal` | Interop boundary envelope for Squid Mesh runtime command signals |

Support code uses lower-level primitives such as `Jido.Thread.EntryNormalizer` and validates built-in storage adapters like `Jido.Storage.File` and `Jido.Storage.Redis`. Workflow authors do not need to use these primitives directly.

Runtime command signals use `SquidMesh.Runtime.Signal` as the stable contract. `SquidMesh.Runtime.Signal.JidoAdapter` converts between `SquidMesh.Runtime.Signal` structs and `Jido.Signal` envelopes for advanced runtime integration. Public callers use Squid Mesh APIs directly and do not need to construct raw `Jido.Signal` values.

Journal-backed runtime commands are persisted as run-thread command receipts before their lifecycle facts. `SquidMesh.inspect_run/2` exposes command history through `snapshot.command_history`, including signal type, payload, actor and comment when supplied, redacted metadata, idempotency key when relevant, and occurrence time.

## Getting Started

Documentation and examples:

| Reference | Description |
| --- | --- |
| [Getting Started](docs/getting_started.md) | Setup and first workflow run |
| [Workflow Authoring](docs/workflow_authoring.md) | Triggers, steps, transitions, retries, and compensation |
| [Host App Integration](docs/host_app_integration.md) | Phoenix and OTP integration |
| [Reference Workflows](docs/reference_workflows.md) | Approval, recovery, saga, and cron examples |
| [Minimal Host App](examples/minimal_host_app/README.md) | Executable example application |
| [Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md) | Backend-owned delivery with leases and retry requeue |
| [Architecture](docs/architecture.md) | Runtime flow and component boundaries |
| [Positioning Guide](docs/positioning.md) | Comparison with adjacent projects |

## Installation

Add Squid Mesh to your dependencies:

```elixir
defp deps do
  [
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

If your host application defines raw `Jido.Action` modules directly, add `:jido` explicitly as well:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

Configure the repo and default queue:

```elixir
config :squid_mesh,
  repo: MiddleEarth.Repo,
  queue: "default"
```

Install and run the migration:

```sh
mix deps.get
mix squid_mesh.install
mix ecto.migrate
```

To keep workflow modules formatted consistently as DSL-style declarations, import Squid Mesh formatter rules in `.formatter.exs`:

```elixir
[
  import_deps: [:squid_mesh],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
```

Finally, start one supervised worker loop. See [Host App Integration](docs/host_app_integration.md) for a minimal worker shape.

### Optional: Bedrock Job Runner And Leases

Use Bedrock when the host application needs backend-owned delivery, delayed
visibility, job leases, heartbeat/lease extension, retry requeue, and recovery.
Keep workflow modules backend-neutral; Bedrock belongs behind host adapter
modules.

If a simple supervised process can call `SquidMesh.execute_next/1` often enough
for your workload, start there. Add Bedrock only when the host needs a durable
job runner to own payload delivery, delayed visibility, worker leases, and
redelivery after worker or node failure.

At a high level:

1. Configure Squid Mesh with the host repo and the journal queue used by the
   Bedrock payload worker.
2. Configure a Bedrock queue for Squid Mesh payload delivery.
3. Start the host repo, Bedrock cluster, and Bedrock job queue under the host
   application's supervision tree.
4. Add a delivery adapter that maps cron payloads to Bedrock jobs.
5. Add a payload worker that calls `SquidMesh.execute_next/1` while the Bedrock
   job lease is held.
6. Configure both lease layers: the Bedrock job lease for payload delivery and
   `heartbeat_interval_ms` for Squid Mesh journal attempt claims.

Those leases are separate. The Bedrock lease protects job delivery; the Squid
Mesh heartbeat protects the workflow attempt claimed by `execute_next/1`.

The payload worker is the executor boundary. Keep these responsibilities
separate:

| Concern | Owner |
| --- | --- |
| Persisted workflow state, step attempts, step retry policy, terminal run status | Squid Mesh |
| Claiming and executing the next visible workflow attempt | `SquidMesh.execute_next/1` |
| Keeping a long-running workflow attempt claim alive | `heartbeat_interval_ms` passed to `execute_next/1` |
| Payload delivery, delayed visibility, job leases, and redelivery after worker failure | Bedrock |

Do not enqueue one Bedrock job per workflow step, and do not model workflow step
retries as Bedrock job retries. A normal step failure, retry, or terminal run is
durable Squid Mesh state returned by `SquidMesh.execute_next/1`. Bedrock should
retry only job-level delivery failures, such as a crashed payload worker or a
transient backend error before the worker can finish draining journal attempts.

The payload worker should usually treat `{:ok, snapshot}` from
`execute_next/1` as successful job progress even when the snapshot describes a
failed workflow run. Return `{:error, reason}` to Bedrock only when the payload
delivery or journal drain itself failed and should be redelivered.

The host-owned wiring looks like this in shape:

```elixir
# config/config.exs
config :squid_mesh,
  repo: MyApp.Repo,
  queue: "tenant_a"

config :my_app, MyApp.SquidMeshDeliveryAdapter,
  queue_id: "tenant_a",
  topic: "squid_mesh:payload"

config :my_app, MyApp.Jobs.SquidMeshPayload,
  journal_heartbeat_interval_ms: 10_000,
  max_journal_attempts: 50
```

```elixir
defmodule MyApp.Jobs.SquidMeshPayload do
  use Bedrock.JobQueue.Job,
    topic: "squid_mesh:payload",
    # Job retry covers payload delivery only. Step retry lives in the workflow DSL.
    max_retries: 3

  alias SquidMesh.Runtime.Runner

  def perform(payload, _meta) when is_map(payload) do
    case Runner.perform(payload) do
      :ok -> drain_journal("tenant_a", 0)
      {:ok, _snapshot} -> drain_journal("tenant_a", 0)
      {:error, reason} -> {:error, reason}
    end
  end

  defp drain_journal(_queue, 50), do: {:error, :journal_drain_limit_exceeded}

  defp drain_journal(queue, count) do
    case SquidMesh.execute_next(
           queue: queue,
           owner_id: "my-app-bedrock-worker",
           heartbeat_interval_ms: 10_000
         ) do
      {:ok, :none} -> :ok
      # The snapshot may be completed, failed, paused, or still running.
      # It is still successful job progress because Squid Mesh persisted it.
      {:ok, _snapshot} -> drain_journal(queue, count + 1)
      # Return an error only for executor/drain failures Bedrock should redeliver.
      {:error, reason} -> {:error, reason}
    end
  end
end
```

For the concrete setup, see
[Bedrock Lease Backend Setup](docs/host_app_integration.md#bedrock-lease-backend-setup)
and the
[Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md).

## Workflows

Workflows are Elixir modules. A trigger declares the entrypoint and validates the payload before the run is persisted. Steps declare their inputs, outputs, retry policy, and compensation behavior. Transitions wire them together.

This workflow demonstrates manual gates, approval flows, conditional routing, retries, saga compensation, and irreversible steps:

```elixir
defmodule MiddleEarth.Workflows.RingErrand do
  use SquidMesh.Workflow

  workflow do
    trigger :leave_shire do
      manual()

      payload do
        field :bearer, :string, default: "Frodo"
        field :ring_id, :string
        field :route_preference, :string, default: "moria"
      end
    end

    step :pack_provisions, Hobbiton.Steps.PackProvisions,
      output: :provisions

    step :hide_at_prancing_pony, :pause

    approval_step :council_vote,
      output: :council

    step :choose_path, Rivendell.Steps.ChoosePath,
      input: [bearer: [:bearer], decision: [:council, :decision]],
      output: :route

    step :cross_moria, Fellowship.Steps.CrossMoria,
      input: [:bearer, :provisions, :route],
      retry: [max_attempts: 3, backoff: [type: :exponential]]

    step :reserve_eagle, Eagles.Steps.ReserveRide,
      compensate: Eagles.Steps.CancelRide

    step :toss_ring, Mordor.Steps.TossRing,
      irreversible: true

    transition :pack_provisions, on: :ok, to: :hide_at_prancing_pony
    transition :hide_at_prancing_pony, on: :ok, to: :council_vote
    transition :council_vote, on: :ok, to: :choose_path
    transition :choose_path, on: :ok, to: :cross_moria
    transition :cross_moria, on: :ok, to: :reserve_eagle
    transition :cross_moria, on: :error, to: :complete, recovery: :undo
    transition :reserve_eagle, on: :ok, to: :toss_ring
    transition :toss_ring, on: :ok, to: :complete
  end
end
```

Cron-triggered workflows use scheduling declarations:

```elixir
defmodule Gondor.Workflows.BeaconWatch do
  use SquidMesh.Workflow

  workflow do
    trigger :nightly_beacon_check do
      cron "0 21 * * *", timezone: "Etc/UTC"

      payload do
        field :beacon_count, :integer, default: 7
      end
    end

    step :inspect_hilltops, Gondor.Steps.InspectHilltops,
      retry: [max_attempts: 3]

    step :light_beacon, Gondor.Steps.LightBeacon,
      compensate: Gondor.Steps.ExtinguishBeacon

    transition :inspect_hilltops, on: :ok, to: :light_beacon
    transition :light_beacon, on: :ok, to: :complete
  end
end
```

Dependency-based workflows use `after: [...]` for parallel execution:

```elixir
defmodule Gondor.Workflows.ParallelAttack do
  use SquidMesh.Workflow

  workflow do
    trigger :start do
      manual()
    end

    step :march_to_gate, Gondor.Steps.MarchToGate
    step :rally_rohan, Rohan.Steps.RallyArmy
    step :distract_sauron, Fellowship.Steps.DistractEnemy

    step :declare_victory, Gondor.Steps.DeclareVictory,
      after: [:march_to_gate, :rally_rohan, :distract_sauron]
  end
end
```

## Running Workflows

Start a workflow run:

```elixir
{:ok, run} =
  SquidMesh.start(
    MiddleEarth.Workflows.RingErrand,
    :leave_shire,
    %{ring_id: "one-ring"}
  )
```

Inspect a run with full history:

```elixir
SquidMesh.inspect_run(run.run_id, include_history: true)
```

Get an operator-facing explanation:

```elixir
{:ok, explanation} = SquidMesh.explain_run(run.run_id)
explanation.reason #=> :waiting_for_retry
explanation.evidence.command_counts #=> %{"start_run" => 1, "cancel_run" => 2}
```

The `explain_run/2` function summarizes the current state, valid next actions, and supporting evidence for dashboards and operational tooling.

## Approvals and Manual Gates

Pause steps and approval steps block progression until explicitly resolved:

```elixir
# Resume a paused step
SquidMesh.resume(run.run_id, %{actor: "strider", reason: "ready to proceed"})

# Approve or reject an approval gate
SquidMesh.approve(run.run_id, %{actor: "elrond", note: "approved"})
SquidMesh.reject(run.run_id, %{actor: "elrond", note: "rejected"})
```

For idempotent command delivery, use explicit runtime signals:

```elixir
alias SquidMesh.Runtime.Signal

{:ok, signal} =
  Signal.approve_run(run.run_id, %{actor: "elrond", note: "approved"},
    idempotency_key: "approval-#{run.run_id}"
  )

{:ok, approved_run} = SquidMesh.apply_signal(signal)
```

Reusing an idempotency key returns the existing result without creating duplicate command receipts. Approval steps persist their resolved targets and output metadata, surviving deploys and restarts.

## Compensation and Recovery

Workflow authors can mark completed side effects as compensatable so operators
and host tools can see the rollback contract when later work fails:

```elixir
step :borrow_rope, Lothlorien.Steps.BorrowRope,
  compensate: Lothlorien.Steps.ReturnRope

step :reserve_eagle, Eagles.Steps.ReserveRide,
  compensate: Eagles.Steps.CancelRide

step :cross_moria, Fellowship.Steps.CrossMoria,
  retry: [max_attempts: 3]
```

A failed `:cross_moria` exposes the completed compensatable steps and their
declared callbacks through `inspect_run/2`, `inspect_run_graph/2`, and
`explain_run/2`. The callback metadata is persisted with each runnable so
dashboards can show rollback availability even if the workflow module changes.

For side effects that cannot be reversed, mark steps as `irreversible: true` or `compensatable: false`. Squid Mesh exposes these boundaries during inspection and blocks replay by default after irreversible execution.

## Child Workflows

Steps can spawn child workflow runs for dynamic work expansion:

```elixir
defmodule Hobbiton.Steps.SendInvites do
  use SquidMesh.Step, name: :send_invites

  @impl true
  def run(%{party_id: party_id, guests: guests}, %SquidMesh.Step.Context{} = context) do
    children =
      for guest <- guests do
        {:ok, child} =
          SquidMesh.start_child_run(
            context,
            Hobbiton.Workflows.DeliverInvite,
            %{party_id: party_id, guest_id: guest.id},
            child_key: "invite_#{guest.id}"
          )

        child.run_id
      end

    {:ok, %{child_run_ids: children}}
  end
end
```

Each child run has independent inspection, retry, replay, and cancellation. Repeating the same `child_key` returns the existing child instead of creating duplicates.

## Inspectable Dynamic Work

Host code can preview, record, or schedule bounded dynamic work for an active
run. Preview is read-only, record persists inspection metadata, and schedule
persists the same dynamic-work fact while planning executable runnable intents:

```elixir
registry = %{"digest.deliver" => MyApp.Steps.DeliverDigest}

{:ok, preview} =
  SquidMesh.preview_dynamic_work(
    run.run_id,
    %{
      dynamic_key: "subscription_digest_fanout",
      origin: %{
        runnable_key: "run_123:schedule_digest:1",
        step: "schedule_digest",
        attempt: 1
      },
      reason: :runtime_fanout,
      nodes: [
        %{id: "deliver_digest:chat_1", action: "digest.deliver"}
      ]
    },
    action_registry: registry
  )

preview.origin_node_id
preview.added_node_ids
preview.added_edge_ids
preview.recordable?
preview.graph.nodes
```

After previewing, choose one durable write path. Use `record_dynamic_work/3`
when the dynamic structure should be inspectable only:

```elixir
{:ok, snapshot} =
  SquidMesh.record_dynamic_work(
    run.run_id,
    %{
      dynamic_key: "subscription_digest_fanout",
      origin: %{
        runnable_key: "run_123:schedule_digest:1",
        step: "schedule_digest",
        attempt: 1
      },
      reason: :runtime_fanout,
      nodes: [
        %{id: "deliver_digest:chat_1", action: "digest.deliver"}
      ]
    },
    action_registry: registry
  )
```

Use `schedule_dynamic_work/3` instead when the dynamic nodes should execute:

```elixir
{:ok, snapshot} =
  SquidMesh.schedule_dynamic_work(
    run.run_id,
    %{
      dynamic_key: "subscription_digest_fanout",
      origin: %{
        runnable_key: "run_123:schedule_digest:1",
        step: "schedule_digest",
        attempt: 1
      },
      reason: :runtime_fanout,
      nodes: [
        %{
          id: "deliver_digest:chat_1",
          action: "digest.deliver",
          input: %{subscription_id: "sub_123"}
        }
      ]
    },
    action_registry: registry
  )
```

`preview_dynamic_work/3`, `record_dynamic_work/3`, and
`schedule_dynamic_work/3` share validation for stable ids, origin metadata,
nodes, and optional edges against the current run snapshot. Scheduled dynamic
work requires `:action_registry`; each executable dynamic node must include an
approved action key and may include an `:input` map for its attempt. The origin
runnable must already be applied before executable dynamic work can be
scheduled.
Preview returns the normalized dynamic work plus a graph overlay without
appending a journal fact. It also exposes stable overlay metadata for visual
editors: the producer node id, added node ids, added edge ids, whether recording
would append a new durable fact, and warnings such as duplicate dynamic work.
Recording appends only the durable inspection fact. Scheduling appends that fact
and planned runnable intents in one run-thread write; the normal
`execute_next/1` worker path claims, executes, retries, applies, and inspects the
dynamic attempts. A scheduled dynamic node may opt into persisted retry with
`retry: [max_attempts: n]`. Dynamic edges are graph-inspection metadata for now;
scheduled dynamic nodes are queued as independent runnable intents. Dynamic
steps are replay-unsafe by default and require manual review before irreversible
replay. Recording and scheduling the same dynamic node are alternatives, not a
promotion flow; scheduling an already-recorded node with the same id is rejected
by duplicate-node validation. Terminal runs reject new dynamic work.
`inspect_run_graph/2` also exposes `dynamic_work_overlays` so dashboards and
visual editors can show producer nodes, added node ids, and added edge ids
without reconstructing them from raw dynamic-work records.

## Long-Running Steps

Workers can ask the journal executor to renew the active claim while a step is
running:

```elixir
SquidMesh.execute_next(
  owner_id: "billing-worker-1",
  lease_for: 30,
  heartbeat_interval_ms: 10_000
)
```

The executor keeps raw claim tokens internal. Durable heartbeat entries store
only the claim-token hash and are fenced by the same claim id and token used for
completion or failure. The minimum heartbeat interval is 50ms; production
workers should choose a much larger interval relative to `lease_for`.

## Runtime-Authored Specs

Host-owned editors or databases can activate validated workflow specs without
runtime code generation. Use stable action keys, resolve them through an
allowlist, then start the resolved spec through the public API:

```elixir
registry = %{"digest.record_delivery" => MyApp.Steps.RecordDigestDelivery}

:ok = SquidMesh.Workflow.validate_spec(spec, action_registry: registry)

{:ok, run} =
  SquidMesh.start_spec(spec, :manual_digest, payload,
    action_registry: registry
  )
```

Squid Mesh persists the resolved definition with the run so workers and
`inspect_run_graph/2` can inspect and execute it later. Replay for
runtime-authored spec runs is intentionally rejected until that lifecycle is
supported.

Visual-editor JSON can use the same host-owned action allowlist before a draft
graph with top-level action keys is accepted:

```elixir
:ok = SquidMesh.Workflow.EditorSpec.validate_map(editor_map, action_registry: registry)
{:ok, graph} = SquidMesh.Workflow.EditorSpec.preview_graph(editor_map, action_registry: registry)
{:ok, diff} = SquidMesh.Workflow.EditorSpec.diff(source_spec, editor_map, action_registry: registry)
```

These editor APIs still validate, preview, and compare data only. Starting a
runtime-authored run remains the separate `start_spec/3` or `start_spec/4`
boundary.

## Cancellation, Replay, and Listing

```elixir
{:ok, running_runs} = SquidMesh.list_runs(status: :running)
{:ok, _} = SquidMesh.cancel(run.run_id)
{:ok, _} = SquidMesh.replay(run.run_id)

# Replay past irreversible steps requires an explicit override
{:ok, _} = SquidMesh.replay(run.run_id, allow_irreversible: true)
```

## Graph Inspection

Inspect the workflow graph with execution state:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run.run_id)

graph
|> SquidMesh.Runs.GraphInspection.to_map()
|> Map.take([:status, :current_node_ids, :nodes, :edges])
```

The graph includes nodes, edges, and the selected transition path for conditional routing.
Nested workflow starts stay as separate runs; parent graph maps include
`child_links` so dashboards and visual editors can render subflow links without
treating child workflows as inline executable nodes.

### Node Visibility and Redaction

Graph nodes can include host-domain inputs, outputs, errors, manual metadata,
and dynamic-work metadata. By default, `inspect_run_graph/2` omits detailed
payload fields; request `include_history: true` only for trusted operator
surfaces.

Before exposing graph payloads outside a trusted boundary, apply a host-owned
visibility policy:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run.run_id, include_history: true)

{:ok, visible_graph} =
  SquidMesh.ReadModel.Visibility.redact(graph, current_actor, MyApp.VisibilityPolicy)
```

External/operator views preserve node ids, status, current state, recovery
availability, dynamic-work shape, and safe edge topology while removing node
payloads, errors, attempt internals, command history, and sensitive metadata.

## Actor Visibility

Squid Mesh provides built-in support for actor-scoped visibility to safely expose workflow data to different users. The runtime tracks actor information in manual actions and provides flexible redaction policies:

```elixir
# Define a visibility policy
defmodule MyApp.VisibilityPolicy do
  @behaviour SquidMesh.ReadModel.Visibility.Policy

  def visibility_scope(actor, _view) do
    cond do
      actor.role == "admin" -> :auditor     # Full access
      actor.role == "support" -> :operator  # Operational details
      true -> :external                     # Minimal information
    end
  end
end

# Apply redaction at API boundaries
{:ok, snapshot} = SquidMesh.inspect(run_id)
safe_view = SquidMesh.ReadModel.Visibility.redact(snapshot, current_user, MyApp.VisibilityPolicy)
```

The three standard scopes provide appropriate data access:
- `:external` - High-level status only, all sensitive data redacted
- `:operator` - Includes operational metrics and debugging information
- `:auditor` - Complete unredacted access for privileged users

See the [Actor Visibility Guide](docs/actor_visibility.md) for comprehensive documentation on implementing multi-tenant access patterns, role-based visibility, and security best practices.

## Optional Dashboard

[SquidSonar](https://github.com/dark-trench/squid_sonar) is the optional read-only Phoenix LiveView dashboard for Squid Mesh. Mount it inside a Phoenix host application to inspect recent runs, filter by status, search runtime metadata, and view run detail pages with diagnosis, history counts, last error information, and workflow graph visualization.

## Contributing

Please review the existing runtime model and workflow semantics before proposing substantial changes. Contributions are most welcome in: runtime reliability, workflow ergonomics, inspection tooling, recovery semantics, documentation improvements, backend integrations, and executable examples.

- [Contributing Guide](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Elixir Forum discussion thread](https://elixirforum.com/t/squid-mesh-workflow-automation-runtime-for-elixir-applications/75162)
- [GitHub Issues](https://github.com/dark-trench/squid_mesh/issues)
- [Squid Mesh channel on the Jido Discord](https://discord.com/channels/1323353012235796550/1504122798027571331)

## License

Copyright 2024, released under the [Apache 2.0 License](LICENSE).
