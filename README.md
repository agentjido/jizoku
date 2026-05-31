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

Host code can also preview and record bounded dynamic work metadata for an
active run without making those nodes executable yet:

```elixir
{:ok, preview} =
  SquidMesh.preview_dynamic_work(run.run_id, %{
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
  })

preview.origin_node_id
preview.added_node_ids
preview.added_edge_ids
preview.recordable?
preview.graph.nodes

{:ok, snapshot} =
  SquidMesh.record_dynamic_work(run.run_id, %{
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
  })
```

`preview_dynamic_work/3` and `record_dynamic_work/3` share validation for stable
ids, origin metadata, nodes, and optional edges against the current run snapshot.
Preview returns the normalized dynamic work plus a graph overlay without
appending a journal fact. It also exposes stable overlay metadata for visual
editors: the producer node id, added node ids, added edge ids, whether recording
would append a new durable fact, and warnings such as duplicate dynamic work.
Recording appends the durable fact. Terminal runs reject new dynamic work. The
recorded structure is visible through
`inspect_run/2`, `inspect_run_graph/2`, and `explain_run/2`, but it remains
inspection-only until executable dynamic graph expansion is added.

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
