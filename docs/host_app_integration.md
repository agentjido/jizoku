# Host App Integration

This document defines the initial integration contract for:

- Phoenix applications
- OTP applications with an existing `Repo`
- existing installations that already run background jobs

## Tested Toolchain

Current CI and onboarding smoke tests run with:

- Erlang/OTP `28.4.1`
- Elixir `1.19.5-otp-28`
- `Jido 2.0+`

## Installation

Add `:squidie` to the host application's dependencies and fetch dependencies
as usual with Mix.

Preferred Hex dependency:

```elixir
defp deps do
  [
    {:squidie, "~> 0.3.5"}
  ]
end
```

If the host app defines custom steps with `use Jido.Action`, add `:jido`
explicitly to the host app as well rather than relying on a transitive
dependency:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squidie, "~> 0.3.5"}
  ]
end
```

Then install Squidie's library-owned migrations into the host app:

```sh
mix squidie.install
mix ecto.migrate
mix squidie.doctor --json --fail-on-drift
```

`mix squidie.install` creates one current-schema Squidie migration in the
host application's `priv/repo/migrations` directory. It does not install or run
migrations for the host application's job backend. The doctor command performs
a read-only structural check and gives CI a nonzero gate when the migrated host
database is behind or incompatible with Squidie's required baseline.

## Configuration

Start with three pieces:

1. Squidie config points at the host repo and runtime boundary.
2. The journal runtime owns its dispatch queue through Squidie config; the
   host app only needs a worker process that calls `Squidie.execute_next/1`.
3. Journal workers call `Squidie.execute_next/1` to claim and execute visible
   attempts.

The host application configures Squidie under the `:squidie` application:

```elixir
config :squidie,
  repo: MyApp.Repo,
  partition: "tenant_acme",
  queue: "default"
```

Host config keys:

- `:repo` - required for the default Ecto-backed journal setup; Squidie uses it
  to infer `{Squidie.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}` when
  `journal_storage:` is omitted

Optional keys:

- `:runtime` - `:journal` by default; routes public start, execution, and
  manual-control APIs through the Jido-native journal runtime
- `:read_model` - `:read_model` by default; routes inspection, graph
  inspection, and explanation through journal projections
- `:journal_storage` - optional for the default Ecto-backed setup; when omitted,
  Squidie uses `{Squidie.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}`.
  Set it only to override the storage adapter. Explicit `nil` is rejected for
  journal-backed runtime or read-model paths.
- `:queue` - `"default"` by default; selects the journal dispatch queue used by
  the configured journal runtime and read model
- `:partition` - omitted by default, preserving the exact legacy journal
  namespace. A validated string scopes run, dispatch, workflow-index,
  global-catalog, and checkpoint identities together.

When a host uses partitions, it must route the same trusted `:partition`
through start, worker, cron, signal, control, replay, and inspection calls.
Run UUIDs and queue names may repeat across partitions. Squidie does not search
another partition when a lookup misses, and a partition is not an authorization
boundary; authorize tenant or domain access in the host before selecting it.

Enabling a partition is a namespace cutover, not an in-place migration.
Existing unpartitioned runs remain in the legacy namespace and do not appear in
partitioned lists or recovery. Drain them there or perform an explicit,
application-owned migration before changing worker routing. Rolling the config
back selects the legacy namespace again; it does not merge partitioned history.

Public `Squidie.start/2`, `start/3`, and `start/4` calls resolve those defaults
through the application environment too. If a host app starts runs manually
from IEx, a Phoenix controller, or another direct boundary, it still needs
`config :squidie, repo: MyApp.Repo` when it wants the default inferred Ecto
storage. Hosts that already own a storage adapter boundary can skip global
`repo:` config and pass an explicit `journal_storage:` override instead.

Stale-worker handling comes from journal claim fencing or the host backend's
lease system.

For most host apps, the inferred Ecto storage is the recommended starting point
when `MyApp.Repo` uses Postgres or a Postgres-compatible Ecto adapter. It
persists Jido threads and checkpoints in Squidie's installed tables and keeps
journal storage in the same transactional database boundary as the host app. The
boundary remains adapter-shaped, so other Jido-compatible stores can be used
later, but production stores must still provide ordered per-thread appends,
durable checkpoint reads, and conflict detection for `:expected_rev`.
See [Storage strategy](storage_strategy.md) for the full adapter contract and
compatibility expectations.

The current journal default covers start, cron start, cancellation, replay,
global and workflow-filtered `list_runs/2`, inspect, explain, graph inspection,
manual resume/approval controls, and `Squidie.execute_next/1`. Journal listing
is backed by a durable run catalog fact rather than a storage-adapter scan, and
returns redacted summaries; use `inspect_run/2` for one run when a caller needs
inputs, outputs, attempts, or claim metadata. Dashboards can call
`list_runs([])` for the index view, then pass the selected summary's
`partition`, `run_id`, and `queue` to
`inspect_run(run_id, partition: partition, queue: queue, include_history: true)` or
`inspect_run_graph(run_id, partition: partition, queue: queue)` for detail views.

Do not serialize inspection or graph detail directly to untrusted clients.
Host apps should authorize the caller, select only the fields the view needs,
and redact host-domain inputs, outputs, errors, manual metadata, idempotency
keys, and claim identifiers before returning the payload. See
[Observability](observability.md#redaction-and-field-selection).

## Runtime Boundaries

Most host apps can use Squidie without writing Jido agents, storage calls, or
Bedrock code. The public integration boundary is:

- workflow modules declare triggers, payloads, steps, transitions, retries, and
  manual controls
- host code starts runs and exposes inspection through `Squidie.start/3`,
  `Squidie.list_runs/2`, `Squidie.inspect_run/2`,
  `Squidie.inspect_run_graph/2`, and `Squidie.explain_run/2`
- host workers provide execution capacity by calling `Squidie.execute_next/1`
- host schedulers may deliver cron activations with
  `Squidie.Executor.Payload.cron/3` and `Squidie.Runtime.Runner.perform/2`

Jido is the runtime foundation behind that boundary. Squidie uses Jido
journals, storage callbacks, actions, and rebuildable agents internally so run
state can be reconstructed from durable facts. Users only need to learn those
details when they are contributing to the runtime, replacing the default journal
storage adapter, or debugging low-level runtime behavior.

Bedrock is optional. Use the basic `execute_next/1` worker loop when a host only
needs Squidie to claim visible journal work from the configured storage. Use
Bedrock or another lease-capable backend when the host needs backend-owned
delivery, delayed visibility, worker leases, heartbeats, retry requeue,
dead-letter handling, or stale-worker recovery outside the Squidie journal.
Those backend concerns belong in adapter modules, not workflow modules.

## Journal Worker Contract

Step execution is pulled by host-owned workers. A minimal worker can be a small
GenServer loop under the host supervision tree:

```elixir
defmodule MyApp.SquidieWorker do
  use GenServer

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    {:ok, %{owner_id: Keyword.get(opts, :owner_id, "my-app-squidie")}, {:continue, :drain}}
  end

  def handle_continue(:drain, state), do: {:noreply, drain_once(state)}
  def handle_info(:drain, state), do: {:noreply, drain_once(state)}

  defp drain_once(state) do
    interval =
      case Squidie.execute_next(
             owner_id: state.owner_id,
             lease_for: 30,
             heartbeat_interval_ms: 10_000
           ) do
        {:ok, :none} -> 100
        {:ok, _snapshot} -> 0
        {:error, _reason} -> 1_000
      end

    Process.send_after(self(), :drain, interval)
    state
  end
end
```

This loop is intentionally small. Production hosts can add capacity limits,
back-pressure, node placement, metrics, and shutdown policy around the same
public call. Squidie still owns the journaled claim, completion, retry,
manual-control, and terminal-state facts.

`lease_for` and `heartbeat_interval_ms` are journal executor controls, not an
external backend requirement. Hosts without Bedrock or another leased job
backend may still pass them when steps can run longer than a claim window. Oban
OSS workers fall into this plain-host category for this purpose: keep Oban job
delivery concerns separate and let `Squidie.execute_next/1` maintain the
journal claim lease. Short step workers can omit `heartbeat_interval_ms`. Hosts
that also use a backend lease must maintain that backend lease separately from
the journal claim lease. The runtime rejects intervals below 50ms to keep
heartbeat write volume bounded.

## Telemetry Integration

Squidie emits public runtime events under `[:squidie, :runtime, ...]` for
command application, executor polls, step invocation, and committed lifecycle
facts. No runtime config is required to enable emission. Hosts attach handlers
or supervise their selected reporter/exporter and can use
`Squidie.Telemetry.metrics/0` as the default bounded-cardinality metric set.

```elixir
defmodule MyApp.Metrics do
  def metrics do
    application_metrics() ++ Squidie.Telemetry.metrics()
  end
end
```

Use `Squidie.Telemetry.partition_metrics/0` only after accepting the tenant or
domain cardinality of the configured partition namespace. Correlation fields
such as run, signal, runnable, and trace IDs are suitable for traces or
authorized diagnostic logs, but not metric labels.

Lifecycle point events follow successful journal appends. Squidie-owned Ecto
step transactions buffer completion events until commit and discard them on
rollback; arbitrary host-owned outer transactions are outside that guarantee.
The events remain best-effort and do not replace journal-backed inspection.
See [Observability](observability.md#runtime-telemetry) for the full event,
metadata, privacy, and delivery contract.

## Cron Payload Contract

Cron starts are the `Squidie.Executor` payload boundary. Hosts
that already have a scheduler can enqueue `Squidie.Executor.Payload.cron/3`
and deliver the stored payload to `Squidie.Runtime.Runner.perform/2`:

```elixir
defmodule MyApp.SquidieCronExecutor do
  @behaviour Squidie.Executor

  alias Squidie.Executor.Payload

  def enqueue_cron(_config, workflow, trigger, opts) do
    workflow
    |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
    |> enqueue(opts)
  end

  defp enqueue(payload, opts) do
    job = %{payload: payload, queue: queue(), schedule_in: opts[:schedule_in]}

    case MyApp.JobQueue.enqueue(job) do
      {:ok, job} ->
        {:ok, %{job_id: job.id, queue: job.queue, schedule_in: opts[:schedule_in]}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp queue do
    :my_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:queue, :squidie)
  end
end
```

The cron callback receives:

- `workflow` and `trigger` - the cron workflow activation target
- `opts[:signal_id]` - optional stable scheduler signal id for a cron activation
- `opts[:intended_window]` - optional logical schedule window for a cron activation

Return `{:ok, metadata}` after enqueueing. Metadata is returned to the caller and
can be included in host-owned logs or telemetry, so useful values are `:job_id`,
`:queue`, `:worker`, and `:scheduled_at`.

The queued job should deliver the stored payload back to Squidie without
knowing workflow details:

```elixir
defmodule MyApp.SquidieJob do
  def perform(%{payload: payload}) do
    Squidie.Runtime.Runner.perform(payload)
  end
end
```

`MyApp.JobQueue` is intentionally a placeholder. In a real host app, replace it
with the app's durable job backend. Cron activation is host-owned; the host
scheduler should call `enqueue_cron/4` or enqueue
`Squidie.Executor.Payload.cron/3`.

When a scheduler can provide deterministic schedule metadata, pass it with the
cron payload instead of adding it to workflow input:

```elixir
Payload.cron(MyApp.Workflows.DailyStandup, :daily_standup,
  signal_id: "daily-standup:2026-05-15T09:00:00Z",
  intended_window: %{
    start_at: "2026-05-15T09:00:00Z",
    end_at: "2026-05-15T10:00:00Z"
  }
)
```

Squidie persists this under `run.context.schedule` before workflow
processing. Steps can read it from `context.state.schedule`, and inspection or
explanation surfaces can show the intended window separately from actual worker
receive time.

If the workflow declares `cron ..., idempotency: :return_existing_run` or
`idempotency: :skip_duplicate`, the scheduler identity also becomes the start
idempotency key. Duplicate delivery of the same workflow, trigger, and key will
not insert a second run. Idempotent cron starts must include `signal_id` or a
complete `intended_window`; otherwise Squidie returns
`{:error, {:missing_schedule_idempotency_key, trigger_name}}`.

With the journal default, cron payload delivery through
`Squidie.Runtime.Runner.perform/2` starts a journal run and persists the
schedule context on the `:run_started` journal fact. Only cron payloads are
accepted because step execution is claimed through
`Squidie.execute_next/1`.

That is the whole execution contract for the journal-backed runtime. Workflow
modules, context modules, and controllers should not need to know which job
backend the scheduler uses.

## Optional Lease Contract

Backends that expose worker leases can also implement
`Squidie.Executor.Leases`. This is separate from the queue delivery adapter: it claims
visible work, heartbeats active claims, completes delivered work, and returns
failed work to the backend's retry or dead-letter policy.

The journal-backed runtime does not require a lease adapter. The behavior exists so
Bedrock or another durable backend can expose lease semantics through a stable
Squidie boundary without changing workflow modules.

## Bedrock Lease Backend Setup

Squidie stays backend-neutral: workflow modules and runtime state do not
depend on Bedrock APIs. For hosts that want backend-owned leasing today, Bedrock
is the recommended reference backend because it already owns durable delivery,
delayed visibility, leases, heartbeats, retry timing, and recovery. That same
ownership model is also a better foundation for distributed workflows, where
multiple workers may claim, heartbeat, fail, or recover work across process and
node boundaries.

Use `examples/bedrock_minimal_host_app` as the concrete setup guide. The example
keeps the storage and lease boundaries explicit:

- `BedrockMinimalHostApp.Repo` stores Squidie workflow and attempt state.
- `BedrockMinimalHostApp.JobQueue` stores queue items, delayed visibility,
  leases, retries, and queue metadata.
- `BedrockMinimalHostApp.SquidieDeliveryAdapter` adapts cron activations to Bedrock
  Job Queue payloads.
- `BedrockMinimalHostApp.SquidieLeaseAdapter` adapts Bedrock claims,
  heartbeats, completion, and failure to `Squidie.Executor.Leases`.
- `BedrockMinimalHostApp.Jobs.SquidiePayload` delivers cron payloads and then
  drains visible journal attempts while the Bedrock lease is held.

There are two independent lease layers in that setup. The Bedrock lease belongs
to the host job backend and controls whether the payload job can be redelivered.
The Squidie journal claim lease belongs to `Squidie.execute_next/1` and
controls whether another workflow worker can reclaim a journal attempt. The
Bedrock example passes `journal_heartbeat_interval_ms` into `execute_next/1` so
long-running journal steps keep their Squidie claim alive while the Bedrock
payload job is executing. That option does not renew the Bedrock job lease; the
host backend must size and renew its own lease separately.

The payload worker is the executor boundary. It should deliver a Squidie
payload, then drain visible journal attempts with `Squidie.execute_next/1`.
Do not enqueue one Bedrock job per workflow step. Do not use Bedrock job retry
settings to represent workflow step retry policy. Step retry, terminal failure,
pause, approval, and compensation routing are Squidie runtime facts driven by
the workflow DSL and persisted by `execute_next/1`.

Treat `{:ok, snapshot}` from `execute_next/1` as successful job progress even
when the snapshot reports a failed workflow run. Return `{:error, reason}` to
Bedrock only when the payload delivery or journal drain itself failed and should
be redelivered by the backend.

A host app using the same shape should:

1. Configure `:squidie` with the host repo and journal queue.
2. Configure the cron adapter's Bedrock queue id and topic.
3. Start the host repo, Bedrock cluster, and Bedrock job queue under
   supervision.
4. Keep workflow definitions backend-neutral; only the Bedrock adapter modules
   should know Bedrock exists.
5. Configure both lease policies explicitly: Bedrock job lease duration for
   payload delivery, and `journal_heartbeat_interval_ms` for long-running Squid
   Mesh attempts.

The example config shape is:

```elixir
config :my_app, MyApp.SquidieDeliveryAdapter,
  queue_id: "tenant_a",
  topic: "squidie:payload"

config :squidie,
  repo: MyApp.Repo,
  queue: "tenant_a"

config :my_app, MyApp.Jobs.SquidiePayload,
  journal_heartbeat_interval_ms: 10_000,
  max_journal_attempts: 50
```

To verify the reference path locally:

```sh
cd examples/bedrock_minimal_host_app
mix setup
MIX_ENV=test mix test test/bedrock_job_queue_stress_test.exs test/bedrock_minimal_host_app/squidie_lease_adapter_test.exs
```

That test path covers Bedrock queue behavior plus the lease adapter contract.
It does not make Bedrock a required Squidie dependency; another durable
delivery adapter can use the same Squidie boundaries if it provides equivalent
delivery, lease, heartbeat, retry, and recovery semantics.

For background on why durable workflow systems often benefit from queueing close
to the data and tenancy model they serve, see Apple's
[QuiCK: A Queuing System in CloudKit](https://www.foundationdb.org/files/QuiCK.pdf)
paper.

## First Run Checklist

For a new integration, the shortest path to a successful first run is:

1. Add `:squidie` to the host app's dependencies.
2. Add or confirm a working Postgres-backed `Repo`.
3. Run `mix squidie.install`.
4. Run `mix ecto.migrate`.
5. Configure `:squidie` with the host app's `Repo`.
6. Start the host app's `Repo` under supervision.
7. Start one workflow through the public API, execute visible attempts with
   `Squidie.execute_next/1`, and inspect it with history enabled.

Add a host job system only when the app needs one for cron scheduling,
backend-owned leases, or other application work.

## Existing Application Setup

For an existing Phoenix or OTP application:

1. Add the `:squidie` dependency.
2. Configure `:repo` to point at the app's existing repo.
3. Call `Squidie.config!/0` during boot or integration setup to verify the
   required contract is present.
4. Integrate Squidie from the host application's contexts, services,
   controllers, or internal APIs.

The host application is responsible for:

- database setup and migrations
- journal worker lifecycle for `Squidie.execute_next/1`
- any HTTP or internal API endpoints exposed to end users

That means the embedded install path assumes:

- the host app already owns its `Repo`
- the host app starts workers that call `Squidie.execute_next/1`
- the host app adds job-backend tables only for its own scheduler or lease backend

## Minimal OTP Host Skeleton

For a plain OTP application, the minimum moving pieces are:

- a `Repo` module
- `Repo` in the application supervision tree
- a supervised worker that periodically calls `Squidie.execute_next/1`
- `:squidie` configuration pointing at that `Repo`
- one host-facing module that calls `Squidie`

Dependency shape:

```elixir
defp deps do
  [
    {:ecto_sql, "~> 3.13"},
    {:postgrex, "~> 0.20"},
    {:squidie, "~> 0.3.5"}
  ]
end
```
Add `:jido` only when the host app defines raw `Jido.Action` steps directly.
Add the host job backend separately.

Application supervision shape:

```elixir
children = [
  MyApp.Repo,
  MyApp.JobQueue
]
```

Host-facing boundary:

```elixir
defmodule MyApp.WorkflowRuns do
  def start_payment_recovery(payload) do
    Squidie.start(MyApp.Workflows.PaymentRecovery, :payment_recovery, payload)
  end

  def inspect_run(run_id) do
    Squidie.inspect_run(run_id, include_history: true)
  end

  def resume(run_id, attrs \\ %{}) do
    Squidie.resume(run_id, attrs)
  end

  def approve(run_id, attrs) do
    Squidie.approve(run_id, attrs)
  end

  def reject(run_id, attrs) do
    Squidie.reject(run_id, attrs)
  end
end
```

If the host app exposes pause-resume or approval workflows, keep the latest
Squidie migrations applied before deploying the feature. Paused step runs
now persist internal resume metadata so `resume/2`, `approve/3`, and
`reject/3` can continue with stable output and transition semantics after
restarts or code changes.

Operational review shape:

```elixir
{:ok, paused_run} = MyApp.WorkflowRuns.inspect_run(run_id)

Enum.map(paused_run.audit_events, &{&1.type, &1.step})
#=> [{:paused, :wait_for_review}]

{:ok, _run} =
  MyApp.WorkflowRuns.approve(run_id, %{
    actor: "ops_123",
    comment: "customer verified",
    metadata: %{ticket: "SUP-42"}
  })

{:ok, completed_run} = MyApp.WorkflowRuns.inspect_run(run_id)

Enum.map(completed_run.audit_events, &{&1.type, &1.actor, &1.comment})
#=> [{:paused, nil, nil}, {:approved, "ops_123", "customer verified"}]
```

`include_history: true` is the public audit boundary. With history enabled, the
run includes chronological `step_runs`, declared `steps` state, and durable
`audit_events` for pause, resume, approval, and rejection actions.

## Minimal Phoenix Host Skeleton

A Phoenix application uses the same runtime contract. The main difference is
that Squidie usually sits behind a context or controller boundary.

Typical shape:

- add `:squidie` to the Phoenix app
- keep using the Phoenix app's existing `Repo`
- start a supervised worker that calls `Squidie.execute_next/1`
- configure `:squidie` to use that `Repo`
- expose workflow operations through a context or controller

Add `:jido` explicitly only when the Phoenix app defines raw `Jido.Action`
modules as an interop path.

Context boundary:

```elixir
defmodule MyApp.WorkflowRuns do
  def start_payment_recovery(attrs) do
    Squidie.start(MyApp.Workflows.PaymentRecovery, :payment_recovery, attrs)
  end

  def inspect_run(run_id) do
    Squidie.inspect_run(run_id, include_history: true)
  end

  def resume(run_id, attrs \\ %{}) do
    Squidie.resume(run_id, attrs)
  end

  def approve(run_id, attrs) do
    Squidie.approve(run_id, attrs)
  end

  def reject(run_id, attrs) do
    Squidie.reject(run_id, attrs)
  end
end
```

Controller shape:

```elixir
def create(conn, params) do
  with {:ok, run} <- MyApp.WorkflowRuns.start_payment_recovery(params) do
    json(conn, %{id: run.run_id, status: run.status})
  end
end
```

## Development Setup

For local development and examples, a minimal host app can provide:

- a local Postgres-backed repo
- a local background job setup
- direct application code calls into Squidie

This uses the same configuration contract as an existing application setup.
In that mode, the example app may also own its job-backend migrations because
it is acting as a standalone development harness rather than an embedded
install.

## Validation

Host applications can validate the contract directly:

```elixir
{:ok, config} = Squidie.config()
```

Or raise on missing required keys:

```elixir
config = Squidie.config!()
```

## Example Development Harness

The example host app smoke-test harness builds on this same contract and is the
reference setup for end-to-end development and verification.

Path:

- `examples/minimal_host_app`

Suggested workflow:

1. Start Postgres for the example app.
2. Run `mix setup` inside `examples/minimal_host_app`.
3. Run `mix example.smoke` to exercise the host app boundary.

Fast verification path:

- run `MIX_ENV=test mix example.smoke` inside `examples/minimal_host_app`

The example app wires:

- its own `MinimalHostApp.Repo`
- journal runtime smoke paths that use inferred Ecto storage and
  `Squidie.execute_next/1`, including cron activation through the journal
  runtime
- a Jido command-signal round trip that proves durable trace lineage across a
  worker handoff and captures a committed lifecycle telemetry event
- cron activation smoke paths that deliver `Squidie.Executor.Payload.cron/3`
  through `Squidie.Runtime.Runner.perform/1`
- Squidie through `MinimalHostApp.WorkflowRuns`

## Inspecting History

For real host apps, `inspect_run/2` is most useful with history enabled:

```elixir
Squidie.inspect_run(run_id, include_history: true)
```

That returns the top-level run plus:

- `steps`: logical per-step state in workflow order, including dependency edges
- `step_runs`: persisted execution history
- `attempts`: persisted retry history for each step run

This split gives host apps both declared per-step state and the raw execution
timeline from one inspection call.

Use `explain_run/2` when an operator surface needs the current reason and safe
next actions instead of the full inspection snapshot:

```elixir
{:ok, explanation} = Squidie.explain_run(run_id)

%{
  status: explanation.status,
  reason: explanation.reason,
  step: explanation.step,
  next_actions: explanation.next_actions
}
```

`inspect_run/2` answers "what persisted state exists?". `explain_run/2` answers
"why is this run here, what evidence supports that, and what can an operator do
next?". The explanation keeps `details` and `evidence` structured so Phoenix
apps, CLIs, and dashboards can render their own messages.
