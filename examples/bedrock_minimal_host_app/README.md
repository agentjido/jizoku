# Bedrock Minimal Host App

Reference host-app harness for testing Squid Mesh with Bedrock Job Queue as the
delivery backend.

This example keeps two storage boundaries visible:

- Squid Mesh workflow state is stored through `BedrockMinimalHostApp.Repo`.
- Bedrock Job Queue owns queued jobs, delayed delivery, leases, retries, and
  queue metadata through the embedded Bedrock cluster.

It also keeps two lease responsibilities separate:

- Bedrock leases protect the delivery job while
  `BedrockMinimalHostApp.Jobs.SquidMeshPayload.perform/2` is running.
- Squid Mesh journal claim leases protect the individual workflow attempt
  claimed by `SquidMesh.execute_next/1`.

Those leases are intentionally not the same thing. Bedrock decides whether the
payload job may be redelivered. Squid Mesh decides whether a journal attempt may
be claimed by another workflow worker. Long-running hosts must size both lease
policies for their real work.

## Setup

Start a local Postgres instance and point `DATABASE_URL` at it. The default is:

```sh
ecto://postgres:postgres@localhost/bedrock_minimal_host_app_dev
```

Then set up the example app:

```sh
mix setup
```

This will:

- create the example app database
- install Squid Mesh migrations into the example app with `mix squid_mesh.install`
- run the example app and Squid Mesh migrations through `mix ecto.migrate`

Bedrock runs embedded for the spike. In local and test mode it uses configured
filesystem paths for cluster state; production hosts should configure durable
Bedrock storage or a real cluster topology.

## Stress Test

Run the Bedrock job queue stress coverage:

```sh
MIX_ENV=test mix test test/action_registry_test.exs test/bedrock_job_queue_stress_test.exs test/bedrock_minimal_host_app/squid_mesh_lease_adapter_test.exs
```

The stress test covers:

- safe action registry validation against the Bedrock example app's host-owned
  step modules
- topic routing and tenant queue isolation
- priority ordering
- delayed job visibility
- leasing and lease extension
- retry requeue and dead-letter behavior
- Squid Mesh cron payloads being mapped into Bedrock jobs
- the `SquidMesh.Executor.Leases` contract through a Bedrock-backed example
  adapter

The Bedrock host app keeps the same payment recovery workflow shape as the
minimal host app. Its successful gateway route uses the persisted numeric
condition syntax, so Bedrock-backed runs expose the same graph metadata as the
plain host-app smoke path. Numeric threshold routing supports both
`greater_than` and `less_than` conditions; this host app exercises
`greater_than` through the real gateway response:

```elixir
transition :check_gateway_status,
  on: :ok,
  to: :notify_customer,
  condition: [path: [:gateway_check, :status_code], greater_than: 199]

transition :check_gateway_status, on: :ok, to: :issue_gateway_credit
```

The gateway check step also copies the durable step-context metadata into its
output under `gateway_check.attempt`, so the Bedrock example demonstrates the
same native context fields as the minimal host app while keeping delivery and
leasing behind the host-owned Bedrock adapter.

`BedrockMinimalHostApp.Jobs.SquidMeshPayload` drains journal attempts while the
Bedrock payload is leased. The job passes `heartbeat_interval_ms` into
`SquidMesh.execute_next/1` so Squid Mesh renews the active journal claim during
long-running steps. That journal heartbeat is separate from the Bedrock job
lease; hosts with backend-owned delivery still need their backend lease policy
to match their job runtime. Configure the journal heartbeat interval through:

```elixir
config :bedrock_minimal_host_app, BedrockMinimalHostApp.Jobs.SquidMeshPayload,
  journal_heartbeat_interval_ms: 10_000,
  max_journal_attempts: 50
```

Set `journal_heartbeat_interval_ms: nil` only when every drained journal step is
short enough to finish inside the Squid Mesh journal claim window. Keep the
Bedrock job lease duration configured in the Bedrock queue policy; changing the
Squid Mesh heartbeat interval does not renew the Bedrock lease.

The `BedrockMinimalHostApp.WorkflowRuns` boundary also demonstrates runtime
control signals: host code builds `SquidMesh.Runtime.Signal` values for
cancel/resume/approve/reject commands and applies them through
`SquidMesh.apply_signal/2`. The example tests cover cancellation and manual
control signals that reach run history, plus a missing-run signal target.

`BedrockMinimalHostApp.RuntimeSignals` is the concrete Jido-facing signal
boundary. It accepts inbound `Jido.Signal` envelopes, converts them with
`SquidMesh.Runtime.Signal.JidoAdapter`, and applies the resulting Squid Mesh
runtime command.

The example intentionally does not include another job backend. That keeps the
adapter boundary clear while the spike evaluates Bedrock as the host-owned
delivery and leasing layer for Jido-native Squid Mesh execution.
