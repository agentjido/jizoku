# Bedrock Minimal Host App

Reference host-app harness for testing Jizoku with Bedrock Job Queue as the
delivery backend.

This example keeps two storage boundaries visible:

- Jizoku workflow state is stored through `BedrockMinimalHostApp.Repo`.
- Bedrock Job Queue owns queued jobs, delayed delivery, leases, retries, and
  queue metadata through the embedded Bedrock cluster.

It also keeps two lease responsibilities separate:

- Bedrock leases protect the delivery job while
  `BedrockMinimalHostApp.Jobs.JizokuPayload.perform/2` is running.
- Jizoku journal claim leases protect the individual workflow attempt
  claimed by `Jizoku.execute_next/1`.

Those leases are intentionally not the same thing. Bedrock decides whether the
payload job may be redelivered. Jizoku decides whether a journal attempt may
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
- install Jizoku migrations into the example app with `mix jizoku.install`
- run the example app and Jizoku migrations through `mix ecto.migrate`

Bedrock runs embedded for the spike. In local and test mode it uses configured
filesystem paths for cluster state; production hosts should configure durable
Bedrock storage or a real cluster topology.

## Stress Test

Run the Bedrock job queue stress coverage:

```sh
MIX_ENV=test mix test test/action_registry_test.exs test/bedrock_job_queue_stress_test.exs test/bedrock_multi_node_consumer_test.exs test/bedrock_minimal_host_app/jizoku_lease_adapter_test.exs
```

The stress test covers:

- safe action registry validation against the Bedrock example app's host-owned
  step modules
- topic routing and tenant queue isolation
- priority ordering
- delayed job visibility
- leasing and lease extension
- two independently identified consumers contending for one queued job without
  duplicate dispatch, including automatic lease renewal and stale-lease
  completion
- retry requeue and dead-letter behavior
- Jizoku cron payloads being mapped into Bedrock jobs
- a Bedrock-leased drain executing a v1 run through its host-registered
  historical definition after the stable workflow module advances to v2
- a paused v1 run migrating durably to v2 before its successor is drained under
  a Bedrock lease
- the `Jizoku.Executor.Leases` contract through a Bedrock-backed example
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

`BedrockMinimalHostApp.Jobs.JizokuPayload` drains journal attempts while the
Bedrock payload is leased. The job passes `heartbeat_interval_ms` into
`Jizoku.execute_next/1` so Jizoku renews the active journal claim during
long-running steps. That journal heartbeat is separate from the Bedrock job
lease; hosts with backend-owned delivery still need their backend lease policy
to match their job runtime. Configure the journal heartbeat interval through:

```elixir
config :bedrock_minimal_host_app, BedrockMinimalHostApp.Jobs.JizokuPayload,
  journal_heartbeat_interval_ms: 10_000,
  max_journal_attempts: 50
```

Set `journal_heartbeat_interval_ms: nil` only when every drained journal step is
short enough to finish inside the Jizoku journal claim window. Keep the
Bedrock job lease duration configured in the Bedrock queue policy; changing the
Jizoku heartbeat interval does not renew the Bedrock lease.

The `BedrockMinimalHostApp.WorkflowRuns` boundary also demonstrates runtime
control signals: host code builds `Jizoku.Runtime.Signal` values for
cancel/resume/approve/reject commands and applies them through
`Jizoku.apply_signal/2`. The example tests cover cancellation and manual
control signals that reach run history, plus a missing-run signal target.

`BedrockMinimalHostApp.RuntimeSignals` is the concrete Jido-facing signal
boundary. It accepts inbound `Jido.Signal` envelopes, converts them with
`Jizoku.Runtime.Signal.JidoAdapter`, and applies the resulting Jizoku
runtime command.

`BedrockMinimalHostApp.Workflows.RawJidoWorkflow` is the corresponding action
example. Its workflow step is a raw `Jido.Action` that returns an ordinary
result with an empty directive list, and the sample test executes it through
the same journal runtime used by native Jizoku steps. Durable Jido
RunInstruction and Emit examples live in the minimal host app, where their
fleet activation and external dispatch boundaries can be exercised directly.

The example intentionally does not include another job backend. That keeps the
adapter boundary clear while the spike evaluates Bedrock as the host-owned
delivery and leasing layer for Jido-native Jizoku execution.
