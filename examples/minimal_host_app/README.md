# Minimal Host App

Reference host-app harness for Jizoku.

This example shows how an application can:

- configure Jizoku with its own `Repo`
- expose workflow operations through an application-facing module
- pause and resume a human-in-the-loop workflow through that boundary
- apply Jizoku runtime command signals, including Jido envelope interop,
  through `MinimalHostApp.RuntimeSignals`
- activate cron workflows through a host-owned scheduler plugin
- run repeatable smoke, resilience, and bounded soak paths during development
- execute native continue-as-new through fresh linked runs and bounded lineage
  inspection
- exercise raw Jido actions through Jizoku's explicit directive compatibility
  boundary
- schedule allowlisted raw `Jido.Instruction` values as durable dynamic work
- enqueue and deliver raw Jido `Emit` directives through a durable outbox

## Setup

Start a local Postgres instance and point `DATABASE_URL` at it. The default is:

```sh
ecto://postgres:postgres@localhost/minimal_host_app_dev
```

Then set up the example app:

```sh
mix setup
```

This will:

- create the example app database
- install Jizoku migrations into the example app with `mix jizoku.install`
- run the example app's scheduler and delivery migration
- run both the example app and Jizoku migrations through `mix ecto.migrate`

This example is the standalone development harness. Unlike the embedded host-app
install path, it owns its own scheduler and delivery wiring so the runtime can
be exercised without depending on another application. The current harness uses
Oban for cron delivery; workflow state still lives in the Jizoku journal.

Run the verification tasks one at a time. They share the same test database,
manual scheduler instance, and local gateway stubs, so parallel runs can
interfere with each other's polling windows.

## Smoke Path

Run the test-mode smoke path:

```sh
MIX_ENV=test mix example.smoke
```

This command creates the test database if needed, runs migrations, starts the
repo, starts the host-owned scheduler and delivery harness, starts a local HTTP
gateway stub, then runs the example smoke path to completion.

Run the development-like path after `mix setup`:

```sh
mix example.smoke
```

Verify the operational CLI against the migrated host database in a fresh Mix
process:

```sh
MIX_ENV=test mix example.operations
```

This runs `mix jizoku.status --json` and
`mix jizoku.doctor --json --fail-on-drift`, decodes both reports, verifies the
database matches Jizoku's required schema, and proves the commands do not
start `MinimalHostApp`'s supervision tree.

## Multi-node Worker Proof

Run the focused host-worker integration suite:

```sh
MIX_ENV=test mix test test/multi_node_host_worker_test.exs
```

The suite runs two independent worker identities against one queue and the
same Postgres-backed Jizoku journal. It proves that concurrent workers cannot
apply one visible attempt twice, a current heartbeat prevents reclaim, an
expired claim can be taken over, and stale completion or failure cannot mutate
the run after takeover. It also checks the inspection and explanation evidence
for current ownership, expired recovery, cancellation, failure, and completion
fences.

This is an embedded host-app pattern, not a Jizoku cluster. Production hosts
still own worker placement and supervision, and side-effecting steps still need
idempotency because an external action may occur before a worker loses its
claim.

## Workflow Version Routing

The host config registers supported historical implementations under the stable
current workflow module. `MinimalHostApp.Verification.WorkflowEvolution` seeds
the durable facts left by a v1 deployment, then drains the attempt after v2 is
current. Jizoku selects only the configured v1 module and still requires its
fingerprint to match the run history exactly.

Run the focused proof with:

```sh
MIX_ENV=test mix test test/workflow_evolution_test.exs
```

Keep historical modules deployed while any non-terminal run references them.
Version labels are operator-facing identities; they never bypass fingerprint
fencing.

The checked-in `test/fixtures/jizoku_histories.exs` fixture records the exact v1
fingerprint and a redacted golden timeline. `mix example.smoke` verifies that
fixture against the deployed `:workflow_versions` registry before exercising the
runtime flows, so removing or changing required historical code fails the sample
CI path.

## Safe-Point Migration

`MinimalHostApp.Verification.WorkflowMigration` starts from checked-in v1
journal facts at a quiescent pause, applies a host-owned migration contract,
then resumes through the v2 graph and action. The persisted migration retains
both exact fingerprints, replaces accumulated context with the bounded
transformed result, and maps the active manual step explicitly.

Run the focused proof with:

```sh
MIX_ENV=test mix test test/workflow_migration_test.exs
```

Migration callbacks must be deterministic and side-effect free. Jizoku may
evaluate them again after an optimistic append conflict, but replay uses only
the persisted result and never invokes migration code.

The smoke task:

- validates a runtime-authored workflow spec through host-owned safe action keys
- executes a durable v1 run through its registered implementation after the v2
  workflow is deployed
- migrates a quiescent paused v1 run and completes it through the v2 workflow
- round-trips a compiled workflow spec through the visual-editor JSON contract
  and previews the draft graph
- validates visual-editor action keys through the host action registry before
  previewing the draft graph
- compares an edited visual-editor draft against its source workflow spec
- schedules executable dynamic work and verifies graph `dynamic_work_overlays`
- starts a manual payment recovery workflow through
  `MinimalHostApp.WorkflowRuns.start_payment_recovery/1`
- verifies the payment recovery graph selected the `greater_than` gateway
  status condition persisted in the run history
- exercises a `202 Accepted` gateway response that returns
  `{:defer, reason, schedule_in: 1}`, then verifies the deferred continuation
  completes without using the workflow retry path
- starts the dependency-based recovery workflow through
  `MinimalHostApp.WorkflowRuns.start_dependency_recovery/1`
- runs `MinimalHostApp.Workflows.RecurringCursor` through a native
  continue-as-new result, verifies the predecessor and successor, and inspects
  the bounded continuation chain
- starts a manual approval workflow through
  `MinimalHostApp.WorkflowRuns.start_manual_approval/1`
- explains the paused approval run through `MinimalHostApp.WorkflowRuns.explain_run/1`
- approves the paused run through `MinimalHostApp.WorkflowRuns.approve/2`
- starts a manual digest run through
  `MinimalHostApp.WorkflowRuns.start_manual_digest/1`
- starts the local ledger checkout workflow through
  `MinimalHostApp.WorkflowRuns.start_local_ledger_checkout/1`
- starts a saga checkout run through
  `MinimalHostApp.WorkflowRuns.start_saga_checkout/1`
- waits for execution, inspects all completed manual workflows, and
  verifies the paused approval run's durable audit history, command receipt
  history, local transaction rollback, and saga rollback compensation history
- round-trips a traced start command through a Jido envelope, executes its
  durable runnable lineage across two workers, and captures the committed
  attempt telemetry event
- activates the same digest workflow through the host app's cron plugin
- verifies both digest triggers complete through the same workflow graph
- archives a terminal run, previews its exact retention identities through
  `MinimalHostApp.WorkflowRuns`, applies the confirmed plan, and verifies the
  retained receipt and physical removal through the host boundary

The sample enables `config :jizoku, continuation_fences: :enabled`,
`jido_effects: :enabled`, and `jido_emit_effects: :enabled` only in development
and test because those smoke paths run one coherent application version. Its
production configuration remains default-off. Production hosts must first
upgrade and drain every worker that can read the affected queues; each flag is
a host readiness assertion, not automatic cluster-version discovery.

The test suite also runs
`MinimalHostApp.Workflows.JidoDirectiveBoundary`, whose raw `Jido.Action`
returns a custom directive. The interoperability boundary rejects the directive
as a redaction-safe, non-retryable failure and proves the action output is not
applied.

`MinimalHostApp.Workflows.JidoEmitWorkflow` demonstrates the supported Emit
boundary. The raw action creates a real `Jido.Signal`; Jizoku records its
completion and outbox enqueue before dispatch, delivers it through a host-owned
route, and appends the acknowledgement after delivery. The test suite exercises
the same workflow through the isolated in-memory runtime and the Ecto journal,
then verifies the redacted snapshot, timeline, explanation, and stable signal
ID used for at-least-once deduplication.

`MinimalHostApp.Workflows.JidoInstructionWorkflow` demonstrates the supported
instruction boundary. Host code supplies an already applied runnable as the
causal origin, and Jizoku resolves the instruction action module through the
host-owned action registry. The instruction ID becomes the idempotent dynamic
work identity; execution, retries, result application, and inspection use the
normal journal runtime.

`MinimalHostApp.Workflows.JidoRunInstructionWorkflow` exercises the native raw
action directive path. Its source action returns
`Jido.Agent.Directive.run_instruction/1`; the test runtime proves source output,
instruction planning, follow-up execution, and terminal completion through both
the in-memory test adapter and the Ecto journal.

## Restart Resilience

Run the restart resilience verification:

```sh
MIX_ENV=test mix example.resilience
```

This path verifies:

- queued work survives a scheduler and delivery restart boundary
- delayed work survives a scheduler and delivery restart boundary
- retrying work survives a scheduler and delivery restart boundary
- a paused manual-approval run survives restart and still approves through the host boundary with the same resume semantics

## Bounded Soak And Load

Run the bounded soak and load verification:

```sh
MIX_ENV=test mix example.soak
```

This path is intentionally not a benchmark. It drives a bounded mix of:

- concurrent successful workflow runs
- retried workflow runs
- replayed workflow runs
- cancelled workflow runs

## Example Boundary

The host-facing boundary is:

```elixir
MinimalHostApp.WorkflowRuns.start_payment_recovery(%{
  account_id: "acct_123",
  invoice_id: "inv_456",
  attempt_id: "attempt_789",
  gateway_url: "http://127.0.0.1:4010/gateway"
})
```

That map is the workflow payload for the `:payment_recovery` trigger declared
in the example workflow.

The payment recovery workflow marks its customer notification step as
non-compensatable:

```elixir
step(:notify_customer, MinimalHostApp.Steps.NotifyCustomer, compensatable: false)
```

That marker makes replay require explicit operator approval after the
notification has completed, instead of silently treating the side effect as
reversible.

It also uses a numeric conditional transition to route successful gateway
responses through the customer notification path, with a fallback credit path
for non-matching success payloads. Numeric threshold routing supports both
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
output under `gateway_check.attempt`. That gives the sample app a concrete
example of using native Jizoku context fields such as `idempotency_key` and
`claim_id` from an ordinary step module.

`MinimalHostApp.RuntimeSignals` is the concrete signal boundary for this sample.
It accepts native `Jizoku.Runtime.Signal` commands and inbound `Jido.Signal`
envelopes and passes both directly to `Jizoku.apply_signal/2`. The adapter
remains available for outbound conversion. The smoke path also verifies that
the host-owned envelope source, ID, and correlation survive the boundary and
that the resulting run/runnable trace remains durable across a worker handoff.

`MinimalHostApp.JidoSignalRoutes` shows the bounded domain-signal path. It
allowlists `minimal_host.dependency_recovery.requested`, maps its data into the
compiled `DependencyRecovery` workflow, and returns a lifecycle command without
accepting module names, storage, queues, or dispatch configuration from the
signal. `RuntimeSignals.apply_domain/1` supplies that resolver to
`Jizoku.apply_signal/2`. Jizoku durably fences the resulting lifecycle
command by the signal source and ID before applying it, so redelivery after a
resolver deploy still repairs the original route.

The saga checkout workflow demonstrates reversible side effects:

```elixir
step :reserve_inventory, MinimalHostApp.Steps.ReserveInventory,
  compensate: MinimalHostApp.Steps.ReleaseInventory

step :authorize_payment, MinimalHostApp.Steps.AuthorizePayment,
  compensate: MinimalHostApp.Steps.VoidPaymentAuthorization

step :capture_payment, MinimalHostApp.Steps.CapturePayment, retry: [max_attempts: 2]
```

The capture step fails after its retry policy is exhausted, then Jizoku
voids the payment authorization and releases inventory in reverse completion
order. The smoke task verifies those compensation results through
`inspect_run(..., include_history: true)`, including the internal
`compensate:authorize_payment` and `compensate:reserve_inventory` attempts.

The same workflow routes exhausted gateway failures to a compensation step:

```elixir
transition(:check_gateway_status,
  on: :error,
  to: :issue_gateway_credit,
  recovery: :compensation
)
```

When that path runs, `inspect_run(run_id, include_history: true)` exposes a
`:compensation_routed` audit event and the failed step's
`recovery.failure.strategy`.

The dependency recovery workflow demonstrates named path input mapping on a
join step. `:load_account` and `:load_invoice` keep their outputs in the run
context, while `:prepare_notification` receives only the nested values it needs:

```elixir
step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
  after: [:load_account, :load_invoice],
  input: [
    account_id: [:account, :id],
    invoice_id: [:invoice, :id],
    account_tier: [:account, :tier]
  ]
```

The smoke task verifies that persisted step history records that mapped input.

The local ledger checkout workflow demonstrates a same-process host repo
transaction group:

```elixir
step :post_local_ledger_entries, MinimalHostApp.Steps.PostLocalLedgerEntries,
  transaction: :repo
```

The step writes two local ledger rows through `MinimalHostApp.Repo`. When the
step returns `{:ok, output}`, both rows commit before Jizoku records the
completed step. When the step returns `{:error, reason}`, both rows roll back
and Jizoku records the durable step failure. This is a local database
boundary only; saga compensation and later workflow steps remain explicit
workflow concerns.

Host apps can expose diagnostics through the same boundary:

```elixir
{:ok, explanation} = MinimalHostApp.WorkflowRuns.explain_run(run_id)

explanation.reason
explanation.details.latest_command.signal_type
explanation.evidence.command_counts
```

The reference workflow and step modules live in:

- `lib/minimal_host_app/workflows/payment_recovery.ex`
- `lib/minimal_host_app/workflows/dependency_recovery.ex`
- `lib/minimal_host_app/workflows/manual_approval.ex`
- `lib/minimal_host_app/workflows/local_ledger_checkout.ex`
- `lib/minimal_host_app/workflows/saga_checkout.ex`
- `lib/minimal_host_app/workflows/daily_digest.ex`
- `lib/minimal_host_app/steps/`

## Reference Workflows

These workflows are the reference shapes for the product lane described in
[Positioning](../../docs/positioning.md) and the
[Reference workflows](../../docs/reference_workflows.md) guide. They stay Squid
Mesh-native in the happy path and show the runtime features the example app is
meant to prove.

| Workflow | What it proves | Source |
| --- | --- | --- |
| `PaymentRecovery` | A customer-facing recovery flow with retries, deferred gateway polling, a non-compensatable side effect, and explicit replay boundaries. | [`lib/minimal_host_app/workflows/payment_recovery.ex`](lib/minimal_host_app/workflows/payment_recovery.ex) |
| `PaymentWebhook` | A durable correlated event wait with host-owned HMAC verification, idempotent provider delivery, safe inspection evidence, and a timeout continuation. | [`lib/minimal_host_app/workflows/payment_webhook.ex`](lib/minimal_host_app/workflows/payment_webhook.ex) |
| `ManualApproval` | Operator pause, approval, rejection, durable resume, and audit history. | [`lib/minimal_host_app/workflows/manual_approval.ex`](lib/minimal_host_app/workflows/manual_approval.ex) |
| `RetryVerification` | Workflow-level retry policy and failure recovery without backend-specific retry assumptions. | [`lib/minimal_host_app/workflows/retry_verification.ex`](lib/minimal_host_app/workflows/retry_verification.ex) |
| `DependencyRecovery` | Recovery-oriented dependency joins, mapped input extraction, and durable inspection of joined work. | [`lib/minimal_host_app/workflows/dependency_recovery.ex`](lib/minimal_host_app/workflows/dependency_recovery.ex) |
| `SagaCheckout` | Reversible side effects, compensation order, and retry exhaustion on a later step. | [`lib/minimal_host_app/workflows/saga_checkout.ex`](lib/minimal_host_app/workflows/saga_checkout.ex) |

The smoke, restart-resilience, and soak harnesses exercise these workflows so
they stay grounded in executable example coverage instead of becoming doc-only
fixtures.

`MinimalHostApp.PaymentWebhook` is the sample host boundary for provider
callbacks. It verifies an HMAC over the untouched body before JSON decoding,
validates fixed fields, maps the callback to the allowlisted
`"payment.completed"` event, and derives the Jizoku idempotency key from the
provider event ID. The smoke path proves an invalid signature leaves the run
unchanged, an exact callback retry is idempotent, successful delivery continues
once, and the no-callback path selects the declared timeout continuation.

## Multi-Trigger Workflow Example

`MinimalHostApp.Workflows.DailyDigest` demonstrates one workflow graph with two
entrypoints:

```elixir
trigger :manual_digest do
  manual()

  payload do
    field :channel, :string
    field :digest_date, :string
  end
end

trigger :daily_digest do
  cron "@reboot", timezone: "Etc/UTC", idempotency: :return_existing_run

  payload do
    field :channel, :string, default: "ops"
    field :digest_date, :string, default: {:today, :iso8601}
  end
end
```

Both triggers run `:announce_digest` and `:record_digest_delivery`. The host app
can start the manual entrypoint through `WorkflowRuns.start_manual_digest/1`,
while the cron plugin starts the same workflow through `:daily_digest`.

The cron trigger opts into scheduled-start idempotency. Because this example
uses a static `@reboot` cron entry, the host plugin supplies one signal id per
plugin boot; duplicate delivery of that same boot activation returns the first
run instead of creating a second one. Normal recurring schedules should provide
a per-window `signal_id` or `intended_window` from the host scheduler.

## Dependency Workflow Example

The example app also includes a dependency-based workflow with two roots and a
join step:

```elixir
defmodule MinimalHostApp.Workflows.DependencyRecovery do
  use Jizoku.Workflow

  workflow do
    trigger :dependency_recovery do
      manual()

      payload do
        field :account_id, :string
        field :invoice_id, :string
        field :attempt_id, :string
      end
    end

    step :load_account, MinimalHostApp.Steps.LoadAccount
    step :load_invoice, MinimalHostApp.Steps.LoadInvoice

    step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
      after: [:load_account, :load_invoice],
      input: [
        account_id: [:account, :id],
        invoice_id: [:invoice, :id],
        account_tier: [:account, :tier]
      ]
  end
end
```

This workflow is exercised through `MinimalHostApp.WorkflowRuns.start_dependency_recovery/1`
and the example app test suite. Ready dependency roots still execute one at a
time today; `after: [...]` guarantees that the join step waits for both inputs.
