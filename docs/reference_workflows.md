# Reference Workflows

The minimal host app contains executable reference workflows for the product
lane described in [Positioning](positioning.md). They use Squidie workflow
and step APIs in the happy path, keep host scheduling and delivery outside the
workflow definition, and run through the same smoke and resilience harnesses as
the rest of the example app.

Use these examples when you want to see how the runtime features fit together
inside a host application without adding a dashboard or a separate workflow
service.

## Where They Live

The reference host app is in
[`examples/minimal_host_app`](../examples/minimal_host_app/README.md).

The app exposes workflow operations through
`MinimalHostApp.WorkflowRuns`, which is the host-facing boundary a Phoenix
context or OTP service would normally wrap. The workflow modules live under
`examples/minimal_host_app/lib/minimal_host_app/workflows`, and their step
modules live under `examples/minimal_host_app/lib/minimal_host_app/steps`.

## Workflow Map

| Workflow | Trigger shape | What it demonstrates |
| --- | --- | --- |
| `PaymentRecovery` | Manual | Retry policy, explicit recovery routing, non-compensatable side effects, and replay boundaries. |
| `ManualApproval` | Manual | Durable operator approval, rejection, pause state, resume, and audit history. |
| `DependencyRecovery` | Manual | Independent roots, dependency joins, named path input mapping, and inspectable joined work. |
| `SagaCheckout` | Manual | Reversible side-effect metadata and retry exhaustion on a later step. |
| `DailyDigest` | Manual and cron | One workflow graph shared by manual and scheduled starts, including cron idempotency metadata. |

## Payment Recovery

`MinimalHostApp.Workflows.PaymentRecovery` models a customer-facing recovery
flow:

```elixir
step :check_gateway_status, MinimalHostApp.Steps.CheckGatewayStatus,
  retry: [max_attempts: 5, backoff: [type: :exponential, min: 1_000, max: 1_000]]

transition :check_gateway_status,
  on: :error,
  to: :issue_gateway_credit,
  recovery: :compensation

step :notify_customer, MinimalHostApp.Steps.NotifyCustomer, compensatable: false
```

This example shows three boundaries:

- Retry policy belongs to workflow progression, not to the host job runner.
- Recovery transitions are durable route choices that show up in inspection.
- Non-compensatable side effects make replay require explicit operator intent
  after the notification step has completed.

The smoke path starts this workflow through
`MinimalHostApp.WorkflowRuns.start_payment_recovery/1`, waits for worker
execution, and inspects the completed run.

## Manual Approval

`MinimalHostApp.Workflows.ManualApproval` uses an approval step:

```elixir
approval_step :wait_for_approval, output: :approval

transition :wait_for_approval, on: :ok, to: :record_approval
transition :wait_for_approval, on: :error, to: :record_rejection
```

The approval step is durable state in the journal. It is not a process waiting
in memory. Host code resolves the boundary through public APIs such as
`Squidie.approve/2` or `Squidie.reject/2`, and inspection history
keeps the pause and resolution facts visible for operator tools.

## Dependency Recovery

`MinimalHostApp.Workflows.DependencyRecovery` starts two independent roots and
joins them before notification preparation:

```elixir
step :load_account, MinimalHostApp.Steps.LoadAccount
step :load_invoice, MinimalHostApp.Steps.LoadInvoice

step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
  after: [:load_account, :load_invoice],
  input: [
    account_id: [:account, :id],
    invoice_id: [:invoice, :id],
    account_tier: [:account, :tier]
  ]
```

The join step consumes named values from durable run context instead of relying
on transient process state. `inspect_run(..., include_history: true)` shows the
mapped input that reached `:prepare_notification`.

## Saga Checkout

`MinimalHostApp.Workflows.SagaCheckout` demonstrates explicit compensation
metadata:

```elixir
step :reserve_inventory, MinimalHostApp.Steps.ReserveInventory,
  compensate: MinimalHostApp.Steps.ReleaseInventory

step :authorize_payment, MinimalHostApp.Steps.AuthorizePayment,
  compensate: MinimalHostApp.Steps.VoidPaymentAuthorization

step :capture_payment, MinimalHostApp.Steps.CapturePayment, retry: [max_attempts: 2]
```

When capture exhausts its retry policy, Squidie keeps the completed
side-effecting steps and their declared rollback callbacks visible through run
inspection, graph inspection, and explanation. The example keeps each external
effect behind a step module, while the workflow definition remains the place
where retry and compensation semantics are visible.

## Daily Digest

`MinimalHostApp.Workflows.DailyDigest` has a manual trigger and a cron trigger
sharing one graph:

```elixir
trigger :manual_digest do
  manual()
end

trigger :daily_digest do
  cron "@reboot", timezone: "Etc/UTC", idempotency: :return_existing_run
end
```

The workflow declares schedule intent. The host app owns recurring delivery and
sends cron payloads into the runtime boundary. The smoke path verifies that a
manual digest and a cron-activated digest complete through the same workflow
graph.

## How To Run Them

Run the full smoke path from the example app:

```sh
cd examples/minimal_host_app
MIX_ENV=test mix example.smoke
```

That command exercises the reference workflows through the host app boundary,
then checks inspection, replay, cancellation, cron activation, local
transactions, and compensation behavior.

For restart-specific behavior, run:

```sh
MIX_ENV=test mix example.resilience
```

For a bounded mixed workload of success, retry, replay, and cancellation, run:

```sh
MIX_ENV=test mix example.soak
```

## Related Reading

- [Getting started](getting_started.md) explains the model with a small
  runnable workflow and detailed inspection output.
- [Workflow authoring](workflow_authoring.md) documents the DSL used by these
  examples.
- [Graph inspection contract](graph_inspection.md) documents the stable
  node-and-edge payload for host UIs.
- [Host app integration](host_app_integration.md) explains the worker loop,
  cron payload boundary, and optional backend-owned leases.
