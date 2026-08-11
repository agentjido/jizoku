# Testing Workflows

`Squidie.Test` provides an isolated in-memory journal and bounded execution
helpers for workflow unit tests. It uses the same public start, execute, and
inspection paths as a host runtime, while keeping each test runtime independent.

## Basic lifecycle

```elixir
test "completes an order workflow" do
  assert {:ok, runtime} =
           Squidie.Test.start_runtime(
             workflow: MyApp.OrderWorkflow,
             now: ~U[2026-08-10 12:00:00Z]
           )

  on_exit(fn -> Squidie.Test.stop_runtime(runtime) end)

  assert {:ok, run} = Squidie.Test.start(runtime, %{order_id: "order-123"})
  assert {:completed, snapshot} = Squidie.Test.drain(runtime, run)
  assert snapshot.run_id == run.run_id
end
```

The storage process also monitors the process that created it, so ExUnit test
process exit cleans up abandoned runtimes. Explicit cleanup remains useful when
a test needs to prove that no state survives the runtime lifecycle.

Each runtime accepts one root run. Use a separate runtime for unrelated runs.
This keeps bounded execution from claiming another test scenario's eligible
work while it is draining the requested workflow tree. Start that root from the
process that created the runtime; helper tasks may execute and inspect it after
the root exists.

## Blocked and bounded execution

`execute_until_blocked/3` and `drain/3` stop when the target run is terminal or
the isolated runtime has no currently eligible work. A nonterminal quiescent run
returns `{:blocked, snapshot}` without polling or sleeping.

Both helpers default to 100 execution steps. Set `max_steps:` on the runtime or
an individual drain when a test needs a different bound:

```elixir
assert {:error, {:execution_limit_reached, diagnostic}} =
         Squidie.Test.drain(runtime, run, max_steps: 5)

assert diagnostic.limit == 5
assert diagnostic.run_id == run.run_id
```

The diagnostic includes the last durable inspection snapshot so an unexpectedly
busy workflow can be understood without a separate read.

Use `execute_until/3` when a test needs to stop at an intermediate durable
state. Pass `max_steps:` through `execute_until/4` when that call needs a
different bound. The predicate receives each inspection snapshot before
terminal, blocked, and bound classification:

```elixir
assert {:reached, snapshot} =
         Squidie.Test.execute_until(runtime, run, fn snapshot ->
           Map.has_key?(snapshot.context, :invoice)
         end)

refute Map.has_key?(snapshot.context, :notification)
assert {:completed, completed} = Squidie.Test.drain(runtime, run)
```

An initially matching predicate returns without executing work. When it never
matches, the helper returns the same terminal, blocked, or execution-limit
result as `drain/3`. Predicate exceptions propagate and still release the
runtime execution lease.

## Virtual time

The runtime clock starts at the `now:` value passed to `start_runtime/1` and
remains frozen until the owner advances it. This makes delayed work, retry
backoff, and deadline classifications testable without sleeping:

```elixir
assert {:blocked, retrying} = Squidie.Test.drain(runtime, run)
assert retrying.next_visible_at == ~U[2026-08-10 12:01:00.000Z]

assert {:ok, ~U[2026-08-10 12:01:00Z]} =
         Squidie.Test.advance_time(runtime, 60, :second)

assert {:completed, snapshot} = Squidie.Test.drain(runtime, run)
```

Use `Squidie.Test.now/1` to read the current instant. The runtime owner may
advance time in seconds, milliseconds, or microseconds. Execution and
inspection each capture one instant from that clock, so lifecycle timestamps
and visibility decisions stay coherent while helper tasks drain or inspect the
run. Advancing while a drain is executing returns `{:error, :runtime_busy}`;
advance again after the helper finishes or reaches a blocked state.

## Cron activation

Start a declared cron trigger through the same durable command-signal path used
by a host scheduler:

```elixir
assert {:ok, run} =
         Squidie.Test.start_cron(
           runtime,
           :hourly_sync,
           %{
             signal_id: "hourly-sync-2026-08-10T12",
             account_id: "account-123"
           },
           metadata: %{source: "workflow-test"}
         )
```

The runtime's frozen clock becomes both the command occurrence time and the
schedule `received_at` value. Storage, queue, and partition also come from the
runtime and cannot be overridden. Callers may provide only signal `:metadata`
and `:idempotency_key`; schedule identity can be supplied through `signal_id`
or a complete `intended_window` in the cron input.

`start_cron/4` is owner-only and uses the runtime's single-root reservation, so
a failed validation does not consume the root while a successful manual or cron
start prevents a second unrelated root. Exact redelivery of an idempotent cron
signal reaches the production duplicate path and returns the existing run;
reusing that identity with different input returns a conflict without writing.
Use `advance_time/3` and `drain/3` for delayed work after activation.

## Runtime-authored action stubs

Pass a runtime-authored workflow spec to `start_runtime/1` when a test needs
deterministic action results through the normal action-registry boundary:

```elixir
assert {:ok, runtime} =
         Squidie.Test.start_runtime(
           workflow: payment_spec,
           action_stubs: %{
             "payments.authorize" => [{:ok, %{authorization: "approved"}}],
             "payments.capture" => [{:ok, %{status: "paid"}}]
           },
           guardrail_registry: payment_guardrails
         )
```

Each stub key becomes a host-approved `ActionRegistry` entry backed by an
internal native step. Trigger payload resolution, runnable input mapping,
guardrails, output mapping, result application, and terminal behavior
still run through production journal paths. Stubs are rejected for
module-authored workflows rather than being silently ignored.

Result sequences are assigned once per new durable runnable identity in the
order calls reach the isolated storage process. If the same runnable is
executed again after an unknown outcome, it receives its previously assigned
result instead of consuming the next one. Remaining results and assignments
survive `restart_runtime/1`. An exhausted sequence fails the action
nonretryably. Use distinct stub keys for concurrently eligible actions when
their results must not depend on scheduler arrival order.

Use `stub_calls/2` to inspect execution-order calls, including input, run,
runnable, step, and attempt identity. Those calls contain application test data
and are not a redacted diagnostic surface. A runtime spec may also combine
stubs with real entries supplied through `:action_registry`; duplicate keys are
rejected.

## Manual controls

Use the named control helpers after a workflow reaches durable manual state:

```elixir
assert {:blocked, %{status: :paused}} = Squidie.Test.drain(runtime, run)

assert {:ok, resumed} =
         Squidie.Test.approve(
           runtime,
           run,
           %{actor: "reviewer-1", comment: "approved"},
           idempotency_key: "approval-order-123"
         )

assert resumed.status == :running
assert {:completed, completed} = Squidie.Test.drain(runtime, run)
```

`approve/4`, `reject/4`, `resume/4`, and `cancel/3` use the same durable public
commands as a host application. They inherit the runtime's isolated storage,
queue, partition, and frozen clock. Callers may provide only `:idempotency_key`
and signal `:metadata`; routing and occurrence time cannot escape the test
runtime. Each helper accepts only the runtime's root run.

## Failure diagnostics

`timeline/2` and `explain/2` expose the same diagnostic views as the public host
read APIs. Use them when an assertion needs stable projected event order or an
actionable reason for a blocked or failed run:

```elixir
assert {:blocked, snapshot} = Squidie.Test.drain(runtime, run)

assert {:ok, explanation} = Squidie.Test.explain(runtime, run)
assert explanation.reason == snapshot.reason
assert explanation.next_actions == [:wait_until_attempt_visible]

assert {:ok, timeline} = Squidie.Test.timeline(runtime, run)
assert Enum.any?(timeline.events, &(&1.type == :attempt_failed))
```

Both helpers are read-only and inherit the runtime's storage, queue, partition,
and frozen clock. Timeline details are redaction-safe. Explanations can include
workflow application data in their evidence, so inspect or sanitize them before
including them in shared CI output. Events with the same timestamp use the
timeline projection's stable tie ordering; treat that order as a deterministic
view, not as an additional causal trace.

## Golden histories

Use `golden_history/2` when a workflow test should detect incompatible changes
to its projected execution history:

```elixir
assert {:ok, golden} = Squidie.Test.golden_history(runtime, run)

assert golden == %{
         schema_version: 1,
         workflow: "Elixir.MyApp.Workflows.Payment",
         queue: "default",
         partition: nil,
         status: :completed,
         terminal_status: :completed,
         events: expected_events
       }
```

The v1 format replaces generated run and runnable identifiers with local
encounter-order aliases and expresses timestamps as microsecond offsets from
run start. It omits timeline summaries and uses a strict event-detail allowlist:
command type, attempt number, relative visibility time, manual kind, and aliased
continuation links. Continuation keys, manual reasons, workflow input, context,
action results, and raw errors are excluded.

Workflow, authored step, queue, and partition names remain as structural
identifiers so fixture diffs stay actionable. Do not place secrets in those
identifiers. Treat `schema_version` changes as an explicit fixture migration,
and review golden updates as compatibility changes rather than regenerating
them automatically.

## Invariant checks

Check one durable inspection snapshot for universal runtime invariants:

```elixir
assert {:ok, snapshot} = Squidie.Test.check_invariants(runtime, run)
```

Healthy running, retry, claim, manual, pending-dispatch, and pending-result
states are accepted. Violations cover projection anomalies, incoherent terminal
state, malformed or duplicate runnable keys, unknown runnable lineage, and
keys that appear in incompatible pending/applied views.

Invariant failures return `{:error, {:invariant_violations, report}}`. The
versioned report contains redaction-safe structural metadata such as run and
runnable identifiers, partition, queue, per-thread revisions, violation codes,
counts, projection reason/source atoms, collection names, and terminal-state
classifications. It does not include workflow input, context, action results,
or raw errors.

The helper is read-only and evaluates one returned public inspection snapshot.
Workflow and dispatch journals are rebuilt sequentially, so the report records
their individual revisions rather than claiming a globally atomic cross-thread
read.

## Checkpoint loss

Delete every projection checkpoint in the isolated runtime to prove that a
workflow can rebuild from its journal history:

```elixir
assert {:reached, before_loss} =
         Squidie.Test.execute_until(runtime, run, fn snapshot ->
           Map.has_key?(snapshot.context, :invoice)
         end)

assert :ok = Squidie.Test.delete_checkpoints(runtime)
assert {:ok, rebuilt} = Squidie.Test.inspect(runtime, run)
assert rebuilt == before_loss
assert {:completed, completed} = Squidie.Test.drain(runtime, run)
```

Deletion is whole-runtime and atomic for the isolated adapter. It removes only
checkpoint hints; journal threads remain unchanged. The runtime owner must call
the helper, and an active drain or control command makes it return
`{:error, :runtime_busy}` so the test cannot delete checkpoints midway through
one bounded operation.

## Runtime restart and stale claims

Restart the isolated runtime process without losing its journal, checkpoints,
root identity, or virtual clock:

```elixir
assert {:ok, restarted_runtime} = Squidie.Test.restart_runtime(runtime)
assert restarted_runtime.id != runtime.id
assert {:error, :runtime_stopped} = Squidie.Test.inspect(runtime, run)
assert {:ok, rebuilt} = Squidie.Test.inspect(restarted_runtime, run)
```

The returned runtime has a fresh worker identity and storage process. Restart is
owner-only and fails with `:runtime_busy` while a live drain, control command,
or start reservation is active. Armed deterministic append faults remain armed.
The old runtime handle is stopped after the replacement has initialized from
one serialized state handoff.

If a test kills a drain after an attempt is durably claimed, restart preserves
that claim. Before its lease expires, the new runtime remains blocked. Advance
the virtual clock to `lease_until` and drain again to exercise production stale
claim takeover without sleeping; the original claim token remains fenced.

## Append conflicts

Inject one expected-revision conflict into the runtime's exact root-run or
configured dispatch thread to exercise normal retry and recovery behavior:

```elixir
assert :ok = Squidie.Test.inject_append_conflict(runtime, :dispatch)
assert {:error, :conflict} = Squidie.Test.drain(runtime, run)
assert {:completed, completed} = Squidie.Test.drain(runtime, run)
```

The conflict is consumed atomically by the first append to the selected
partitioned thread. Appends to other queues, partitions, and journal threads do
not consume it. Production operations may handle a conflict internally, so a
run-thread conflict can complete in the same helper call while still exercising
the real expected-revision retry path.

Only the runtime owner may arm a conflict, and the root run must already exist.
Injection returns `{:error, :runtime_busy}` while a drain or control helper is
active and rejects a second fault until the armed conflict has been consumed.

## Current scope

The test runtime covers isolated in-memory storage, manual, cron, and
runtime-authored workflow starts, bounded and predicate-based execution,
blocked-state detection, virtual time, registry-backed deterministic action
stubs, named manual controls, inspection, failure diagnostics, and
checkpoint-loss replay. It also supports durable runtime restart, stale-claim
recovery, deterministic one-shot append conflicts, invariant checks, and
versioned golden histories. Generic external events require a corresponding
production signal contract and are not synthesized by the test kit.

Use the configured Ecto adapter for integration tests that need database
transactions, migrations, or query behavior. The in-memory runtime is intended
for fast workflow behavior tests, not as production storage.
