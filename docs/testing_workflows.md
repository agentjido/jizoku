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

The test runtime covers isolated in-memory storage, normal workflow starts,
bounded and predicate-based execution, blocked-state detection, virtual time,
named manual controls, inspection, failure diagnostics, and checkpoint-loss
replay. It also supports deterministic one-shot append conflicts. Generic
signals, broader deterministic faults, restarts, golden histories, and reusable
invariant assertions will build on this same runtime contract.

Use the configured Ecto adapter for integration tests that need database
transactions, migrations, or query behavior. The in-memory runtime is intended
for fast workflow behavior tests, not as production storage.
