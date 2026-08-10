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

## Current scope

The initial test runtime covers isolated in-memory storage, normal workflow
starts, bounded execution, blocked-state detection, and inspection. Virtual
time advancement, signals and manual commands, deterministic faults, restarts,
golden histories, and reusable invariant assertions will build on this same
runtime contract.

Use the configured Ecto adapter for integration tests that need database
transactions, migrations, or query behavior. The in-memory runtime is intended
for fast workflow behavior tests, not as production storage.
