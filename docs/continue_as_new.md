# Continue As New

Continue-as-new closes one active workflow run and starts one fresh successor
with explicit durable lineage. Use it for recurring or paginated workflows that
would otherwise accumulate an unbounded run thread.

The predecessor becomes terminal with status `:continued`. The successor has a
new run id, fresh run history, the selected current workflow definition, and
only the declared successor input. Squidie does not copy accumulated workflow
context into the successor.

## Native Step Return

A native `Squidie.Step` can request continuation at its execution boundary:

```elixir
defmodule Billing.Steps.AdvanceCursor do
  use Squidie.Step, name: :advance_cursor

  @impl Squidie.Step
  def run(%{cursor: cursor}, _context) when cursor < 100 do
    {:continue_as_new, %{cursor: cursor + 1},
     key: "cursor-#{cursor + 1}", definition: :current}
  end

  def run(%{cursor: cursor}, _context) do
    {:ok, %{completed_cursor: cursor}}
  end
end
```

The continuation key must be a stable, non-empty string. `definition: :current`
is required. Input must be a storage-safe map accepted by the selected trigger's
payload contract. The native control result is distinct from ordinary success;
do not place `:continue_as_new` inside `{:ok, output, opts}`.

When a worker receives the native result, Squidie durably completes the source
attempt and fences the predecessor in one dispatch append. Recovery then applies
the source, records the continuation intent, terminalizes the predecessor,
starts or repairs the deterministic successor, and records repair completion.
Crashes and duplicate delivery converge on the same successor without rerunning
an already committed source action.

## Public Command

Host code can continue a quiescent active run directly:

```elixir
{:ok, successor} =
  Squidie.continue_as_new(run_id,
    input: %{cursor: next_cursor},
    continuation_key: "cursor-#{next_cursor}"
  )
```

The public command is appropriate when host-owned control logic chooses the
boundary after all planned work has applied. It rejects unsafe states such as
active or pending attempts, manual gates, compensation or recovery work,
dynamic work, graph mutation, and unresolved child starts. The native return is
the usual choice when the workflow step itself decides to recur.

Exact retries return the same successor. Reusing the predecessor continuation
with a different key or input fails closed. Queue, partition, trace, trigger,
definition version, and definition fingerprint are preserved or validated from
durable state rather than accepted as caller-controlled lineage.

## Choosing The Right Primitive

| Need | Use | Run/history behavior |
| --- | --- | --- |
| Bound one recurring workflow's history | Continue as new | Predecessor becomes `:continued`; one fresh linked successor starts. |
| Recheck the same step later without consuming retry budget | `{:defer, reason, schedule_in: seconds}` | Same run and logical step continue after durable delayed visibility. |
| Start separately managed work discovered by a step | `start_child_run/4` or `/5` | Independent child lifecycle with parent-child lineage. |
| Re-run prior workflow history for operator recovery | `replay/2` | New replay run linked to a source run under replay safety rules. |
| Start a workflow on a schedule | Cron trigger and host scheduler | Independent scheduled runs with host-owned delivery and idempotency. |
| Add bounded executable nodes to the active run | `schedule_dynamic_work/3` | Same run gains durable dynamic nodes and graph overlays. |

Continue-as-new is not a retry, replay, child start, or graph mutation. It is a
terminal lifecycle transition followed by a fresh run with one explicit
continuation edge.

## Inspection And History Bounds

Single-run read models expose immediate lineage without recursively loading the
chain:

- `inspect_run/2` and `list_runs/2` include `continuation.continued_from` and
  `continuation.continued_to` plus a `history` size classification.
- `inspect_run_graph/2` includes explicit `:continuation` links.
- `inspect_run_timeline/2` includes `:run_continued_from` and
  `:run_continued_to` events.
- `explain_run/2` identifies `:continued` terminal runs and points operators to
  the immediate successor.

Traverse more than one edge only through the bounded chain API:

```elixir
{:ok, chain} =
  Squidie.inspect_continuation_chain(successor_run_id,
    direction: :backward,
    max_hops: 25
  )

chain.runs
chain.hops
chain.truncated?
chain.warnings
```

Traversal follows continuation edges only. It never follows child or replay
lineage and loads at most `max_hops + 1` run projections.

Hosts can tune warnings and the default traversal limit:

```elixir
config :squidie, :continuation_history,
  run_warning_threshold: 5_000,
  run_critical_threshold: 20_000,
  chain_warning_hops: 25,
  max_chain_hops: 100
```

These thresholds classify durable thread size for operator tooling; they do not
delete or compact history.

## Activation And Rollout

Fence emission is disabled by default because old workers do not understand the
new dispatch control facts. Roll out in two phases:

1. Deploy a continuation-aware Squidie release to every worker, recovery loop,
   and scheduler that can read the affected queues. Drain or replace older
   workers and prevent an older image from returning.
2. Enable emission at the trusted host configuration boundary:

   ```elixir
   config :squidie, continuation_fences: :enabled
   ```

The flag is a host readiness assertion, not automatic cluster discovery. Do not
expose it as a request option. After the first continuation fence is written,
rollback may disable new emission, but workers must remain continuation-aware
until every durable fence has been repaired or aborted.

The minimal host app enables the flag in development and test because those
smoke paths run one coherent application version. Its production configuration
keeps emission disabled; multi-node activation remains subject to the
all-workers rollout barrier.
