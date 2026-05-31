# Graph Inspection Contract

`SquidMesh.inspect_run_graph/2` returns a graph-oriented view of one durable
workflow run. Use it when a host app, CLI, dashboard, or visual workflow tool
needs nodes and edges instead of raw journal history.

The public API still returns structs:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run_id)
```

Use `SquidMesh.Runs.GraphInspection.to_map/1` at the host boundary when a UI
needs a stable map payload:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run_id)
payload = SquidMesh.Runs.GraphInspection.to_map(graph)
```

That keeps existing struct callers compatible while giving UI serializers an
explicit shape.

## Top-Level Shape

The map shape is:

```elixir
%{
  run_id: "run_123",
  workflow: "Elixir.MyApp.Workflows.EmailReply",
  source: :read_model,
  status: :running,
  current_node_id: "draft_reply",
  current_node_ids: ["draft_reply"],
  terminal?: false,
  nodes: [...],
  edges: [...],
  child_runs: [],
  child_links: [],
  dynamic_work: [],
  anomalies: []
}
```

Workflow modules are serialized with `Atom.to_string/1`, so Elixir modules use
the normal `"Elixir."` prefix. Persisted serialized workflow definitions keep
their stored string value.

For runtime-authored specs started with `SquidMesh.start_spec/3` or
`SquidMesh.start_spec/4`, graph inspection uses the resolved definition
persisted on the run. The stored workflow value remains a stable identity, but
nodes, edges, action keys, and selected transitions come from the durable spec
rather than from loading a workflow module.

`current_node_id` is the first active node for simple callers.
`current_node_ids` preserves parallel runnable nodes in dependency workflows.
`terminal?` is true when the run is in a terminal state such as `:completed`,
`:failed`, or `:cancelled`.

## Node Shape

Nodes represent workflow steps:

```elixir
%{
  id: "draft_reply",
  action: nil,
  status: :running,
  current?: true,
  input: nil,
  output: nil,
  error: nil,
  recovery: nil,
  transition: nil,
  manual_state: nil,
  attempts: []
}
```

`action` is the stable host-owned action key when the node came from a spec
resolved through `SquidMesh.Workflow.resolve_spec_actions/2`. Compiled
module-authored workflows usually leave it nil.

`recovery` is populated from the durable runnable recovery policy when the
runtime has one. For compensatable steps it includes the callback module name
and status, letting host dashboards show rollback availability without parsing
journal entries or loading the current workflow module.

Node status values are:

- `:waiting` - no runnable work has been recorded for the node
- `:pending` - work is visible or scheduled
- `:running` - a worker has an active claim
- `:retrying` - a failed attempt scheduled another try
- `:paused` - the node is waiting for manual intervention
- `:completed` - durable terminal step success exists
- `:failed` - durable terminal step failure exists

By default, inputs, outputs, errors, manual state, and attempt details are nil
or empty because they can contain host-domain data. Request details explicitly:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run_id, include_history: true)
payload = SquidMesh.Runs.GraphInspection.to_map(graph)
```

With history enabled, a node can include fields such as:

```elixir
%{
  id: "review_draft",
  status: :paused,
  current?: true,
  manual_state: %{step: "review_draft", kind: :approval},
  attempts: [%{attempt_number: 1, status: :completed}],
  output: %{drafts: [%{subject: "hello"}]}
}
```

Host apps should still authorize and redact this payload before exposing it
outside trusted operator surfaces. For field-selection guidance, see
[Observability: redaction and field selection](observability.md#redaction-and-field-selection).

Dynamic work nodes are inspectable runtime-authored structure. Preview them
through `SquidMesh.preview_dynamic_work/3` when a UI or visual editor needs to
validate a candidate payload and render the graph overlay before writing. Record
them through `SquidMesh.record_dynamic_work/3` so the runtime validates the
stable dynamic key, producer origin, node ids, and optional dynamic edges
against an active run snapshot before appending durable metadata. Graph
projections mark those nodes with `dynamic?: true` so graph UIs can distinguish
them from declared workflow steps:

```elixir
%{
  id: "deliver_digest:chat_1",
  action: "digest.deliver",
  status: :recorded,
  current?: false,
  dynamic?: true,
  origin: %{step: "schedule_digest", runnable_key: "run_123:schedule_digest:1", attempt: 1},
  metadata: %{chat_id: "chat_1"}
}
```

`SquidMesh.Runs.DynamicWorkPreview.to_map/1` exposes stable overlay metadata for
editor controls without requiring clients to diff graphs themselves:

```elixir
%{
  run_id: "run_123",
  duplicate?: false,
  recordable?: true,
  origin_node_id: "schedule_digest",
  added_node_ids: ["deliver_digest:chat_1"],
  added_edge_ids: ["schedule_digest:dynamic:deliver_digest:chat_1"],
  warnings: [],
  dynamic_work: %{dynamic_key: "subscription_digest_fanout"},
  graph: %{nodes: [...], edges: [...]}
}
```

`recordable?` means recording would append a new durable dynamic-work fact.
Exact duplicate previews are still valid but return `duplicate?: true`,
`recordable?: false`, empty added id lists, and
`warnings: [:duplicate_dynamic_work]`.

The current dynamic-work support is inspection-only. It lets dashboards and
visual editors show bounded runtime-generated structure before Squid Mesh adds
execution semantics for dynamic in-run graph expansion. Previewing or recording
dynamic work does not schedule dispatch attempts, alter dependency readiness, or
change terminal-state decisions. Terminal runs reject new dynamic-work previews
and records.

## Child Run Links

Nested workflow starts remain separate durable runs. The parent graph exposes
their factual records through `child_runs` and a derived `child_links` overlay
that graph UIs can render as subflow links without treating the child as an
inline executable node:

```elixir
%{
  id: "start_nested_invite:child_run:child_run_123",
  from: "start_nested_invite",
  to: "child_run_123",
  type: :child_run,
  status: :linked,
  child_run_id: "child_run_123",
  child_workflow: "Elixir.MyApp.Workflows.InviteDelivery",
  child_trigger: "deliver_invite",
  child_key: "invite_guest_456",
  origin: %{step: "start_nested_invite", runnable_key: "run_123:start_nested_invite:1", attempt: 1},
  metadata: %{guest_id: "guest_456"},
  started_at: ~U[2026-05-30 12:00:00Z]
}
```

Use `child_links` for visual inspection and editor-friendly stable ids. Use
`child_runs` when the UI needs the full child-run fact, and call
`inspect_run/2` or `inspect_run_graph/2` on `child_run_id` for the child
workflow's own status, nodes, and edges. Child links are not workflow transition
edges and do not affect dependency readiness, retry behavior, replay behavior,
or cancellation boundaries. `started_at` is optional and appears only when the
durable child-run fact includes it. Stale child-run facts without both
`child_run_id` and an origin step remain visible in `child_runs`, but they do
not produce a `child_links` entry.

## Edge Shape

Edges represent transitions or dependencies:

```elixir
%{
  id: "fetch_emails:ok:draft_reply",
  from: "fetch_emails",
  to: "draft_reply",
  type: :transition,
  status: :selected,
  selected?: true,
  skipped?: false,
  pending?: false,
  blocked?: false,
  outcome: :ok,
  condition: nil,
  recovery: nil
}
```

Edge status values are:

- `:selected` - durable step state proves this path was taken
- `:skipped` - a sibling path or terminal outcome won
- `:pending` - the source step or dependency has not terminally resolved
- `:blocked` - a dependency failed before this edge could become runnable

Conditional transition edges include their condition and deterministic ids:

```elixir
%{
  id: "classify:ok:auto_approve:condition:0",
  from: "classify",
  to: "auto_approve",
  type: :transition,
  outcome: :ok,
  condition: %{path: [:routing, :decision], equals: "auto"},
  status: :selected,
  selected?: true,
  skipped?: false,
  pending?: false,
  blocked?: false
}
```

Dependency workflows use dependency edges:

```elixir
%{
  id: "load_invoice:dependency:send_email",
  from: "load_invoice",
  to: "send_email",
  type: :dependency,
  outcome: nil,
  status: :pending,
  selected?: false,
  skipped?: false,
  pending?: true,
  blocked?: false
}
```

Dynamic edges connect the producer step to recorded dynamic nodes:

```elixir
%{
  id: "schedule_digest:dynamic:deliver_digest:chat_1",
  from: "schedule_digest",
  to: "deliver_digest:chat_1",
  type: :dynamic,
  status: :pending,
  selected?: false,
  skipped?: false,
  pending?: true,
  blocked?: false
}
```

`dynamic_work` keeps the grouped durable facts behind those nodes and edges:

```elixir
%{
  dynamic_key: "subscription_digest_fanout",
  status: :recorded,
  reason: :runtime_fanout,
  origin: %{step: "schedule_digest", runnable_key: "run_123:schedule_digest:1", attempt: 1},
  nodes: [%{id: "deliver_digest:chat_1", action: "digest.deliver"}],
  edges: [%{type: :dynamic, from: "schedule_digest", to: "deliver_digest:chat_1"}]
}
```

## Compatibility

The graph map contract is intended for host UI and tooling integration. Squid
Mesh may add optional fields in future releases, but the existing field names,
identifier semantics, node statuses, edge statuses, and default detail redaction
are stable compatibility points.

If the workflow module can no longer be loaded, Squid Mesh still returns any
durable node state it can infer from the run. `edges` is empty in that degraded
state because topology belongs to the workflow definition.

The default payload does not include claim tokens, storage configuration,
adapter internals, process identifiers, or raw journal entries.
