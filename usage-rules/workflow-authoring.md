# Squid Mesh Workflow Authoring Usage Rules

## Workflow Shape

- Define workflows as compiled Elixir modules with `use SquidMesh.Workflow`.
- Use business names for triggers, steps, and transitions.
- Declare `version "..."` inside `workflow do` when operators need a stable
  human-readable definition label across deploys.
- Keep workflow branches, retries, waits, recovery routes, and manual gates in
  the workflow definition when operators need to understand them.
- Use `SquidMesh.Workflow.to_spec/1` and `SquidMesh.Workflow.validate_spec/1`
  when tooling needs a normalized data representation.
- Use `SquidMesh.Workflow.validate_spec/2` with `:action_registry` before
  trusting runtime-authored spec data that references executable actions.
- Use `SquidMesh.start_spec/3` or `SquidMesh.start_spec/4` to activate
  runtime-authored specs only after action keys resolve through a host-owned
  registry.
- Use `SquidMesh.Workflow.EditorSpec` for visual-editor JSON round trips and
  draft graph previews. Do not treat editor preview data as an execution
  boundary.
- Pass `:action_registry` to `SquidMesh.Workflow.EditorSpec.validate_map/2` and
  `SquidMesh.Workflow.EditorSpec.preview_graph/2` when editor-owned specs use
  top-level action keys.
- Use `SquidMesh.Workflow.EditorSpec.diff/2` or
  `SquidMesh.Workflow.EditorSpec.diff/3` for visual-editor change inspection;
  diff output is not an execution boundary.
- Pass `:action_registry` to `SquidMesh.Workflow.EditorSpec.diff/3` when
  comparing editor drafts that use top-level action keys.
- Do not activate runtime-authored workflows directly from request input; route
  them through the host registry and Squid Mesh start boundary.

## Steps

- Prefer `use SquidMesh.Step` for custom steps.
- Use `SquidMesh.start_child_run/4` or `SquidMesh.start_child_run/5` only from
  native steps that receive `SquidMesh.Step.Context`.
- Provide a stable, storage-safe `:child_key` for every child run; treat it as
  the idempotency key for the parent run and parent step.
- Keep child workflow modules backend-neutral, the same as parent workflows.
- Return `{:ok, output}` for success.
- Return `{:error, reason}` for terminal failure governed by workflow routing.
- Return `{:retry, reason}` or `{:retry, reason, opts}` for retryable failure.
- Keep side-effect idempotency inside the step or host domain boundary.
- Use `context.idempotency_key` and `context.claim_id` for external
  reconciliation and action idempotency. Never expose or persist claim tokens in
  step output, logs, or host-facing errors.
- Use raw `Jido.Action` modules only for explicit interop.

## Data Mapping

- Use payload contracts for start input validation.
- Use step `input:` to select only the data a step needs.
- Use step `output:` to place returned data under stable keys.
- Use conditional transitions for inspectable routing decisions.
- Use `equals` for exact matches and `greater_than` or `less_than` for numeric
  threshold routing.
- Keep condition values JSON-safe so selected routes can be persisted.

## Manual And Long-Running Work

- Use `:pause` or `approval_step/2` for operator-controlled boundaries.
- Resolve manual gates through `resume/3`, `approve/3`, and `reject/3`.
- Use `:wait` for workflow-scale delays, not arbitrary timers.
- Prefer cron or host scheduling when the whole workflow should start later.

## Recovery

- Mark irreversible external side effects with `irreversible: true` or
  `compensatable: false`.
- Use `recovery: :compensation` or `recovery: :undo` on error transitions when
  the route has operational meaning.
- Treat child runs as separate replay, retry, cancellation, and inspection
  boundaries. Do not mutate already-run parent steps to simulate dynamic
  expansion.
- Use `SquidMesh.record_dynamic_work/3` for bounded dynamic work that should be
  visible to operators but should not execute.
- Use `SquidMesh.schedule_dynamic_work/3` for bounded dynamic work that should
  be persisted and executed through the journal dispatch path.
- Schedule dynamic work only after the origin runnable has applied; do not use
  dynamic scheduling to speculate ahead of the producer step.
- Use `SquidMesh.preview_dynamic_work/3` before recording when tooling needs to
  validate and render the candidate graph overlay without appending. Use the
  preview's added id lists and warnings instead of client-side graph diffing.
- Pass `:action_registry` to dynamic-work preview and record calls when the
  overlay represents future executable work; pass it to every schedule call.
  Each executable dynamic node must use a host-approved action key.
- Use dynamic node `retry: [max_attempts: n]` only when the host action is safe
  for repeated delivery. Treat dynamic edges as graph metadata, not dependency
  ordering.
- Do not rely on "this step should only run once" as the side-effect safety
  model.
