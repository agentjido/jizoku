# Squid Mesh Tooling Usage Rules

## Dashboard And Visual Editor Surfaces

- Use `SquidMesh.list_runs/2` for global and workflow-filtered run indexes.
- Use `SquidMesh.inspect_run/2` for factual run details.
- Use `SquidMesh.inspect_run_graph/2` for graph-oriented dashboard and visual
  editor views.
- Use `GraphInspection.to_map/1` `child_links` to render parent-to-child
  subflow overlays; do not invent client-side child link ids from raw history.
- Use `SquidMesh.explain_run/2` for operator-facing diagnosis and next actions.
- Use `SquidMesh.record_dynamic_work/3` to persist validated dynamic nodes and
  edges that visual editors or dashboards should show on a run graph without
  executing them.
- Use `SquidMesh.schedule_dynamic_work/3` when host-approved dynamic nodes
  should be persisted and executed through the journal dispatch path.
- Only offer dynamic scheduling controls after the origin attempt has applied;
  preview and record remain available for inspection-only overlays on active
  runs.
- Use `SquidMesh.preview_dynamic_work/3` to validate candidate dynamic nodes and
  render the graph overlay before committing them to the journal. Prefer its
  `origin_node_id`, `added_node_ids`, `added_edge_ids`, `recordable?`, and
  `warnings` fields over ad hoc graph diffing in visual editor clients.
- Use `dynamic_work_overlays` from `SquidMesh.inspect_run_graph/2` or
  `GraphInspection.to_map/1` to inspect recorded dynamic work groups; use raw
  `dynamic_work` only when the caller needs the full normalized durable record.
- Pass host-owned `:action_registry` options when previewing or recording
  dynamic nodes that represent executable work candidates, and always pass it
  when scheduling dynamic work.
- Render dynamic edges as visual structure only unless a later API explicitly
  reports dependency ordering for scheduled dynamic nodes.
- Keep list responses redacted by default; fetch detailed history only when the
  caller asks for it.
- Use `definition_version` from list, inspection, graph, and explanation
  surfaces as an operator label only; the definition fingerprint remains the
  compatibility guard.

## Workflow Specs

- Use `SquidMesh.Workflow.to_spec/1` for normalized workflow definitions.
- Use `SquidMesh.Workflow.validate_spec/1` before trusting compiled workflow
  specs in tooling.
- Use `SquidMesh.Workflow.validate_spec/2` with `:action_registry` before
  trusting runtime-authored specs that reference executable actions.
- Use `SquidMesh.start_spec/3` or `SquidMesh.start_spec/4` for runtime-authored
  activation after validation; the persisted run definition is the execution
  and graph-inspection source of truth.
- Use `SquidMesh.Workflow.EditorSpec.to_map/1`,
  `SquidMesh.Workflow.EditorSpec.validate_map/1`, and
  `SquidMesh.Workflow.EditorSpec.preview_graph/1` for JSON-safe visual editor
  round trips and draft graph previews that do not start runs.
- Use `SquidMesh.Workflow.EditorSpec.validate_map/2` and
  `SquidMesh.Workflow.EditorSpec.preview_graph/2` with `:action_registry` when
  editor JSON carries runtime-authored top-level action keys.
- Use `SquidMesh.Workflow.EditorSpec.diff/2` or
  `SquidMesh.Workflow.EditorSpec.diff/3` to compare a source spec and an edited
  draft without starting a run.
- Pass `:action_registry` to `SquidMesh.Workflow.EditorSpec.diff/3` when either
  side includes runtime-authored top-level action keys.
- Treat unresolved specs as data for editors, diagrams, and validation; only
  the host-owned action registry is the module ownership allowlist.
- Do not offer replay controls for runtime-authored spec runs until Squid Mesh
  exposes replay support for that definition source.
- Reject client edits to runtime-owned fields such as fingerprints, run ids,
  journal history, attempts, dispatches, and audit history.
- Preserve stable ids for workflow, trigger, step, transition, and condition
  data so visual editors can round-trip safely.

## SquidSonar

- SquidSonar should list existing workflows and runs through public Squid Mesh
  APIs rather than reading storage adapter internals.
- SquidSonar should fetch specific workflow internals by module/spec and
  specific run internals by run id.
