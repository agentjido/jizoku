# Jizoku Tooling Usage Rules

## Dashboard And Visual Editor Surfaces

- Use `Jizoku.list_runs/2` for global and workflow-filtered run indexes.
- Use `Jizoku.inspect_run/2` for factual run details.
- Use `Jizoku.inspect_run_graph/2` for graph-oriented dashboard and visual
  editor views.
- Use `GraphInspection.to_map/1` `child_links` to render parent-to-child
  subflow overlays; do not invent client-side child link ids from raw history.
- Use `Jizoku.explain_run/2` for operator-facing diagnosis and next actions.
- Carry `partition` with every run identity in links, selections, caches, and
  operator actions. Run UUIDs and queue names may intentionally repeat across
  partitions, and read APIs never search another partition.
- Authorize partition access at the host boundary; partition-aware storage and
  output do not replace authorization.
- Use `Jizoku.record_dynamic_work/3` to persist validated dynamic nodes and
  edges that visual editors or dashboards should show on a run graph without
  executing them.
- Use `Jizoku.schedule_dynamic_work/3` when host-approved dynamic nodes
  should be persisted and executed through the journal dispatch path.
- Only offer dynamic scheduling controls after the origin attempt has applied;
  preview and record remain available for inspection-only overlays on active
  runs.
- Use `Jizoku.preview_dynamic_work/3` to validate candidate dynamic nodes and
  render the graph overlay before committing them to the journal. Prefer its
  `origin_node_id`, `added_node_ids`, `added_edge_ids`, `recordable?`, and
  `warnings` fields over ad hoc graph diffing in visual editor clients. The
  preview and its graph also expose the selected `partition`.
- Use `dynamic_work_overlays` from `Jizoku.inspect_run_graph/2` or
  `GraphInspection.to_map/1` to inspect recorded dynamic work groups; use raw
  `dynamic_work` only when the caller needs the full normalized durable record.
- Pass host-owned `:action_registry` options when previewing or recording
  dynamic nodes that represent executable work candidates, and always pass it
  when scheduling dynamic work.
- Render dynamic edges as visual structure only unless a later API explicitly
  reports dependency ordering for scheduled dynamic nodes.
- Use `graph_version`, provenance and active/tombstoned identity fields,
  ready/blocked node IDs, redacted `mutation_history`, and
  `reconciliation_status` from graph inspection for versioned mutation UIs.
- Offer a repair action only when `reconciliation_status == :required`; invoke
  `Jizoku.reconcile_dynamic_graph/2` instead of editing dispatch storage.
- Keep list responses redacted by default; fetch detailed history only when the
  caller asks for it.
- Use `first:` and `after:` for bounded cursor pages. Reuse the same filters and
  selected partition until `next_cursor` is `nil`; do not decode or edit cursor
  contents in host UI code.
- Filter only host-approved search attributes. Keep payloads, outputs, errors,
  secrets, credentials, and arbitrary metadata out of the search schema.
- Treat `visibility_policy: :auditor` as privileged. External and operator
  pages may filter by approved attributes but return them redacted.
- Apply the additive Ecto run-search migration and rebuild each configured
  partition explicitly. Other adapters and incomplete projections use the
  journal catalog fallback.
- Use `definition_version` from list, inspection, graph, and explanation
  surfaces as an operator label only; the definition fingerprint remains the
  compatibility guard.

## Workflow Specs

- Use `Jizoku.Workflow.to_spec/1` for normalized workflow definitions.
- Use `Jizoku.Workflow.validate_spec/1` before trusting compiled workflow
  specs in tooling.
- Use `Jizoku.Workflow.action_catalog/1` to render editor-safe action palettes
  from a host-owned registry; treat catalog metadata as display data, not an
  execution boundary.
- Use `Jizoku.Step.HTTP` for host-approved HTTP nodes in runtime-authored
  specs. Validate request config with `Jizoku.Step.HTTP.validate_request/1`,
  use registry-owned `action_opts: [allowed_hosts: [...]]` for destination
  policy, expose it only through an action-registry key, and represent
  credentials as host-owned references instead of request values. Keep URL
  query strings, userinfo, secret-bearing headers, and secret-bearing payload
  keys out of HTTP action requests.
- Use `Jizoku.Step.Elixir` for runtime-authored specs that need custom
  host-owned Elixir logic. Expose it only through an action-registry key, keep
  executable adapters in registry-owned `action_opts: [adapters: ...]`, pass
  `:action_registry` at both start and execution boundaries, and override
  `input_contract` when editor catalogs need adapter choices. Runtime inputs
  may name an approved adapter key and params only; do not create atoms, load
  modules, select functions, or evaluate code from editor input.
- Use `Jizoku.Workflow.validate_spec/2` with `:action_registry` before
  trusting runtime-authored specs that reference executable actions.
- Use `Jizoku.start_spec/3` or `Jizoku.start_spec/4` for runtime-authored
  activation after validation; the persisted run definition is the execution
  and graph-inspection source of truth.
- Use `Jizoku.Workflow.EditorSpec.to_map/1`,
  `Jizoku.Workflow.EditorSpec.validate_map/1`, and
  `Jizoku.Workflow.EditorSpec.preview_graph/1` for JSON-safe visual editor
  round trips and draft graph previews that do not start runs.
- Use `Jizoku.Workflow.EditorSpec.validate_map/2` and
  `Jizoku.Workflow.EditorSpec.preview_graph/2` with `:action_registry` when
  editor JSON carries runtime-authored top-level action keys.
- Use `Jizoku.Workflow.EditorSpec.diff/2` or
  `Jizoku.Workflow.EditorSpec.diff/3` to compare a source spec and an edited
  draft without starting a run.
- Pass `:action_registry` to `Jizoku.Workflow.EditorSpec.diff/3` when either
  side includes runtime-authored top-level action keys.
- Treat unresolved specs as data for editors, diagrams, and validation; only
  the host-owned action registry is the module ownership allowlist.
- Do not offer replay controls for runtime-authored spec runs until Jizoku
  exposes replay support for that definition source.
- Reject client edits to runtime-owned fields such as fingerprints, run ids,
  journal history, attempts, dispatches, and audit history.
- Preserve stable ids for workflow, trigger, step, transition, and condition
  data so visual editors can round-trip safely.

## Kansoku

- Kansoku should list existing workflows and runs through public Jizoku
  APIs rather than reading storage adapter internals.
- Kansoku should fetch specific workflow internals by module/spec and
  specific run internals by run id.
