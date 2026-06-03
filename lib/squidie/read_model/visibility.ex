defmodule Squidie.ReadModel.Visibility do
  @moduledoc """
  Actor-scoped read-model redaction helpers.

  Visibility is a host-owned authorization boundary. Squidie keeps durable
  history immutable and returns factual read models; host applications can call
  this module at their HTTP, LiveView, CLI, or dashboard boundary to derive a
  less-sensitive view for a specific actor.
  """

  alias Squidie.ReadModel.Explanation.Diagnostic
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.ReadModel.Listing.Summary
  alias Squidie.Runs.GraphInspection
  alias Squidie.Runs.GraphInspection.Node

  @visibility_scopes [:external, :operator, :auditor]
  @manual_state_fields [
    :step,
    :kind,
    :status,
    :reason,
    :paused_at,
    :requested_at,
    :resolved_at,
    :deadline
  ]
  @run_summary_fields [
    :run_id,
    :workflow,
    :definition_version,
    :status,
    :terminal?,
    :terminal_status
  ]
  @deadline_fields [
    :status,
    :step,
    :runnable_key,
    :started_at,
    :due_at,
    :due_soon_at,
    :escalated_at,
    :overdue?,
    :remaining_ms,
    :escalation
  ]
  @runnable_fields [:runnable_key, :key, :step, :status, :attempt_number, :visible_at, :deadline]
  @attempt_fields [
    :runnable_key,
    :status,
    :attempt_number,
    :step,
    :visible_at,
    :deadline,
    :wakeup_emitted?,
    :applied?
  ]
  @anomaly_fields [:source, :reason, :entry_type, :run_id, :step, :runnable_key]
  @dynamic_work_fields [:dynamic_key, :status, :reason, :origin, :nodes, :edges, :recorded_at]
  @dynamic_work_overlay_fields [
    :dynamic_key,
    :status,
    :reason,
    :origin,
    :origin_node_id,
    :added_node_ids,
    :added_edge_ids,
    :node_count,
    :edge_count,
    :recorded_at
  ]
  @dynamic_origin_fields [:runnable_key, :step, :attempt]
  @dynamic_node_fields [:id, :action, :status]
  @dynamic_edge_fields [:id, :from, :to, :type, :status]
  @child_link_fields [
    :id,
    :from,
    :to,
    :type,
    :status,
    :child_run_id,
    :child_workflow,
    :child_trigger,
    :child_key,
    :origin
  ]

  @type scope :: :external | :operator | :auditor
  @type policy ::
          scope()
          | module()
          | {module(), term()}
          | (term(), term() -> scope() | {:ok, scope()})
  @type visibility_error ::
          {:invalid_visibility_policy, :missing_callback | {:scope, term()} | {:policy, term()}}

  @doc """
  Applies a host-owned visibility policy to a read-model view.

  Supported policy forms:

  - `:external`, `:operator`, or `:auditor`
  - a two-arity function `(actor, view -> scope | {:ok, scope})`
  - a module exporting `visibility_scope/2`
  - `{module, opts}` for modules exporting `visibility_scope/3`

  `:auditor` returns the original view. `:external` and `:operator` preserve
  high-level status and current/manual task shape while removing payloads,
  command history, claim metadata, attempt inputs/results/errors, and other
  nested runtime evidence.
  """
  @spec redact(term(), term()) :: {:ok, term()} | {:error, visibility_error()}
  @spec redact(term(), term(), policy()) :: {:ok, term()} | {:error, visibility_error()}
  def redact(view, actor, policy \\ :external) do
    with {:ok, scope} <- visibility_scope(policy, actor, view) do
      {:ok, redact_view(view, scope)}
    end
  end

  defp visibility_scope(scope, _actor, _view) when scope in @visibility_scopes do
    {:ok, scope}
  end

  defp visibility_scope(policy, actor, view) when is_function(policy, 2) do
    normalize_scope(policy.(actor, view))
  end

  defp visibility_scope({module, opts}, actor, view) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :visibility_scope, 3) do
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      normalize_scope(apply(module, :visibility_scope, [actor, view, opts]))
    else
      {:error, {:invalid_visibility_policy, :missing_callback}}
    end
  end

  defp visibility_scope(module, actor, view) when is_atom(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :visibility_scope, 2) do
      # credo:disable-for-next-line Credo.Check.Refactor.Apply
      normalize_scope(apply(module, :visibility_scope, [actor, view]))
    else
      {:error, {:invalid_visibility_policy, :missing_callback}}
    end
  end

  defp visibility_scope(policy, _actor, _view) do
    {:error, {:invalid_visibility_policy, {:policy, policy}}}
  end

  defp normalize_scope({:ok, scope}) when scope in @visibility_scopes, do: {:ok, scope}
  defp normalize_scope(scope) when scope in @visibility_scopes, do: {:ok, scope}
  defp normalize_scope(scope), do: {:error, {:invalid_visibility_policy, {:scope, scope}}}

  defp redact_view(view, :auditor), do: view

  defp redact_view(%Summary{} = summary, _scope), do: summary

  defp redact_view(%Snapshot{} = snapshot, _scope) do
    %Snapshot{
      snapshot
      | input: nil,
        context: %{},
        terminal_error: nil,
        parent_run: summarize_run(snapshot.parent_run),
        child_runs: Enum.map(snapshot.child_runs, &summarize_run/1),
        dynamic_work: Enum.map(snapshot.dynamic_work, &summarize_dynamic_work/1),
        command_history: [],
        deadline: summarize_deadline(snapshot.deadline),
        manual_state: summarize_manual_state(snapshot.manual_state),
        planned_runnables: Enum.map(snapshot.planned_runnables, &summarize_runnable/1),
        pending_dispatches: Enum.map(snapshot.pending_dispatches, &summarize_runnable/1),
        pending_results: Enum.map(snapshot.pending_results, &summarize_attempt/1),
        visible_attempts: Enum.map(snapshot.visible_attempts, &summarize_attempt/1),
        scheduled_attempts: Enum.map(snapshot.scheduled_attempts, &summarize_attempt/1),
        expired_claims: Enum.map(snapshot.expired_claims, &summarize_attempt/1),
        attempts: Enum.map(snapshot.attempts, &summarize_attempt/1),
        anomalies: Enum.map(snapshot.anomalies, &summarize_anomaly/1)
    }
  end

  defp redact_view(%GraphInspection{} = graph, _scope) do
    %GraphInspection{
      graph
      | nodes: Enum.map(graph.nodes, &summarize_node/1),
        child_runs: Enum.map(graph.child_runs, &summarize_run/1),
        child_links: Enum.map(graph.child_links, &summarize_child_link/1),
        dynamic_work: Enum.map(graph.dynamic_work, &summarize_dynamic_work/1),
        dynamic_work_overlays:
          Enum.map(graph.dynamic_work_overlays, &summarize_dynamic_work_overlay/1),
        anomalies: Enum.map(graph.anomalies, &summarize_anomaly/1)
    }
  end

  defp redact_view(%Diagnostic{} = diagnostic, _scope) do
    %Diagnostic{
      diagnostic
      | details: summarize_details(diagnostic.details),
        evidence: summarize_evidence(diagnostic.evidence)
    }
  end

  defp redact_view(view, _scope) when is_map(view) do
    if graph_map?(view) do
      view
      |> remove_sensitive_nested()
      |> update_dual_list(:child_runs, &summarize_run/1)
      |> update_dual_list(:child_links, &summarize_child_link/1)
      |> update_dual_list(:dynamic_work, &summarize_dynamic_work/1)
      |> update_dual_list(:dynamic_work_overlays, &summarize_dynamic_work_overlay/1)
      |> update_dual_list(:anomalies, &summarize_anomaly/1)
    else
      remove_sensitive_nested(view)
    end
  end

  defp redact_view(view, scope) when is_list(view), do: Enum.map(view, &redact_view(&1, scope))

  defp redact_view(view, _scope), do: view

  defp graph_map?(view) when is_map(view) do
    is_list(value(view, :nodes)) and is_list(value(view, :edges)) and
      (not is_nil(value(view, :run_id)) or not is_nil(value(view, :workflow)))
  end

  defp update_dual_list(map, key, fun)
       when is_map(map) and is_atom(key) and is_function(fun, 1) do
    map
    |> update_list_key(key, fun)
    |> update_list_key(Atom.to_string(key), fun)
  end

  defp update_list_key(map, key, fun) do
    if Map.has_key?(map, key) do
      Map.update!(map, key, &summarize_list(&1, fun))
    else
      map
    end
  end

  defp summarize_list(value, fun) when is_list(value), do: Enum.map(value, fun)
  defp summarize_list(_value, _fun), do: []

  defp summarize_node(%Node{} = node) do
    %Node{
      node
      | input: nil,
        output: nil,
        error: nil,
        deadline: summarize_deadline(node.deadline),
        metadata: %{},
        origin: summarize_dynamic_origin(node.origin),
        manual_state: summarize_manual_state(node.manual_state),
        attempts: []
    }
  end

  defp summarize_manual_state(nil), do: nil

  defp summarize_manual_state(manual_state) when is_map(manual_state) do
    manual_state
    |> take_dual_keys(@manual_state_fields)
    |> Map.update(:deadline, nil, &summarize_deadline/1)
    |> compact()
  end

  defp summarize_manual_state(_manual_state), do: nil

  defp summarize_run(nil), do: nil

  defp summarize_run(run) when is_map(run) do
    run
    |> take_dual_keys(@run_summary_fields)
    |> compact()
  end

  defp summarize_run(_run), do: nil

  defp summarize_runnable(runnable) when is_map(runnable) do
    runnable
    |> take_dual_keys(@runnable_fields)
    |> Map.update(:deadline, nil, &summarize_deadline/1)
    |> compact()
  end

  defp summarize_runnable(_runnable), do: %{}

  defp summarize_attempt(attempt) when is_map(attempt) do
    attempt
    |> take_dual_keys(@attempt_fields)
    |> Map.update(:deadline, nil, &summarize_deadline/1)
    |> compact()
  end

  defp summarize_attempt(_attempt), do: %{}

  defp summarize_deadline(deadline) when is_map(deadline) do
    deadline
    |> take_dual_keys(@deadline_fields)
    |> Map.update(:escalation, nil, &summarize_escalation/1)
    |> compact()
  end

  defp summarize_deadline(_deadline), do: nil

  defp summarize_escalation(escalation) when is_map(escalation) do
    escalation
    |> take_dual_keys([:outcome])
    |> compact()
  end

  defp summarize_escalation(_escalation), do: nil

  defp summarize_anomaly(anomaly) when is_map(anomaly) do
    anomaly
    |> take_dual_keys(@anomaly_fields)
    |> compact()
  end

  defp summarize_anomaly(_anomaly), do: %{}

  defp summarize_dynamic_work(dynamic_work) when is_map(dynamic_work) do
    dynamic_work
    |> take_dual_keys(@dynamic_work_fields)
    |> Map.update(:origin, nil, &summarize_dynamic_origin/1)
    |> Map.update(:nodes, [], &summarize_dynamic_nodes/1)
    |> Map.update(:edges, [], &summarize_dynamic_edges/1)
    |> compact()
  end

  defp summarize_dynamic_work(_dynamic_work), do: %{}

  defp summarize_dynamic_work_overlay(overlay) when is_map(overlay) do
    overlay
    |> take_dual_keys(@dynamic_work_overlay_fields)
    |> Map.update(:origin, nil, &summarize_dynamic_origin/1)
    |> compact()
  end

  defp summarize_dynamic_work_overlay(_overlay), do: %{}

  defp summarize_dynamic_origin(origin) when is_map(origin) do
    origin
    |> take_dual_keys(@dynamic_origin_fields)
    |> compact()
  end

  defp summarize_dynamic_origin(_origin), do: nil

  defp summarize_dynamic_nodes(nodes) when is_list(nodes) do
    Enum.map(nodes, &summarize_dynamic_node/1)
  end

  defp summarize_dynamic_nodes(_nodes), do: []

  defp summarize_dynamic_node(node) when is_map(node) do
    node
    |> take_dual_keys(@dynamic_node_fields)
    |> compact()
  end

  defp summarize_dynamic_node(_node), do: %{}

  defp summarize_dynamic_edges(edges) when is_list(edges) do
    Enum.map(edges, &summarize_dynamic_edge/1)
  end

  defp summarize_dynamic_edges(_edges), do: []

  defp summarize_dynamic_edge(edge) when is_map(edge) do
    edge
    |> take_dual_keys(@dynamic_edge_fields)
    |> compact()
  end

  defp summarize_dynamic_edge(_edge), do: %{}

  defp summarize_child_link(child_link) when is_map(child_link) do
    child_link
    |> take_dual_keys(@child_link_fields)
    |> Map.update(:origin, nil, &summarize_dynamic_origin/1)
    |> compact()
  end

  defp summarize_child_link(_child_link), do: %{}

  defp summarize_details(details) when is_map(details) do
    if manual_details?(details) do
      summarize_manual_state(details)
    else
      details
      |> remove_sensitive_nested()
      |> Map.drop([:terminal_error, "terminal_error"])
    end
  end

  defp summarize_details(_details), do: %{}

  defp summarize_evidence(evidence) when is_map(evidence) do
    %{
      snapshot_reason: value(evidence, :snapshot_reason),
      definition_version: value(evidence, :definition_version),
      terminal_status: value(evidence, :terminal_status),
      planned_count: evidence_count(evidence, :planned_count, :planned_runnable_keys),
      applied_count: evidence_count(evidence, :applied_count, :applied_runnable_keys),
      anomaly_count: evidence_count(evidence, :anomaly_count, :anomalies)
    }
  end

  defp summarize_evidence(_evidence), do: %{}

  defp evidence_count(evidence, count_key, collection_key) do
    case value(evidence, count_key) do
      count when is_integer(count) ->
        count

      _missing ->
        evidence
        |> value(collection_key)
        |> collection_count()
    end
  end

  defp collection_count(collection) when is_list(collection), do: length(collection)
  defp collection_count(collection) when is_map(collection), do: map_size(collection)
  defp collection_count(_collection), do: 0

  defp manual_details?(details) when is_map(details) do
    not is_nil(value(details, :step)) and
      (not is_nil(value(details, :kind)) or not is_nil(value(details, :paused_at)))
  end

  defp remove_sensitive_nested(value) when is_list(value) do
    Enum.map(value, &remove_sensitive_nested/1)
  end

  defp remove_sensitive_nested(%_struct{} = value), do: value

  defp remove_sensitive_nested(value) when is_map(value) do
    value
    |> Enum.reject(fn {key, _value} -> sensitive_key?(key) end)
    |> Map.new(fn {key, nested_value} -> {key, remove_sensitive_nested(nested_value)} end)
  end

  defp remove_sensitive_nested(value), do: value

  defp sensitive_key?(key) when is_atom(key) do
    key in [
      :input,
      :output,
      :result,
      :error,
      :payload,
      :metadata,
      :command_history,
      :attempts,
      :idempotency_key,
      :claim_id,
      :owner_id,
      :lease_until,
      :started_at,
      :token,
      :secret
    ]
  end

  defp sensitive_key?(key) when is_binary(key) do
    key in [
      "input",
      "output",
      "result",
      "error",
      "payload",
      "metadata",
      "command_history",
      "attempts",
      "idempotency_key",
      "claim_id",
      "owner_id",
      "lease_until",
      "started_at",
      "token",
      "secret"
    ]
  end

  defp sensitive_key?(_key), do: false

  defp take_dual_keys(map, fields) when is_map(map) do
    Map.new(fields, fn field -> {field, value(map, field)} end)
  end

  defp value(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp compact(map) when is_map(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
