defmodule SquidMesh.ReadModel.Visibility do
  @moduledoc """
  Actor-scoped read-model redaction helpers.

  Visibility is a host-owned authorization boundary. Squid Mesh keeps durable
  history immutable and returns factual read models; host applications can call
  this module at their HTTP, LiveView, CLI, or dashboard boundary to derive a
  less-sensitive view for a specific actor.
  """

  alias SquidMesh.ReadModel.Explanation.Diagnostic
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.ReadModel.Listing.Summary
  alias SquidMesh.Runs.GraphInspection
  alias SquidMesh.Runs.GraphInspection.Node

  @visibility_scopes [:external, :operator, :auditor]
  @manual_state_fields [:step, :kind, :status, :reason, :paused_at, :requested_at, :resolved_at]
  @run_summary_fields [
    :run_id,
    :workflow,
    :definition_version,
    :status,
    :terminal?,
    :terminal_status
  ]
  @runnable_fields [:runnable_key, :key, :step, :status, :attempt_number, :visible_at]
  @attempt_fields [
    :runnable_key,
    :status,
    :attempt_number,
    :step,
    :visible_at,
    :wakeup_emitted?,
    :applied?
  ]
  @anomaly_fields [:source, :reason, :entry_type, :run_id, :step, :runnable_key]

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
        parent_run: summarize_run(snapshot.parent_run),
        child_runs: Enum.map(snapshot.child_runs, &summarize_run/1),
        command_history: [],
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

  defp redact_view(view, _scope) when is_map(view), do: remove_sensitive_nested(view)

  defp redact_view(view, scope) when is_list(view), do: Enum.map(view, &redact_view(&1, scope))

  defp redact_view(view, _scope), do: view

  defp summarize_node(%Node{} = node) do
    %Node{
      node
      | input: nil,
        output: nil,
        error: nil,
        manual_state: summarize_manual_state(node.manual_state),
        attempts: []
    }
  end

  defp summarize_manual_state(nil), do: nil

  defp summarize_manual_state(manual_state) when is_map(manual_state) do
    manual_state
    |> take_dual_keys(@manual_state_fields)
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
    |> compact()
  end

  defp summarize_runnable(_runnable), do: %{}

  defp summarize_attempt(attempt) when is_map(attempt) do
    attempt
    |> take_dual_keys(@attempt_fields)
    |> compact()
  end

  defp summarize_attempt(_attempt), do: %{}

  defp summarize_anomaly(anomaly) when is_map(anomaly) do
    anomaly
    |> take_dual_keys(@anomaly_fields)
    |> compact()
  end

  defp summarize_anomaly(_anomaly), do: %{}

  defp summarize_details(details) when is_map(details) do
    if manual_details?(details) do
      summarize_manual_state(details)
    else
      remove_sensitive_nested(details)
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
