# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.DispatchProtocol do
  @moduledoc """
  Defines the durable dispatch journal contract.

  The protocol separates durable facts from live effects:

  - run-thread entries record workflow lifecycle facts
  - dispatch-thread entries record runnable intent, claims, leases, heartbeats,
    completions, failures, retries, and live wakeups
  - run-index and run-catalog entries support rebuildable lookup projections

  A live wakeup or action execution is valid only after the runnable intent is
  appended. Claims are fenced by `claim_id` and `claim_token_hash`;
  completions, failures, and heartbeats from stale claim owners are ignored by
  the projection and surfaced as anomalies.
  """

  alias Jizoku.Runtime.DispatchProtocol.Entry
  alias Jizoku.Runtime.Trace

  @graph_operation_field_names %{
    "kind" => :kind,
    "id" => :id,
    "action" => :action,
    "input" => :input,
    "queue" => :queue,
    "from" => :from,
    "to" => :to
  }

  @type entry_type ::
          :run_signal_received
          | :run_started
          | :run_archived
          | :run_unarchived
          | :search_attributes_updated
          | :run_definition_migrated
          | :runnables_planned
          | :runnable_applied
          | :child_run_started
          | :run_continuation_requested
          | :run_continued_from
          | :dynamic_work_recorded
          | :dynamic_graph_mutated
          | :manual_step_paused
          | :manual_step_resolved
          | :external_event_wait_opened
          | :external_event_received
          | :external_event_wait_timeout_selected
          | :external_event_wait_resolved
          | :run_terminal
          | :run_indexed
          | :run_cataloged
          | :run_queued
          | :run_continuation_fenced
          | :run_continuation_repaired
          | :run_continuation_aborted
          | :jido_signal_enqueued
          | :jido_signal_delivery_acknowledged
          | :jido_signal_resolved
          | :attempt_scheduled
          | :attempt_claimed
          | :attempt_heartbeat
          | :attempt_completed
          | :attempt_failed
          | :live_wakeup_emitted

  @manual_entry_types [:manual_step_paused, :manual_step_resolved]
  @event_wait_entry_types [
    :external_event_wait_opened,
    :external_event_received,
    :external_event_wait_timeout_selected,
    :external_event_wait_resolved
  ]

  @run_entry_types [
    :run_signal_received,
    :run_started,
    :run_archived,
    :run_unarchived,
    :search_attributes_updated,
    :run_definition_migrated,
    :runnables_planned,
    :runnable_applied,
    :child_run_started,
    :run_continuation_requested,
    :run_continued_from,
    :dynamic_work_recorded,
    :dynamic_graph_mutated,
    :jido_signal_enqueued,
    :jido_signal_delivery_acknowledged,
    :manual_step_paused,
    :manual_step_resolved,
    :external_event_wait_opened,
    :external_event_received,
    :external_event_wait_timeout_selected,
    :external_event_wait_resolved,
    :run_terminal
  ]

  @dispatch_entry_types [
    :run_queued,
    :run_continuation_fenced,
    :run_continuation_repaired,
    :run_continuation_aborted,
    :attempt_scheduled,
    :attempt_claimed,
    :attempt_heartbeat,
    :attempt_completed,
    :attempt_failed,
    :live_wakeup_emitted
  ]

  @run_index_entry_types [:run_indexed]
  @run_catalog_entry_types [:run_cataloged]
  @jido_signal_entry_types [:jido_signal_resolved]

  @required_fields %{
    run_signal_received: [:run_id, :signal_type, :payload, :metadata, :occurred_at],
    run_started: [:run_id, :workflow, :occurred_at],
    run_archived: [:run_id, :reason, :occurred_at],
    run_unarchived: [:run_id, :occurred_at],
    search_attributes_updated: [
      :run_id,
      :changes,
      :fingerprint,
      :idempotency_key,
      :occurred_at
    ],
    run_definition_migrated: [
      :run_id,
      :migration_key,
      :source_version,
      :source_fingerprint,
      :target_version,
      :target_fingerprint,
      :context,
      :manual_state,
      :occurred_at
    ],
    runnables_planned: [:run_id, :runnables, :occurred_at],
    runnable_applied: [:run_id, :runnable_key, :occurred_at],
    child_run_started: [
      :run_id,
      :child_run_id,
      :child_workflow,
      :child_trigger,
      :child_key,
      :origin,
      :occurred_at
    ],
    run_continuation_requested: [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint,
      :occurred_at
    ],
    run_continued_from: [
      :run_id,
      :predecessor_run_id,
      :continuation_key,
      :occurred_at
    ],
    dynamic_work_recorded: [:run_id, :dynamic_key, :origin, :nodes, :occurred_at],
    dynamic_graph_mutated: [
      :run_id,
      :mutation_id,
      :expected_version,
      :result_version,
      :origin,
      :occurred_at
    ],
    jido_signal_enqueued: [:run_id, :signal_id, :resolved_signal, :occurred_at],
    jido_signal_delivery_acknowledged: [:run_id, :signal_id, :occurred_at],
    manual_step_paused: [:run_id, :step, :kind, :occurred_at],
    manual_step_resolved: [:run_id, :step, :action, :occurred_at],
    external_event_wait_opened: [
      :run_id,
      :wait_id,
      :step,
      :event,
      :correlation,
      :opened_at,
      :occurred_at
    ],
    external_event_received: [
      :run_id,
      :wait_id,
      :event,
      :correlation,
      :payload,
      :idempotency_key,
      :occurred_at
    ],
    external_event_wait_timeout_selected: [
      :run_id,
      :wait_id,
      :timeout_runnable_key,
      :step,
      :event,
      :correlation,
      :target,
      :selected_at,
      :occurred_at
    ],
    external_event_wait_resolved: [
      :run_id,
      :wait_id,
      :step,
      :event,
      :correlation,
      :action,
      :result,
      :occurred_at
    ],
    run_terminal: [:run_id, :status, :occurred_at],
    run_indexed: [:run_id, :workflow, :queue, :occurred_at],
    run_cataloged: [:run_id, :workflow, :queue, :occurred_at],
    run_queued: [:run_id, :queue, :occurred_at],
    run_continuation_fenced: [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint,
      :queue,
      :trace,
      :occurred_at
    ],
    run_continuation_repaired: [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint,
      :queue,
      :trace,
      :occurred_at
    ],
    run_continuation_aborted: [
      :run_id,
      :successor_run_id,
      :continuation_key,
      :workflow,
      :trigger,
      :input,
      :definition,
      :definition_fingerprint,
      :queue,
      :trace,
      :abort_reason,
      :occurred_at
    ],
    jido_signal_resolved: [
      :event_key,
      :signal_id,
      :source,
      :envelope_fingerprint,
      :resolved_signal,
      :queue,
      :occurred_at
    ],
    attempt_scheduled: [
      :run_id,
      :runnable_key,
      :idempotency_key,
      :attempt_number,
      :queue,
      :step,
      :input,
      :visible_at,
      :occurred_at
    ],
    attempt_claimed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :owner_id,
      :queue,
      :lease_until,
      :occurred_at
    ],
    attempt_heartbeat: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :lease_until,
      :occurred_at
    ],
    attempt_completed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :result,
      :occurred_at
    ],
    attempt_failed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :error,
      :occurred_at
    ],
    live_wakeup_emitted: [:run_id, :runnable_key, :queue, :occurred_at]
  }

  @entry_types @run_entry_types ++
                 @dispatch_entry_types ++
                 @jido_signal_entry_types ++
                 @run_index_entry_types ++
                 @run_catalog_entry_types

  @doc false
  @spec new_entry(entry_type(), map() | keyword()) ::
          {:ok, Entry.t()} | {:error, {:unknown_entry_type, atom()} | {:missing_fields, [atom()]}}
  def new_entry(type, attrs) when is_atom(type) and type in @entry_types do
    attrs =
      attrs
      |> Map.new()
      |> normalize_attrs(type)

    with :ok <- require_fields(type, attrs) do
      {:ok,
       %Entry{
         type: type,
         thread: thread_for(type, attrs),
         data: attrs,
         occurred_at: attrs.occurred_at
       }}
    end
  end

  def new_entry(type, _attrs) when is_atom(type), do: {:error, {:unknown_entry_type, type}}

  defp normalize_attrs(attrs, type)
       when type in [
              :run_continuation_fenced,
              :run_continuation_repaired,
              :run_continuation_aborted
            ] do
    attrs
    |> Map.put_new(:definition_version, nil)
    |> Map.update(:continuation_key, nil, &normalize_thread_id/1)
    |> Map.update(:workflow, nil, &normalize_workflow/1)
    |> Map.update(:trigger, nil, &normalize_thread_id/1)
    |> Map.update(:queue, "default", &normalize_queue/1)
    |> Map.update(:trace, nil, &normalize_trace/1)
  end

  defp normalize_attrs(attrs, type) when type in @dispatch_entry_types do
    Map.update(attrs, :queue, "default", &normalize_queue/1)
  end

  defp normalize_attrs(attrs, type) when type in @manual_entry_types do
    attrs
    |> Map.update(:step, nil, &normalize_manual_value/1)
    |> Map.update(:kind, nil, &normalize_manual_value/1)
    |> Map.update(:action, nil, &normalize_manual_value/1)
    |> Map.update(:metadata, %{}, &redact_metadata/1)
    |> Map.put_new(:result, %{})
  end

  defp normalize_attrs(attrs, type) when type in @event_wait_entry_types do
    attrs
    |> Map.update(:wait_id, nil, &normalize_thread_id/1)
    |> Map.update(:timeout_runnable_key, nil, &normalize_thread_id/1)
    |> Map.update(:step, nil, &normalize_thread_id/1)
    |> Map.update(:event, nil, &normalize_thread_id/1)
    |> Map.update(:correlation, nil, &normalize_thread_id/1)
    |> Map.update(:action, nil, &normalize_thread_id/1)
    |> Map.update(:target, nil, &normalize_thread_id/1)
  end

  defp normalize_attrs(attrs, :run_signal_received) do
    attrs
    |> Map.update(:signal_type, nil, &normalize_thread_id/1)
    |> Map.update(:metadata, %{}, &redact_metadata/1)
  end

  defp normalize_attrs(attrs, :child_run_started) do
    attrs
    |> Map.update(:child_workflow, nil, &normalize_workflow/1)
    |> Map.update(:child_trigger, nil, &normalize_thread_id/1)
    |> Map.update(:child_key, nil, &normalize_thread_id/1)
    |> Map.update(:origin, nil, &normalize_origin/1)
    |> Map.put_new(:metadata, %{})
  end

  defp normalize_attrs(attrs, :run_continuation_requested) do
    attrs
    |> Map.put_new(:definition_version, nil)
    |> Map.update(:continuation_key, nil, &normalize_thread_id/1)
    |> Map.update(:workflow, nil, &normalize_workflow/1)
    |> Map.update(:trigger, nil, &normalize_thread_id/1)
  end

  defp normalize_attrs(attrs, :run_continued_from) do
    Map.update(attrs, :continuation_key, nil, &normalize_thread_id/1)
  end

  defp normalize_attrs(attrs, :dynamic_work_recorded) do
    attrs
    |> Map.update(:dynamic_key, nil, &normalize_thread_id/1)
    |> Map.update(:origin, nil, &normalize_origin/1)
    |> Map.update(:nodes, [], &normalize_dynamic_nodes/1)
    |> Map.update(:edges, [], &normalize_dynamic_edges/1)
    |> Map.update(:metadata, %{}, &redact_metadata/1)
    |> Map.update(:reason, nil, &normalize_dynamic_value/1)
    |> Map.update(:status, :recorded, &normalize_dynamic_value/1)
  end

  defp normalize_attrs(attrs, :dynamic_graph_mutated) do
    attrs
    |> Map.update(:mutation_id, nil, &normalize_graph_identifier/1)
    |> Map.update(:origin, nil, &normalize_graph_identifier/1)
    |> Map.update(:additions, [], &normalize_graph_operations/1)
    |> Map.update(:removals, [], &normalize_graph_operations/1)
    |> Map.update(
      :runnable_intent_fingerprints,
      %{},
      &normalize_runnable_intent_fingerprints/1
    )
  end

  defp normalize_attrs(attrs, type) when type in @run_index_entry_types do
    attrs
    |> Map.update(:workflow, nil, &normalize_workflow/1)
    |> Map.update(:queue, nil, &normalize_queue/1)
  end

  defp normalize_attrs(attrs, type) when type in @run_catalog_entry_types do
    attrs
    |> Map.update(:workflow, nil, &normalize_workflow/1)
    |> Map.update(:queue, nil, &normalize_queue/1)
  end

  defp normalize_attrs(attrs, _type), do: attrs

  defp normalize_trace(trace) do
    case Trace.normalize(trace) do
      {:ok, normalized} -> normalized
      {:error, _reason} -> trace
    end
  end

  defp require_fields(type, attrs) do
    missing_fields =
      type
      |> then(&Map.fetch!(@required_fields, &1))
      |> Enum.reject(&(Map.has_key?(attrs, &1) and not is_nil(Map.fetch!(attrs, &1))))

    case missing_fields do
      [] -> :ok
      missing -> {:error, {:missing_fields, missing}}
    end
  end

  defp thread_for(type, attrs) when type in @run_entry_types, do: {:run, attrs.run_id}

  defp thread_for(type, attrs) when type in @run_index_entry_types,
    do: {:run_index, attrs.workflow}

  defp thread_for(type, _attrs) when type in @run_catalog_entry_types,
    do: {:run_catalog, "all"}

  defp thread_for(type, attrs) when type in @jido_signal_entry_types,
    do: {:jido_signal, attrs.event_key}

  defp thread_for(type, attrs) when type in @dispatch_entry_types do
    {:dispatch, attrs.queue}
  end

  defp normalize_workflow(nil), do: nil

  defp normalize_workflow(workflow) when is_atom(workflow),
    do: Jizoku.Workflow.Definition.serialize_workflow(workflow)

  defp normalize_workflow(workflow), do: normalize_thread_id(workflow)

  defp normalize_queue(nil), do: nil
  defp normalize_queue(queue), do: normalize_thread_id(queue)

  defp normalize_manual_value(nil), do: nil
  defp normalize_manual_value(value), do: normalize_thread_id(value)

  defp normalize_origin(nil), do: nil

  defp normalize_origin(origin) when is_map(origin) do
    origin
    |> Map.new()
    |> Map.update(:step, nil, &normalize_manual_value/1)
  end

  defp normalize_origin(origin), do: origin

  defp normalize_dynamic_nodes(nodes) when is_list(nodes) do
    Enum.map(nodes, &normalize_dynamic_node/1)
  end

  defp normalize_dynamic_nodes(_nodes), do: []

  defp normalize_dynamic_node(node) when is_map(node) do
    node
    |> Map.new()
    |> Map.update(:id, nil, &normalize_thread_id/1)
    |> Map.update(:action, nil, &normalize_dynamic_value/1)
    |> Map.update(:status, :recorded, &normalize_dynamic_value/1)
    |> Map.update(:metadata, %{}, &redact_metadata/1)
  end

  defp normalize_dynamic_node(node), do: node

  defp normalize_dynamic_edges(edges) when is_list(edges) do
    Enum.map(edges, &normalize_dynamic_edge/1)
  end

  defp normalize_dynamic_edges(_edges), do: []

  defp normalize_dynamic_edge(edge) when is_map(edge) do
    edge
    |> Map.new()
    |> Map.update(:id, nil, &normalize_thread_id/1)
    |> Map.update(:from, nil, &normalize_thread_id/1)
    |> Map.update(:to, nil, &normalize_thread_id/1)
    |> Map.update(:type, :dynamic, &normalize_dynamic_value/1)
    |> Map.update(:status, :pending, &normalize_dynamic_value/1)
  end

  defp normalize_dynamic_edge(edge), do: edge

  defp normalize_dynamic_value(nil), do: nil
  defp normalize_dynamic_value(value) when is_atom(value), do: value
  defp normalize_dynamic_value(value), do: normalize_thread_id(value)

  defp normalize_graph_operations(operations) when is_list(operations) do
    Enum.map(operations, &normalize_graph_operation/1)
  end

  defp normalize_graph_operations(operations) do
    operations
  end

  defp normalize_graph_operation(operation) when is_map(operation) do
    operation
    |> Map.new(fn {key, value} ->
      {normalize_graph_operation_key(key), value}
    end)
    |> normalize_graph_operation_field(:kind, &normalize_graph_operation_kind/1)
    |> normalize_graph_operation_field(:id, &normalize_graph_identifier/1)
    |> normalize_graph_operation_field(:action, &normalize_graph_identifier/1)
    |> normalize_graph_operation_field(:queue, &normalize_graph_identifier/1)
    |> normalize_graph_operation_field(:from, &normalize_graph_identifier/1)
    |> normalize_graph_operation_field(:to, &normalize_graph_identifier/1)
  end

  defp normalize_graph_operation(operation) do
    operation
  end

  defp normalize_graph_operation_key(key) when is_binary(key) do
    Map.get(@graph_operation_field_names, key, key)
  end

  defp normalize_graph_operation_key(key) do
    key
  end

  defp normalize_graph_operation_field(operation, key, normalizer) do
    case Map.fetch(operation, key) do
      {:ok, value} -> Map.put(operation, key, normalizer.(value))
      :error -> operation
    end
  end

  defp normalize_graph_operation_kind(kind) when kind in [:node, "node"] do
    :node
  end

  defp normalize_graph_operation_kind(kind) when kind in [:edge, "edge"] do
    :edge
  end

  defp normalize_graph_operation_kind(kind) do
    kind
  end

  defp normalize_graph_identifier(nil) do
    nil
  end

  defp normalize_graph_identifier(identifier) do
    normalize_thread_id(identifier)
  end

  defp normalize_runnable_intent_fingerprints(fingerprints) when is_map(fingerprints) do
    Map.new(fingerprints, fn {node_id, fingerprint} ->
      {normalize_graph_identifier(node_id), normalize_graph_identifier(fingerprint)}
    end)
  end

  defp normalize_runnable_intent_fingerprints(fingerprints) do
    fingerprints
  end

  defp redact_metadata(metadata) when is_map(metadata) do
    Map.new(metadata, fn {key, value} ->
      if sensitive_metadata_key?(key) do
        {key, "[REDACTED]"}
      else
        {key, redact_metadata_value(value)}
      end
    end)
  end

  defp redact_metadata(_metadata), do: %{}

  defp redact_metadata_value(value) when is_map(value), do: redact_metadata(value)

  defp redact_metadata_value(value) when is_list(value) do
    Enum.map(value, fn
      item when is_map(item) -> redact_metadata(item)
      item -> item
    end)
  end

  defp redact_metadata_value(value), do: value

  defp sensitive_metadata_key?(key) do
    key
    |> to_string()
    |> String.downcase()
    |> then(&(&1 in sensitive_metadata_keys()))
  end

  defp sensitive_metadata_keys do
    [
      "access_token",
      "api_key",
      "authorization",
      "claim_token",
      "credential",
      "password",
      "private_key",
      "refresh_token",
      "secret",
      "token"
    ]
  end

  defp normalize_thread_id(id) when is_binary(id), do: id
  defp normalize_thread_id(id), do: to_string(id)
end
