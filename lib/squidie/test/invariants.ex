defmodule Squidie.Test.Invariants do
  @moduledoc false

  alias Squidie.ReadModel.Inspection.Snapshot

  @report_version 1
  @terminal_statuses [:cancelled, :completed, :continued, :failed]
  @key_collections [
    :planned_runnable_keys,
    :applied_runnable_keys,
    :pending_dispatches,
    :pending_results
  ]

  @type violation_code ::
          :duplicate_runnable_key
          | :malformed_runnable_key
          | :pending_and_applied
          | :pending_in_multiple_views
          | :projection_anomaly
          | :terminal_state_incoherent
          | :unknown_runnable
  @type violation :: %{
          required(:code) => violation_code(),
          required(:details) => map()
        }
  @type report :: %{
          required(:version) => pos_integer(),
          required(:run_id) => String.t(),
          required(:partition) => String.t() | nil,
          required(:queue) => String.t(),
          required(:thread_revisions) => %{run: non_neg_integer(), dispatch: non_neg_integer()},
          required(:violations) => [violation()]
        }

  @doc false
  @spec check(Snapshot.t()) :: :ok | {:error, {:invariant_violations, report()}}
  def check(%Snapshot{} = snapshot) do
    violations =
      anomaly_violations(snapshot) ++
        terminal_violations(snapshot) ++
        duplicate_key_violations(snapshot) ++
        malformed_key_violations(snapshot) ++
        unknown_key_violations(snapshot) ++
        conflicting_key_violations(snapshot)

    case violations do
      [] -> :ok
      violations -> {:error, {:invariant_violations, report(snapshot, violations)}}
    end
  end

  defp anomaly_violations(%Snapshot{anomalies: []}) do
    []
  end

  defp anomaly_violations(%Snapshot{anomalies: anomalies}) when is_list(anomalies) do
    [
      violation(:projection_anomaly, %{
        count: length(anomalies),
        reasons: safe_anomaly_values(anomalies, :reason),
        sources: safe_anomaly_values(anomalies, :source)
      })
    ]
  end

  defp anomaly_violations(%Snapshot{}) do
    [violation(:projection_anomaly, %{count: :malformed, reasons: [], sources: []})]
  end

  defp terminal_violations(%Snapshot{} = snapshot) do
    if terminal_coherent?(snapshot) do
      []
    else
      [
        violation(:terminal_state_incoherent, %{
          active_fields: terminal_active_fields(snapshot),
          reason: safe_classification(snapshot.reason),
          status: safe_classification(snapshot.status),
          terminal?: safe_boolean(snapshot.terminal?),
          terminal_at?: match?(%DateTime{}, snapshot.terminal_at),
          terminal_status: safe_classification(snapshot.terminal_status)
        })
      ]
    end
  end

  defp terminal_coherent?(%Snapshot{terminal?: true} = snapshot) do
    snapshot.terminal_status in @terminal_statuses and
      snapshot.status == snapshot.terminal_status and
      snapshot.reason == :terminal and
      match?(%DateTime{}, snapshot.terminal_at) and
      terminal_active_fields(snapshot) == []
  end

  defp terminal_coherent?(%Snapshot{terminal?: false} = snapshot) do
    is_nil(snapshot.terminal_status) and
      is_nil(snapshot.terminal_at) and
      snapshot.status not in @terminal_statuses and
      snapshot.reason != :terminal
  end

  defp terminal_coherent?(%Snapshot{}) do
    false
  end

  defp terminal_active_fields(%Snapshot{} = snapshot) do
    [
      manual_state: snapshot.manual_state,
      deadline: snapshot.deadline,
      next_visible_at: snapshot.next_visible_at,
      pending_dispatches: snapshot.pending_dispatches,
      pending_results: snapshot.pending_results,
      visible_attempts: snapshot.visible_attempts,
      scheduled_attempts: snapshot.scheduled_attempts,
      expired_claims: snapshot.expired_claims
    ]
    |> Enum.reject(fn {_field, value} -> value in [nil, []] end)
    |> Enum.map(fn {field, _value} -> field end)
  end

  defp duplicate_key_violations(%Snapshot{} = snapshot) do
    Enum.flat_map(@key_collections, fn collection ->
      {keys, _malformed_count} = collection_keys(snapshot, collection)
      duplicates = duplicate_keys(keys)

      if duplicates == [] do
        []
      else
        [violation(:duplicate_runnable_key, %{collection: collection, runnable_keys: duplicates})]
      end
    end)
  end

  defp malformed_key_violations(%Snapshot{} = snapshot) do
    Enum.flat_map(@key_collections, fn collection ->
      {_keys, malformed_count} = collection_keys(snapshot, collection)

      if malformed_count == 0 do
        []
      else
        [violation(:malformed_runnable_key, %{collection: collection, count: malformed_count})]
      end
    end)
  end

  defp unknown_key_violations(%Snapshot{} = snapshot) do
    planned = key_set(snapshot, :planned_runnable_keys)

    Enum.flat_map(
      [:applied_runnable_keys, :pending_dispatches, :pending_results],
      fn collection ->
        unknown = sorted_set(MapSet.difference(key_set(snapshot, collection), planned))

        if unknown == [] do
          []
        else
          [violation(:unknown_runnable, %{collection: collection, runnable_keys: unknown})]
        end
      end
    )
  end

  defp conflicting_key_violations(%Snapshot{} = snapshot) do
    applied = key_set(snapshot, :applied_runnable_keys)
    pending_dispatch = key_set(snapshot, :pending_dispatches)
    pending_results = key_set(snapshot, :pending_results)

    pending_applied =
      Enum.flat_map([:pending_dispatches, :pending_results], fn collection ->
        conflicts = sorted_set(MapSet.intersection(key_set(snapshot, collection), applied))

        if conflicts == [] do
          []
        else
          [violation(:pending_and_applied, %{collection: collection, runnable_keys: conflicts})]
        end
      end)

    pending_overlap = sorted_set(MapSet.intersection(pending_dispatch, pending_results))

    pending_overlap_violations =
      if pending_overlap == [] do
        []
      else
        [violation(:pending_in_multiple_views, %{runnable_keys: pending_overlap})]
      end

    pending_applied ++ pending_overlap_violations
  end

  defp collection_keys(%Snapshot{} = snapshot, collection)
       when collection in [:planned_runnable_keys, :applied_runnable_keys] do
    case Map.fetch!(snapshot, collection) do
      values when is_list(values) ->
        values
        |> Enum.reduce({[], 0}, fn
          key, {keys, malformed} when is_binary(key) and byte_size(key) > 0 ->
            {[key | keys], malformed}

          _invalid, {keys, malformed} ->
            {keys, malformed + 1}
        end)
        |> reverse_keys()

      _malformed ->
        {[], 1}
    end
  end

  defp collection_keys(%Snapshot{} = snapshot, collection)
       when collection in [:pending_dispatches, :pending_results] do
    case Map.fetch!(snapshot, collection) do
      values when is_list(values) ->
        values
        |> Enum.reduce({[], 0}, &collect_runnable_key/2)
        |> reverse_keys()

      _malformed ->
        {[], 1}
    end
  end

  defp reverse_keys({keys, malformed_count}) do
    {Enum.reverse(keys), malformed_count}
  end

  defp collect_runnable_key(item, {keys, malformed}) do
    case runnable_key(item) do
      key when is_binary(key) and byte_size(key) > 0 -> {[key | keys], malformed}
      _invalid -> {keys, malformed + 1}
    end
  end

  defp runnable_key(%{runnable_key: key}) do
    key
  end

  defp runnable_key(%{"runnable_key" => key}) do
    key
  end

  defp runnable_key(_item) do
    nil
  end

  defp key_set(snapshot, collection) do
    {keys, _malformed_count} = collection_keys(snapshot, collection)
    MapSet.new(keys)
  end

  defp duplicate_keys(keys) do
    {_seen, duplicates} =
      Enum.reduce(keys, {MapSet.new(), MapSet.new()}, fn key, {seen, duplicates} ->
        if MapSet.member?(seen, key) do
          {seen, MapSet.put(duplicates, key)}
        else
          {MapSet.put(seen, key), duplicates}
        end
      end)

    sorted_set(duplicates)
  end

  defp sorted_set(set) do
    set
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp safe_anomaly_values(anomalies, field) do
    anomalies
    |> Enum.flat_map(fn
      anomaly when is_map(anomaly) ->
        case Map.get(anomaly, field) do
          value when is_atom(value) -> [value]
          _unsafe -> []
        end

      _malformed ->
        []
    end)
    |> Enum.uniq()
    |> Enum.sort_by(&to_string/1)
  end

  defp safe_classification(nil) do
    nil
  end

  defp safe_classification(value) when is_atom(value) do
    value
  end

  defp safe_classification(_value) do
    :malformed
  end

  defp safe_boolean(value) when is_boolean(value) do
    value
  end

  defp safe_boolean(_value) do
    :malformed
  end

  defp violation(code, details) do
    %{code: code, details: details}
  end

  defp report(%Snapshot{} = snapshot, violations) do
    %{
      version: @report_version,
      run_id: snapshot.run_id,
      partition: snapshot.partition,
      queue: snapshot.queue,
      thread_revisions: snapshot.thread_revisions,
      violations: violations
    }
  end
end
