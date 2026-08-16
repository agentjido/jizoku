defmodule Jizoku.Runtime.Journal.Commands.Migration do
  @moduledoc """
  Journal-backed workflow-definition migration at quiescent manual boundaries.

  The command appends its receipt and migration fact atomically on the run
  thread. It never mutates dispatch facts or reinterprets completed results.
  """

  alias Jido.Agent
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.CommandReceipt
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.Migration
  alias Jizoku.Workflow.VersionRegistry

  @run_append_retries 25

  @type migration_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:invalid_workflow_migration, term()}
          | {:invalid_workflow_migration_result, term()}
          | {:workflow_migration_failed, term()}
          | {:migration_key_conflict, map()}
          | {:stale_migration_source, map()}
          | {:unsafe_workflow_migration, [map()]}
          | term()

  @doc """
  Applies one host-owned migration contract to a quiescent paused run.
  """
  @spec migrate(String.t(), keyword(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, migration_error()}
  def migrate(run_id, migration_opts, runtime_opts \\ [])

  def migrate(run_id, migration_opts, runtime_opts)
      when is_binary(run_id) and is_list(migration_opts) and is_list(runtime_opts) do
    with {:ok, run_id} <- Options.uuid_run_id(run_id),
         {:ok, storage} <- Options.storage_from_opts(runtime_opts),
         {:ok, queue} <- Options.queue_from_opts(runtime_opts),
         {:ok, now} <- Options.now_from_opts(runtime_opts),
         {:ok, target_version} <- target_version(migration_opts),
         {:ok, contract} <- migration_contract(migration_opts, target_version),
         {:ok, _agent} <-
           migrate_or_repair(
             storage,
             run_id,
             contract,
             now,
             @run_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def migrate(_run_id, _migration_opts, _runtime_opts) do
    {:error, :invalid_run_id}
  end

  defp migrate_or_repair(_storage, _run_id, _contract, _now, 0) do
    {:error, :conflict}
  end

  defp migrate_or_repair(storage, run_id, contract, now, retries_left) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id) do
      case migration_mode(workflow_agent, contract) do
        :duplicate ->
          {:ok, workflow_agent}

        :append ->
          migrate_workflow_agent(
            storage,
            workflow_agent,
            contract,
            now,
            retries_left
          )

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp migrate_workflow_agent(storage, workflow_agent, contract, now, retries_left) do
    projection = workflow_agent.state.projection

    with :ok <- module_authored_run(storage, workflow_agent.state.run_id),
         :ok <- current_source(projection, contract),
         :ok <- safe_boundary(projection),
         {:ok, workflow, source_definition} <-
           WorkflowDefinitionLoader.load(
             storage,
             workflow_agent.state.run_id,
             workflow_agent.state.workflow
           ),
         :ok <- exact_source_fingerprint(projection, source_definition),
         {:ok, target_definition} <- target_definition(workflow, contract.target_version),
         target_fingerprint = Definition.fingerprint(target_definition),
         {:ok, result} <-
           Migration.apply(
             contract,
             migration_state(projection, contract, target_fingerprint)
           ),
         {:ok, manual_state} <- migrated_manual_state(projection, target_definition, result),
         {:ok, receipt} <- command_receipt(workflow_agent.state.run_id, contract, now),
         {:ok, migration_entry} <-
           migration_entry(
             workflow_agent,
             contract,
             target_fingerprint,
             result.context,
             manual_state,
             now
           ) do
      append_migration(
        storage,
        workflow_agent,
        [receipt, migration_entry],
        contract,
        now,
        retries_left
      )
    end
  end

  defp append_migration(storage, workflow_agent, entries, contract, now, retries_left) do
    case Journal.append_entries(storage, entries,
           expected_rev: workflow_agent.state.thread_rev,
           telemetry_projection: workflow_agent.state.projection
         ) do
      {:ok, _thread} ->
        with {:ok, updated_agent} <-
               WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          _checkpoint_result =
            WorkflowAgent.put_checkpoint(storage, updated_agent, updated_at: now)

          {:ok, updated_agent}
        end

      {:error, :conflict} ->
        migrate_or_repair(
          storage,
          workflow_agent.state.run_id,
          contract,
          now,
          retries_left - 1
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp migration_mode(
         %Agent{agent_module: WorkflowAgent, state: %{projection: projection}},
         contract
       ) do
    case Enum.find(
           Projection.definition_migrations(projection),
           &(Map.get(&1, :migration_key) == contract.key)
         ) do
      nil ->
        :append

      existing ->
        if exact_duplicate?(existing, contract) do
          :duplicate
        else
          {:error,
           {:migration_key_conflict,
            %{
              migration_key: contract.key,
              existing_source_version: Map.get(existing, :source_version),
              existing_target_version: Map.get(existing, :target_version),
              requested_source_version: contract.source_version,
              requested_target_version: contract.target_version
            }}}
        end
    end
  end

  defp exact_duplicate?(existing, contract) do
    Map.get(existing, :source_version) == contract.source_version and
      Map.get(existing, :target_version) == contract.target_version
  end

  defp current_source(projection, contract) do
    if projection.definition_version == contract.source_version do
      :ok
    else
      {:error,
       {:stale_migration_source,
        %{
          expected_version: contract.source_version,
          actual_version: projection.definition_version,
          actual_fingerprint: projection.definition_fingerprint
        }}}
    end
  end

  defp exact_source_fingerprint(projection, source_definition) do
    resolved_fingerprint = Definition.fingerprint(source_definition)

    if resolved_fingerprint == projection.definition_fingerprint do
      :ok
    else
      {:error,
       {:stale_migration_source,
        %{
          expected_fingerprint: projection.definition_fingerprint,
          resolved_fingerprint: resolved_fingerprint
        }}}
    end
  end

  defp safe_boundary(%Projection{} = projection) do
    blockers = migration_blockers(projection)

    case blockers do
      [] -> :ok
      blockers -> {:error, {:unsafe_workflow_migration, blockers}}
    end
  end

  defp migration_blockers(projection) do
    []
    |> add_boundary_blocker(projection)
    |> add_pending_runnable_blocker(projection)
    |> add_state_blocker(:dynamic_work, Projection.dynamic_work(projection))
    |> add_state_blocker(:child_runs, Projection.child_runs(projection))
    |> add_graph_blocker(projection)
    |> add_continuation_blocker(projection)
    |> add_anomaly_blocker(projection)
    |> Enum.reverse()
  end

  defp add_boundary_blocker(blockers, %Projection{status: :paused, manual_state: manual_state})
       when is_map(manual_state) do
    blockers
  end

  defp add_boundary_blocker(blockers, projection) do
    [%{reason: :unsupported_boundary, status: projection.status} | blockers]
  end

  defp add_pending_runnable_blocker(blockers, projection) do
    pending_keys =
      projection
      |> Projection.planned_runnable_keys()
      |> Enum.reject(&MapSet.member?(projection.applied_runnable_keys, &1))

    case pending_keys do
      [] -> blockers
      keys -> [%{reason: :pending_runnables, runnable_keys: keys} | blockers]
    end
  end

  defp add_state_blocker(blockers, _reason, []) do
    blockers
  end

  defp add_state_blocker(blockers, reason, items) do
    [%{reason: reason, count: length(items)} | blockers]
  end

  defp add_graph_blocker(blockers, %{graph: %{version: 0}}) do
    blockers
  end

  defp add_graph_blocker(blockers, %{graph: graph}) do
    [%{reason: :dynamic_graph, version: Map.get(graph, :version)} | blockers]
  end

  defp add_continuation_blocker(blockers, %{continuation_request: nil}) do
    blockers
  end

  defp add_continuation_blocker(blockers, _projection) do
    [%{reason: :continuation_pending} | blockers]
  end

  defp add_anomaly_blocker(blockers, projection) do
    case Projection.anomalies(projection) do
      [] -> blockers
      anomalies -> [%{reason: :projection_anomalies, count: length(anomalies)} | blockers]
    end
  end

  defp migration_state(projection, contract, target_fingerprint) do
    %{
      context:
        projection
        |> Projection.applied_result_context()
        |> Map.merge(projection.context),
      manual_state: projection.manual_state,
      source_version: projection.definition_version,
      source_fingerprint: projection.definition_fingerprint,
      target_version: contract.target_version,
      target_fingerprint: target_fingerprint
    }
  end

  defp migrated_manual_state(projection, target_definition, result) do
    source_state = projection.manual_state

    with {:ok, step_name} <- target_manual_step(target_definition, result.manual_step),
         {:ok, %{module: target_kind}} <- Definition.step(target_definition, step_name),
         :ok <- same_manual_kind(source_state.kind, target_kind) do
      {:ok,
       source_state
       |> Map.put(:step, Definition.serialize_step(step_name))
       |> Map.put(:metadata, migrated_manual_metadata(source_state))}
    else
      {:error, _reason} = error -> error
    end
  end

  defp target_manual_step(definition, step) when is_atom(step) do
    case Definition.step(definition, step) do
      {:ok, _definition_step} -> {:ok, step}
      {:error, _reason} -> {:error, {:invalid_workflow_migration_result, :manual_step}}
    end
  end

  defp target_manual_step(definition, step) when is_binary(step) do
    case Definition.deserialize_step(definition, step) do
      step_name when is_atom(step_name) -> {:ok, step_name}
      _unknown -> {:error, {:invalid_workflow_migration_result, :manual_step}}
    end
  end

  defp same_manual_kind("pause", :pause) do
    :ok
  end

  defp same_manual_kind("approval", :approval) do
    :ok
  end

  defp same_manual_kind(_source_kind, _target_kind) do
    {:error, {:invalid_workflow_migration_result, :manual_step_kind}}
  end

  defp migrated_manual_metadata(%{kind: "pause", metadata: metadata}) when is_map(metadata) do
    case Jizoku.MapField.get(metadata, :output) do
      output when is_map(output) -> %{output: output}
      _missing -> %{}
    end
  end

  defp migrated_manual_metadata(_manual_state) do
    %{}
  end

  defp target_definition(workflow, target_version) do
    with {:ok, registry} <- configured_registry(),
         {:ok, ^workflow, definition} <-
           VersionRegistry.fetch(workflow, target_version, registry) do
      {:ok, definition}
    end
  end

  defp configured_registry do
    case Application.fetch_env(:jizoku, :workflow_versions) do
      {:ok, registry} -> {:ok, registry}
      :error -> {:error, {:invalid_option, {:workflow_versions, :required}}}
    end
  end

  defp module_authored_run(storage, run_id) do
    case WorkflowDefinitionLoader.runtime_spec_run?(storage, run_id) do
      {:ok, false} -> :ok
      {:ok, true} -> {:error, {:unsafe_workflow_migration, [%{reason: :runtime_spec}]}}
      {:error, _reason} = error -> error
    end
  end

  defp migration_entry(workflow_agent, contract, target_fingerprint, context, manual_state, now) do
    source_state = workflow_agent.state.projection.manual_state

    DispatchProtocol.new_entry(:run_definition_migrated, %{
      run_id: workflow_agent.state.run_id,
      migration_key: contract.key,
      source_version: contract.source_version,
      source_fingerprint: workflow_agent.state.projection.definition_fingerprint,
      target_version: contract.target_version,
      target_fingerprint: target_fingerprint,
      source_manual_step: source_state.step,
      target_manual_step: manual_state.step,
      context: context,
      manual_state: manual_state,
      occurred_at: now
    })
  end

  defp command_receipt(run_id, contract, now) do
    CommandReceipt.new(
      :migrate_run,
      %{
        run_id: run_id,
        payload: %{
          run_id: run_id,
          migration_key: contract.key,
          source_version: contract.source_version,
          target_version: contract.target_version
        },
        metadata: %{},
        idempotency_key: contract.key
      },
      now
    )
  end

  defp target_version(opts) do
    case Keyword.fetch(opts, :to) do
      {:ok, version} when is_binary(version) and version != "" -> {:ok, version}
      _missing_or_invalid -> {:error, {:invalid_option, {:to, :invalid}}}
    end
  end

  defp migration_contract(opts, target_version) do
    with {:ok, module} <- Keyword.fetch(opts, :migration),
         {:ok, contract} <- Migration.contract(module),
         true <- contract.target_version == target_version do
      {:ok, contract}
    else
      :error -> {:error, {:invalid_option, {:migration, :required}}}
      false -> {:error, {:invalid_option, {:to, :migration_mismatch}}}
      {:error, _reason} = error -> error
    end
  end

  defp rebuild_workflow_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end
end
