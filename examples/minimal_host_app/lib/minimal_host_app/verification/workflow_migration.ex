defmodule MinimalHostApp.Verification.WorkflowMigration do
  @moduledoc "Exercises an explicit v1-to-v2 migration at a paused durable boundary."

  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto, as: JournalStorage
  alias Jizoku.Runs.GraphInspection
  alias Jizoku.Workflow.Definition
  alias MinimalHostApp.Migrations.RoutingV1ToV2
  alias MinimalHostApp.Repo
  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.Workflows.MigratedRouting

  @queue "minimal-host-workflow-migration"
  @storage {JournalStorage, repo: Repo}

  @spec run!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run! do
    RuntimeHarness.ensure_runtime_started()

    run_id = Ecto.UUID.generate()
    paused_at = DateTime.utc_now()
    seed_paused_run!(run_id, paused_at)

    {:ok, migrated} =
      Jizoku.migrate_run(run_id,
        to: "v2",
        migration: RoutingV1ToV2,
        journal_storage: @storage,
        queue: @queue,
        now: paused_at
      )

    unless migrated.status == :paused and migrated.definition_version == "v2" do
      raise "workflow migration did not persist the v2 paused state"
    end

    resumed_at = DateTime.add(paused_at, 1, :second)

    {:ok, %{status: :running}} =
      Jizoku.resume(
        run_id,
        %{actor: "minimal-host-migration"},
        runtime: :journal,
        journal_storage: @storage,
        queue: @queue,
        idempotency_key: "resume-#{run_id}",
        now: resumed_at
      )

    case Jizoku.execute_next(
           journal_storage: @storage,
           queue: @queue,
           owner_id: "minimal-host-migration-worker",
           now: resumed_at
         ) do
      {:ok, %{status: :completed} = snapshot} -> verify_diagnostics!(snapshot.run_id)
      other -> raise "workflow migration execution failed: #{inspect(other)}"
    end
  end

  defp verify_diagnostics!(run_id) do
    opts = [journal_storage: @storage, queue: @queue]
    target_fingerprint = Definition.fingerprint(MigratedRouting.workflow_definition())

    {:ok, inspected} = Jizoku.inspect_run(run_id, opts)
    {:ok, graph} = Jizoku.inspect_run_graph(run_id, opts)
    {:ok, timeline} = Jizoku.inspect_run_timeline(run_id, opts)
    {:ok, explanation} = Jizoku.explain_run(run_id, opts)
    graph = GraphInspection.to_map(graph)

    migrated? =
      Enum.any?(timeline.events, fn event ->
        event.type == :workflow_definition_migrated and
          event.details.migration_key == "minimal-host-routing-v1-to-v2"
      end)

    valid? =
      inspected.definition_fingerprint == target_fingerprint and
        inspected.definition_resolution.status == :resolved and
        graph.definition_migrations == inspected.definition_migrations and
        graph.definition_resolution == inspected.definition_resolution and
        explanation.evidence.definition_resolution == inspected.definition_resolution and
        migrated?

    if valid? do
      inspected
    else
      raise "workflow migration diagnostics did not retain exact definition evidence"
    end
  end

  defp seed_paused_run!(run_id, paused_at) do
    definition = MigratedRouting.V1.workflow_definition()
    runnable_key = "#{run_id}:legacy_gate:1"

    entries = [
      entry!(:run_started, %{
        run_id: run_id,
        workflow: Definition.serialize_workflow(MigratedRouting),
        trigger: "manual",
        context: %{account_id: "acct-migration", legacy: true, schema: 1},
        definition_version: definition.definition_version,
        definition_fingerprint: Definition.fingerprint(definition),
        occurred_at: paused_at
      }),
      entry!(:runnables_planned, %{
        run_id: run_id,
        runnables: [runnable(run_id, runnable_key, paused_at)],
        occurred_at: paused_at
      }),
      entry!(:runnable_applied, %{
        run_id: run_id,
        runnable_key: runnable_key,
        result: %{legacy_output: true},
        occurred_at: paused_at
      }),
      entry!(:manual_step_paused, %{
        run_id: run_id,
        step: "legacy_gate",
        kind: "pause",
        metadata: %{output: %{legacy_output: true}},
        occurred_at: paused_at
      })
    ]

    {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp runnable(run_id, runnable_key, visible_at) do
    %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: 1,
      queue: @queue,
      step: "legacy_gate",
      input: %{},
      visible_at: visible_at
    }
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end
end
