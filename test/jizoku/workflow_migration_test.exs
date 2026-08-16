defmodule Jizoku.WorkflowMigrationTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.Runs.GraphInspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  @storage {ETS, table: :jizoku_workflow_migration_test}
  @queue "workflow-migration"
  @paused_at ~U[2026-08-16 12:00:00Z]

  defmodule FinishV1 do
    use Jizoku.Step, name: "workflow_migration_finish_v1", input_schema: []

    @impl Jizoku.Step
    def run(input, _context) do
      {:ok, %{implementation: "v1", schema: input.schema}}
    end
  end

  defmodule FinishV2 do
    use Jizoku.Step, name: "workflow_migration_finish_v2", input_schema: []

    @impl Jizoku.Step
    def run(input, _context) do
      {:ok, %{implementation: "v2", schema: input.schema}}
    end
  end

  defmodule HistoricalV1 do
    use Jizoku.Workflow

    workflow do
      version "v1"

      trigger :manual do
        manual()
      end

      step :legacy_gate, :pause
      step :legacy_finish, FinishV1
      transition :legacy_gate, on: :ok, to: :legacy_finish
      transition :legacy_finish, on: :ok, to: :complete
    end
  end

  defmodule CurrentWorkflow do
    use Jizoku.Workflow

    workflow do
      version "v2"

      trigger :manual do
        manual()
      end

      step :gate, :pause
      step :finish, FinishV2
      transition :gate, on: :ok, to: :finish
      transition :finish, on: :ok, to: :complete
    end
  end

  defmodule V1ToV2 do
    @behaviour Jizoku.Workflow.Migration

    @impl Jizoku.Workflow.Migration
    def key do
      "workflow-migration-v1-to-v2"
    end

    @impl Jizoku.Workflow.Migration
    def source_version do
      "v1"
    end

    @impl Jizoku.Workflow.Migration
    def target_version do
      "v2"
    end

    @impl Jizoku.Workflow.Migration
    def migrate(%{context: context, manual_state: %{step: "legacy_gate"}}) do
      {:ok,
       %{
         context:
           context
           |> Map.delete(:legacy)
           |> Map.delete(:legacy_output)
           |> Map.put(:schema, 2),
         manual_step: :gate
       }}
    end
  end

  defmodule ConflictingMigration do
    @behaviour Jizoku.Workflow.Migration

    @impl Jizoku.Workflow.Migration
    def key do
      "workflow-migration-v1-to-v2"
    end

    @impl Jizoku.Workflow.Migration
    def source_version do
      "v1"
    end

    @impl Jizoku.Workflow.Migration
    def target_version do
      "v3"
    end

    @impl Jizoku.Workflow.Migration
    def migrate(state) do
      {:ok, %{context: state.context, manual_step: :legacy_gate}}
    end
  end

  defmodule V2ToV1 do
    @behaviour Jizoku.Workflow.Migration

    @impl Jizoku.Workflow.Migration
    def key do
      "workflow-migration-v2-to-v1"
    end

    @impl Jizoku.Workflow.Migration
    def source_version do
      "v2"
    end

    @impl Jizoku.Workflow.Migration
    def target_version do
      "v1"
    end

    @impl Jizoku.Workflow.Migration
    def migrate(%{context: context, manual_state: %{step: "gate"}}) do
      {:ok,
       %{
         context: Map.put(context, :schema, 1),
         manual_step: :legacy_gate
       }}
    end
  end

  defmodule StaleSourceMigration do
    @behaviour Jizoku.Workflow.Migration

    @impl Jizoku.Workflow.Migration
    def key do
      "workflow-migration-stale-source"
    end

    @impl Jizoku.Workflow.Migration
    def source_version do
      "v0"
    end

    @impl Jizoku.Workflow.Migration
    def target_version do
      "v2"
    end

    @impl Jizoku.Workflow.Migration
    def migrate(state) do
      {:ok, %{context: state.context, manual_step: :gate}}
    end
  end

  setup do
    cleanup_storage()
    previous = Application.fetch_env(:jizoku, :workflow_versions)

    Application.put_env(:jizoku, :workflow_versions, %{
      CurrentWorkflow => %{"v1" => HistoricalV1, "v2" => CurrentWorkflow}
    })

    on_exit(fn ->
      cleanup_storage()

      case previous do
        {:ok, value} -> Application.put_env(:jizoku, :workflow_versions, value)
        :error -> Application.delete_env(:jizoku, :workflow_versions)
      end
    end)
  end

  test "migrates a quiescent paused run and resumes through the target definition" do
    run_id = Ecto.UUID.generate()
    seed_paused_run!(run_id, applied?: true)

    assert {:ok, migrated} =
             Jizoku.migrate_run(run_id,
               to: "v2",
               migration: V1ToV2,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert migrated.status == :paused
    assert migrated.definition_version == "v2"
    assert migrated.context == %{account_id: "acct-123", schema: 2}
    assert migrated.manual_state.step == "gate"

    assert [
             %{
               migration_key: "workflow-migration-v1-to-v2",
               source_version: "v1",
               target_version: "v2"
             }
           ] = migrated.definition_migrations

    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, run_id)
    assert agent.state.projection.definition_version == "v2"

    assert Projection.definition_migrations(agent.state.projection) ==
             migrated.definition_migrations

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    replayed = Projection.rebuild(entries)
    assert replayed.definition_version == "v2"
    assert replayed.context == migrated.context

    assert {:ok, CurrentWorkflow, loaded_definition} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )

    assert loaded_definition.definition_version == "v2"

    target_fingerprint = Definition.fingerprint(loaded_definition)

    assert {:ok, inspected} =
             Jizoku.inspect_run(run_id,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert inspected.definition_fingerprint == target_fingerprint

    assert inspected.definition_resolution == %{
             status: :resolved,
             definition_version: "v2",
             definition_fingerprint: target_fingerprint
           }

    assert {:ok, graph} =
             Jizoku.inspect_run_graph(run_id,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    graph = GraphInspection.to_map(graph)
    assert graph.definition_fingerprint == target_fingerprint
    assert graph.definition_resolution == inspected.definition_resolution
    assert graph.definition_migrations == inspected.definition_migrations

    assert {:ok, timeline} =
             Jizoku.inspect_run_timeline(run_id,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert timeline.definition_fingerprint == target_fingerprint
    assert timeline.definition_resolution == inspected.definition_resolution

    assert %{
             type: :workflow_definition_migrated,
             status: :migrated,
             details: %{
               migration_key: "workflow-migration-v1-to-v2",
               source_version: "v1",
               target_version: "v2"
             }
           } = Enum.find(timeline.events, &(&1.type == :workflow_definition_migrated))

    assert {:ok, explanation} =
             Jizoku.explain_run(run_id,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert explanation.evidence.definition_fingerprint == target_fingerprint
    assert explanation.evidence.definition_resolution == inspected.definition_resolution
    assert explanation.evidence.definition_migrations == inspected.definition_migrations

    assert {:ok, %{status: :running}} =
             Jizoku.resume(
               run_id,
               %{actor: "migration-test"},
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               idempotency_key: "resume-migrated-run",
               now: DateTime.add(@paused_at, 1, :second)
             )

    assert {:ok, completed} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: @queue,
               owner_id: "migration-worker",
               now: DateTime.add(@paused_at, 1, :second)
             )

    assert completed.status == :completed,
           inspect(%{terminal_error: completed.terminal_error, attempts: completed.attempts})

    assert completed.context.schema == 2
    assert Enum.any?(completed.attempts, &(&1.result == %{implementation: "v2", schema: 2}))
  end

  test "exact duplicate delivery is idempotent and conflicting key reuse fails closed" do
    run_id = Ecto.UUID.generate()
    seed_paused_run!(run_id, applied?: true)

    opts = [
      to: "v2",
      migration: V1ToV2,
      journal_storage: @storage,
      queue: @queue,
      now: @paused_at
    ]

    assert {:ok, first} = Jizoku.migrate_run(run_id, opts)
    assert {:ok, duplicate} = Jizoku.migrate_run(run_id, opts)
    assert duplicate.definition_migrations == first.definition_migrations
    assert duplicate.thread_revisions == first.thread_revisions

    assert {:error, {:migration_key_conflict, %{migration_key: key}}} =
             Jizoku.migrate_run(run_id,
               to: "v3",
               migration: ConflictingMigration,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert key == "workflow-migration-v1-to-v2"

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    assert Enum.count(entries, &(&1.type == :run_definition_migrated)) == 1

    assert Enum.count(entries, fn entry ->
             entry.type == :run_signal_received and
               entry.data.signal_type == "migrate_run"
           end) == 1
  end

  test "rejects a paused run while any planned runnable remains unapplied" do
    run_id = Ecto.UUID.generate()
    seed_paused_run!(run_id, applied?: false)

    assert {:error,
            {:unsafe_workflow_migration,
             [%{reason: :pending_runnables, runnable_keys: [runnable_key]}]}} =
             Jizoku.migrate_run(run_id,
               to: "v2",
               migration: V1ToV2,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert runnable_key == "#{run_id}:legacy_gate:1"

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, run_id})
    refute Enum.any?(entries, &(&1.type == :run_definition_migrated))
  end

  test "rejects stale source state and supports an explicit rollback migration" do
    run_id = Ecto.UUID.generate()
    seed_paused_run!(run_id, applied?: true)

    assert {:error, {:stale_migration_source, %{expected_version: "v0", actual_version: "v1"}}} =
             Jizoku.migrate_run(run_id,
               to: "v2",
               migration: StaleSourceMigration,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert {:ok, %{definition_version: "v2"}} =
             Jizoku.migrate_run(run_id,
               to: "v2",
               migration: V1ToV2,
               journal_storage: @storage,
               queue: @queue,
               now: @paused_at
             )

    assert {:ok, rolled_back} =
             Jizoku.migrate_run(run_id,
               to: "v1",
               migration: V2ToV1,
               journal_storage: @storage,
               queue: @queue,
               now: DateTime.add(@paused_at, 1, :second)
             )

    assert rolled_back.definition_version == "v1"
    assert rolled_back.manual_state.step == "legacy_gate"

    assert Enum.map(rolled_back.definition_migrations, & &1.migration_key) == [
             "workflow-migration-v1-to-v2",
             "workflow-migration-v2-to-v1"
           ]

    assert {:ok, CurrentWorkflow, loaded_definition} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )

    assert loaded_definition.definition_version == "v1"
  end

  defp seed_paused_run!(run_id, opts) do
    definition = HistoricalV1.workflow_definition()
    runnable_key = "#{run_id}:legacy_gate:1"

    runnable = %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: 1,
      queue: @queue,
      step: "legacy_gate",
      input: %{},
      visible_at: @paused_at
    }

    base_entries = [
      entry!(:run_started, %{
        run_id: run_id,
        workflow: Definition.serialize_workflow(CurrentWorkflow),
        trigger: "manual",
        context: %{account_id: "acct-123", legacy: true, schema: 1},
        definition_version: definition.definition_version,
        definition_fingerprint: Definition.fingerprint(definition),
        occurred_at: @paused_at
      }),
      entry!(:runnables_planned, %{
        run_id: run_id,
        runnables: [runnable],
        occurred_at: @paused_at
      })
    ]

    manual_entry =
      entry!(:manual_step_paused, %{
        run_id: run_id,
        step: "legacy_gate",
        kind: "pause",
        metadata: %{output: %{legacy_output: true}, target: "legacy_finish"},
        occurred_at: @paused_at
      })

    entries =
      Enum.concat([
        base_entries,
        applied_entries(run_id, runnable_key, Keyword.fetch!(opts, :applied?)),
        [manual_entry]
      ])

    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp applied_entries(_run_id, _runnable_key, false) do
    []
  end

  defp applied_entries(run_id, runnable_key, true) do
    [
      entry!(:runnable_applied, %{
        run_id: run_id,
        runnable_key: runnable_key,
        result: %{legacy_output: true},
        occurred_at: @paused_at
      })
    ]
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp cleanup_storage do
    case :ets.whereis(:jizoku_workflow_migration_test) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end
  end
end
