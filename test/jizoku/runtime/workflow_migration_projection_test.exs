defmodule Jizoku.Runtime.WorkflowMigrationProjectionTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition

  @storage {ETS, table: :jizoku_workflow_migration_projection_test}
  @queue "migration-projection"
  @occurred_at ~U[2026-08-16 12:00:00Z]

  defmodule StepV1 do
    use Jizoku.Step, name: "migration_projection_v1", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{legacy_output: true}}
    end
  end

  defmodule StepV2 do
    use Jizoku.Step, name: "migration_projection_v2", input_schema: []

    @impl Jizoku.Step
    def run(input, _context) do
      {:ok, %{schema: input.schema}}
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
      step :legacy_finish, StepV1
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
      step :finish, StepV2
      transition :gate, on: :ok, to: :finish
      transition :finish, on: :ok, to: :complete
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

  test "replay selects the migrated definition and treats transformed context as authoritative" do
    run_id = Ecto.UUID.generate()
    source_definition = HistoricalV1.workflow_definition()
    target_definition = CurrentWorkflow.workflow_definition()
    runnable_key = "#{run_id}:legacy_gate:1"

    entries = [
      entry!(:run_started, %{
        run_id: run_id,
        workflow: Definition.serialize_workflow(CurrentWorkflow),
        trigger: "manual",
        context: %{schema: 1},
        definition_version: source_definition.definition_version,
        definition_fingerprint: Definition.fingerprint(source_definition),
        occurred_at: @occurred_at
      }),
      entry!(:runnables_planned, %{
        run_id: run_id,
        runnables: [runnable(run_id, runnable_key)],
        occurred_at: @occurred_at
      }),
      entry!(:runnable_applied, %{
        run_id: run_id,
        runnable_key: runnable_key,
        result: %{legacy_output: true},
        occurred_at: @occurred_at
      }),
      entry!(:manual_step_paused, %{
        run_id: run_id,
        step: "legacy_gate",
        kind: "pause",
        metadata: %{output: %{legacy_output: true}},
        occurred_at: @occurred_at
      }),
      entry!(:run_definition_migrated, %{
        run_id: run_id,
        migration_key: "projection-v1-to-v2",
        source_version: "v1",
        source_fingerprint: Definition.fingerprint(source_definition),
        target_version: "v2",
        target_fingerprint: Definition.fingerprint(target_definition),
        source_manual_step: "legacy_gate",
        target_manual_step: "gate",
        context: %{schema: 2},
        manual_state: %{
          step: "gate",
          kind: "pause",
          paused_at: @occurred_at,
          metadata: %{output: %{legacy_output: true}}
        },
        occurred_at: @occurred_at
      })
    ]

    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
    assert {:ok, agent} = WorkflowAgent.rebuild(@storage, run_id)

    projection = agent.state.projection
    assert projection.definition_version == "v2"
    assert projection.context == %{schema: 2}
    assert Projection.applied_result_context(projection) == %{}

    assert [%{migration_key: "projection-v1-to-v2"}] =
             Projection.definition_migrations(projection)

    assert {:ok, snapshot} = Inspection.snapshot(@storage, run_id, queue: @queue)
    assert snapshot.context == %{schema: 2}
    assert snapshot.manual_state.step == "gate"
    assert snapshot.definition_version == "v2"

    assert {:ok, CurrentWorkflow, loaded} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )

    assert loaded.definition_version == "v2"

    stale_entry =
      entry!(:run_definition_migrated, %{
        run_id: run_id,
        migration_key: "stale-projection-migration",
        source_version: "v1",
        source_fingerprint: Definition.fingerprint(source_definition),
        target_version: "v1",
        target_fingerprint: Definition.fingerprint(source_definition),
        context: %{schema: 1},
        manual_state: %{
          step: "legacy_gate",
          kind: "pause",
          paused_at: @occurred_at,
          metadata: %{}
        },
        occurred_at: DateTime.add(@occurred_at, 1, :second)
      })

    assert {:ok, _thread} = Journal.append_entries(@storage, [stale_entry])

    assert {:ok, CurrentWorkflow, still_loaded} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )

    assert still_loaded.definition_version == "v2"

    assert {:ok, %{status: :running}} =
             Jizoku.resume(
               run_id,
               %{actor: "projection-test"},
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               idempotency_key: "resume-projected-migration",
               now: DateTime.add(@occurred_at, 1, :second)
             )

    assert {:ok, completed} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: @queue,
               owner_id: "projected-migration-worker",
               now: DateTime.add(@occurred_at, 1, :second)
             )

    assert completed.status == :completed
    assert Enum.any?(completed.attempts, &(&1.result == %{schema: 2}))
  end

  defp runnable(run_id, runnable_key) do
    %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: 1,
      queue: @queue,
      step: "legacy_gate",
      input: %{},
      visible_at: @occurred_at
    }
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp cleanup_storage do
    case :ets.whereis(:jizoku_workflow_migration_projection_test) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end
  end
end
