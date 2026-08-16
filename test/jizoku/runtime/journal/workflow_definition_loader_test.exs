defmodule Jizoku.Runtime.Journal.WorkflowDefinitionLoaderTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.Runs.GraphInspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.WorkflowDefinitionLoader
  alias Jizoku.Workflow.Definition

  @storage {ETS, table: :jizoku_workflow_definition_loader_test}
  @started_at ~U[2026-08-16 00:00:00Z]

  defmodule StepV1 do
    use Jizoku.Step, name: "definition_loader_step_v1", input_schema: []

    @impl Jizoku.Step
    def run(_input, context) do
      {:ok, %{version: 1, workflow: Atom.to_string(context.workflow)}}
    end
  end

  defmodule StepV2 do
    use Jizoku.Step, name: "definition_loader_step_v2", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 2}}
    end
  end

  defmodule HistoricalV1 do
    use Jizoku.Workflow

    workflow do
      version "v1"

      trigger :manual do
        manual()
      end

      step :process, Jizoku.Runtime.Journal.WorkflowDefinitionLoaderTest.StepV1
      transition :process, on: :ok, to: :complete
    end
  end

  defmodule CurrentWorkflow do
    use Jizoku.Workflow

    workflow do
      version "v2"

      trigger :manual do
        manual()
      end

      step :process, Jizoku.Runtime.Journal.WorkflowDefinitionLoaderTest.StepV2
      transition :process, on: :ok, to: :complete
    end
  end

  defmodule HistoricalPauseV1 do
    use Jizoku.Workflow

    workflow do
      version "pause-v1"

      trigger :manual do
        manual()
      end

      step :gate, :pause
      step :process, Jizoku.Runtime.Journal.WorkflowDefinitionLoaderTest.StepV1
      transition :gate, on: :ok, to: :process
      transition :process, on: :ok, to: :complete
    end
  end

  defmodule CurrentPauseWorkflow do
    use Jizoku.Workflow

    workflow do
      version "pause-v2"

      trigger :manual do
        manual()
      end

      step :gate, :pause
      step :process, Jizoku.Runtime.Journal.WorkflowDefinitionLoaderTest.StepV2
      transition :gate, on: :ok, to: :process
      transition :process, on: :ok, to: :complete
    end
  end

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "loads the historical implementation selected by durable version and fingerprint" do
    run_id = "historical-version"
    historical = HistoricalV1.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, historical)

    assert {:ok, CurrentWorkflow, loaded} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow),
               workflow_versions: workflow_versions()
             )

    assert loaded.definition_version == "v1"
    assert Definition.fingerprint(loaded) == Definition.fingerprint(historical)
  end

  test "execute_next continues a durable run on its registered historical implementation" do
    run_id = Ecto.UUID.generate()
    historical = HistoricalV1.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, historical)
    seed_attempt!(run_id)
    configure_workflow_versions!(workflow_versions())

    assert {:ok, snapshot} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: "version-routing",
               owner_id: "historical-worker",
               now: @started_at
             )

    assert snapshot.status == :completed
    assert snapshot.context == %{version: 1, workflow: Atom.to_string(CurrentWorkflow)}

    assert [
             %{
               status: :completed,
               result: %{version: 1, workflow: workflow}
             }
           ] = snapshot.attempts

    assert workflow == Atom.to_string(CurrentWorkflow)
  end

  test "manual controls keep using the historical graph and stable workflow identity" do
    run_id = Ecto.UUID.generate()
    historical = HistoricalPauseV1.workflow_definition()
    seed_run!(run_id, CurrentPauseWorkflow, historical)
    seed_attempt!(run_id, "gate")
    configure_workflow_versions!(pause_workflow_versions())

    assert {:ok, %{status: :paused}} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: "version-routing",
               owner_id: "historical-pause-worker",
               now: @started_at
             )

    resumed_at = DateTime.add(@started_at, 1, :second)

    assert {:ok, resumed} =
             Jizoku.resume(
               run_id,
               %{actor: "workflow-version-test"},
               runtime: :journal,
               journal_storage: @storage,
               queue: "version-routing",
               idempotency_key: "resume-historical-pause",
               now: resumed_at
             )

    assert resumed.status == :running
    assert [%{step: "process", status: :available}] = resumed.visible_attempts

    assert {:ok, completed} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: "version-routing",
               owner_id: "historical-resumed-worker",
               now: resumed_at
             )

    assert completed.status == :completed

    assert completed.context == %{
             version: 1,
             workflow: Atom.to_string(CurrentPauseWorkflow)
           }
  end

  test "keeps current unconfigured behavior when the current definition matches" do
    run_id = "current-version"
    current = CurrentWorkflow.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, current)

    assert {:ok, CurrentWorkflow, ^current} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )
  end

  test "fails closed with an actionable missing historical version diagnostic" do
    run_id = "missing-historical-version"
    historical = HistoricalV1.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, historical)

    assert {:error,
            %{
              code: "workflow_version_unavailable",
              requested_version: "v1",
              available_versions: ["v2"]
            }} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow),
               workflow_versions: %{CurrentWorkflow => %{"v2" => CurrentWorkflow}}
             )
  end

  test "execute_next preserves missing-version diagnostics without running current code" do
    run_id = Ecto.UUID.generate()
    historical = HistoricalV1.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, historical)
    seed_attempt!(run_id)
    configure_workflow_versions!(%{CurrentWorkflow => %{"v2" => CurrentWorkflow}})

    assert {:ok, inspected} =
             Jizoku.inspect_run(run_id,
               journal_storage: @storage,
               queue: "version-routing",
               now: @started_at
             )

    assert inspected.definition_fingerprint == Definition.fingerprint(historical)

    assert inspected.definition_resolution == %{
             status: :unavailable,
             error: %{
               code: "workflow_version_unavailable",
               requested_version: "v1",
               available_versions: ["v2"]
             }
           }

    assert {:ok, explanation} =
             Jizoku.explain_run(run_id,
               journal_storage: @storage,
               queue: "version-routing",
               now: @started_at
             )

    assert explanation.summary ==
             "The run requires a historical workflow version that is not registered."

    assert explanation.next_actions == [
             :restore_historical_workflow_version,
             :verify_workflow_histories
           ]

    assert explanation.evidence.definition_resolution == inspected.definition_resolution

    assert {:ok, graph} =
             Jizoku.inspect_run_graph(run_id,
               journal_storage: @storage,
               queue: "version-routing",
               now: @started_at
             )

    graph = GraphInspection.to_map(graph)
    assert graph.definition_fingerprint == inspected.definition_fingerprint
    assert graph.definition_resolution == inspected.definition_resolution

    assert {:ok, snapshot} =
             Jizoku.execute_next(
               journal_storage: @storage,
               queue: "version-routing",
               owner_id: "missing-version-worker",
               now: @started_at
             )

    assert snapshot.status == :failed

    assert [
             %{
               status: :failed,
               error: %{
                 code: "workflow_version_unavailable",
                 message: "registered historical workflow version is unavailable",
                 requested_version: "v1",
                 available_versions: ["v2"]
               }
             }
           ] = snapshot.attempts
  end

  test "fails with the existing fingerprint diagnostic when no registry is configured" do
    run_id = "unconfigured-historical-version"
    historical = HistoricalV1.workflow_definition()
    seed_run!(run_id, CurrentWorkflow, historical)

    assert {:error,
            %{
              code: "incompatible_workflow_definition",
              persisted_definition_version: "v1",
              current_definition_version: "v2"
            }} =
             WorkflowDefinitionLoader.load(
               @storage,
               run_id,
               Definition.serialize_workflow(CurrentWorkflow)
             )
  end

  defp seed_run!(run_id, workflow, definition) do
    assert {:ok, run_started} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: run_id,
               workflow: Definition.serialize_workflow(workflow),
               definition_version: definition.definition_version,
               definition_fingerprint: Definition.fingerprint(definition),
               occurred_at: @started_at
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [run_started])
  end

  defp workflow_versions do
    %{CurrentWorkflow => %{"v1" => HistoricalV1, "v2" => CurrentWorkflow}}
  end

  defp pause_workflow_versions do
    %{
      CurrentPauseWorkflow => %{
        "pause-v1" => HistoricalPauseV1,
        "pause-v2" => CurrentPauseWorkflow
      }
    }
  end

  defp seed_attempt!(run_id) do
    seed_attempt!(run_id, "process")
  end

  defp seed_attempt!(run_id, step) do
    runnable = %{
      run_id: run_id,
      runnable_key: "#{run_id}:#{step}:1",
      idempotency_key: "#{run_id}:#{step}:1",
      attempt_number: 1,
      queue: "version-routing",
      step: step,
      input: %{},
      visible_at: @started_at
    }

    assert {:ok, runnables_planned} =
             DispatchProtocol.new_entry(:runnables_planned, %{
               run_id: run_id,
               runnables: [runnable],
               occurred_at: @started_at
             })

    assert {:ok, attempt_scheduled} =
             DispatchProtocol.new_entry(
               :attempt_scheduled,
               Map.put(runnable, :occurred_at, @started_at)
             )

    assert {:ok, _run_thread} = Journal.append_entries(@storage, [runnables_planned])
    assert {:ok, _dispatch_thread} = Journal.append_entries(@storage, [attempt_scheduled])
  end

  defp configure_workflow_versions!(registry) do
    previous = Application.fetch_env(:jizoku, :workflow_versions)
    Application.put_env(:jizoku, :workflow_versions, registry)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:jizoku, :workflow_versions, value)
        :error -> Application.delete_env(:jizoku, :workflow_versions)
      end
    end)
  end

  defp cleanup_storage do
    case :ets.whereis(:jizoku_workflow_definition_loader_test) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end
  end
end
