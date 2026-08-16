defmodule MinimalHostApp.Verification.WorkflowEvolution do
  @moduledoc """
  Exercises a v1 durable run after the host has deployed the v2 workflow.

  The fixture writes the same authoritative facts an older deployment would
  have left in shared storage, then drains them through the current host worker.
  """

  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto, as: JournalStorage
  alias Jizoku.Workflow.Definition
  alias MinimalHostApp.Repo
  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.Workflows.VersionedRouting

  @queue "minimal-host-workflow-evolution"
  @storage {JournalStorage, repo: Repo}

  @spec run!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run! do
    RuntimeHarness.ensure_runtime_started()

    run_id = Ecto.UUID.generate()
    now = DateTime.utc_now()
    historical_definition = VersionedRouting.V1.workflow_definition()
    runnable = runnable(run_id, now)

    append_run!(run_id, historical_definition, runnable, now)
    append_attempt!(runnable, now)
    stable_workflow = Atom.to_string(VersionedRouting)

    case Jizoku.execute_next(
           journal_storage: @storage,
           queue: @queue,
           owner_id: "minimal-host-version-routing",
           now: now
         ) do
      {:ok,
       %{
         status: :completed,
         context: %{
           implementation: "v1",
           workflow: ^stable_workflow
         }
       } = snapshot} ->
        snapshot

      other ->
        raise "historical workflow routing failed: #{inspect(other)}"
    end
  end

  defp append_run!(run_id, definition, runnable, now) do
    {:ok, run_started} =
      DispatchProtocol.new_entry(:run_started, %{
        run_id: run_id,
        workflow: Definition.serialize_workflow(VersionedRouting),
        trigger: "manual",
        definition_version: definition.definition_version,
        definition_fingerprint: Definition.fingerprint(definition),
        occurred_at: now
      })

    {:ok, runnables_planned} =
      DispatchProtocol.new_entry(:runnables_planned, %{
        run_id: run_id,
        runnables: [runnable],
        occurred_at: now
      })

    {:ok, _thread} = Journal.append_entries(@storage, [run_started, runnables_planned])
  end

  defp append_attempt!(runnable, now) do
    {:ok, attempt_scheduled} =
      DispatchProtocol.new_entry(
        :attempt_scheduled,
        Map.put(runnable, :occurred_at, now)
      )

    {:ok, _thread} = Journal.append_entries(@storage, [attempt_scheduled])
  end

  defp runnable(run_id, now) do
    key = "#{run_id}:record_version:1"

    %{
      run_id: run_id,
      runnable_key: key,
      idempotency_key: key,
      attempt_number: 1,
      queue: @queue,
      step: "record_version",
      input: %{},
      visible_at: now
    }
  end
end
