defmodule Jizoku.RetentionApplyTest do
  use Jizoku.DataCase, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Persistence.RetentionReceipt
  alias Jizoku.Persistence.RunSearch
  alias Jizoku.Retention.Plan
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto

  @storage {Ecto, repo: Repo}
  @queue "retention-apply-test"
  @now ~U[2026-08-16 22:00:00Z]
  @target_id "018f6373-8b9c-7f20-9000-000000000011"
  @survivor_id "018f6373-8b9c-7f20-9000-000000000012"
  @second_target_id "018f6373-8b9c-7f20-9000-000000000013"
  @new_run_id "018f6373-8b9c-7f20-9000-000000000014"

  defmodule Record do
    use Jizoku.Step, name: "retention_apply_record"

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{recorded: true}}
    end
  end

  defmodule Workflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :secret, :string, required: false
        end
      end

      step :record, Record
      transition :record, on: :ok, to: :complete
    end
  end

  defmodule ToggleHoldPolicy do
    @behaviour Jizoku.Retention.Policy

    @impl Jizoku.Retention.Policy
    def evaluate(_snapshot, _context, opts) do
      if Agent.get(Keyword.fetch!(opts, :toggle), & &1) do
        {:block, :legal_hold}
      else
        :allow
      end
    end
  end

  test "atomically deletes eligible history while preserving shared survivors" do
    secret = "retention-payload-must-not-survive"
    reason = "retention-reason-must-not-survive"

    assert {:ok, _archived} = archived_run(@target_id, %{secret: secret}, reason)
    assert {:ok, survivor} = start_run(@survivor_id)
    assert survivor.status == :running

    assert {:ok, %Plan{} = plan} = preview()
    assert [%Plan.Candidate{run_id: @target_id} = candidate] = plan.eligible
    assert candidate.dispatch_entry_count > 0

    assert {:ok, %Jizoku.Retention.Receipt{} = receipt} = apply_plan(plan)
    refute receipt.idempotent?
    assert receipt.run_ids == [@target_id]
    assert receipt.run_count == 1
    assert receipt.run_entries_deleted == candidate.run_revision
    assert receipt.dispatch_entries_deleted == candidate.dispatch_entry_count

    assert {:error, :not_found} = Jizoku.inspect_run(@target_id, runtime_options())
    assert {:ok, inspected_survivor} = Jizoku.inspect_run(@survivor_id, runtime_options())
    assert inspected_survivor.status == :running

    assert {:error, :not_found} = Journal.load_thread(@storage, {:run, @target_id})

    assert {:ok, dispatch} = Journal.load_thread(@storage, {:dispatch, @queue})
    assert dispatch.rev == candidate.dispatch_revision + 1
    refute Enum.any?(dispatch.entries, &(Map.get(&1.data, :run_id) == @target_id))
    assert Enum.any?(dispatch.entries, &(Map.get(&1.data, :run_id) == @survivor_id))

    assert {:ok, catalog} = Journal.load_thread(@storage, {:run_catalog, "all"})
    assert catalog.rev == plan.catalog_revision + 1
    refute Enum.any?(catalog.entries, &(Map.get(&1.data, :run_id) == @target_id))
    assert Enum.any?(catalog.entries, &(Map.get(&1.data, :run_id) == @survivor_id))

    assert {:ok, index} = Journal.load_thread(@storage, {:run_index, workflow_name()})
    refute Enum.any?(index.entries, &(Map.get(&1.data, :run_id) == @target_id))
    assert Enum.any?(index.entries, &(Map.get(&1.data, :run_id) == @survivor_id))

    refute Repo.get_by(RunSearch, partition_key: "", run_id: @target_id)

    assert %RetentionReceipt{} =
             stored =
             Repo.get_by!(RetentionReceipt, partition_key: "", run_id: @target_id)

    refute inspect(stored) =~ secret
    refute inspect(stored) =~ reason
    assert stored.plan_digest == plan.confirmation_token

    assert {:error, {:retained_run, @target_id}} = start_run(@target_id)
    assert {:ok, new_run} = start_run(@new_run_id)
    assert new_run.status == :running
  end

  test "returns the original receipt for an exact retry after plan expiry" do
    assert {:ok, _archived} = archived_run(@target_id)
    assert {:ok, plan} = preview()

    assert {:ok, first} = apply_plan(plan)
    refute first.idempotent?

    assert {:ok, duplicate} =
             Jizoku.apply_retention(
               plan,
               plan.confirmation_token,
               retention_options(now: DateTime.add(plan.expires_at, 1, :second))
             )

    assert duplicate.idempotent?
    assert %{duplicate | idempotent?: false} == first
    assert Repo.aggregate(RetentionReceipt, :count) == 1
  end

  test "serializes concurrent exact retries behind the retained run identity" do
    assert {:ok, _archived} = archived_run(@target_id)
    assert {:ok, plan} = preview()

    parent = self()

    tasks =
      Enum.map(1..2, fn _index ->
        Task.async(fn ->
          Sandbox.allow(Repo, parent, self())
          send(parent, {:retention_apply_ready, self()})

          receive do
            :apply_retention -> apply_plan(plan)
          end
        end)
      end)

    task_pids =
      Enum.map(tasks, fn _task ->
        assert_receive {:retention_apply_ready, task_pid}, 5_000
        task_pid
      end)

    Enum.each(task_pids, &send(&1, :apply_retention))
    results = Enum.map(tasks, &Task.await(&1, 10_000))

    assert Enum.count(results, &match?({:ok, %{idempotent?: false}}, &1)) == 1
    assert Enum.count(results, &match?({:ok, %{idempotent?: true}}, &1)) == 1
    assert Repo.aggregate(RetentionReceipt, :count) == 1
  end

  test "revalidates trusted policy without requiring a source revision change" do
    assert {:ok, _archived} = archived_run(@target_id)

    previous_policy = Application.get_env(:jizoku, :retention_policy)
    {:ok, toggle} = Agent.start_link(fn -> false end)
    Application.put_env(:jizoku, :retention_policy, {ToggleHoldPolicy, toggle: toggle})

    on_exit(fn -> restore_retention_policy(previous_policy) end)

    assert {:ok, plan} = preview()
    assert [%Plan.Candidate{run_id: @target_id}] = plan.eligible

    Agent.update(toggle, fn _held? -> true end)

    assert {:error, {:retention_candidate_blocked, @target_id, [:legal_hold]}} =
             apply_plan(plan)

    assert {:ok, _snapshot} = Jizoku.inspect_run(@target_id, runtime_options())
    assert Repo.aggregate(RetentionReceipt, :count) == 0
  end

  test "deletes a bounded same-queue batch and advances shared revisions once" do
    assert {:ok, _archived} = archived_run(@target_id)

    assert {:ok, _archived} =
             archived_run(@second_target_id, %{}, "retention_policy_second")

    assert {:ok, plan} = preview()
    assert [_first_candidate, _second_candidate] = plan.eligible

    assert {:ok, receipt} = apply_plan(plan)
    assert receipt.run_ids == Enum.sort([@target_id, @second_target_id])
    assert receipt.run_count == 2

    assert {:error, :not_found} = Jizoku.inspect_run(@target_id, runtime_options())
    assert {:error, :not_found} = Jizoku.inspect_run(@second_target_id, runtime_options())

    assert {:ok, dispatch} = Journal.load_thread(@storage, {:dispatch, @queue})
    assert dispatch.rev == hd(plan.eligible).dispatch_revision + 1

    assert {:ok, duplicate} =
             Jizoku.apply_retention(
               plan,
               plan.confirmation_token,
               retention_options(now: DateTime.add(plan.expires_at, 1, :second))
             )

    assert duplicate.idempotent?
    assert duplicate.run_ids == receipt.run_ids
    assert Repo.aggregate(RetentionReceipt, :count) == 2
  end

  test "rejects expired, modified, stale, and cross-partition plans without deletion" do
    assert {:ok, _archived} = archived_run(@target_id)
    assert {:ok, plan} = preview()

    assert {:error, :invalid_retention_confirmation} =
             Jizoku.apply_retention(plan, "wrong", retention_options())

    assert {:error, :invalid_retention_confirmation} =
             Jizoku.apply_retention(
               %{plan | limit: plan.limit + 1},
               plan.confirmation_token,
               retention_options()
             )

    assert {:error, :expired_retention_plan} =
             Jizoku.apply_retention(
               plan,
               plan.confirmation_token,
               retention_options(now: plan.expires_at)
             )

    assert {:error, :retention_partition_mismatch} =
             Jizoku.apply_retention(
               plan,
               plan.confirmation_token,
               retention_options(partition: "other")
             )

    assert {:ok, _unarchived} = Jizoku.unarchive_run(@target_id, runtime_options())

    assert {:error, {:stale_retention_plan, _thread_id}} = apply_plan(plan)
    assert {:ok, _snapshot} = Jizoku.inspect_run(@target_id, runtime_options())
    assert Repo.aggregate(RetentionReceipt, :count) == 0
  end

  test "rejects empty plans and storage adapters without transactional deletion support" do
    assert {:ok, _active} = start_run(@survivor_id)
    assert {:ok, empty_plan} = preview()
    assert empty_plan.eligible == []

    assert {:error, :empty_retention_plan} = apply_plan(empty_plan)

    assert {:error, {:unsupported_retention_apply, Jido.Storage.ETS}} =
             Jizoku.apply_retention(
               empty_plan,
               empty_plan.confirmation_token,
               journal_storage: Jido.Storage.ETS,
               now: DateTime.add(@now, 180, :second)
             )
  end

  test "fails closed when shared ownership has not been backfilled" do
    assert {:ok, _archived} = archived_run(@target_id)
    assert {:ok, plan} = preview()

    catalog_id = Journal.thread_id({:run_catalog, "all"})

    Repo.update_all(
      from(entry in JournalEntry,
        where: entry.thread_id == ^catalog_id and entry.retention_run_id == ^@target_id
      ),
      set: [retention_run_id: nil]
    )

    assert {:error, {:retention_ownership_backfill_required, thread_ids}} = apply_plan(plan)
    assert catalog_id in thread_ids
    assert {:ok, _snapshot} = Jizoku.inspect_run(@target_id, runtime_options())
    assert Repo.aggregate(RetentionReceipt, :count) == 0
  end

  test "rolls back every deletion when receipt insertion conflicts" do
    assert {:ok, _archived} = archived_run(@target_id)
    assert {:ok, plan} = preview()

    now = DateTime.add(DateTime.add(@now, 30, :second), 0, :microsecond)

    Repo.insert!(%RetentionReceipt{
      partition_key: "",
      run_id: @target_id,
      plan_digest: "different-plan",
      workflow: workflow_name(),
      queue: @queue,
      terminal_status: "cancelled",
      run_entries_deleted: 1,
      dispatch_entries_deleted: 1,
      deleted_at: now
    })

    assert {:error, :retention_receipt_conflict} = apply_plan(plan)
    assert {:ok, _snapshot} = Jizoku.inspect_run(@target_id, runtime_options())
    assert {:ok, entries} = Journal.load_entries(@storage, {:run_catalog, "all"})
    assert Enum.any?(entries, &(Map.get(&1.data, :run_id) == @target_id))
    assert Repo.get_by!(RunSearch, partition_key: "", run_id: @target_id)

    Repo.delete_all(from(receipt in RetentionReceipt, where: receipt.run_id == ^@target_id))

    assert {:ok, resumed} = apply_plan(plan)
    refute resumed.idempotent?
    assert {:error, :not_found} = Jizoku.inspect_run(@target_id, runtime_options())
  end

  defp archived_run(run_id, payload \\ %{}, reason \\ "retention_policy") do
    with {:ok, started} <- start_run(run_id, payload),
         {:ok, _cancelled} <- Jizoku.cancel(started.run_id, runtime_options()) do
      Jizoku.archive_run(
        started.run_id,
        runtime_options(reason: reason, now: DateTime.add(@now, 1, :second))
      )
    end
  end

  defp start_run(run_id, payload \\ %{}) do
    Jizoku.start(
      Workflow,
      :manual,
      payload,
      runtime_options(run_id: run_id)
    )
  end

  defp preview do
    Jizoku.preview_retention(
      [terminal_before: DateTime.add(@now, 60, :second)],
      retention_options(now: DateTime.add(@now, 120, :second))
    )
  end

  defp apply_plan(plan) do
    Jizoku.apply_retention(
      plan,
      plan.confirmation_token,
      retention_options(now: DateTime.add(@now, 180, :second))
    )
  end

  defp workflow_name do
    Jizoku.Workflow.Definition.serialize_workflow(Workflow)
  end

  defp runtime_options(overrides \\ []) do
    Keyword.merge(
      [journal_storage: @storage, queue: @queue, now: @now],
      overrides
    )
  end

  defp retention_options(overrides \\ []) do
    Keyword.merge([journal_storage: @storage, now: @now], overrides)
  end

  defp restore_retention_policy(nil) do
    Application.delete_env(:jizoku, :retention_policy)
  end

  defp restore_retention_policy(value) do
    Application.put_env(:jizoku, :retention_policy, value)
  end
end
