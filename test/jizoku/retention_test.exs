defmodule Jizoku.RetentionTest do
  use Jizoku.DataCase, async: false

  alias Jizoku.Retention
  alias Jizoku.Retention.Plan
  alias Jizoku.Runtime.Journal.Storage.Ecto

  @storage {Ecto, repo: Repo}
  @queue "retention-preview-test"
  @now ~U[2026-08-16 22:00:00Z]

  defmodule Record do
    use Jizoku.Step, name: "retention_preview_record"

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

  defmodule HoldPolicy do
    @behaviour Jizoku.Retention.Policy

    @impl Jizoku.Retention.Policy
    def evaluate(snapshot, _context, opts) do
      if snapshot.run_id in Keyword.fetch!(opts, :held_run_ids) do
        {:block, :legal_hold}
      else
        :allow
      end
    end
  end

  test "builds deterministic, expiring evidence without exposing payloads" do
    secret = "do-not-copy-this-customer-secret"
    run_id = "018f6373-8b9c-7f20-9000-000000000001"

    assert {:ok, _archived} = archived_run(run_id, %{secret: secret})

    filters = [terminal_before: DateTime.add(@now, 60, :second)]
    opts = retention_options(now: DateTime.add(@now, 120, :second))

    assert {:ok, %Plan{} = first} = Jizoku.preview_retention(filters, opts)
    assert {:ok, %Plan{} = duplicate} = Jizoku.preview_retention(filters, opts)

    assert first == duplicate
    assert first.partition == nil
    assert first.catalog_revision > 0
    assert first.blocked == []
    assert [%Plan.Candidate{} = candidate] = first.eligible
    assert candidate.run_id == run_id
    assert candidate.terminal_status == :cancelled
    assert candidate.archived_at == DateTime.add(@now, 1, :second)
    assert candidate.run_revision > 0
    assert candidate.dispatch_revision > 0
    assert candidate.estimated_entries > candidate.run_revision
    assert candidate.affected.search_row == %{partition: nil, run_id: run_id}
    assert is_binary(candidate.affected.run_thread)
    assert is_binary(candidate.affected.dispatch_thread)

    refute inspect(first) =~ secret
    assert byte_size(first.confirmation_token) == 43
    assert Retention.valid_confirmation?(first, first.confirmation_token, first.created_at)
    refute Retention.valid_confirmation?(first, "wrong", first.created_at)
    refute Retention.valid_confirmation?(first, first.confirmation_token, first.expires_at)
  end

  test "reports intrinsic and trusted host blocks without allowing policy bypass" do
    unarchived_id = "018f6373-8b9c-7f20-9000-000000000002"
    held_id = "018f6373-8b9c-7f20-9000-000000000003"

    assert {:ok, started} = start_run(unarchived_id)
    assert {:ok, _cancelled} = Jizoku.cancel(started.run_id, runtime_options())
    assert {:ok, _archived} = archived_run(held_id)

    previous = Application.get_env(:jizoku, :retention_policy)

    Application.put_env(
      :jizoku,
      :retention_policy,
      {HoldPolicy, held_run_ids: [held_id]}
    )

    on_exit(fn -> restore_retention_policy(previous) end)

    assert {:ok, plan} =
             Jizoku.preview_retention(
               [terminal_before: DateTime.add(@now, 60, :second)],
               retention_options(now: DateTime.add(@now, 120, :second))
             )

    assert plan.eligible == []

    assert [unarchived, held] = Enum.sort_by(plan.blocked, & &1.run_id)
    assert unarchived.run_id == unarchived_id
    assert unarchived.reasons == [:not_archived]
    assert held.run_id == held_id
    assert held.reasons == [:legal_hold]
  end

  test "keeps active and other-partition runs outside the selected scope" do
    active_id = "018f6373-8b9c-7f20-9000-000000000004"
    partitioned_id = "018f6373-8b9c-7f20-9000-000000000005"

    assert {:ok, _active} = start_run(active_id)

    assert {:ok, _archived} =
             archived_run(partitioned_id, %{}, partition: "tenant-retention")

    filters = [terminal_before: DateTime.add(@now, 60, :second)]

    assert {:ok, default_plan} =
             Jizoku.preview_retention(
               filters,
               retention_options(now: DateTime.add(@now, 120, :second))
             )

    assert default_plan.eligible == []
    assert default_plan.blocked == []

    assert {:ok, partition_plan} =
             Jizoku.preview_retention(
               filters,
               retention_options(
                 partition: "tenant-retention",
                 now: DateTime.add(@now, 120, :second)
               )
             )

    assert [%{run_id: ^partitioned_id}] = partition_plan.eligible
  end

  test "validates filters and public options before reading storage" do
    assert {:error, {:invalid_option, {:terminal_before, :invalid}}} =
             Jizoku.preview_retention([], retention_options())

    assert {:error, {:invalid_option, {:statuses, :invalid}}} =
             Jizoku.preview_retention(
               [terminal_before: @now, statuses: [:running]],
               retention_options()
             )

    assert {:error, {:invalid_option, {:limit, :invalid}}} =
             Jizoku.preview_retention(
               [terminal_before: @now, limit: 501],
               retention_options()
             )

    assert {:error, {:invalid_option, {:filter, :unknown}}} =
             Jizoku.preview_retention(
               [terminal_before: @now, unknown: true],
               retention_options()
             )

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             Jizoku.preview_retention(
               [terminal_before: @now],
               retention_options(unknown: true)
             )
  end

  defp archived_run(run_id, payload \\ %{}, overrides \\ []) do
    opts = runtime_options(overrides)

    with {:ok, started} <- start_run(run_id, payload, overrides),
         {:ok, _cancelled} <- Jizoku.cancel(started.run_id, opts) do
      Jizoku.archive_run(
        started.run_id,
        Keyword.merge(opts, reason: "retention_policy", now: DateTime.add(@now, 1, :second))
      )
    end
  end

  defp start_run(run_id, payload \\ %{}, overrides \\ []) do
    Jizoku.start(
      Workflow,
      :manual,
      payload,
      runtime_options(Keyword.put(overrides, :run_id, run_id))
    )
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
