defmodule Jizoku.ReadModel.RunSearchEctoTest do
  use Jizoku.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Persistence.RunSearch
  alias Jizoku.ReadModel.Listing.Page
  alias Jizoku.ReadModel.RunSearch.EctoQuery
  alias Jizoku.Runtime.Journal

  @storage {Jizoku.Runtime.Journal.Storage.Ecto, repo: Repo}
  @queue "run-search-ecto"
  @now ~U[2026-08-16 21:00:00Z]
  @schema %{"account_id" => :string, "priority" => :integer}

  defmodule Record do
    use Jizoku.Step, name: "run_search_ecto_record"

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
      end

      step :record, Record
      transition :record, on: :ok, to: :complete
    end
  end

  setup do
    Repo.delete_all(RunSearch)
    :ok
  end

  test "projects starts, updates, and terminal state in the journal transaction" do
    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_123", "priority" => 1})
             )

    started_projection = Repo.get_by!(RunSearch, partition_key: "", run_id: started.run_id)

    assert started_projection.workflow == "Elixir.Jizoku.ReadModel.RunSearchEctoTest.Workflow"
    assert started_projection.status == "running"

    assert started_projection.search_attributes == %{
             "account_id" => "acct_123",
             "priority" => 1
           }

    assert DateTime.compare(started_projection.started_at, @now) == :eq
    assert started_projection.thread_revision == started.thread_revisions.run

    assert {:ok, updated} =
             Jizoku.update_search_attributes(
               started.run_id,
               %{"priority" => 2},
               runtime_options(idempotency_key: "dashboard:set-priority")
             )

    updated_projection = Repo.get_by!(RunSearch, partition_key: "", run_id: started.run_id)

    assert updated_projection.search_attributes == %{
             "account_id" => "acct_123",
             "priority" => 2
           }

    assert updated_projection.thread_revision == updated.thread_revisions.run

    assert {:ok, cancelled} = Jizoku.cancel(started.run_id, runtime_options())
    assert cancelled.status == :cancelled

    cancelled_projection = Repo.get_by!(RunSearch, partition_key: "", run_id: started.run_id)
    assert cancelled_projection.status == "cancelled"
    assert cancelled_projection.terminal_status == "cancelled"
    assert DateTime.compare(cancelled_projection.terminal_at, @now) == :eq
    assert cancelled_projection.thread_revision == cancelled.thread_revisions.run
  end

  test "uses the Ecto candidate projection without loading nonmatching run threads" do
    assert {:ok, matching} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(
                 now: DateTime.add(@now, -60, :second),
                 search_attributes: %{"account_id" => "acct_match"}
               )
             )

    assert {:ok, nonmatching} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(
                 now: DateTime.add(@now, -30, :second),
                 search_attributes: %{"account_id" => "acct_other"}
               )
             )

    corrupt_run_thread(nonmatching.run_id)

    assert {:ok, %Page{items: [listed], next_cursor: nil}} =
             Jizoku.list_runs(
               [first: 10, attributes: %{"account_id" => "acct_match"}],
               runtime_options(visibility_policy: :auditor)
             )

    assert listed.run_id == matching.run_id
    assert listed.search_attributes == %{"account_id" => "acct_match"}
  end

  test "common status and attribute queries have index-backed plans" do
    insert_plan_rows()
    SQL.query!(Repo, "ANALYZE jizoku_run_search", [])
    SQL.query!(Repo, "SET LOCAL enable_seqscan = off", [])

    status_plan = explain(candidate_query(status: :running))
    assert status_plan =~ "jizoku_run_search_status_idx"

    attribute_plan = explain(candidate_query(attributes: %{"account_id" => "needle"}))
    assert attribute_plan =~ "jizoku_run_search_attributes_gin_idx"
  end

  test "keeps journal writes available before the additive projection migration" do
    SQL.query!(Repo, "ALTER TABLE jizoku_run_search RENAME TO unavailable_run_search", [])

    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_fallback"})
             )

    assert {:ok, inspected} = Jizoku.inspect_run(started.run_id, runtime_options())
    assert inspected.search_attributes == %{"account_id" => "acct_fallback"}

    assert {:ok, [listed]} = Jizoku.list_runs([], runtime_options())
    assert listed.run_id == started.run_id
  end

  test "falls back safely before the additive archive migration" do
    SQL.query!(
      Repo,
      "ALTER TABLE jizoku_run_search DROP COLUMN archive_reason, DROP COLUMN archived_at",
      []
    )

    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_archive_fallback"})
             )

    assert {:ok, _cancelled} = Jizoku.cancel(started.run_id, runtime_options())

    assert {:ok, archived} =
             Jizoku.archive_run(
               started.run_id,
               Keyword.delete(
                 runtime_options(reason: "migration_pending"),
                 :search_attribute_schema
               )
             )

    assert archived.archived?

    assert {:ok, []} = Jizoku.list_runs([], runtime_options())

    assert {:ok, [listed]} =
             Jizoku.list_runs(
               [archived: :only],
               runtime_options(visibility_policy: :auditor)
             )

    assert listed.run_id == started.run_id
    assert listed.archive_reason == "migration_pending"
  end

  test "falls back to journals while an existing projection is incomplete" do
    assert {:ok, matching} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_incomplete"})
             )

    assert {:ok, other} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_other"})
             )

    Repo.delete_all(
      from(run in RunSearch,
        where: run.partition_key == "" and run.run_id == ^other.run_id
      )
    )

    assert {:ok, %Page{items: [listed], next_cursor: nil}} =
             Jizoku.list_runs(
               [first: 10, attributes: %{"account_id" => "acct_incomplete"}],
               runtime_options(visibility_policy: :auditor)
             )

    assert listed.run_id == matching.run_id
  end

  test "rebuilds missing rows from journals within each explicit partition" do
    assert {:ok, legacy_run} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_legacy"})
             )

    assert {:ok, partitioned_run} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(
                 partition: "tenant_acme",
                 search_attributes: %{"account_id" => "acct_partitioned"}
               )
             )

    Repo.delete_all(RunSearch)

    assert {:ok, %{partition: nil, rebuilt: 1, catalog_revision: catalog_revision}} =
             Jizoku.rebuild_run_search_projection(journal_storage: @storage)

    assert catalog_revision > 0

    assert %RunSearch{search_attributes: %{"account_id" => "acct_legacy"}} =
             Repo.get_by!(RunSearch, partition_key: "", run_id: legacy_run.run_id)

    Repo.update_all(
      from(run in RunSearch, where: run.partition_key == "" and run.run_id == ^legacy_run.run_id),
      set: [status: "stale", search_attributes: %{}]
    )

    assert {:ok, %{partition: nil, rebuilt: 1}} =
             Jizoku.rebuild_run_search_projection(journal_storage: @storage)

    assert %RunSearch{status: "running", search_attributes: %{"account_id" => "acct_legacy"}} =
             Repo.get_by!(RunSearch, partition_key: "", run_id: legacy_run.run_id)

    refute Repo.get_by(RunSearch,
             partition_key: "tenant_acme",
             run_id: partitioned_run.run_id
           )

    assert {:ok,
            %{partition: "tenant_acme", rebuilt: 1, catalog_revision: partition_catalog_revision}} =
             Jizoku.rebuild_run_search_projection(
               journal_storage: @storage,
               partition: "tenant_acme"
             )

    assert partition_catalog_revision > 0

    assert %RunSearch{search_attributes: %{"account_id" => "acct_partitioned"}} =
             Repo.get_by!(RunSearch,
               partition_key: "tenant_acme",
               run_id: partitioned_run.run_id
             )
  end

  defp runtime_options(overrides \\ []) do
    Keyword.merge(
      [
        journal_storage: @storage,
        queue: @queue,
        now: @now,
        search_attribute_schema: @schema
      ],
      overrides
    )
  end

  defp corrupt_run_thread(run_id) do
    thread_id = Journal.thread_id({:run, run_id})

    {count, _rows} =
      Repo.update_all(
        from(entry in JournalEntry, where: entry.thread_id == ^thread_id),
        set: [entry: <<0, 1, 2>>]
      )

    assert count > 0
  end

  defp insert_plan_rows do
    now = DateTime.utc_now(:microsecond)

    rows =
      Enum.map(1..500, fn index ->
        %{
          partition_key: "plan",
          partition: "plan",
          run_id: "run-#{index}",
          workflow: "PlanWorkflow",
          status: if(rem(index, 2) == 0, do: "running", else: "completed"),
          terminal_status: nil,
          definition_version: "v1",
          search_attributes: %{
            "account_id" => if(index == 250, do: "needle", else: "acct-#{index}")
          },
          started_at:
            @now
            |> DateTime.add(-index, :second)
            |> DateTime.add(0, :microsecond),
          terminal_at: nil,
          thread_revision: 1,
          inserted_at: now,
          updated_at: now
        }
      end)

    assert {500, nil} = Repo.insert_all(RunSearch, rows)
  end

  defp candidate_query(overrides) do
    query = %{
      workflow: nil,
      status: nil,
      definition_version: nil,
      attributes: %{},
      started_after: nil,
      started_before: nil,
      terminal_after: nil,
      terminal_before: nil,
      cursor_position: nil,
      collection_limit: 10
    }

    EctoQuery.candidate_query(Map.merge(query, Map.new(overrides)), "plan")
  end

  defp explain(query) do
    {sql, params} = Repo.to_sql(:all, query)
    result = SQL.query!(Repo, "EXPLAIN (FORMAT JSON) " <> sql, params)
    Jason.encode!(result.rows)
  end
end
