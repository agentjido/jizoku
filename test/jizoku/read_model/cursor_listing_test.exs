defmodule Jizoku.ReadModel.CursorListingTest do
  use ExUnit.Case, async: false

  alias Jizoku.ReadModel.Listing
  alias Jizoku.ReadModel.Listing.Page
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage

  @storage {Jido.Storage.ETS, table: :jizoku_cursor_listing_test}
  @queue "default"
  @workflow "PaymentWorkflow"
  @now ~U[2026-08-16 20:00:00Z]
  @schema %{"account_id" => :string, "priority" => :integer}

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "pages every matching run exactly once in stable newest-first order" do
    started_at = DateTime.add(@now, -300, :second)

    Enum.each(
      [
        {"run-a", started_at},
        {"run-b", DateTime.add(started_at, 1, :second)},
        {"run-c", DateTime.add(started_at, 1, :second)},
        {"run-d", DateTime.add(started_at, 2, :second)},
        {"run-e", DateTime.add(started_at, 3, :second)}
      ],
      fn {run_id, time} -> append_run(@storage, run_id, time) end
    )

    assert {:ok, %Page{items: first, next_cursor: first_cursor}} =
             Listing.list(@storage, [first: 2], now: @now)

    assert Enum.map(first, & &1.run_id) == ["run-e", "run-d"]
    assert is_binary(first_cursor)

    assert {:ok, %Page{items: second, next_cursor: second_cursor}} =
             Listing.list(@storage, [first: 2, after: first_cursor], now: @now)

    assert Enum.map(second, & &1.run_id) == ["run-c", "run-b"]
    assert is_binary(second_cursor)

    assert {:ok, %Page{items: third, next_cursor: nil}} =
             Listing.list(@storage, [first: 2, after: second_cursor], now: @now)

    assert Enum.map(third, & &1.run_id) == ["run-a"]

    all_ids = Enum.map(first ++ second ++ third, & &1.run_id)
    assert all_ids == Enum.uniq(all_ids)

    assert {:ok, legacy_runs} = Listing.list(@storage, [limit: 2], now: @now)
    assert is_list(legacy_runs)
    assert Enum.map(legacy_runs, & &1.run_id) == ["run-e", "run-d"]
  end

  test "returns structured errors for malformed, expired, version, and query mismatches" do
    append_run(@storage, "run-a", DateTime.add(@now, -60, :second))
    append_run(@storage, "run-b", DateTime.add(@now, -30, :second))

    assert {:error, {:invalid_cursor, :malformed}} =
             Listing.list(@storage, [first: 1, after: "not-a-cursor"], now: @now)

    assert {:ok, %Page{next_cursor: cursor}} =
             Listing.list(@storage, [first: 1], now: @now)

    assert {:error, {:invalid_cursor, :expired}} =
             Listing.list(
               @storage,
               [first: 1, after: cursor],
               now: DateTime.add(@now, 3_600, :second)
             )

    assert {:error, {:invalid_cursor, :query_mismatch}} =
             Listing.list(
               @storage,
               [first: 1, after: cursor, status: :running],
               now: @now
             )

    version_cursor =
      %{
        "version" => 999,
        "started_at_us" => DateTime.to_unix(@now, :microsecond),
        "run_id" => "run-a",
        "query" => "irrelevant",
        "expires_at_us" => DateTime.to_unix(DateTime.add(@now, 60, :second), :microsecond)
      }
      |> Jason.encode!()
      |> Base.url_encode64(padding: false)

    assert {:error, {:invalid_cursor, {:unsupported_version, 999}}} =
             Listing.list(@storage, [first: 1, after: version_cursor], now: @now)
  end

  test "filters only approved operational fields and applies listing visibility" do
    matching_started_at = DateTime.add(@now, -120, :second)
    matching_terminal_at = DateTime.add(@now, -30, :second)

    append_run(@storage, "run-match", matching_started_at,
      search_attributes: %{"account_id" => "acct_123", "priority" => 3},
      definition_version: "v2",
      terminal_at: matching_terminal_at,
      terminal_status: :completed
    )

    append_run(@storage, "run-other", DateTime.add(@now, -60, :second),
      search_attributes: %{"account_id" => "acct_999", "priority" => 3},
      definition_version: "v1"
    )

    filters = [
      status: :completed,
      attributes: %{"account_id" => "acct_123"},
      definition_version: "v2",
      partition: nil,
      started_after: DateTime.add(@now, -180, :second),
      started_before: DateTime.add(@now, -90, :second),
      terminal_after: DateTime.add(@now, -60, :second),
      terminal_before: @now
    ]

    assert {:ok, [external]} =
             Listing.list(@storage, filters,
               now: @now,
               search_attribute_schema: @schema
             )

    assert external.run_id == "run-match"
    assert external.started_at == matching_started_at
    assert external.terminal_at == matching_terminal_at
    assert external.search_attributes == %{}

    assert {:ok, [auditor]} =
             Listing.list(@storage, filters,
               now: @now,
               search_attribute_schema: @schema,
               visibility_policy: :auditor
             )

    assert auditor.search_attributes == %{"account_id" => "acct_123", "priority" => 3}

    assert {:ok, []} =
             Listing.list(@storage, Keyword.put(filters, :partition, "tenant-other"),
               now: @now,
               search_attribute_schema: @schema
             )
  end

  test "binds cursors to the explicitly selected storage partition" do
    {:ok, tenant_a} = Storage.scope(@storage, "tenant-a")
    {:ok, tenant_b} = Storage.scope(@storage, "tenant-b")

    append_run(tenant_a, "run-a1", DateTime.add(@now, -20, :second))
    append_run(tenant_a, "run-a2", DateTime.add(@now, -10, :second))
    append_run(tenant_b, "run-b1", DateTime.add(@now, -10, :second))

    assert {:ok, %Page{items: [tenant_a_run], next_cursor: cursor}} =
             Listing.list(tenant_a, [first: 1], now: @now)

    assert tenant_a_run.run_id == "run-a2"
    assert tenant_a_run.partition == "tenant-a"

    assert {:error, {:invalid_cursor, :query_mismatch}} =
             Listing.list(tenant_b, [first: 1, after: cursor], now: @now)

    assert {:ok, tenant_b_runs} = Listing.list(tenant_b, [], now: @now)
    assert Enum.map(tenant_b_runs, & &1.run_id) == ["run-b1"]
  end

  test "exposes paged queries through the public list API" do
    append_run(@storage, "run-public", DateTime.add(@now, -10, :second),
      search_attributes: %{"account_id" => "acct_public"}
    )

    assert {:ok, %Page{items: [run], next_cursor: nil}} =
             Jizoku.list_runs(
               [first: 10, attributes: %{"account_id" => "acct_public"}],
               journal_storage: @storage,
               now: @now,
               search_attribute_schema: @schema,
               visibility_policy: :auditor
             )

    assert run.run_id == "run-public"
    assert run.search_attributes == %{"account_id" => "acct_public"}
  end

  test "rejects invalid page and approved-filter contracts" do
    assert {:error, {:invalid_option, {:first, :invalid}}} =
             Listing.list(@storage, [first: 101], now: @now)

    assert {:error, {:invalid_option, {:first, :invalid}}} =
             Listing.list(@storage, [first: nil], now: @now)

    assert {:error, {:invalid_option, {:pagination, :conflicting}}} =
             Listing.list(@storage, [first: 10, limit: 10], now: @now)

    assert {:error, {:invalid_option, {:attributes, :invalid}}} =
             Listing.list(
               @storage,
               [attributes: %{"unknown" => "value"}],
               now: @now,
               search_attribute_schema: @schema
             )

    assert {:error, {:invalid_option, {:partition, :invalid}}} =
             Listing.list(@storage, [partition: :tenant], now: @now)

    assert {:error, {:invalid_option, {:terminal_after, :invalid}}} =
             Listing.list(@storage, [terminal_after: "yesterday"], now: @now)
  end

  defp append_run(storage, run_id, started_at, opts \\ []) do
    search_attributes = Keyword.get(opts, :search_attributes, %{})
    definition_version = Keyword.get(opts, :definition_version, "v1")

    append_entry(
      storage,
      :run_cataloged,
      %{
        run_id: run_id,
        workflow: @workflow,
        queue: @queue,
        occurred_at: started_at
      }
    )

    append_entry(
      storage,
      :run_started,
      %{
        run_id: run_id,
        workflow: @workflow,
        search_attributes: search_attributes,
        definition_version: definition_version,
        occurred_at: started_at
      }
    )

    case Keyword.get(opts, :terminal_at) do
      %DateTime{} = terminal_at ->
        append_entry(
          storage,
          :run_terminal,
          %{
            run_id: run_id,
            status: Keyword.fetch!(opts, :terminal_status),
            occurred_at: terminal_at
          }
        )

      nil ->
        :ok
    end
  end

  defp append_entry(storage, type, data) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, data)
    assert {:ok, _thread} = Journal.append_entries(storage, [entry])
  end

  defp cleanup_storage do
    Enum.each(
      [
        :jizoku_cursor_listing_test_checkpoints,
        :jizoku_cursor_listing_test_threads,
        :jizoku_cursor_listing_test_thread_meta
      ],
      &delete_table/1
    )
  end

  defp delete_table(table_name) do
    case :ets.whereis(table_name) do
      :undefined -> :ok
      table -> :ets.delete(table)
    end
  end
end
