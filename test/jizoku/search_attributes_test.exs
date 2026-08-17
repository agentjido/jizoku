defmodule Jizoku.SearchAttributesTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Jizoku.ReadModel.Visibility
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.SearchAttributes

  @storage {ETS, table: :jizoku_search_attributes_test}
  @queue "search-attributes"
  @started_at ~U[2026-08-16 18:00:00Z]
  @schema %{
    "account_id" => :string,
    "active" => :boolean,
    "priority" => :integer,
    "regions" => {:list, :string}
  }

  defmodule Record do
    use Jizoku.Step, name: "search_attributes_record"

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
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "validates bounded allowlisted JSON-safe attributes" do
    assert {:ok,
            %{
              "account_id" => "acct_123",
              "active" => true,
              "priority" => 3,
              "regions" => ["eu-west", "us-east"]
            }} =
             SearchAttributes.normalize(
               %{
                 "regions" => ["eu-west", "us-east"],
                 "priority" => 3,
                 "active" => true,
                 "account_id" => "acct_123"
               },
               @schema
             )

    assert {:error, {:invalid_search_attributes, errors}} =
             SearchAttributes.normalize(%{"unknown" => "value"}, @schema)

    assert Enum.any?(errors, &(&1.code == :unknown_key and &1.key == "unknown"))

    assert {:error, {:invalid_search_attributes, atom_key_errors}} =
             SearchAttributes.normalize(%{account_id: "acct_123"}, @schema)

    assert Enum.any?(atom_key_errors, &(&1.code == :invalid_key))

    assert {:error, {:invalid_search_attributes, oversized_errors}} =
             SearchAttributes.normalize(
               %{"account_id" => String.duplicate("x", 257)},
               @schema
             )

    assert Enum.any?(oversized_errors, &(&1.code == :value_too_large))

    assert {:error, {:invalid_search_attributes, invalid_oversized_errors}} =
             SearchAttributes.normalize(
               %{"account_id" => :binary.copy(<<255>>, 257)},
               @schema
             )

    assert Enum.any?(invalid_oversized_errors, &(&1.code == :value_too_large))

    assert {:error, {:invalid_search_attributes, list_errors}} =
             SearchAttributes.normalize(
               %{"regions" => Enum.map(1..17, &"region-#{&1}")},
               @schema
             )

    assert Enum.any?(list_errors, &(&1.code == :list_too_large))

    refute SearchAttributes.valid_persisted?(%{
             "regions" => Enum.map(1..17, &"region-#{&1}")
           })

    refute SearchAttributes.valid_persisted?(%{"regions" => ["eu" | "improper"]})

    high_cardinality_schema =
      Map.new(1..33, fn index -> {"attribute_#{index}", :string} end)

    assert {:error, {:invalid_search_attribute_schema, cardinality_errors}} =
             SearchAttributes.validate_schema(high_cardinality_schema)

    assert Enum.any?(cardinality_errors, &(&1.code == :too_many_keys))

    assert SearchAttributes.valid_idempotency_key?("support:update")
    refute SearchAttributes.valid_idempotency_key?(String.duplicate("x", 513))
    refute SearchAttributes.valid_idempotency_key?(<<255>>)
  end

  test "persists initial attributes and idempotent fenced updates across checkpoint loss" do
    initial = %{"account_id" => "acct_123", "priority" => 1}

    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: initial)
             )

    assert started.search_attributes == initial

    updated_at = DateTime.add(@started_at, 1, :second)
    changes = %{"active" => true, "priority" => 2}

    assert {:ok, updated} =
             Jizoku.update_search_attributes(
               started.run_id,
               changes,
               runtime_options(
                 now: updated_at,
                 idempotency_key: "support:update-priority"
               )
             )

    assert updated.search_attributes == %{
             "account_id" => "acct_123",
             "active" => true,
             "priority" => 2
           }

    assert {:ok, external} = Visibility.redact(updated, %{}, :external)
    assert external.search_attributes == %{}

    assert {:ok, auditor} = Visibility.redact(updated, %{}, :auditor)
    assert auditor.search_attributes == updated.search_attributes

    assert {:ok, duplicate} =
             Jizoku.update_search_attributes(
               started.run_id,
               changes,
               runtime_options(
                 now: DateTime.add(updated_at, 1, :second),
                 idempotency_key: "support:update-priority"
               )
             )

    assert duplicate.thread_revisions == updated.thread_revisions

    assert {:error, {:idempotency_conflict, "support:update-priority"}} =
             Jizoku.update_search_attributes(
               started.run_id,
               %{"priority" => 3},
               runtime_options(
                 now: DateTime.add(updated_at, 2, :second),
                 idempotency_key: "support:update-priority"
               )
             )

    assert :ok = delete_run_checkpoint(started.run_id)
    assert {:ok, rebuilt} = Jizoku.inspect_run(started.run_id, runtime_options())
    assert rebuilt.search_attributes == updated.search_attributes

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, started.run_id})
    assert Enum.count(entries, &(&1.type == :search_attributes_updated)) == 1
  end

  test "merges concurrent updates through optimistic run-thread fencing" do
    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_concurrent"})
             )

    updates =
      Enum.map(1..8, fn index ->
        {%{"regions" => ["region-#{index}"]}, "support:set-region-#{index}"}
      end)

    results =
      updates
      |> Task.async_stream(
        fn {changes, idempotency_key} ->
          Jizoku.update_search_attributes(
            started.run_id,
            changes,
            runtime_options(idempotency_key: idempotency_key)
          )
        end,
        max_concurrency: 8,
        ordered: false
      )
      |> Enum.to_list()

    assert Enum.all?(results, fn result -> match?({:ok, {:ok, _snapshot}}, result) end)

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, started.run_id})
    assert Enum.count(entries, &(&1.type == :search_attributes_updated)) == 8

    assert {:ok, inspected} = Jizoku.inspect_run(started.run_id, runtime_options())

    assert inspected.search_attributes["account_id"] == "acct_concurrent"
    assert inspected.search_attributes["regions"] in Enum.map(1..8, &["region-#{&1}"])
  end

  test "uses versioned canonical fingerprints and accepts the legacy persisted digest" do
    changes = %{
      "account_id" => "acct_123",
      "active" => true,
      "priority" => 3,
      "regions" => ["eu-west", "us-east"]
    }

    legacy_fingerprint = "TDnZqlSc53vtGxwQMSDQR4UcwxWXnwELBi2pGIfjR6g"
    versioned_fingerprint = "v1:5ScnbOgiMfFuh2PUZIJpRsHOaRYUCxABS6hRSk5vl7Y"

    assert SearchAttributes.fingerprint(changes) == versioned_fingerprint

    assert SearchAttributes.fingerprint(Map.new(Enum.reverse(Enum.to_list(changes)))) ==
             versioned_fingerprint

    assert SearchAttributes.fingerprint_matches?(changes, legacy_fingerprint)
    assert SearchAttributes.fingerprint_matches?(changes, versioned_fingerprint)
    refute SearchAttributes.fingerprint_matches?(changes, "v1:invalid")

    assert {:ok, started} = Jizoku.start(Workflow, :manual, %{}, runtime_options())

    assert {:ok, entry} =
             DispatchProtocol.new_entry(:search_attributes_updated, %{
               run_id: started.run_id,
               changes: changes,
               fingerprint: legacy_fingerprint,
               idempotency_key: "support:legacy-fingerprint",
               occurred_at: @started_at
             })

    assert {:ok, _thread} = Journal.append_entries(@storage, [entry])
    assert :ok = delete_run_checkpoint(started.run_id)
    assert {:ok, rebuilt} = Jizoku.inspect_run(started.run_id, runtime_options())
    assert rebuilt.search_attributes == changes
    assert rebuilt.anomalies == []

    assert {:ok, duplicate} =
             Jizoku.update_search_attributes(
               started.run_id,
               changes,
               runtime_options(idempotency_key: "support:legacy-fingerprint")
             )

    assert duplicate.thread_revisions == rebuilt.thread_revisions

    assert {:ok, %{entries: entries}} = Journal.load_thread(@storage, {:run, started.run_id})
    assert Enum.count(entries, &(&1.type == :search_attributes_updated)) == 1
  end

  test "loads the host allowlist from application configuration" do
    previous_schema = Application.get_env(:jizoku, :search_attribute_schema)
    Application.put_env(:jizoku, :search_attribute_schema, @schema)

    on_exit(fn ->
      case previous_schema do
        nil -> Application.delete_env(:jizoku, :search_attribute_schema)
        schema -> Application.put_env(:jizoku, :search_attribute_schema, schema)
      end
    end)

    opts =
      [search_attributes: %{"account_id" => "acct_configured"}]
      |> runtime_options()
      |> Keyword.delete(:search_attribute_schema)

    assert {:ok, started} = Jizoku.start(Workflow, :manual, %{}, opts)
    assert started.search_attributes == %{"account_id" => "acct_configured"}
  end

  test "includes initial attributes in duplicate-start identity" do
    run_id = Ecto.UUID.generate()

    opts =
      runtime_options(
        run_id: run_id,
        search_attributes: %{"account_id" => "acct_duplicate"}
      )

    assert {:ok, started} = Jizoku.start(Workflow, :manual, %{}, opts)
    assert {:ok, duplicate} = Jizoku.start(Workflow, :manual, %{}, opts)
    assert duplicate.thread_revisions == started.thread_revisions

    assert {:error, :conflict} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               Keyword.put(opts, :search_attributes, %{"account_id" => "acct_conflict"})
             )
  end

  test "allows operational attributes to be updated after terminalization" do
    assert {:ok, started} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"account_id" => "acct_terminal"})
             )

    assert {:ok, completed} =
             Jizoku.execute_next(
               [owner_id: "search-attribute-worker"]
               |> runtime_options()
               |> Keyword.delete(:search_attribute_schema)
             )

    assert completed.status == :completed

    assert {:ok, updated} =
             Jizoku.update_search_attributes(
               started.run_id,
               %{"priority" => 9},
               runtime_options(idempotency_key: "support:terminal-priority")
             )

    assert updated.status == :completed

    assert updated.search_attributes == %{
             "account_id" => "acct_terminal",
             "priority" => 9
           }

    assert updated.anomalies == []
  end

  test "fails closed when attributes are not allowlisted or schema is missing" do
    assert {:error, {:invalid_search_attributes, errors}} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(search_attributes: %{"unknown" => "value"})
             )

    assert Enum.any?(errors, &(&1.code == :unknown_key))

    assert {:error, {:invalid_search_attributes, missing_schema_errors}} =
             Jizoku.start(
               Workflow,
               :manual,
               %{},
               runtime_options(
                 search_attributes: %{"account_id" => "acct_123"},
                 search_attribute_schema: nil
               )
             )

    assert Enum.any?(missing_schema_errors, &(&1.code == :schema_required))
  end

  defp runtime_options(overrides \\ []) do
    Keyword.merge(
      [
        journal_storage: @storage,
        queue: @queue,
        now: @started_at,
        search_attribute_schema: @schema
      ],
      overrides
    )
  end

  defp delete_run_checkpoint(run_id) do
    {adapter, opts} = @storage

    adapter.delete_checkpoint(
      {"jizoku", :checkpoint, Journal.thread_id({:run, run_id})},
      opts
    )
  end

  defp cleanup_storage do
    Enum.each(
      [
        :jizoku_search_attributes_test_checkpoints,
        :jizoku_search_attributes_test_threads,
        :jizoku_search_attributes_test_thread_meta
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
