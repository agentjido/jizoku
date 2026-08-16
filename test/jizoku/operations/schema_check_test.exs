defmodule Jizoku.Operations.SchemaCheckTest do
  use Jizoku.DataCase, async: true

  alias Jizoku.Config
  alias Jizoku.Operations.SchemaCheck
  alias Jizoku.Persistence.Schema
  alias Jizoku.Runtime.Journal.Storage

  test "reports the migrated test database as current" do
    config = Config.load!()

    assert %{
             status: :current,
             baseline: 3,
             prefix: "public",
             missing: [],
             mismatched: []
           } = SchemaCheck.check(config)
  end

  test "reports missing baseline objects as behind" do
    {columns, indexes, foreign_keys} = current_catalog()

    missing_table_columns =
      Enum.reject(columns, fn [table | _rest] -> table == "jizoku_journal_checkpoints" end)

    missing_table_indexes =
      Enum.reject(indexes, fn [table | _rest] -> table == "jizoku_journal_checkpoints" end)

    assert %{status: :behind, missing: missing} =
             SchemaCheck.from_catalog(
               "public",
               missing_table_columns,
               missing_table_indexes,
               foreign_keys
             )

    assert %{kind: :table, table: "jizoku_journal_checkpoints"} in missing
  end

  test "reports wrong column definitions as incompatible" do
    {columns, indexes, foreign_keys} = current_catalog()

    incompatible_columns =
      Enum.map(columns, fn
        ["jizoku_journal_threads", "rev", "int8", "NO", nil, nil] ->
          ["jizoku_journal_threads", "rev", "text", "YES", nil, nil]

        row ->
          row
      end)

    assert %{status: :incompatible, mismatched: mismatched} =
             SchemaCheck.from_catalog("public", incompatible_columns, indexes, foreign_keys)

    assert Enum.any?(mismatched, &(&1.kind == :column and &1.column == "rev"))
  end

  test "reports a missing required column as behind" do
    {columns, indexes, foreign_keys} = current_catalog()

    columns_without_rev =
      Enum.reject(columns, fn [table, column | _rest] ->
        table == "jizoku_journal_threads" and column == "rev"
      end)

    assert %{status: :behind, missing: missing} =
             SchemaCheck.from_catalog("public", columns_without_rev, indexes, foreign_keys)

    assert %{kind: :column, table: "jizoku_journal_threads", column: "rev"} in missing
  end

  test "reports a missing unique index as behind" do
    {columns, indexes, foreign_keys} = current_catalog()

    indexes_without_unique =
      Enum.reject(indexes, fn [_table, primary? | _rest] -> not primary? end)

    assert %{status: :behind, missing: missing} =
             SchemaCheck.from_catalog("public", columns, indexes_without_unique, foreign_keys)

    assert Enum.any?(missing, &(&1.kind == :unique_index))
  end

  test "reports partial primary keys as incompatible" do
    {columns, indexes, foreign_keys} = current_catalog()

    partial_primary_indexes =
      Enum.map(indexes, fn
        ["jizoku_journal_threads", true, true, false, columns] ->
          ["jizoku_journal_threads", true, true, true, columns]

        index ->
          index
      end)

    assert %{status: :incompatible, mismatched: mismatched} =
             SchemaCheck.from_catalog("public", columns, partial_primary_indexes, foreign_keys)

    assert Enum.any?(mismatched, &(&1.kind == :primary_key))
  end

  test "reports non-unique matching indexes as incompatible" do
    {columns, indexes, foreign_keys} = current_catalog()

    non_unique_indexes =
      Enum.map(indexes, fn
        ["jizoku_journal_entries", false, true, false, columns] ->
          ["jizoku_journal_entries", false, false, false, columns]

        index ->
          index
      end)

    assert %{status: :incompatible, mismatched: mismatched} =
             SchemaCheck.from_catalog("public", columns, non_unique_indexes, foreign_keys)

    assert Enum.any?(mismatched, &(&1.kind == :unique_index))
  end

  test "reports an incompatible foreign-key delete rule" do
    {columns, indexes, [foreign_key]} = current_catalog()
    incompatible_foreign_key = List.replace_at(foreign_key, 4, "RESTRICT")

    assert %{status: :incompatible, mismatched: mismatched} =
             SchemaCheck.from_catalog("public", columns, indexes, [incompatible_foreign_key])

    assert Enum.any?(mismatched, &(&1.kind == :foreign_key))
  end

  test "reports SQL schema checks as not applicable for non-Ecto storage" do
    storage = %Storage{adapter: Jido.Storage.ETS, opts: [], config: Jido.Storage.ETS}

    assert %{status: :not_applicable, prefix: nil} = SchemaCheck.check(storage)
  end

  test "reports unexpected columns without treating them as drift" do
    {columns, indexes, foreign_keys} = current_catalog()

    columns_with_legacy = [
      ["jizoku_journal_threads", "legacy_value", "text", "YES", nil, nil] | columns
    ]

    assert %{
             status: :current,
             unexpected: [
               %{
                 kind: :column,
                 table: "jizoku_journal_threads",
                 column: "legacy_value"
               }
             ]
           } = SchemaCheck.from_catalog("public", columns_with_legacy, indexes, foreign_keys)
  end

  test "reports invalid storage values as unavailable" do
    assert %{
             status: :unavailable,
             reason: :invalid_storage,
             next_actions: [:verify_repo_connectivity_and_catalog_permissions]
           } =
             SchemaCheck.check(:invalid)
  end

  test "sanitizes query failures from an unavailable repo" do
    storage = %Storage{
      adapter: Jizoku.Runtime.Journal.Storage.Ecto,
      opts: [repo: __MODULE__.MissingRepo, prefix: "public"],
      config: Jizoku.Runtime.Journal.Storage.Ecto
    }

    assert %{
             status: :unavailable,
             reason: :query_failed,
             next_actions: [:verify_repo_connectivity_and_catalog_permissions]
           } = SchemaCheck.check(storage)
  end

  defp current_catalog do
    columns =
      Enum.flat_map(Schema.tables(), fn {table, table_spec} ->
        Enum.map(table_spec.columns, fn {column, spec} ->
          [
            table,
            column,
            spec.type,
            if(spec.nullable?, do: "YES", else: "NO"),
            Map.get(spec, :length),
            Map.get(spec, :precision)
          ]
        end)
      end)

    indexes =
      Enum.flat_map(Schema.tables(), fn {table, table_spec} ->
        [[table, true, true, false, table_spec.primary_key]] ++
          Enum.map(Map.get(table_spec, :unique, []), &[table, false, true, false, &1]) ++
          Enum.map(Map.get(table_spec, :indexes, []), &[table, false, false, false, &1])
      end)

    foreign_keys = [
      [
        "jizoku_journal_entries",
        "thread_id",
        "jizoku_journal_threads",
        "id",
        "CASCADE"
      ]
    ]

    {columns, indexes, foreign_keys}
  end
end
