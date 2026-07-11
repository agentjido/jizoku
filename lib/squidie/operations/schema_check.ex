defmodule Squidie.Operations.SchemaCheck do
  @moduledoc """
  Performs a read-only structural check of the configured Squidie database schema.

  The check compares required tables, columns, indexes, and foreign keys with
  Squidie's current baseline. It never runs migrations or writes migration
  metadata.
  """

  alias Ecto.Adapters.SQL
  alias Squidie.Config
  alias Squidie.Persistence.Schema
  alias Squidie.Runtime.Journal.Storage

  @columns_sql """
  SELECT table_name,
         column_name,
         udt_name,
         is_nullable,
         character_maximum_length,
         datetime_precision
  FROM information_schema.columns
  WHERE table_schema = $1 AND table_name = ANY($2)
  ORDER BY table_name, ordinal_position
  """

  @indexes_sql """
  SELECT table_relation.relname,
         index_definition.indisprimary,
         index_definition.indisunique,
         index_definition.indpred IS NOT NULL,
         array_agg(attribute.attname ORDER BY indexed_column.ordinality)
  FROM pg_catalog.pg_class AS table_relation
  JOIN pg_catalog.pg_namespace AS namespace
    ON namespace.oid = table_relation.relnamespace
  JOIN pg_catalog.pg_index AS index_definition
    ON index_definition.indrelid = table_relation.oid
  JOIN LATERAL unnest(index_definition.indkey)
    WITH ORDINALITY AS indexed_column(attribute_number, ordinality)
    ON TRUE
  JOIN pg_catalog.pg_attribute AS attribute
    ON attribute.attrelid = table_relation.oid
   AND attribute.attnum = indexed_column.attribute_number
  WHERE namespace.nspname = $1 AND table_relation.relname = ANY($2)
  GROUP BY table_relation.relname,
           index_definition.indexrelid,
           index_definition.indisprimary,
           index_definition.indisunique,
           index_definition.indpred
  ORDER BY table_relation.relname, index_definition.indexrelid
  """

  @foreign_keys_sql """
  SELECT source_constraint.table_name,
         source_column.column_name,
         target_constraint.table_name,
         target_column.column_name,
         referential.delete_rule
  FROM information_schema.table_constraints AS source_constraint
  JOIN information_schema.key_column_usage AS source_column
    ON source_column.constraint_schema = source_constraint.constraint_schema
   AND source_column.constraint_name = source_constraint.constraint_name
  JOIN information_schema.referential_constraints AS referential
    ON referential.constraint_schema = source_constraint.constraint_schema
   AND referential.constraint_name = source_constraint.constraint_name
  JOIN information_schema.table_constraints AS target_constraint
    ON target_constraint.constraint_schema = referential.unique_constraint_schema
   AND target_constraint.constraint_name = referential.unique_constraint_name
  JOIN information_schema.key_column_usage AS target_column
    ON target_column.constraint_schema = target_constraint.constraint_schema
   AND target_column.constraint_name = target_constraint.constraint_name
   AND target_column.ordinal_position = source_column.position_in_unique_constraint
  WHERE source_constraint.constraint_type = 'FOREIGN KEY'
    AND source_constraint.table_schema = $1
    AND source_constraint.table_name = ANY($2)
  ORDER BY source_constraint.table_name, source_column.ordinal_position
  """

  @type result :: map()

  @spec check(Config.t() | Storage.t()) :: result()
  @doc "Checks the configured Ecto schema or reports that SQL drift is not applicable."
  def check(%Config{journal_storage: storage}), do: check(storage)

  def check(%Storage{adapter: Squidie.Runtime.Journal.Storage.Ecto, opts: opts}) do
    repo = Keyword.fetch!(opts, :repo)

    with {:ok, prefix} <- schema_prefix(repo, opts),
         {:ok, columns} <- query_rows(repo, @columns_sql, [prefix, Schema.table_names()]),
         {:ok, indexes} <- query_rows(repo, @indexes_sql, [prefix, Schema.table_names()]),
         {:ok, foreign_keys} <-
           query_rows(repo, @foreign_keys_sql, [prefix, Schema.table_names()]) do
      from_catalog(prefix, columns, indexes, foreign_keys)
    else
      {:error, reason} -> unavailable(reason)
    end
  end

  def check(%Storage{}) do
    base_result(:not_applicable, nil, [], [], [], [
      :verify_custom_journal_storage_separately
    ])
  end

  def check(_invalid), do: unavailable(:invalid_storage)

  @doc "Builds a schema result from normalized PostgreSQL catalog rows."
  @spec from_catalog(String.t(), list(), list(), list()) :: result()
  def from_catalog(prefix, column_rows, index_rows, foreign_key_rows) do
    compare(prefix, column_rows, index_rows, foreign_key_rows)
  end

  defp schema_prefix(repo, opts) do
    case Keyword.get(opts, :prefix) do
      prefix when is_binary(prefix) and prefix != "" -> {:ok, prefix}
      _missing -> current_schema(repo)
    end
  end

  defp current_schema(repo) do
    case query_rows(repo, "SELECT current_schema()", []) do
      {:ok, [[prefix]]} when is_binary(prefix) -> {:ok, prefix}
      {:ok, _unexpected} -> {:error, :invalid_current_schema}
      {:error, _reason} = error -> error
    end
  end

  defp query_rows(repo, sql, params) do
    case SQL.query(repo, sql, params) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, reason} -> {:error, query_failure_reason(reason)}
    end
  rescue
    exception in [
      ArgumentError,
      RuntimeError,
      DBConnection.ConnectionError,
      Ecto.QueryError,
      Postgrex.Error
    ] ->
      {:error, query_failure_reason(exception)}
  end

  defp compare(prefix, column_rows, index_rows, foreign_key_rows) do
    actual_columns =
      Map.new(column_rows, fn [table, column, type, nullable, length, precision] ->
        spec =
          %{type: type, nullable?: nullable == "YES"}
          |> maybe_put_measurement(:length, length)
          |> maybe_put_measurement(:precision, precision)

        {{table, column}, spec}
      end)

    actual_tables = MapSet.new(actual_columns, fn {{table, _column}, _spec} -> table end)

    expected = Schema.tables()

    missing_tables =
      expected
      |> Map.keys()
      |> Enum.reject(&MapSet.member?(actual_tables, &1))
      |> Enum.map(&%{kind: :table, table: &1})

    {missing_columns, mismatched_columns} = compare_columns(expected, actual_columns)
    {missing_indexes, mismatched_indexes} = compare_indexes(expected, index_rows)

    {missing_foreign_keys, mismatched_foreign_keys} =
      compare_foreign_keys(expected, foreign_key_rows)

    missing =
      Enum.sort_by(
        missing_tables ++ missing_columns ++ missing_indexes ++ missing_foreign_keys,
        &inspect/1
      )

    mismatched =
      Enum.sort_by(
        mismatched_columns ++ mismatched_indexes ++ mismatched_foreign_keys,
        &inspect/1
      )

    unexpected = unexpected_columns(expected, actual_columns)
    status = classify(missing, mismatched)

    base_result(status, prefix, missing, mismatched, unexpected, next_actions(status))
  end

  defp compare_columns(expected, actual) do
    Enum.reduce(expected, {[], []}, fn {table, table_spec}, acc ->
      Enum.reduce(table_spec.columns, acc, fn {column, column_spec}, column_acc ->
        compare_column(table, column, column_spec, actual, column_acc)
      end)
    end)
  end

  defp compare_column(table, column, column_spec, actual, {missing, mismatched}) do
    case Map.fetch(actual, {table, column}) do
      :error ->
        {[%{kind: :column, table: table, column: column} | missing], mismatched}

      {:ok, ^column_spec} ->
        {missing, mismatched}

      {:ok, actual_spec} ->
        mismatch = %{
          kind: :column,
          table: table,
          column: column,
          expected: column_spec,
          actual: actual_spec
        }

        {missing, [mismatch | mismatched]}
    end
  end

  defp compare_indexes(expected, rows) do
    indexes =
      Enum.map(rows, fn [table, primary?, unique?, partial?, columns] ->
        %{
          table: table,
          primary?: primary?,
          unique?: unique?,
          partial?: partial?,
          columns: columns
        }
      end)

    Enum.reduce(expected, {[], []}, fn {table, table_spec}, {missing, mismatched} ->
      requirements =
        [%{kind: :primary_key, columns: table_spec.primary_key}] ++
          Enum.map(Map.get(table_spec, :unique, []), &%{kind: :unique_index, columns: &1})

      Enum.reduce(requirements, {missing, mismatched}, fn requirement, acc ->
        compare_index_requirement(table, requirement, indexes, acc)
      end)
    end)
  end

  defp compare_index_requirement(table, requirement, indexes, {missing, mismatched}) do
    matches = Enum.filter(indexes, &(&1.table == table and &1.columns == requirement.columns))

    case {requirement.kind, matches} do
      {_kind, []} ->
        {[object_finding(requirement.kind, table, requirement.columns) | missing], mismatched}

      {:primary_key, matches} ->
        matches
        |> Enum.any?(&(&1.primary? and not &1.partial?))
        |> compare_index_match(:primary_key, table, requirement.columns, missing, mismatched)

      {:unique_index, matches} ->
        matches
        |> Enum.any?(&(&1.unique? and not &1.partial?))
        |> compare_index_match(:unique_index, table, requirement.columns, missing, mismatched)
    end
  end

  defp compare_index_match(true, _kind, _table, _columns, missing, mismatched),
    do: {missing, mismatched}

  defp compare_index_match(false, kind, table, columns, missing, mismatched),
    do: {missing, [object_finding(kind, table, columns) | mismatched]}

  defp compare_foreign_keys(expected, rows) do
    actual =
      Enum.map(rows, fn [table, column, target_table, target_column, on_delete] ->
        %{
          table: table,
          columns: [column],
          references: %{table: target_table, columns: [target_column]},
          on_delete: on_delete
        }
      end)

    expected
    |> Enum.flat_map(fn {table, table_spec} ->
      Enum.map(Map.get(table_spec, :foreign_keys, []), &Map.put(&1, :table, table))
    end)
    |> Enum.reduce({[], []}, fn requirement, {missing, mismatched} ->
      same_columns =
        Enum.filter(
          actual,
          &(&1.table == requirement.table and &1.columns == requirement.columns)
        )

      cond do
        same_columns == [] ->
          {[object_finding(:foreign_key, requirement.table, requirement.columns) | missing],
           mismatched}

        Enum.any?(same_columns, &(&1 == requirement)) ->
          {missing, mismatched}

        true ->
          {missing,
           [
             %{
               kind: :foreign_key,
               table: requirement.table,
               columns: requirement.columns,
               expected: Map.drop(requirement, [:table]),
               actual: Enum.map(same_columns, &Map.drop(&1, [:table]))
             }
             | mismatched
           ]}
      end
    end)
  end

  defp unexpected_columns(expected, actual) do
    actual
    |> Enum.reject(fn {{table, column}, _spec} ->
      match?(%{columns: %{^column => _}}, Map.get(expected, table))
    end)
    |> Enum.map(fn {{table, column}, spec} ->
      %{kind: :column, table: table, column: column, actual: spec}
    end)
    |> Enum.sort_by(&{&1.table, &1.column})
  end

  defp object_finding(kind, table, columns) do
    %{kind: kind, table: table, columns: columns}
  end

  defp maybe_put_measurement(spec, _key, nil), do: spec
  defp maybe_put_measurement(spec, key, value), do: Map.put(spec, key, value)

  defp classify([], []), do: :current
  defp classify(_missing, [_mismatch | _rest]), do: :incompatible
  defp classify([_missing | _rest], []), do: :behind

  defp next_actions(:current), do: []
  defp next_actions(:behind), do: [:install_and_run_current_squidie_migrations]
  defp next_actions(:incompatible), do: [:inspect_schema_before_migrating]

  defp unavailable(reason) do
    Map.put(
      base_result(:unavailable, nil, [], [], [], [
        :verify_repo_connectivity_and_catalog_permissions
      ]),
      :reason,
      reason
    )
  end

  defp query_failure_reason(%DBConnection.ConnectionError{}), do: :connection_failed

  defp query_failure_reason(%Postgrex.Error{postgres: %{code: :insufficient_privilege}}),
    do: :permission_denied

  defp query_failure_reason(_reason), do: :query_failed

  defp base_result(status, prefix, missing, mismatched, unexpected, next_actions) do
    %{
      check: :schema,
      status: status,
      baseline: Schema.baseline(),
      prefix: prefix,
      missing: missing,
      mismatched: mismatched,
      unexpected: unexpected,
      next_actions: next_actions
    }
  end
end
