defmodule Squidie.Persistence.Schema do
  @moduledoc """
  Defines the versioned structural baseline for Squidie's persisted journal.

  The installer and operational schema checker share this manifest so copied
  host migrations and live-database diagnostics use the same required tables.
  """

  @baseline 1
  @timestamp %{type: "timestamp", nullable?: false, precision: 6}
  @tables %{
    "squidie_journal_threads" => %{
      columns: %{
        "id" => %{type: "text", nullable?: false},
        "rev" => %{type: "int8", nullable?: false},
        "metadata" => %{type: "jsonb", nullable?: false},
        "created_at_ms" => %{type: "int8", nullable?: false},
        "updated_at_ms" => %{type: "int8", nullable?: false},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["id"]
    },
    "squidie_journal_entries" => %{
      columns: %{
        "id" => %{type: "uuid", nullable?: false},
        "thread_id" => %{type: "text", nullable?: false},
        "seq" => %{type: "int8", nullable?: false},
        "entry" => %{type: "bytea", nullable?: false},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["id"],
      unique: [["thread_id", "seq"]],
      foreign_keys: [
        %{
          columns: ["thread_id"],
          references: %{table: "squidie_journal_threads", columns: ["id"]},
          on_delete: "CASCADE"
        }
      ]
    },
    "squidie_journal_checkpoints" => %{
      columns: %{
        "key_hash" => %{type: "varchar", nullable?: false, length: 255},
        "key" => %{type: "bytea", nullable?: false},
        "checkpoint" => %{type: "bytea", nullable?: false},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["key_hash"]
    }
  }

  @spec baseline() :: pos_integer()
  @doc "Returns the current structural schema baseline version."
  def baseline, do: @baseline

  @spec tables() :: map()
  @doc "Returns required table, column, index, and foreign-key definitions."
  def tables, do: @tables

  @spec table_names() :: [String.t()]
  @doc "Returns required Squidie table names in deterministic order."
  def table_names, do: Enum.sort(Map.keys(@tables))
end
