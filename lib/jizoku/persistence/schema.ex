defmodule Jizoku.Persistence.Schema do
  @moduledoc """
  Defines the versioned structural baseline for Jizoku's persisted journal.

  The installer and operational schema checker share this manifest so copied
  host migrations and live-database diagnostics use the same required tables.
  """

  @baseline 4
  @timestamp %{type: "timestamp", nullable?: false, precision: 6}
  @tables %{
    "jizoku_journal_threads" => %{
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
    "jizoku_journal_entries" => %{
      columns: %{
        "id" => %{type: "uuid", nullable?: false},
        "thread_id" => %{type: "text", nullable?: false},
        "seq" => %{type: "int8", nullable?: false},
        "entry" => %{type: "bytea", nullable?: false},
        "retention_run_id" => %{type: "text", nullable?: true},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["id"],
      unique: [["thread_id", "seq"]],
      indexes: [["thread_id", "retention_run_id"]],
      foreign_keys: [
        %{
          columns: ["thread_id"],
          references: %{table: "jizoku_journal_threads", columns: ["id"]},
          on_delete: "CASCADE"
        }
      ]
    },
    "jizoku_journal_checkpoints" => %{
      columns: %{
        "key_hash" => %{type: "varchar", nullable?: false, length: 255},
        "key" => %{type: "bytea", nullable?: false},
        "checkpoint" => %{type: "bytea", nullable?: false},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["key_hash"]
    },
    "jizoku_run_search" => %{
      columns: %{
        "partition_key" => %{type: "text", nullable?: false},
        "run_id" => %{type: "text", nullable?: false},
        "partition" => %{type: "text", nullable?: true},
        "workflow" => %{type: "text", nullable?: false},
        "status" => %{type: "text", nullable?: false},
        "terminal_status" => %{type: "text", nullable?: true},
        "definition_version" => %{type: "text", nullable?: true},
        "search_attributes" => %{type: "jsonb", nullable?: false},
        "started_at" => @timestamp,
        "terminal_at" => %{type: "timestamp", nullable?: true, precision: 6},
        "archived_at" => %{type: "timestamp", nullable?: true, precision: 6},
        "archive_reason" => %{type: "text", nullable?: true},
        "thread_revision" => %{type: "int8", nullable?: false},
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["partition_key", "run_id"],
      indexes: [
        ["partition_key", "started_at", "run_id"],
        ["partition_key", "workflow", "started_at", "run_id"],
        ["partition_key", "status", "started_at", "run_id"],
        ["partition_key", "definition_version", "started_at", "run_id"],
        ["partition_key", "terminal_at", "run_id"],
        ["partition_key", "archived_at", "started_at", "run_id"],
        ["search_attributes"]
      ]
    },
    "jizoku_retention_receipts" => %{
      columns: %{
        "partition_key" => %{type: "text", nullable?: false},
        "run_id" => %{type: "text", nullable?: false},
        "plan_digest" => %{type: "varchar", nullable?: false, length: 255},
        "workflow" => %{type: "text", nullable?: false},
        "queue" => %{type: "text", nullable?: false},
        "terminal_status" => %{type: "text", nullable?: false},
        "run_entries_deleted" => %{type: "int8", nullable?: false},
        "dispatch_entries_deleted" => %{type: "int8", nullable?: false},
        "deleted_at" => @timestamp,
        "inserted_at" => @timestamp,
        "updated_at" => @timestamp
      },
      primary_key: ["partition_key", "run_id"],
      indexes: [
        ["plan_digest"],
        ["partition_key", "deleted_at", "run_id"]
      ]
    }
  }

  @spec baseline() :: pos_integer()
  @doc "Returns the current structural schema baseline version."
  def baseline, do: @baseline

  @spec tables() :: map()
  @doc "Returns required table, column, index, and foreign-key definitions."
  def tables, do: @tables

  @spec table_names() :: [String.t()]
  @doc "Returns required Jizoku table names in deterministic order."
  def table_names, do: Enum.sort(Map.keys(@tables))
end
