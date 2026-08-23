defmodule Jizoku.RebrandContractTest do
  use ExUnit.Case, async: true

  test "uses the Jizoku application and public namespace" do
    assert Mix.Project.config()[:app] == :jizoku
    assert Code.ensure_loaded?(Jizoku)
    assert function_exported?(Jizoku, :start, 2)
  end

  test "publishes the Jizoku package identity" do
    package = Mix.Project.config()[:package]

    assert package[:name] == "jizoku"
    assert Mix.Project.config()[:source_url] == "https://github.com/agentjido/jizoku"
  end

  test "owns a fresh Jizoku journal schema" do
    assert Jizoku.Persistence.Schema.table_names() == [
             "jizoku_journal_checkpoints",
             "jizoku_journal_entries",
             "jizoku_journal_threads",
             "jizoku_retention_receipts",
             "jizoku_run_search"
           ]

    assert [migration] =
             :jizoku
             |> Application.app_dir("priv/repo/migrations/*create_jizoku_schema.exs")
             |> Path.wildcard()

    assert Path.basename(migration) == "20260815000000_create_jizoku_schema.exs"
    refute Path.basename(migration) == "20260428000000_create_jizoku_schema.exs"
  end
end
