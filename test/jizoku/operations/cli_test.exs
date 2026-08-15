defmodule Jizoku.Operations.CLITest do
  use ExUnit.Case, async: false

  alias Jizoku.Operations.CLI

  test "formats operational failures without exposing configuration values" do
    assert CLI.format_error({:missing_config, [:repo, :queue]}) ==
             "missing configuration: :repo, :queue"

    assert CLI.format_error({:invalid_config, repo: "secret-url", queue: "secret-queue"}) ==
             "invalid configuration: :repo, :queue"

    assert CLI.format_error({:invalid_option, {:now, "secret-value"}}) ==
             "invalid option: :now"

    assert CLI.format_error({:repo_start_failed, "secret-url"}) ==
             "runtime state is unavailable"
  end

  test "restores the logger level when JSON collection raises" do
    previous_level = :logger.get_primary_config()[:level]

    assert_raise RuntimeError, "collection failed", fn ->
      CLI.with_json_log_level([json: true], fn ->
        assert :logger.get_primary_config()[:level] == :warning
        raise "collection failed"
      end)
    end

    assert :logger.get_primary_config()[:level] == previous_level
  end
end
