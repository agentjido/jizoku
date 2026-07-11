defmodule Squidie.Operations.CLI do
  @moduledoc """
  Starts only the configured repository boundary needed by operational Mix tasks.

  It avoids starting the complete host supervision tree and sanitizes errors
  before they reach operator-facing task output.
  """

  alias Squidie.Config
  alias Squidie.Runtime.Journal.Storage

  @doc "Parses strict Mix task options and raises with the task name when arguments are invalid."
  @spec parse_options!(String.t(), [String.t()], keyword()) :: keyword()
  def parse_options!(task_name, args, switches)
      when is_binary(task_name) and is_list(args) and is_list(switches) do
    case OptionParser.parse(args, strict: switches) do
      {opts, [], []} ->
        opts

      {_opts, positional, invalid} ->
        values = positional ++ Enum.map(invalid, fn {option, _value} -> option end)
        Mix.raise("Invalid #{task_name} options: #{Enum.join(values, ", ")}")
    end
  end

  @doc "Runs a callback with warning-only logging for JSON output and restores the prior level."
  @spec with_json_log_level(keyword(), (-> result)) :: result when result: var
  def with_json_log_level(opts, fun) when is_list(opts) and is_function(fun, 0) do
    run_with_json_log_level(Keyword.get(opts, :json, false), fun)
  end

  defp run_with_json_log_level(true, fun) do
    previous_level = :logger.get_primary_config()[:level]
    Logger.configure(level: :warning)

    try do
      fun.()
    after
      Logger.configure(level: previous_level)
    end
  end

  defp run_with_json_log_level(false, fun), do: fun.()

  @spec run((-> {:ok, map()} | {:error, term()})) :: {:ok, map()} | {:error, term()}
  @doc "Runs a status callback after validating configuration and briefly starting its Ecto repo."
  def run(fun) when is_function(fun, 0) do
    Mix.Task.run("app.config")
    load_project_application()

    with {:ok, %Config{} = config} <- Config.load() do
      run_with_storage(config.journal_storage, fun)
    end
  end

  @spec diagnose((-> {:ok, map()} | {:error, term()})) :: {:ok, map()} | {:error, term()}
  @doc "Runs a doctor callback even when configuration is invalid so it can report that finding."
  def diagnose(fun) when is_function(fun, 0) do
    Mix.Task.run("app.config")
    load_project_application()

    case Config.load() do
      {:ok, %Config{} = config} -> run_with_storage(config.journal_storage, fun)
      {:error, _reason} -> fun.()
    end
  end

  @spec format_error(term()) :: String.t()
  @doc "Formats an operational error without exposing repository configuration or exception details."
  def format_error({:missing_config, keys}) do
    "missing configuration: #{Enum.map_join(keys, ", ", &inspect/1)}"
  end

  def format_error({:invalid_config, details}) do
    keys = Keyword.keys(details)
    "invalid configuration: #{Enum.map_join(keys, ", ", &inspect/1)}"
  end

  def format_error({:invalid_option, {key, _reason}}), do: "invalid option: #{inspect(key)}"
  def format_error(_reason), do: "runtime state is unavailable"

  defp run_with_storage(%Storage{adapter: Squidie.Runtime.Journal.Storage.Ecto, opts: opts}, fun) do
    repo = Keyword.fetch!(opts, :repo)

    case Ecto.Migrator.with_repo(repo, fn _repo -> fun.() end, pool_size: 1) do
      {:ok, result, _started_apps} -> result
      {:error, reason} -> {:error, {:repo_start_failed, reason}}
    end
  end

  defp run_with_storage(%Storage{}, fun), do: fun.()

  defp load_project_application do
    case Application.load(Mix.Project.config()[:app]) do
      :ok -> :ok
      {:error, {:already_loaded, _application}} -> :ok
    end
  end
end
