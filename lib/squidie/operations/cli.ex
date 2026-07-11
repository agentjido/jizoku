defmodule Squidie.Operations.CLI do
  @moduledoc """
  Starts only the configured repository boundary needed by operational Mix tasks.

  It avoids starting the complete host supervision tree and sanitizes errors
  before they reach operator-facing task output.
  """

  alias Squidie.Config
  alias Squidie.Runtime.Journal.Storage

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
