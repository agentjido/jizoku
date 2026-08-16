defmodule Mix.Tasks.Jizoku.VerifyHistories do
  @moduledoc """
  Verifies trusted, checked-in golden histories against registered workflow versions.

      mix jizoku.verify_histories test/fixtures/jizoku_histories.exs
      mix jizoku.verify_histories --json test/fixtures/jizoku_histories.exs

  The fixture file must evaluate to a list accepted by
  `Jizoku.Workflow.verify_history_fixtures/2`. Only evaluate repository-owned
  fixture files because `.exs` files execute as host application code.
  """

  @shortdoc "Verifies checked-in workflow histories against registered code"
  @requirements ["app.config"]

  use Mix.Task

  @switches [json: :boolean]

  @impl Mix.Task
  def run(args) do
    {opts, path} = parse_options!(args)

    with {:ok, fixtures} <- load_fixtures(path),
         registry <- Application.get_env(:jizoku, :workflow_versions, %{}),
         result <- Jizoku.Workflow.verify_history_fixtures(fixtures, registry) do
      render(result, opts)
    else
      {:error, :fixture_unavailable} ->
        Mix.raise("Could not load trusted workflow history fixtures")
    end
  end

  defp parse_options!(args) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [path], []} ->
        {opts, path}

      {_opts, positional, invalid} ->
        values = positional ++ Enum.map(invalid, fn {option, _value} -> option end)
        Mix.raise("Invalid jizoku.verify_histories options: #{Enum.join(values, ", ")}")
    end
  end

  defp load_fixtures(path) do
    case Code.eval_file(path) do
      {fixtures, _binding} when is_list(fixtures) -> {:ok, fixtures}
      {_invalid, _binding} -> {:error, :fixture_unavailable}
    end
  rescue
    _error in [File.Error, Code.LoadError, SyntaxError, TokenMissingError, CompileError] ->
      {:error, :fixture_unavailable}
  end

  defp render({:ok, report}, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    else
      Mix.shell().info("Verified #{report.verified} workflow history fixture(s).")
    end
  end

  defp render({:error, %{total: total, incompatible: incompatible} = report}, opts) do
    if Keyword.get(opts, :json, false) do
      Mix.shell().info(Jason.encode!(report))
    end

    Mix.raise("#{incompatible} of #{total} workflow history fixtures are incompatible")
  end

  defp render({:error, _reason}, _opts) do
    Mix.raise("Workflow version registry is invalid")
  end
end
