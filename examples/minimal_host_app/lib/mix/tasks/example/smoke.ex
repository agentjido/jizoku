defmodule Mix.Tasks.Example.Smoke do
  @moduledoc """
  Runs the example host app smoke test.
  """

  use Mix.Task

  @shortdoc "Runs the example host app smoke test"

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("jizoku.verify_histories", [history_fixture_path()])
    Mix.Task.run("app.start")
    MinimalHostApp.Smoke.run_all!()
  end

  defp history_fixture_path do
    Path.expand("../../../../test/fixtures/jizoku_histories.exs", __DIR__)
  end
end
