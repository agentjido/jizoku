defmodule MinimalHostApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :minimal_host_app,
      version: "0.1.0",
      elixir: "~> 1.17",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {MinimalHostApp.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bypass, "~> 2.1", only: :test},
      {:ecto_sql, "~> 3.13"},
      {:oban, "~> 2.22"},
      {:postgrex, ">= 0.0.0"},
      {:squidie, path: "../.."}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "squidie.install", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"],
      smoke: ["run -e 'MinimalHostApp.Smoke.run_all!()'"]
    ]
  end
end
