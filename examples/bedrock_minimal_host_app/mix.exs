defmodule BedrockMinimalHostApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock_minimal_host_app,
      version: "0.1.0",
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {BedrockMinimalHostApp.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bypass, "~> 2.1", only: :test},
      {:bedrock, "~> 0.7.2"},
      {:bedrock_job_queue, "~> 0.4.0"},
      {:hackney, "~> 4.8", override: true},
      {:ecto_sql, "~> 3.14"},
      {:postgrex, "~> 0.22.2"},
      {:jizoku, path: "../.."},
      {:zoi, "~> 0.18.11"}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get", "jizoku.install", "ecto.setup"],
      "ecto.setup": ["ecto.create", "ecto.migrate"],
      "ecto.reset": ["ecto.drop", "ecto.setup"]
    ]
  end
end
