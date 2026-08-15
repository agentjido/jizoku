defmodule BedrockMinimalHostApp.MixProject do
  use Mix.Project

  def project do
    [
      app: :bedrock_minimal_host_app,
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
      mod: {BedrockMinimalHostApp.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:bypass, "~> 2.1", only: :test},
      {:bedrock, "~> 0.5.2"},
      {:bedrock_job_queue, "~> 0.3.0"},
      {:hackney, "~> 4.7.4", override: true},
      {:ecto_sql, "~> 3.14"},
      {:postgrex, "~> 0.22.2"},
      {:jizoku, path: "../.."},
      # Jido currently allows an older Zoi range that keeps Decimal on 2.x.
      # The example pins Zoi forward so its isolated lockfile can use Decimal 3.
      {:zoi, "~> 0.18.4", override: true}
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
