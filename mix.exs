defmodule Jizoku.MixProject do
  use Mix.Project

  @version "0.4.0"
  @source_url "https://github.com/agentjido/jizoku"
  @description "Durable workflow runtime for Elixir applications."

  def project do
    [
      app: :jizoku,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      description: @description,
      source_url: @source_url,
      homepage_url: @source_url,
      docs: docs(),
      package: package(),
      aliases: aliases(),
      deps: deps(),
      test_coverage: [tool: ExCoveralls, summary: [threshold: 84], export: "cov"],
      dialyzer: [
        plt_add_apps: [:mix, :ex_unit],
        plt_file: {:no_warn, "priv/plts/dialyzer.plt"}
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Jizoku.Application, []}
    ]
  end

  def cli do
    [
      preferred_envs: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.html": :test,
        "coveralls.json": :test,
        precommit: :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      name: "jizoku",
      maintainers: ["Mike Hostetler"],
      licenses: ["Apache-2.0"],
      files:
        ~w(lib priv/repo docs usage-rules usage-rules.md .formatter.exs mix.exs mix.lock README* CHANGELOG* LICENSE* CONTRIBUTING* CODE_OF_CONDUCT*),
      links: %{
        "Changelog" => "https://hexdocs.pm/jizoku/changelog.html",
        "Discord" => "https://jido.run/discord",
        "Documentation" => "https://hexdocs.pm/jizoku",
        "GitHub" => @source_url,
        "Website" => "https://jido.run"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: [
        "docs/index.md",
        "README.md",
        "docs/getting_started.md",
        "docs/getting_started.livemd",
        "docs/workflow_authoring.livemd",
        "docs/architecture.md",
        "docs/jido_runtime_architecture.md",
        "docs/durable_dispatch_protocol.md",
        "docs/positioning.md",
        "docs/compatibility.md",
        "docs/tool_adapters.md",
        "docs/quality_gates.md",
        "docs/observability.md",
        "docs/actor_visibility.md",
        "docs/storage_strategy.md",
        "docs/workflow_authoring.md",
        "docs/continue_as_new.md",
        "docs/testing_workflows.md",
        "docs/graph_inspection.md",
        "docs/reference_workflows.md",
        "docs/host_app_integration.md",
        "docs/operations.md",
        "docs/retention.md",
        "docs/production_readiness.md",
        "docs/migration_guide.md",
        "usage-rules.md",
        "usage-rules/runtime.md",
        "usage-rules/host-apps.md",
        "usage-rules/workflow-authoring.md",
        "usage-rules/testing.md",
        "usage-rules/documentation.md",
        "usage-rules/tooling.md",
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "CODE_OF_CONDUCT.md",
        "LICENSE"
      ],
      groups_for_extras: [
        "Start Here": [
          "docs/index.md",
          "README.md",
          "docs/getting_started.md"
        ],
        Livebooks: ~r/\.livemd$/,
        Guides: [
          "docs/host_app_integration.md",
          "docs/workflow_authoring.md",
          "docs/continue_as_new.md",
          "docs/testing_workflows.md",
          "docs/operations.md",
          "docs/retention.md",
          "docs/observability.md",
          "docs/actor_visibility.md"
        ],
        Reference: [
          "docs/graph_inspection.md",
          "docs/reference_workflows.md",
          "docs/tool_adapters.md",
          "docs/quality_gates.md",
          "docs/compatibility.md",
          "docs/storage_strategy.md",
          "docs/production_readiness.md",
          "docs/migration_guide.md"
        ],
        Internals: [
          "docs/architecture.md",
          "docs/jido_runtime_architecture.md",
          "docs/durable_dispatch_protocol.md",
          "docs/positioning.md"
        ],
        "AI Usage Rules": ~r"usage-rules"
      ]
    ]
  end

  defp deps do
    [
      {:ecto_sql, "~> 3.13"},
      {:telemetry, "~> 1.3"},
      {:telemetry_metrics, "~> 1.1"},
      {:bypass, "~> 2.1", only: :test},
      {:jason, "~> 1.4"},
      {:jido, "~> 2.3"},
      {:req, "~> 0.5"},
      {:runic, "~> 0.1.0-alpha"},
      {:spark, "~> 2.7"},
      {:ex_dna, "~> 1.5", only: [:dev, :test], runtime: false},
      {:ex_slop, "~> 0.4.2", only: [:dev, :test], runtime: false},
      {:postgrex, "~> 0.20"},
      {:reach, "~> 2.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:doctor, "~> 0.23.0", only: [:dev, :test], runtime: false},
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:git_hooks, "~> 0.8", only: [:dev, :test], runtime: false},
      {:git_ops, "~> 2.9", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      setup: ["deps.get"],
      install_hooks: ["git_hooks.install"],
      q: ["quality"],
      quality: [
        "format --check-formatted",
        "compile --warnings-as-errors",
        "credo --min-priority higher",
        "dialyzer",
        "doctor --raise"
      ],
      precommit: [
        "compile --warnings-as-errors",
        "xref graph --format cycles --label compile-connected --fail-above 0",
        "deps.unlock --check-unused",
        "brand.audit",
        "format --check-formatted",
        "credo --strict",
        "quality_gates",
        "doctor --raise",
        "deps.audit --ignore-file config/deps_audit.ignore",
        "dialyzer",
        "test"
      ],
      quality_gates: ["quality.ex_dna", "quality.reach"],
      "brand.audit": ["run scripts/jizoku_rebrand_audit.exs"],
      "quality.ex_dna": ["ex_dna --min-mass 40 --max-clones 0 --format console"],
      "quality.reach": ["reach.check --smells --strict"]
    ]
  end
end
