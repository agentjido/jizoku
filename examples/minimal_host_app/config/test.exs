import Config

config :minimal_host_app,
  runtime_children: []

config :minimal_host_app, MinimalHostApp.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  show_sensitive_data_on_connection_error: true,
  stacktrace: true,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres@localhost:5432/minimal_host_app_test"

config :minimal_host_app, Oban,
  name: Oban,
  repo: MinimalHostApp.Repo,
  testing: :manual,
  plugins: [
    {MinimalHostApp.CronPlugin, workflows: [MinimalHostApp.Workflows.DailyDigest]}
  ],
  queues: [squidie: 5]

config :minimal_host_app, MinimalHostApp.SquidieDeliveryAdapter,
  oban_name: Oban,
  queue: :squidie

config :squidie,
  repo: MinimalHostApp.Repo,
  runtime: :journal,
  read_model: :read_model,
  continuation_fences: :enabled,
  jido_effects: :enabled

config :logger, level: :warning
