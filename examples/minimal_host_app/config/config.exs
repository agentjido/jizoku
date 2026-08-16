import Config

config :minimal_host_app,
  ecto_repos: [MinimalHostApp.Repo]

config :minimal_host_app, MinimalHostApp.Repo,
  url: System.get_env("DATABASE_URL", "ecto://postgres:postgres@localhost/minimal_host_app_dev"),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10")),
  show_sensitive_data_on_connection_error: true,
  stacktrace: true

config :minimal_host_app, Oban,
  repo: MinimalHostApp.Repo,
  plugins: [
    {MinimalHostApp.CronPlugin, workflows: [MinimalHostApp.Workflows.DailyDigest]}
  ],
  queues: [jizoku: 5]

config :minimal_host_app, MinimalHostApp.JizokuDeliveryAdapter,
  oban_name: Oban,
  queue: :jizoku

config :jizoku,
  repo: MinimalHostApp.Repo,
  workflow_versions: %{
    MinimalHostApp.Workflows.VersionedRouting => %{
      "v1" => MinimalHostApp.Workflows.VersionedRouting.V1,
      "v2" => MinimalHostApp.Workflows.VersionedRouting
    },
    MinimalHostApp.Workflows.MigratedRouting => %{
      "v1" => MinimalHostApp.Workflows.MigratedRouting.V1,
      "v2" => MinimalHostApp.Workflows.MigratedRouting
    }
  }

import_config "#{config_env()}.exs"
