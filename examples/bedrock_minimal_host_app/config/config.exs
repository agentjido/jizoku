import Config

config :bedrock_minimal_host_app,
  ecto_repos: [BedrockMinimalHostApp.Repo]

config :bedrock_minimal_host_app, BedrockMinimalHostApp.Repo,
  url:
    System.get_env(
      "DATABASE_URL",
      "ecto://postgres:postgres@localhost/bedrock_minimal_host_app_dev"
    ),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10")),
  show_sensitive_data_on_connection_error: true,
  stacktrace: true

config :bedrock_minimal_host_app, BedrockMinimalHostApp.SquidieDeliveryAdapter,
  queue_id: "tenant_a",
  topic: "squidie:payload"

config :squidie,
  repo: BedrockMinimalHostApp.Repo

import_config "#{config_env()}.exs"
