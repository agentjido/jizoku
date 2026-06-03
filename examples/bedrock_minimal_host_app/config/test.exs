import Config

config :bedrock_minimal_host_app,
  runtime_children: []

config :bedrock_minimal_host_app, BedrockMinimalHostApp.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  show_sensitive_data_on_connection_error: true,
  stacktrace: true,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres@localhost:5432/bedrock_minimal_host_app_test"

config :bedrock_minimal_host_app, BedrockMinimalHostApp.SquidieDeliveryAdapter,
  queue_id: "tenant_a",
  topic: "squidie:payload"

config :squidie,
  repo: BedrockMinimalHostApp.Repo

config :logger, level: :warning
