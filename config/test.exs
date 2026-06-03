import Config

config :squidie,
  ecto_repos: [Squidie.Test.Repo],
  repo: Squidie.Test.Repo

config :squidie, Squidie.Test.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "priv/repo",
  show_sensitive_data_on_connection_error: true,
  stacktrace: true,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres@localhost:5432/squidie_test"

config :logger, level: :warning
