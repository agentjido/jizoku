import Config

config :jizoku,
  ecto_repos: [Jizoku.Test.Repo],
  repo: Jizoku.Test.Repo

config :jizoku, Jizoku.Test.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "priv/repo",
  show_sensitive_data_on_connection_error: true,
  stacktrace: true,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres@localhost:5432/jizoku_test"

config :logger, level: :warning
