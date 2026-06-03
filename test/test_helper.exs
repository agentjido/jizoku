maybe_put = fn config, key, value ->
  if is_nil(value), do: config, else: Keyword.put(config, key, value)
end

repo_config =
  :squidie
  |> Application.fetch_env!(Squidie.Test.Repo)
  |> then(fn config ->
    case Keyword.fetch(config, :url) do
      {:ok, url} ->
        uri = URI.parse(url)

        {username, password} =
          case String.split(uri.userinfo || "", ":", parts: 2) do
            [user, pass] -> {user, pass}
            [user] -> {user, nil}
            _other -> {nil, nil}
          end

        database = String.trim_leading(uri.path || "", "/")

        config
        |> Keyword.delete(:url)
        |> Keyword.put(:hostname, uri.host)
        |> Keyword.put(:port, uri.port || 5432)
        |> Keyword.put(:database, database)
        |> maybe_put.(:username, username)
        |> maybe_put.(:password, password)

      :error ->
        config
    end
  end)

case Ecto.Adapters.Postgres.storage_up(repo_config) do
  :ok -> :ok
  {:error, :already_up} -> :ok
  {:error, term} -> raise "failed to create test database: #{inspect(term)}"
end

{:ok, _pid} = Squidie.Test.Repo.start_link()

Ecto.Migrator.with_repo(Squidie.Test.Repo, fn repo ->
  Ecto.Migrator.run(repo, Application.app_dir(:squidie, "priv/repo/migrations"), :up, all: true)

  Ecto.Migrator.run(repo, Path.expand("support/migrations", __DIR__), :up, all: true)
end)

Ecto.Adapters.SQL.Sandbox.mode(Squidie.Test.Repo, :manual)

ExUnit.start()
