maybe_put = fn config, key, value ->
  if is_nil(value), do: config, else: Keyword.put(config, key, value)
end

repo_config =
  :minimal_host_app
  |> Application.fetch_env!(MinimalHostApp.Repo)
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

{:ok, _pid} = MinimalHostApp.Repo.start_link()

Ecto.Migrator.with_repo(MinimalHostApp.Repo, fn repo ->
  Ecto.Migrator.run(repo, Path.expand("../priv/repo/migrations", __DIR__), :up, all: true)

  Ecto.Migrator.run(repo, Application.app_dir(:jizoku, "priv/repo/migrations"), :up, all: true)
end)

{:ok, _apps} = Application.ensure_all_started(:bypass)

{:ok, _pid} =
  {Oban, Application.fetch_env!(:minimal_host_app, Oban)}
  |> Supervisor.child_spec(id: :minimal_host_app_test_oban)
  |> then(&Supervisor.start_link([&1], strategy: :one_for_one))

Ecto.Adapters.SQL.Sandbox.mode(MinimalHostApp.Repo, :manual)

ExUnit.start()
