maybe_put = fn config, key, value ->
  if is_nil(value), do: config, else: Keyword.put(config, key, value)
end

repo_config =
  :bedrock_minimal_host_app
  |> Application.fetch_env!(BedrockMinimalHostApp.Repo)
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

{:ok, _pid} = BedrockMinimalHostApp.Repo.start_link()

Ecto.Migrator.with_repo(BedrockMinimalHostApp.Repo, fn repo ->
  Ecto.Migrator.run(repo, Path.expand("../priv/repo/migrations", __DIR__), :up, all: true)

  Ecto.Migrator.run(repo, Application.app_dir(:jizoku, "priv/repo/migrations"), :up, all: true)
end)

case Node.start(:bedrock_minimal_host_app_test, :shortnames) do
  {:ok, _pid} ->
    :ok

  {:error, {:already_started, _pid}} ->
    :ok

  {:error, reason} ->
    raise "failed to start distributed node for Bedrock test cluster: #{inspect(reason)}"
end

Node.set_cookie(:bedrock_minimal_host_app)

bedrock_path = Path.join(System.tmp_dir!(), "bedrock_minimal_host_app")
# The embedded Bedrock cluster is local test infrastructure. Reusing files from
# a killed or interrupted run can leave recovery state that keeps retrying.
File.rm_rf!(bedrock_path)
File.mkdir_p!(bedrock_path)

{:ok, _pid} =
  Supervisor.start_link(
    [{BedrockMinimalHostApp.BedrockCluster, []}],
    strategy: :one_for_one,
    name: BedrockMinimalHostApp.BedrockTestSupervisor
  )

await_bedrock_ready = fn await_bedrock_ready, deadline ->
  ready =
    try do
      case BedrockMinimalHostApp.BedrockRepo.transact(
             fn ->
               ready_keyspace = Bedrock.Keyspace.new("__bedrock_ready__/")
               range = BedrockMinimalHostApp.BedrockRepo.get_range(ready_keyspace, limit: 1)
               Enum.to_list(range)
               :ok
             end,
             retry_limit: 1
           ) do
        :ok -> true
        {:ok, :ok} -> true
        _other -> false
      end
    rescue
      RuntimeError -> false
    end

  cond do
    ready ->
      :ok

    System.monotonic_time(:millisecond) >= deadline ->
      raise "timed out waiting for Bedrock test cluster read path"

    true ->
      Process.sleep(100)
      await_bedrock_ready.(await_bedrock_ready, deadline)
  end
end

# Bedrock can accept cluster startup before the materialized read path is ready.
# The stress tests exercise range reads, so wait for that path explicitly.
await_bedrock_ready.(await_bedrock_ready, System.monotonic_time(:millisecond) + 15_000)

Ecto.Adapters.SQL.Sandbox.mode(BedrockMinimalHostApp.Repo, :manual)

ExUnit.start()
