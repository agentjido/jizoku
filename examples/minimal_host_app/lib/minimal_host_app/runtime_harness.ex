defmodule MinimalHostApp.RuntimeHarness do
  @moduledoc """
  Shared runtime and verification helpers for the example host app.

  The smoke, resilience, and soak validations all exercise the same embedded
  Squid Mesh boundary, so runtime setup and durable run polling live here
  instead of being duplicated across scripts and Mix tasks.
  """

  alias Ecto.Adapters.Postgres

  alias MinimalHostApp.WorkflowRuns

  @default_poll_attempts 40
  @default_poll_interval_ms 50

  @type gateway_responder :: (pos_integer() -> iodata())

  @spec ensure_runtime_started() :: :ok
  def ensure_runtime_started do
    ensure_repo_started()
    ensure_migrated()
    ensure_oban_started()
    :ok
  end

  @spec wait_for_execution() :: :ok
  def wait_for_execution do
    if manual_oban_testing?() do
      _result = Oban.drain_queue(queue: :squid_mesh, with_recursion: true)
      :ok
    else
      :ok
    end
  end

  @spec drain_available_jobs(pos_integer()) :: map() | :ok
  def drain_available_jobs(limit \\ 1) when is_integer(limit) and limit > 0 do
    if manual_oban_testing?() do
      Oban.drain_queue(queue: :squid_mesh, with_limit: limit)
    else
      :ok
    end
  end

  @spec await_terminal_run(Ecto.UUID.t(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()} | {:error, :timeout | term()}
  def await_terminal_run(run_id, opts \\ []) when is_binary(run_id) do
    attempts = Keyword.get(opts, :attempts, @default_poll_attempts)
    interval_ms = Keyword.get(opts, :interval_ms, @default_poll_interval_ms)

    do_await_terminal_run(run_id, attempts, interval_ms)
  end

  @spec start_gateway_server(gateway_responder(), pos_integer()) :: {pid(), pos_integer()}
  def start_gateway_server(responder, max_requests \\ 20)
      when is_function(responder, 1) and is_integer(max_requests) and max_requests > 0 do
    {:ok, socket} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(socket)

    {:ok, pid} =
      Task.start(fn ->
        serve_gateway(socket, responder, 1, max_requests)
      end)

    {pid, port}
  end

  @spec endpoint_url(pos_integer(), String.t()) :: String.t()
  def endpoint_url(port, path) do
    "http://127.0.0.1:#{port}#{path}"
  end

  @spec success_gateway_response(String.t()) :: iodata()
  def success_gateway_response(body \\ "ok") do
    "HTTP/1.1 200 OK\r\ncontent-length: #{byte_size(body)}\r\n\r\n#{body}"
  end

  @spec accepted_gateway_response(String.t()) :: iodata()
  def accepted_gateway_response(body \\ "pending") do
    "HTTP/1.1 202 Accepted\r\ncontent-length: #{byte_size(body)}\r\n\r\n#{body}"
  end

  @spec failure_gateway_response(pos_integer(), String.t()) :: iodata()
  def failure_gateway_response(status_code \\ 500, body \\ "retry_later") do
    reason_phrase =
      case status_code do
        500 -> "Internal Server Error"
        502 -> "Bad Gateway"
        503 -> "Service Unavailable"
        _other -> "Request Failed"
      end

    "HTTP/1.1 #{status_code} #{reason_phrase}\r\ncontent-length: #{byte_size(body)}\r\n\r\n#{body}"
  end

  @spec restart_oban!() :: :ok
  def restart_oban! do
    stop_oban()
    ensure_oban_started()
  end

  @spec perform_scheduled_step!(Ecto.UUID.t(), String.t(), keyword()) :: :ok
  def perform_scheduled_step!(run_id, step, opts \\ [])
      when is_binary(run_id) and is_binary(step) do
    attempts = Keyword.get(opts, :attempts, @default_poll_attempts)
    interval_ms = Keyword.get(opts, :interval_ms, @default_poll_interval_ms)

    case await_and_execute_scheduled_step(run_id, step, attempts, interval_ms) do
      :ok -> :ok
      {:error, reason} -> raise "expected scheduled job for #{step}: #{inspect(reason)}"
    end
  end

  @spec stop_gateway_server(pid()) :: :ok
  def stop_gateway_server(server_pid) when is_pid(server_pid) do
    Process.exit(server_pid, :kill)
    :ok
  end

  defp do_await_terminal_run(_run_id, 0, _interval_ms), do: {:error, :timeout}

  defp do_await_terminal_run(run_id, attempts_remaining, interval_ms)
       when attempts_remaining > 0 do
    case WorkflowRuns.inspect_run(run_id) do
      {:ok, run} when run.status in [:completed, :failed, :cancelled] ->
        {:ok, run}

      {:ok, run} ->
        _result =
          SquidMesh.execute_next(
            owner_id: "minimal-host-app-runtime-harness",
            now: next_runtime_tick(run)
          )

        Process.sleep(interval_ms)
        do_await_terminal_run(run_id, attempts_remaining - 1, interval_ms)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec await_and_execute_scheduled_step(
          Ecto.UUID.t(),
          String.t(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok | {:error, :timeout}
  defp await_and_execute_scheduled_step(_run_id, _step, 0, _interval_ms), do: {:error, :timeout}

  defp await_and_execute_scheduled_step(run_id, step, attempts_remaining, interval_ms)
       when attempts_remaining > 0 do
    case WorkflowRuns.inspect_run(run_id) do
      {:ok, %{scheduled_attempts: scheduled_attempts, visible_attempts: visible_attempts} = run} ->
        if Enum.any?(scheduled_attempts ++ visible_attempts, &(Map.get(&1, :step) == step)) do
          _result =
            SquidMesh.execute_next(
              owner_id: "minimal-host-app-runtime-harness",
              now: next_runtime_tick(run)
            )

          :ok
        else
          Process.sleep(interval_ms)
          await_and_execute_scheduled_step(run_id, step, attempts_remaining - 1, interval_ms)
        end

      _other ->
        Process.sleep(interval_ms)
        await_and_execute_scheduled_step(run_id, step, attempts_remaining - 1, interval_ms)
    end
  end

  @spec manual_oban_testing?() :: boolean()
  defp manual_oban_testing? do
    case Application.fetch_env(:minimal_host_app, Oban) do
      {:ok, config} -> Keyword.get(config, :testing) == :manual
      :error -> false
    end
  end

  defp next_runtime_tick(%{next_visible_at: %DateTime{} = next_visible_at}), do: next_visible_at
  defp next_runtime_tick(_run), do: DateTime.utc_now(:microsecond)

  @spec ensure_repo_started() :: :ok
  defp ensure_repo_started do
    if is_nil(Process.whereis(MinimalHostApp.Repo)) do
      repo_config = repo_config()

      case Postgres.storage_up(repo_config) do
        :ok -> :ok
        {:error, :already_up} -> :ok
        {:error, term} -> raise "failed to create runtime database: #{inspect(term)}"
      end

      {:ok, _pid} = MinimalHostApp.Repo.start_link()
    end

    :ok
  end

  @spec ensure_migrated() :: :ok
  defp ensure_migrated do
    Ecto.Migrator.with_repo(MinimalHostApp.Repo, fn repo ->
      Ecto.Migrator.run(repo, app_migrations_path(), :up, all: true)
      Ecto.Migrator.run(repo, library_migrations_path(), :up, all: true)
    end)

    :ok
  end

  @spec ensure_oban_started() :: :ok
  defp ensure_oban_started do
    oban_config = Application.fetch_env!(:minimal_host_app, Oban)
    oban_name = Keyword.get(oban_config, :name, Oban)

    if is_nil(Process.whereis(oban_name)) do
      case Oban.start_link(oban_config) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end

    :ok
  end

  @spec stop_oban() :: :ok
  defp stop_oban do
    oban_name =
      :minimal_host_app
      |> Application.fetch_env!(Oban)
      |> Keyword.get(:name, Oban)

    case Process.whereis(oban_name) do
      nil ->
        :ok

      pid ->
        :ok = GenServer.stop(pid, :normal, 5_000)
        :ok
    end
  end

  @spec serve_gateway(port(), gateway_responder(), pos_integer(), pos_integer()) :: :ok
  defp serve_gateway(socket, responder, attempt, max_requests) when attempt <= max_requests do
    case :gen_tcp.accept(socket) do
      {:ok, client} ->
        {:ok, _request} = :gen_tcp.recv(client, 0)

        response = responder.(attempt)

        :ok = :gen_tcp.send(client, response)
        :ok = :gen_tcp.close(client)

        if attempt < max_requests do
          serve_gateway(socket, responder, attempt + 1, max_requests)
        else
          :gen_tcp.close(socket)
          :ok
        end

      {:error, :closed} ->
        :ok
    end
  end

  @spec app_migrations_path() :: String.t()
  defp app_migrations_path do
    Application.app_dir(:minimal_host_app, "priv/repo/migrations")
  end

  @spec library_migrations_path() :: String.t()
  defp library_migrations_path do
    Application.app_dir(:squid_mesh, "priv/repo/migrations")
  end

  @spec repo_config() :: keyword()
  defp repo_config do
    maybe_put = fn config, key, value ->
      if is_nil(value), do: config, else: Keyword.put(config, key, value)
    end

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
  end
end
