defmodule Squidie.Tools.HTTPTest do
  use ExUnit.Case, async: true

  alias Squidie.Tools
  alias Squidie.Tools.Error
  alias Squidie.Tools.HTTP
  alias Squidie.Tools.Result

  describe "invoke/4 with the HTTP adapter" do
    test "normalizes successful HTTP responses" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/gateway", fn conn ->
        Plug.Conn.resp(conn, 200, "retry_required")
      end)

      assert {:ok, %Result{} = result} =
               Tools.invoke(HTTP, %{
                 method: :get,
                 url: endpoint_url(bypass.port, "/gateway")
               })

      assert result.adapter == HTTP
      assert result.payload.status == 200
      assert result.payload.body == "retry_required"
      assert result.metadata.method == :get
      assert result.metadata.url == endpoint_url(bypass.port, "/gateway")
    end

    test "normalizes HTTP status failures" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "POST", "/notify", fn conn ->
        Plug.Conn.resp(conn, 503, "gateway_unavailable")
      end)

      assert {:error, %Error{} = error} =
               Tools.invoke(HTTP, %{
                 method: :post,
                 url: endpoint_url(bypass.port, "/notify"),
                 body: "payload"
               })

      assert error.adapter == HTTP
      assert error.kind == :http
      assert error.retryable? == true
      assert error.details.status == 503
      assert error.details.body == "gateway_unavailable"
    end

    test "normalizes timeout failures" do
      {server_pid, port} = start_hanging_server()
      on_exit(fn -> Process.exit(server_pid, :kill) end)

      assert {:error, %Error{} = error} =
               Tools.invoke(HTTP, %{
                 method: :get,
                 url: endpoint_url(port, "/slow"),
                 timeout: 50
               })

      assert error.adapter == HTTP
      assert error.kind == :timeout
      assert error.retryable? == true
    end

    test "normalizes transport failures" do
      port = unused_port()

      assert {:error, %Error{} = error} =
               Tools.invoke(HTTP, %{
                 method: :get,
                 url: endpoint_url(port, "/unreachable"),
                 timeout: 10
               })

      assert error.adapter == HTTP
      assert error.kind == :transport
      assert error.retryable? == true
    end
  end

  defp endpoint_url(port, path) do
    "http://127.0.0.1:#{port}#{path}"
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp start_hanging_server do
    parent = self()

    {:ok, socket} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(socket)

    {:ok, pid} =
      Task.start_link(fn ->
        send(parent, {:server_ready, port})
        {:ok, client} = :gen_tcp.accept(socket)
        Process.sleep(500)
        :gen_tcp.close(client)
        :gen_tcp.close(socket)
      end)

    assert_receive {:server_ready, ^port}

    {pid, port}
  end
end
