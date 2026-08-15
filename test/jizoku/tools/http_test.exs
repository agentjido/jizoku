defmodule Jizoku.Tools.HTTPTest do
  use ExUnit.Case, async: true

  alias Jizoku.Tools
  alias Jizoku.Tools.Error
  alias Jizoku.Tools.HTTP
  alias Jizoku.Tools.Result

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

    test "sends optional headers, params, and JSON request fields" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "POST", "/search", fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert conn.query_params == %{"page" => "2"}
        assert Plug.Conn.get_req_header(conn, "x-request-id") == ["req_123"]
        assert Jason.decode!(body) == %{"term" => "workflow"}

        Plug.Conn.resp(conn, 201, "created")
      end)

      assert {:ok, %Result{} = result} =
               Tools.invoke(HTTP, %{
                 method: :post,
                 url: endpoint_url(bypass.port, "/search"),
                 headers: [{"x-request-id", "req_123"}],
                 params: %{page: 2},
                 json: %{term: "workflow"}
               })

      assert result.payload.status == 201
      assert result.payload.body == "created"
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

    test "treats throttling and request timeout responses as retryable" do
      for status <- [408, 429] do
        bypass = Bypass.open()

        Bypass.expect_once(bypass, "GET", "/retryable", fn conn ->
          Plug.Conn.resp(conn, status, "try_later")
        end)

        assert {:error, %Error{} = error} =
                 Tools.invoke(HTTP, %{
                   method: :get,
                   url: endpoint_url(bypass.port, "/retryable")
                 })

        assert error.retryable? == true
        assert error.details.status == status
      end
    end

    test "does not follow redirects automatically" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/redirect", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("location", "http://metadata.internal/latest")
        |> Plug.Conn.resp(302, "redirect")
      end)

      assert {:ok, %Result{} = result} =
               Tools.invoke(HTTP, %{
                 method: :get,
                 url: endpoint_url(bypass.port, "/redirect")
               })

      assert result.payload.status == 302
      assert result.payload.body == "redirect"
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

    test "rejects non-map HTTP requests" do
      assert {:error, %Error{} = error} = HTTP.invoke({:get, "/gateway"}, %{}, [])

      assert error.kind == :invalid_request
      assert error.message == "HTTP tool requests must be maps"
      assert error.details == %{reason: :expected_map}
      assert error.retryable? == false
    end

    test "rejects HTTP request maps without a method and URL" do
      assert {:error, %Error{} = error} = Tools.invoke(HTTP, %{method: "GET", url: ""})

      assert error.kind == :invalid_request
      assert error.message == "HTTP tool requests require an atom :method and binary :url"
      assert error.details == %{request: %{method: "GET", url: ""}}
      assert error.retryable? == false
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
