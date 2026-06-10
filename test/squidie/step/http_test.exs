defmodule Squidie.Step.HTTPTest do
  use ExUnit.Case, async: true

  alias Squidie.Step.HTTP
  alias Squidie.Workflow.ActionRegistry

  describe "run/2" do
    test "invokes the shared HTTP tool adapter and returns a structured response" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "POST", "/events", fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert conn.query_params == %{"page" => "2"}
        assert Plug.Conn.get_req_header(conn, "x-request-id") == ["req_123"]
        assert Jason.decode!(body) == %{"event" => "published"}

        Plug.Conn.resp(conn, 201, "created")
      end)

      assert {:ok, %{http_response: response}} =
               HTTP.run(
                 %{
                   request: %{
                     method: "POST",
                     url: endpoint_url(bypass.port, "/events"),
                     headers: %{"x-request-id" => "req_123"},
                     query_params: %{"page" => 2},
                     json: %{event: "published"},
                     timeout: 1_000
                   }
                 },
                 context(allowed_hosts: ["127.0.0.1"], persist_response_body?: true)
               )

      assert response.status == 201
      assert response.body == "created"
    end

    test "returns retryable structured errors for retryable HTTP failures" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/gateway", fn conn ->
        Plug.Conn.resp(conn, 503, "unavailable")
      end)

      assert {:retry, error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: endpoint_url(bypass.port, "/gateway")
                   }
                 },
                 context(allowed_hosts: ["127.0.0.1"], persist_response_body?: true)
               )

      assert error.kind == :http
      assert error.retryable? == true
      assert error.details.status == 503
      assert error.details.body == "unavailable"
    end

    test "returns non-retryable structured errors for rejected HTTP responses" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/missing", fn conn ->
        Plug.Conn.resp(conn, 404, "missing")
      end)

      assert {:error, error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: endpoint_url(bypass.port, "/missing")
                   }
                 },
                 context(allowed_hosts: ["127.0.0.1"], persist_response_body?: true)
               )

      assert error.kind == :http
      assert error.retryable? == false
      assert error.details.status == 404
      assert error.details.body == "missing"
    end

    test "expands URL templates from bindings without creating atoms from input" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/invoices/inv_123", fn conn ->
        Plug.Conn.resp(conn, 200, "ok")
      end)

      assert {:ok, %{http_response: response}} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url_template: endpoint_url(bypass.port, "/invoices/{{ invoice_id }}"),
                     bindings: %{"invoice_id" => "inv_123"}
                   }
                 },
                 context(allowed_hosts: ["127.0.0.1"], persist_response_body?: true)
               )

      assert response.status == 200
      assert response.body == "ok"
    end

    test "accepts string-keyed request configuration with header pairs and body" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "PUT", "/events", fn conn ->
        conn = Plug.Conn.fetch_query_params(conn)
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert conn.query_params == %{"kind" => "sync"}
        assert Plug.Conn.get_req_header(conn, "x-mode") == ["replace"]
        assert body == "payload"

        Plug.Conn.resp(conn, 200, "updated")
      end)

      assert {:ok, %{http_response: response}} =
               HTTP.run(
                 %{
                   request: %{
                     "method" => "PUT",
                     "url" => endpoint_url(bypass.port, "/events"),
                     "headers" => [{"x-mode", :replace}],
                     "params" => %{"kind" => "sync"},
                     "body" => "payload"
                   }
                 },
                 context(
                   allowed_hosts: ["127.0.0.1"],
                   allow_body?: true,
                   persist_response_body?: true
                 )
               )

      assert response.status == 200
      assert response.body == "updated"
    end

    test "requires execution-time allowed hosts from host-owned action options" do
      assert {:error, error} =
               HTTP.run(
                 %{request: %{method: :get, url: "https://api.example.test/invoices"}},
                 context()
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{allowed_hosts: "allowed_hosts policy is required"},
               retryable?: false
             }
    end

    test "redacts sensitive response headers and truncates persisted response bodies" do
      bypass = Bypass.open()
      body = String.duplicate("a", 20)

      Bypass.expect_once(bypass, "GET", "/large", fn conn ->
        conn
        |> Plug.Conn.put_resp_header("set-cookie", "session=secret")
        |> Plug.Conn.put_resp_header("x-request-id", "req_123")
        |> Plug.Conn.resp(200, body)
      end)

      assert {:ok, %{http_response: response}} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: endpoint_url(bypass.port, "/large")
                   }
                 },
                 context(
                   allowed_hosts: ["127.0.0.1"],
                   max_body_bytes: 8,
                   persist_response_body?: true
                 )
               )

      assert response.status == 200
      assert response.body == "aaaaaaaa"
      assert response.body_truncated? == true
      assert response.headers["set-cookie"] == "[REDACTED]"
      assert response.headers["x-request-id"] == "req_123"
    end

    test "omits response bodies unless the host opts into persistence" do
      bypass = Bypass.open()

      Bypass.expect_once(bypass, "GET", "/sensitive", fn conn ->
        Plug.Conn.resp(conn, 200, "secret response")
      end)

      assert {:ok, %{http_response: response}} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: endpoint_url(bypass.port, "/sensitive")
                   }
                 },
                 context(allowed_hosts: ["127.0.0.1"])
               )

      assert response.status == 200
      assert response.body == nil
      assert response.body_truncated? == false
    end

    test "rejects invalid credential references" do
      assert {:error, error} =
               HTTP.run(
                 %{
                   request: %{method: :get, url: "https://example.test/invoices"},
                   credential_refs: %{billing_api: ""}
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{credential_refs: "credential references must be strings"},
               retryable?: false
             }
    end

    test "rejects non-map credential references" do
      assert {:error, error} =
               HTTP.run(
                 %{
                   request: %{method: :get, url: "https://example.test/invoices"},
                   credential_refs: "billing_api"
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{credential_refs: "credential references must be a map"},
               retryable?: false
             }
    end

    test "rejects direct credential values in request configuration" do
      assert {:error, error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: "https://example.test/invoices",
                     credentials: %{api_key: "secret"}
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{credentials: "credential values are not allowed"},
               retryable?: false
             }
    end

    test "rejects secret-bearing headers and payload keys" do
      assert {:error, error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :post,
                     url: "https://example.test/invoices",
                     headers: %{"api-key" => "secret"},
                     json: %{api_key: "secret"},
                     query_params: %{token: "secret"}
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert error.validation_errors == %{
               headers: "secret-bearing request headers are not allowed",
               json: "secret-bearing request values are not allowed",
               query_params: "secret-bearing request values are not allowed"
             }
    end

    test "rejects URL query strings, userinfo, fragments, and raw body without host opt-in" do
      assert {:error, url_error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: "https://user:pass@example.test/invoices?token=secret"
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert url_error.validation_errors == %{url: "url must not include userinfo"}

      assert {:error, query_error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: "https://example.test/invoices?token=secret"
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert query_error.validation_errors == %{
               url: "url must not include query string; use query_params instead"
             }

      assert {:error, fragment_error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :get,
                     url: "https://example.test/invoices#access_token=secret"
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert fragment_error.validation_errors == %{url: "url must not include fragment"}

      assert {:error, body_error} =
               HTTP.run(
                 %{
                   request: %{
                     method: :post,
                     url: "https://example.test/invoices",
                     body: "payload"
                   }
                 },
                 context(allowed_hosts: ["example.test"])
               )

      assert body_error.validation_errors == %{
               body: "raw body requires host action option allow_body?: true"
             }
    end

    test "rejects missing request input" do
      assert {:error, error} = HTTP.run(%{}, context())

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{request: "HTTP action request is required"},
               retryable?: false
             }
    end

    test "validates a planned action input with host-owned action options" do
      assert :ok =
               HTTP.validate_action_input(
                 %{request: %{method: "GET", url: "https://api.example.test/invoices"}},
                 allowed_hosts: ["api.example.test"]
               )

      assert {:error, error} =
               HTTP.validate_action_input(
                 %{request: %{method: "GET", url: "https://metadata.internal/latest"}},
                 allowed_hosts: ["api.example.test"]
               )

      assert error.validation_errors == %{url: "host is not allowed"}
    end

    test "rejects unexpected top-level action input fields before persistence" do
      assert {:error, error} =
               HTTP.validate_action_input(
                 %{
                   request: %{method: "GET", url: "https://api.example.test/invoices"},
                   secrets: %{api_key: "secret"}
                 },
                 allowed_hosts: ["api.example.test"]
               )

      assert error.validation_errors == %{
               input: "unsupported HTTP action input fields: secrets"
             }
    end
  end

  describe "validate_request/1" do
    test "validates HTTP config without performing a request" do
      assert :ok =
               HTTP.validate_request(%{
                 method: "GET",
                 url_template: "https://example.test/invoices/{{ invoice_id }}",
                 bindings: %{"invoice_id" => "inv_123"},
                 headers: %{"accept" => "application/json"},
                 query_params: %{page: 1},
                 timeout: 1_000
               })
    end

    test "rejects missing URL template bindings before execution" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: "GET",
                 url_template: "https://example.test/invoices/{{ invoice_id }}",
                 bindings: %{}
               })

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{url: "url_template binding invoice_id is required"},
               retryable?: false
             }
    end

    test "rejects non-map request config" do
      assert {:error, error} = HTTP.validate_request("not a map")

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{request: "HTTP action request must be a map"},
               retryable?: false
             }
    end

    test "rejects missing URL, invalid method, headers, params, and timeout together" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: :trace,
                 headers: [{"x-ok", "yes"}, {"x-bad", %{nested: true}}],
                 query_params: ["page", "1"],
                 timeout: 0
               })

      assert error.validation_errors == %{
               method: "method must be one of delete, get, head, options, patch, post, put",
               url: "url or url_template is required",
               headers: "headers must be a map or list of name/value pairs",
               query_params: "query params must be a map",
               timeout: "timeout must be a positive integer"
             }
    end

    test "rejects invalid map header values without raising" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: :get,
                 url: "https://example.test/invoices",
                 headers: %{"x-bad" => %{nested: true}}
               })

      assert error.validation_errors == %{
               headers: "headers must be a map or list of name/value pairs"
             }
    end

    test "rejects invalid URL template bindings shape" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: "GET",
                 url_template: "https://example.test/invoices/{{ invoice_id }}",
                 bindings: "invoice_id"
               })

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{url: "url_template bindings must be a map"},
               retryable?: false
             }
    end

    test "rejects invalid URL template binding values without raising" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: "GET",
                 url_template: "https://example.test/invoices/{{ invoice_id }}",
                 bindings: %{invoice_id: %{nested: true}}
               })

      assert error.validation_errors == %{
               url: "url_template bindings must be strings, numbers, or booleans"
             }
    end

    test "expands false URL template bindings" do
      assert :ok =
               HTTP.validate_request(%{
                 method: "GET",
                 url_template: "https://example.test/invoices/{{ dry_run }}",
                 bindings: %{dry_run: false}
               })
    end
  end

  describe "validate_request/2" do
    test "supports host allowed-destination policy checks" do
      request = %{
        method: "GET",
        url: "https://metadata.internal/latest"
      }

      assert {:error, error} = HTTP.validate_request(request, allowed_hosts: ["api.example.test"])

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{url: "host is not allowed"},
               retryable?: false
             }
    end

    test "rejects non-HTTP URLs before execution" do
      assert {:error, error} =
               HTTP.validate_request(%{
                 method: "GET",
                 url: "file:///etc/passwd"
               })

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{url: "url must use http or https and include a host"},
               retryable?: false
             }
    end

    test "rejects invalid allowed host policy" do
      assert {:error, error} =
               HTTP.validate_request(
                 %{
                   method: "GET",
                   url: "https://api.example.test/invoices"
                 },
                 allowed_hosts: "api.example.test"
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{url: "allowed_hosts must be a list"},
               retryable?: false
             }
    end

    test "rejects invalid validation options" do
      assert {:error, error} =
               HTTP.validate_request(
                 %{
                   method: "GET",
                   url: "https://api.example.test/invoices"
                 },
                 :invalid
               )

      assert error == %{
               message: "HTTP action request validation failed",
               validation_errors: %{opts: "validation options must be a keyword list"},
               retryable?: false
             }
    end
  end

  describe "action registry catalog" do
    test "exposes the HTTP action as an editor-safe approved action with credential references" do
      registry = %{
        "http.request" => [
          module: HTTP,
          category: "HTTP",
          credential_requirements: [%{name: "billing_api", required?: true}]
        ]
      }

      assert :ok = ActionRegistry.validate_action("http.request", registry)
      assert {:ok, [entry]} = ActionRegistry.catalog(registry)

      assert entry.key == "http.request"
      assert entry.display_name == "Http request"
      assert entry.category == "HTTP"

      assert entry.credential_requirements == [
               %{"name" => "billing_api", "required?" => true}
             ]

      assert entry.input_contract["request"]["required"] == true
      assert entry.output_contract["http_response"]["required"] == true
      refute inspect(entry) =~ "secret"
    end
  end

  defp endpoint_url(port, path) do
    "http://127.0.0.1:#{port}#{path}"
  end

  defp context(opts \\ []) do
    %Squidie.Step.Context{
      run_id: "00000000-0000-4000-8000-000000000358",
      workflow: __MODULE__.RuntimeWorkflow,
      step: :http_request,
      attempt: 1,
      step_opts: [action_opts: opts],
      state: %{}
    }
  end
end
