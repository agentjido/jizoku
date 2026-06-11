defmodule Squidie.Step.HTTP do
  @moduledoc """
  Native HTTP action for runtime-authored workflow specs.

  Hosts expose this module through their action registry under stable action
  keys. The step validates request configuration, keeps credentials as
  host-owned references, delegates transport to `Squidie.Tools.HTTP`, and
  returns structured workflow output or errors.
  """

  use Squidie.Step,
    name: :http_request,
    description: "Performs a host-approved HTTP request",
    input_schema: [
      request: [type: :map, required: true],
      credential_refs: [type: :map, required: false]
    ],
    output_schema: [
      http_response: [type: :map, required: true]
    ]

  alias Squidie.Step.Context
  alias Squidie.Tools
  alias Squidie.Tools.Error

  @allowed_methods %{
    "delete" => :delete,
    "get" => :get,
    "head" => :head,
    "options" => :options,
    "patch" => :patch,
    "post" => :post,
    "put" => :put
  }

  @credential_value_fields [:credentials, :credential_values, :secrets]
  @secret_header_names [
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-api-key",
    "x-auth-token"
  ]
  @secret_field_fragments [
    "api_key",
    "apikey",
    "auth",
    "credential",
    "password",
    "secret",
    "token"
  ]
  @default_max_body_bytes 8_192

  @doc """
  Validates planned HTTP action input with host-owned action options.
  """
  @spec validate_action_input(term(), keyword()) :: :ok | {:error, map()}
  def validate_action_input(%{request: request} = input, opts)
      when is_map(request) and is_list(opts) do
    with :ok <- validate_action_policy(opts),
         :ok <- validate_action_input_fields(input),
         :ok <- validate_credential_refs(Map.get(input, :credential_refs, %{})) do
      validate_request(request, opts)
    end
  end

  def validate_action_input(_input, opts) when is_list(opts) do
    {:error, validation_error(%{request: "HTTP action request is required"})}
  end

  def validate_action_input(_input, _opts) do
    {:error, validation_error(%{opts: "validation options must be a keyword list"})}
  end

  @doc """
  Validates HTTP action request configuration without performing network I/O.
  """
  @spec validate_request(term()) :: :ok | {:error, map()}
  def validate_request(request), do: validate_request(request, [])

  @doc """
  Validates HTTP action request configuration with host policy options.

  Supported options:

    * `:allowed_hosts` - list of hostnames the request URL may target.
  """
  @spec validate_request(term(), keyword()) :: :ok | {:error, map()}
  def validate_request(request, opts) when is_map(request) and is_list(opts) do
    case normalize_request(request, opts) do
      {:ok, _request} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def validate_request(_request, opts) when is_list(opts) do
    {:error, validation_error(%{request: "HTTP action request must be a map"})}
  end

  def validate_request(_request, _opts) do
    {:error, validation_error(%{opts: "validation options must be a keyword list"})}
  end

  @impl Squidie.Step
  def run(%{request: request} = input, %Context{} = context) when is_map(request) do
    action_opts = action_opts(context)

    with :ok <- validate_action_input(input, action_opts),
         {:ok, request} <- normalize_request(request, action_opts) do
      Tools.HTTP
      |> Tools.invoke(request, tool_context(context))
      |> normalize_tool_result(action_opts)
    end
  end

  def run(_input, _context) do
    {:error, validation_error(%{request: "HTTP action request is required"})}
  end

  defp normalize_request(request, opts) do
    errors =
      %{}
      |> validate_no_credential_values(request)
      |> validate_method(request)
      |> validate_url(request, opts)
      |> validate_headers(request)
      |> validate_params(request)
      |> validate_secret_values(
        :query_params,
        field(request, :query_params) || field(request, :params)
      )
      |> validate_secret_values(:json, field(request, :json))
      |> validate_body(request, opts)
      |> validate_timeout(request)

    if map_size(errors) == 0 do
      {:ok, build_request(request, opts)}
    else
      {:error, validation_error(errors)}
    end
  end

  defp validate_credential_refs(refs) when is_map(refs) do
    invalid? =
      Enum.any?(refs, fn
        {_name, ref} when is_binary(ref) -> ref == ""
        _other -> true
      end)

    if invalid? do
      {:error, validation_error(%{credential_refs: "credential references must be strings"})}
    else
      :ok
    end
  end

  defp validate_credential_refs(_refs) do
    {:error, validation_error(%{credential_refs: "credential references must be a map"})}
  end

  defp validate_action_input_fields(input) do
    allowed_keys = [:request, :credential_refs, "request", "credential_refs"]

    case Enum.reject(Map.keys(input), &(&1 in allowed_keys)) do
      [] ->
        :ok

      fields ->
        {:error,
         validation_error(%{
           input:
             "unsupported HTTP action input fields: #{Enum.map_join(fields, ", ", &to_string/1)}"
         })}
    end
  end

  defp validate_no_credential_values(errors, request) do
    Enum.reduce(@credential_value_fields, errors, fn field, acc ->
      if has_field?(request, field) do
        Map.put(acc, field, "credential values are not allowed")
      else
        acc
      end
    end)
  end

  defp validate_method(errors, request) do
    case normalize_method(field(request, :method)) do
      {:ok, _method} -> errors
      {:error, message} -> Map.put(errors, :method, message)
    end
  end

  defp validate_url(errors, request, opts) do
    case normalize_url(request, opts) do
      {:ok, _url} -> errors
      {:error, message} -> Map.put(errors, :url, message)
    end
  end

  defp validate_headers(errors, request) do
    case normalize_headers(field(request, :headers)) do
      {:ok, headers} -> validate_secret_headers(errors, headers)
      {:error, message} -> Map.put(errors, :headers, message)
    end
  end

  defp validate_params(errors, request) do
    case normalize_params(field(request, :query_params) || field(request, :params)) do
      {:ok, _params} -> errors
      {:error, message} -> Map.put(errors, :query_params, message)
    end
  end

  defp validate_timeout(errors, request) do
    case field(request, :timeout) do
      nil -> errors
      timeout when is_integer(timeout) and timeout > 0 -> errors
      _timeout -> Map.put(errors, :timeout, "timeout must be a positive integer")
    end
  end

  defp validate_body(errors, request, opts) do
    case field(request, :body) do
      nil ->
        errors

      body when is_binary(body) ->
        if Keyword.get(opts, :allow_body?, false) == true do
          errors
        else
          Map.put(errors, :body, "raw body requires host action option allow_body?: true")
        end

      _body ->
        Map.put(errors, :body, "body must be a string")
    end
  end

  defp validate_action_policy(opts) do
    case Keyword.get(opts, :allowed_hosts) do
      hosts when is_list(hosts) and hosts != [] ->
        :ok

      _missing_or_invalid ->
        {:error, validation_error(%{allowed_hosts: "allowed_hosts policy is required"})}
    end
  end

  defp validate_secret_headers(errors, nil), do: errors

  defp validate_secret_headers(errors, headers) do
    if Enum.any?(headers, fn {name, _value} -> secret_header?(name) end) do
      Map.put(errors, :headers, "secret-bearing request headers are not allowed")
    else
      errors
    end
  end

  defp validate_secret_values(errors, _field, nil), do: errors

  defp validate_secret_values(errors, field, value) do
    if secret_value?(value) do
      Map.put(errors, field, "secret-bearing request values are not allowed")
    else
      errors
    end
  end

  defp build_request(request, opts) do
    {:ok, method} = normalize_method(field(request, :method))
    {:ok, url} = normalize_url(request, opts)
    {:ok, headers} = normalize_headers(field(request, :headers))
    {:ok, params} = normalize_params(field(request, :query_params) || field(request, :params))

    %{method: method, url: url}
    |> maybe_put(:headers, headers)
    |> maybe_put(:params, params)
    |> maybe_put(:json, field(request, :json))
    |> maybe_put(:body, field(request, :body))
    |> maybe_put(:timeout, field(request, :timeout))
  end

  defp normalize_method(method) when is_atom(method) do
    if method in Map.values(@allowed_methods) do
      {:ok, method}
    else
      {:error, "method must be one of #{allowed_methods()}"}
    end
  end

  defp normalize_method(method) when is_binary(method) do
    method
    |> String.downcase()
    |> then(fn method ->
      case Map.fetch(@allowed_methods, method) do
        {:ok, method} -> {:ok, method}
        :error -> {:error, "method must be one of #{allowed_methods()}"}
      end
    end)
  end

  defp normalize_method(_method), do: {:error, "method must be one of #{allowed_methods()}"}

  defp normalize_url(request, opts) do
    with {:ok, url} <- url_from_request(request),
         :ok <- validate_http_url(url),
         :ok <- validate_allowed_host(url, opts) do
      {:ok, url}
    end
  end

  defp url_from_request(request) do
    cond do
      non_empty_binary?(field(request, :url)) ->
        {:ok, field(request, :url)}

      non_empty_binary?(field(request, :url_template)) ->
        expand_url_template(field(request, :url_template), field(request, :bindings) || %{})

      true ->
        {:error, "url or url_template is required"}
    end
  end

  defp validate_http_url(url) do
    uri = URI.parse(url)

    cond do
      uri.scheme not in ["http", "https"] or not non_empty_binary?(uri.host) ->
        {:error, "url must use http or https and include a host"}

      non_empty_binary?(uri.userinfo) ->
        {:error, "url must not include userinfo"}

      non_empty_binary?(uri.query) ->
        {:error, "url must not include query string; use query_params instead"}

      non_empty_binary?(uri.fragment) ->
        {:error, "url must not include fragment"}

      true ->
        :ok
    end
  end

  defp validate_allowed_host(url, opts) do
    case Keyword.get(opts, :allowed_hosts) do
      nil ->
        :ok

      allowed_hosts when is_list(allowed_hosts) and allowed_hosts != [] ->
        host = URI.parse(url).host

        if host in allowed_hosts do
          :ok
        else
          {:error, "host is not allowed"}
        end

      _invalid ->
        {:error, "allowed_hosts must be a list"}
    end
  end

  defp expand_url_template(template, bindings) when is_map(bindings) do
    placeholders =
      ~r/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/
      |> Regex.scan(template, capture: :all_but_first)
      |> List.flatten()

    cond do
      missing = Enum.find(placeholders, &is_nil(field(bindings, &1))) ->
        {:error, "url_template binding #{missing} is required"}

      invalid_template_binding?(placeholders, bindings) ->
        {:error, "url_template bindings must be strings, numbers, or booleans"}

      true ->
        {:ok,
         Regex.replace(~r/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/, template, fn _match, key ->
           bindings
           |> field(key)
           |> to_string()
           |> URI.encode_www_form()
         end)}
    end
  end

  defp expand_url_template(_template, _bindings) do
    {:error, "url_template bindings must be a map"}
  end

  defp normalize_headers(nil), do: {:ok, nil}

  defp normalize_headers(headers) when is_map(headers) do
    if Enum.all?(headers, &header_pair?/1) do
      {:ok, Enum.map(headers, fn {name, value} -> {to_string(name), to_string(value)} end)}
    else
      {:error, "headers must be a map or list of name/value pairs"}
    end
  end

  defp normalize_headers(headers) when is_list(headers) do
    if Enum.all?(headers, &header_pair?/1) do
      {:ok, Enum.map(headers, fn {name, value} -> {to_string(name), to_string(value)} end)}
    else
      {:error, "headers must be a map or list of name/value pairs"}
    end
  end

  defp normalize_headers(_headers),
    do: {:error, "headers must be a map or list of name/value pairs"}

  defp normalize_params(nil), do: {:ok, nil}
  defp normalize_params(params) when is_map(params), do: {:ok, params}
  defp normalize_params(_params), do: {:error, "query params must be a map"}

  defp normalize_tool_result({:ok, result}, opts) do
    {:ok, %{http_response: safe_response(result.payload, opts)}}
  end

  defp normalize_tool_result({:error, %Error{retryable?: true} = error}, opts) do
    {:retry, safe_error(error, opts)}
  end

  defp normalize_tool_result({:error, %Error{} = error}, opts) do
    {:error, safe_error(error, opts)}
  end

  defp tool_context(%Context{} = context) do
    %{
      run_id: context.run_id,
      workflow: context.workflow,
      step: context.step,
      attempt: context.attempt,
      runnable_key: context.runnable_key,
      idempotency_key: context.idempotency_key,
      claim_id: context.claim_id
    }
  end

  defp action_opts(%Context{step_opts: step_opts}) when is_list(step_opts) do
    Keyword.get(step_opts, :action_opts, [])
  end

  defp action_opts(%Context{}), do: []

  defp safe_response(response, opts) when is_map(response) do
    %{
      status: Map.get(response, :status),
      headers: safe_headers(Map.get(response, :headers)),
      trailers: safe_headers(Map.get(response, :trailers)),
      body: safe_body(Map.get(response, :body), opts),
      body_truncated?: body_truncated?(Map.get(response, :body), opts)
    }
  end

  defp safe_error(%Error{} = error, opts) do
    error
    |> Error.to_map()
    |> Map.update(:details, %{}, &safe_error_details(&1, opts))
  end

  defp safe_error_details(details, opts) when is_map(details) do
    details
    |> Map.update(:headers, nil, &safe_headers/1)
    |> Map.update(:trailers, nil, &safe_headers/1)
    |> Map.update(:body, nil, &safe_body(&1, opts))
    |> Map.put(:body_truncated?, body_truncated?(Map.get(details, :body), opts))
    |> Map.update(:url, nil, &redact_url/1)
  end

  defp safe_headers(nil), do: nil

  defp safe_headers(headers) when is_map(headers) do
    Map.new(headers, fn {name, value} -> {to_string(name), safe_header_value(name, value)} end)
  end

  defp safe_headers(headers) when is_list(headers) do
    headers
    |> Enum.map(fn {name, value} -> {to_string(name), safe_header_value(name, value)} end)
    |> Map.new()
  end

  defp safe_headers(_headers), do: nil

  defp safe_header_value(name, value) when is_binary(name) do
    if secret_header?(name), do: "[REDACTED]", else: to_string(value)
  end

  defp safe_header_value(name, value) do
    safe_header_value(to_string(name), value)
  end

  defp safe_body(nil, _opts), do: nil

  defp safe_body(body, opts) do
    if Keyword.get(opts, :persist_response_body?, false) == true do
      do_safe_body(body, opts)
    end
  end

  defp do_safe_body(body, opts) when is_binary(body) do
    binary_part(body, 0, min(byte_size(body), max_body_bytes(opts)))
  end

  defp do_safe_body(body, opts) do
    body
    |> inspect(limit: 50)
    |> do_safe_body(opts)
  end

  defp body_truncated?(body, opts) do
    Keyword.get(opts, :persist_response_body?, false) == true and do_body_truncated?(body, opts)
  end

  defp do_body_truncated?(body, opts) when is_binary(body),
    do: byte_size(body) > max_body_bytes(opts)

  defp do_body_truncated?(nil, _opts), do: false

  defp do_body_truncated?(body, opts) do
    body
    |> inspect(limit: 50)
    |> do_body_truncated?(opts)
  end

  defp max_body_bytes(opts) do
    case Keyword.get(opts, :max_body_bytes, @default_max_body_bytes) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> @default_max_body_bytes
    end
  end

  defp redact_url(nil), do: nil

  defp redact_url(url) when is_binary(url) do
    uri = URI.parse(url)
    URI.to_string(%{uri | fragment: nil, query: nil, userinfo: nil})
  end

  defp redact_url(url), do: url

  defp validation_error(errors) do
    Squidie.Step.ErrorPayload.validation_failed("HTTP action request validation failed", errors)
  end

  defp field(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp field(map, key) when is_map(map) and is_binary(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, existing_atom(key))
    end
  rescue
    ArgumentError -> Map.get(map, key)
  end

  defp existing_atom(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end

  defp has_field?(map, key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, :headers, []), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp header_pair?({name, value}) do
    (is_atom(name) or is_binary(name)) and
      (is_atom(value) or is_binary(value) or is_number(value))
  end

  defp header_pair?(_pair), do: false

  defp secret_header?(name) do
    key = secret_key_name(name)

    Enum.any?(@secret_field_fragments, &String.contains?(key, &1)) or
      key in Enum.map(@secret_header_names, &secret_key_name/1)
  end

  defp secret_value?(value) when is_map(value) do
    Enum.any?(value, fn {key, item} -> secret_key?(key) or secret_value?(item) end)
  end

  defp secret_value?(value) when is_list(value), do: Enum.any?(value, &secret_value?/1)
  defp secret_value?(_value), do: false

  defp secret_key?(key) do
    key = secret_key_name(key)

    Enum.any?(@secret_field_fragments, &String.contains?(key, &1))
  end

  defp secret_key_name(key) do
    key
    |> to_string()
    |> String.downcase()
    |> String.replace(~r/[^a-z0-9]/, "")
  end

  defp invalid_template_binding?(placeholders, bindings) do
    Enum.any?(placeholders, fn key ->
      value = field(bindings, key)
      not (is_binary(value) or is_number(value) or is_boolean(value))
    end)
  end

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp allowed_methods do
    Enum.map_join(@allowed_methods, ", ", fn {method, _value} -> method end)
  end
end
