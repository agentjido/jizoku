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
    with :ok <- validate_credential_refs(Map.get(input, :credential_refs, %{})),
         {:ok, request} <- normalize_request(request) do
      Tools.HTTP
      |> Tools.invoke(request, tool_context(context))
      |> normalize_tool_result()
    end
  end

  def run(_input, _context) do
    {:error, validation_error(%{request: "HTTP action request is required"})}
  end

  defp normalize_request(request, opts \\ []) do
    errors =
      %{}
      |> validate_no_credential_values(request)
      |> validate_method(request)
      |> validate_url(request, opts)
      |> validate_headers(request)
      |> validate_params(request)
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
      {:ok, _headers} -> errors
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

    if uri.scheme in ["http", "https"] and non_empty_binary?(uri.host) do
      :ok
    else
      {:error, "url must use http or https and include a host"}
    end
  end

  defp validate_allowed_host(url, opts) do
    case Keyword.get(opts, :allowed_hosts) do
      nil ->
        :ok

      allowed_hosts when is_list(allowed_hosts) ->
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

    case Enum.find(placeholders, &is_nil(field(bindings, &1))) do
      nil ->
        {:ok,
         Regex.replace(~r/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/, template, fn _match, key ->
           bindings
           |> field(key)
           |> to_string()
           |> URI.encode_www_form()
         end)}

      missing ->
        {:error, "url_template binding #{missing} is required"}
    end
  end

  defp expand_url_template(_template, _bindings) do
    {:error, "url_template bindings must be a map"}
  end

  defp normalize_headers(nil), do: {:ok, nil}

  defp normalize_headers(headers) when is_map(headers) do
    headers
    |> Enum.map(fn {name, value} -> {to_string(name), to_string(value)} end)
    |> then(&{:ok, &1})
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

  defp normalize_tool_result({:ok, result}) do
    {:ok, %{http_response: result.payload}}
  end

  defp normalize_tool_result({:error, %Error{retryable?: true} = error}) do
    {:retry, Error.to_map(error)}
  end

  defp normalize_tool_result({:error, %Error{} = error}) do
    {:error, Error.to_map(error)}
  end

  defp tool_context(%Context{} = context) do
    %{
      run_id: context.run_id,
      workflow: context.workflow,
      step: context.step,
      attempt: context.attempt
    }
  end

  defp validation_error(errors) do
    %{
      message: "HTTP action request validation failed",
      validation_errors: errors,
      retryable?: false
    }
  end

  defp field(map, key) when is_map(map) and is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp field(map, key) when is_map(map) and is_binary(key) do
    Map.get(map, key) || Map.get(map, existing_atom(key))
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

  defp non_empty_binary?(value), do: is_binary(value) and value != ""

  defp allowed_methods do
    @allowed_methods
    |> Map.keys()
    |> Enum.join(", ")
  end
end
