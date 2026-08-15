defmodule Jizoku.Tools.HTTP do
  @moduledoc """
  HTTP tool adapter backed by Req.
  """

  @behaviour Jizoku.Tools.Adapter

  alias Jizoku.Tools.Error
  alias Jizoku.Tools.Result

  @type request :: %{
          required(:method) => atom(),
          required(:url) => String.t(),
          optional(:body) => term(),
          optional(:headers) => keyword() | [{String.t(), String.t()}],
          optional(:json) => term(),
          optional(:params) => map(),
          optional(:timeout) => pos_integer()
        }

  @impl Jizoku.Tools.Adapter
  @spec invoke(request(), map(), keyword()) :: {:ok, Result.t()} | {:error, Error.t()}
  def invoke(request, _context, _opts) when is_map(request) do
    with :ok <- validate_request(request) do
      request
      |> build_req_options()
      |> Req.request()
      |> normalize_response(request)
    end
  end

  def invoke(_request, _context, _opts) do
    {:error,
     Error.new(
       adapter: __MODULE__,
       kind: :invalid_request,
       message: "HTTP tool requests must be maps",
       details: %{reason: :expected_map},
       retryable?: false
     )}
  end

  @spec validate_request(map()) :: :ok | {:error, Error.t()}
  defp validate_request(%{method: method, url: url})
       when is_atom(method) and is_binary(url) and byte_size(url) > 0,
       do: :ok

  defp validate_request(request) do
    {:error,
     Error.new(
       adapter: __MODULE__,
       kind: :invalid_request,
       message: "HTTP tool requests require an atom :method and binary :url",
       details: %{request: request},
       retryable?: false
     )}
  end

  @spec build_req_options(request()) :: keyword()
  defp build_req_options(request) do
    Enum.reduce(
      request,
      [method: request.method, redirect: false, retry: false, url: request.url],
      fn
        {:headers, headers}, opts ->
          Keyword.put(opts, :headers, headers)

        {:params, params}, opts ->
          Keyword.put(opts, :params, params)

        {:json, json}, opts ->
          Keyword.put(opts, :json, json)

        {:body, body}, opts ->
          Keyword.put(opts, :body, body)

        {:timeout, timeout}, opts when is_integer(timeout) and timeout > 0 ->
          opts
          |> Keyword.put(:receive_timeout, timeout)
          |> Keyword.put(:connect_options, timeout: timeout)

        {_key, _value}, opts ->
          opts
      end
    )
  end

  @spec normalize_response({:ok, Req.Response.t()} | {:error, term()}, request()) ::
          {:ok, Result.t()} | {:error, Error.t()}
  defp normalize_response({:ok, %Req.Response{} = response}, request) do
    if response.status in 200..399 do
      {:ok,
       %Result{
         adapter: __MODULE__,
         payload: Req.Response.to_map(response),
         metadata: %{method: request.method, url: request.url}
       }}
    else
      {:error,
       Error.new(
         adapter: __MODULE__,
         kind: :http,
         message: "HTTP request failed with status #{response.status}",
         details:
           response
           |> Req.Response.to_map()
           |> Map.put(:method, request.method)
           |> Map.put(:url, request.url),
         retryable?: retryable_status?(response.status)
       )}
    end
  end

  defp normalize_response({:error, %Req.TransportError{reason: reason}}, request) do
    kind = if timeout_reason?(reason), do: :timeout, else: :transport

    {:error,
     Error.new(
       adapter: __MODULE__,
       kind: kind,
       message: Req.TransportError.message(%Req.TransportError{reason: reason}),
       details: %{reason: inspect(reason), method: request.method, url: request.url},
       retryable?: true
     )}
  end

  defp normalize_response({:error, reason}, request) do
    {:error,
     Error.new(
       adapter: __MODULE__,
       kind: :transport,
       message: "HTTP request failed",
       details: %{reason: inspect(reason), method: request.method, url: request.url},
       retryable?: true
     )}
  end

  @spec timeout_reason?(term()) :: boolean()
  defp timeout_reason?(reason) do
    reason in [:timeout, :connect_timeout]
  end

  defp retryable_status?(status) when status in [408, 429], do: true
  defp retryable_status?(status), do: status >= 500
end
