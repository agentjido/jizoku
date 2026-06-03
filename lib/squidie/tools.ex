defmodule Squidie.Tools do
  @moduledoc """
  Public boundary for invoking external tools from workflow steps.

  Tool adapters provide a stable integration layer for steps that need to talk
  to external systems without leaking transport-specific response or error
  shapes into workflow code.
  """

  alias Squidie.Tools.Error
  alias Squidie.Tools.Result

  @type adapter :: module()
  @type request :: map()
  @type context :: map()
  @type invoke_error ::
          {:invalid_request, :expected_map}
          | {:invalid_context, :expected_map}
          | {:invalid_adapter, module()}

  @doc """
  Invokes a tool adapter through the shared contract.
  """
  @spec invoke(adapter(), request(), context(), keyword()) ::
          {:ok, Result.t()} | {:error, Error.t()}
  def invoke(adapter, request, context \\ %{}, opts \\ [])

  def invoke(adapter, request, context, opts)
      when is_atom(adapter) and is_map(request) and is_map(context) and is_list(opts) do
    with :ok <- ensure_adapter(adapter),
         adapter_result <- adapter.invoke(request, context, opts) do
      normalize_adapter_result(adapter, adapter_result)
    else
      {:error, reason} ->
        {:error, invalid_contract_error(adapter, reason)}
    end
  end

  def invoke(adapter, request, _context, _opts) when not is_map(request) do
    {:error,
     Error.new(
       adapter: adapter,
       kind: :invalid_request,
       message: "tool requests must be maps",
       details: %{reason: :expected_map},
       retryable?: false
     )}
  end

  def invoke(adapter, _request, context, _opts) when not is_map(context) do
    {:error,
     Error.new(
       adapter: adapter,
       kind: :invalid_context,
       message: "tool contexts must be maps",
       details: %{reason: :expected_map},
       retryable?: false
     )}
  end

  @spec ensure_adapter(adapter()) :: :ok | {:error, invoke_error()}
  defp ensure_adapter(adapter) do
    case Code.ensure_loaded(adapter) do
      {:module, ^adapter} ->
        if function_exported?(adapter, :invoke, 3) do
          :ok
        else
          {:error, {:invalid_adapter, adapter}}
        end

      {:error, _reason} ->
        {:error, {:invalid_adapter, adapter}}
    end
  end

  @spec normalize_adapter_result(adapter(), term()) :: {:ok, Result.t()} | {:error, Error.t()}
  defp normalize_adapter_result(_adapter, {:ok, %Result{} = result}), do: {:ok, result}
  defp normalize_adapter_result(_adapter, {:error, %Error{} = error}), do: {:error, error}

  defp normalize_adapter_result(adapter, other) do
    {:error, invalid_contract_error(adapter, {:unexpected_result, other})}
  end

  @spec invalid_contract_error(adapter(), term()) :: Error.t()
  defp invalid_contract_error(adapter, reason) do
    Error.new(
      adapter: adapter,
      kind: :adapter_contract,
      message: "tool adapter returned an invalid result",
      details: %{reason: inspect(reason)},
      retryable?: false
    )
  end
end
