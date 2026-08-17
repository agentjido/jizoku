# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Workflow.EventWait do
  @moduledoc false

  @max_event_name_bytes 255
  @max_correlation_bytes 1_024
  @max_runtime_identifier_bytes 1_024
  @timeout_keys [:after, :on_timeout]
  @runtime_timeout_keys [
    :after,
    :correlation,
    :due_at,
    :event,
    :target,
    :timeout_runnable_key,
    :wait_id
  ]

  @doc false
  @spec valid_event?(term()) :: boolean()
  def valid_event?(event) do
    storage_safe_string?(event, @max_event_name_bytes)
  end

  @doc false
  @spec valid_correlation_declaration?(term()) :: boolean()
  def valid_correlation_declaration?(correlation) when is_binary(correlation) do
    valid_correlation_value?(correlation)
  end

  def valid_correlation_declaration?(correlation) when is_list(correlation) do
    correlation != [] and Enum.all?(correlation, &is_atom/1)
  end

  def valid_correlation_declaration?(_correlation) do
    false
  end

  @doc false
  @spec valid_correlation_value?(term()) :: boolean()
  def valid_correlation_value?(correlation) do
    storage_safe_string?(correlation, @max_correlation_bytes)
  end

  @doc false
  @spec timeout_from_opts(keyword()) :: {:ok, map() | nil} | {:error, term()}
  def timeout_from_opts(opts) when is_list(opts) do
    opts
    |> Keyword.get(:timeout)
    |> normalize_timeout()
  end

  @doc false
  @spec normalize_timeout(term()) :: {:ok, map() | nil} | {:error, term()}
  def normalize_timeout(nil) do
    {:ok, nil}
  end

  def normalize_timeout(timeout) when is_list(timeout) do
    if Keyword.keyword?(timeout) and unique_timeout_keys?(timeout) do
      timeout
      |> Map.new()
      |> normalize_timeout()
    else
      {:error, {:invalid_event_timeout, timeout}}
    end
  end

  def normalize_timeout(timeout) when is_map(timeout) do
    if Enum.sort(Map.keys(timeout)) == Enum.sort(@timeout_keys) and
         is_integer(Map.get(timeout, :after)) and Map.get(timeout, :after) > 0 and
         is_atom(Map.get(timeout, :on_timeout)) do
      {:ok, Map.take(timeout, @timeout_keys)}
    else
      {:error, {:invalid_event_timeout, timeout}}
    end
  end

  def normalize_timeout(timeout) do
    {:error, {:invalid_event_timeout, timeout}}
  end

  @doc false
  @spec timeout_target(keyword()) :: atom() | nil
  def timeout_target(opts) when is_list(opts) do
    case timeout_from_opts(opts) do
      {:ok, %{on_timeout: target}} -> target
      _missing_or_invalid -> nil
    end
  end

  def timeout_target(_opts) do
    nil
  end

  @doc false
  @spec valid_runtime_timeout?(term()) :: boolean()
  def valid_runtime_timeout?(timeout) when is_map(timeout) do
    Enum.sort(Map.keys(timeout)) == Enum.sort(@runtime_timeout_keys) and
      is_integer(Map.get(timeout, :after)) and Map.get(timeout, :after) > 0 and
      valid_correlation_value?(Map.get(timeout, :correlation)) and
      match?(%DateTime{}, Map.get(timeout, :due_at)) and
      valid_event?(Map.get(timeout, :event)) and
      storage_safe_string?(Map.get(timeout, :target), @max_runtime_identifier_bytes) and
      storage_safe_string?(
        Map.get(timeout, :timeout_runnable_key),
        @max_runtime_identifier_bytes
      ) and
      storage_safe_string?(Map.get(timeout, :wait_id), @max_runtime_identifier_bytes)
  end

  def valid_runtime_timeout?(_timeout) do
    false
  end

  defp storage_safe_string?(value, max_bytes) do
    is_binary(value) and value != "" and byte_size(value) <= max_bytes and String.valid?(value)
  end

  defp unique_timeout_keys?(timeout) do
    keys = Keyword.keys(timeout)
    length(keys) == length(Enum.uniq(keys))
  end
end
