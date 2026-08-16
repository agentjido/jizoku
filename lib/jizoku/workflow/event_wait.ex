# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Workflow.EventWait do
  @moduledoc false

  @max_event_name_bytes 255
  @max_correlation_bytes 1_024

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

  defp storage_safe_string?(value, max_bytes) do
    is_binary(value) and value != "" and byte_size(value) <= max_bytes and String.valid?(value)
  end
end
