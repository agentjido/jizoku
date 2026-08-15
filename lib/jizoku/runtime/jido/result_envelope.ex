# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Jido.ResultEnvelope do
  @moduledoc false

  @envelope_key "__jizoku_jido_result__"
  @run_instruction_encoding %{"effect" => "run_instruction", "version" => 1}
  @emit_encoding %{"effect" => "emit_signal", "version" => 1}
  @version 1

  @type decoded ::
          {:ordinary, map()}
          | {:emit, map(), map()}
          | {:run_instruction, map(), map()}

  @doc false
  @spec wrap_run_instruction(map(), map()) :: map()
  def wrap_run_instruction(output, plan) when is_map(output) and is_map(plan) do
    wrap("run_instruction", output, "plan", plan)
  end

  @doc false
  @spec wrap_emit(map(), map()) :: map()
  def wrap_emit(output, intent) when is_map(output) and is_map(intent) do
    wrap("emit_signal", output, "intent", intent)
  end

  @doc false
  @spec completion_encoding() :: map()
  def completion_encoding do
    @run_instruction_encoding
  end

  @doc false
  @spec emit_completion_encoding() :: map()
  def emit_completion_encoding do
    @emit_encoding
  end

  @doc false
  @spec native_effect?(term()) :: boolean()
  def native_effect?(nil), do: false
  def native_effect?(_encoding), do: true

  @doc false
  @spec supported_native_effect?(term()) :: boolean()
  def supported_native_effect?(@run_instruction_encoding), do: true
  def supported_native_effect?(@emit_encoding), do: true
  def supported_native_effect?(_encoding), do: false

  @doc false
  @spec decode(map(), term()) :: {:ok, decoded()} | {:error, :malformed_jido_result_envelope}
  def decode(result, completion_encoding) when is_map(result) do
    case completion_encoding do
      nil -> {:ok, {:ordinary, result}}
      @run_instruction_encoding -> decode_native_effect(result, "run_instruction", "plan")
      @emit_encoding -> decode_native_effect(result, "emit_signal", "intent")
      _unsupported -> {:error, :malformed_jido_result_envelope}
    end
  end

  @doc false
  @spec public_result(map() | nil, term()) :: map() | nil
  def public_result(nil, _completion_encoding), do: nil

  def public_result(result, completion_encoding) when is_map(result) do
    case decode(result, completion_encoding) do
      {:ok, {:ordinary, output}} -> output
      {:ok, {:emit, output, _intent}} -> output
      {:ok, {:run_instruction, output, _plan}} -> output
      {:error, :malformed_jido_result_envelope} -> nil
    end
  end

  @doc false
  @spec reserved_output?(map()) :: boolean()
  def reserved_output?(output) when is_map(output) do
    Map.has_key?(output, @envelope_key)
  end

  defp wrap(kind, output, value_key, value) do
    payload = %{
      "kind" => kind,
      "output" => output,
      value_key => value,
      "version" => @version
    }

    %{@envelope_key => Map.put(payload, "fingerprint", fingerprint(payload))}
  end

  defp decode_native_effect(result, kind, value_key) do
    case Map.fetch(result, @envelope_key) do
      {:ok,
       %{
         "kind" => ^kind,
         "fingerprint" => fingerprint,
         "output" => output,
         "version" => @version
       } = envelope}
      when map_size(result) == 1 and map_size(envelope) == 5 and is_binary(fingerprint) and
             is_map(output) ->
        payload = Map.delete(envelope, "fingerprint")
        value = Map.get(envelope, value_key)

        if is_map(value) and fingerprint(payload) == fingerprint do
          decoded_effect(kind, output, value)
        else
          {:error, :malformed_jido_result_envelope}
        end

      {:ok, _malformed} ->
        {:error, :malformed_jido_result_envelope}

      :error ->
        {:error, :malformed_jido_result_envelope}
    end
  end

  defp decoded_effect("run_instruction", output, plan) do
    {:ok, {:run_instruction, output, plan}}
  end

  defp decoded_effect("emit_signal", output, intent) do
    {:ok, {:emit, output, intent}}
  end

  defp fingerprint(payload) do
    payload
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end
end
