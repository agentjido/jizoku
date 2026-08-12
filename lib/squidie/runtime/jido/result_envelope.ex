# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Jido.ResultEnvelope do
  @moduledoc false

  @envelope_key "__squidie_jido_result__"
  @completion_encoding %{"effect" => "run_instruction", "version" => 1}
  @version 1

  @type decoded :: {:ordinary, map()} | {:run_instruction, map(), map()}

  @doc false
  @spec wrap_run_instruction(map(), map()) :: map()
  def wrap_run_instruction(output, plan) when is_map(output) and is_map(plan) do
    payload = %{
      "kind" => "run_instruction",
      "output" => output,
      "plan" => plan,
      "version" => @version
    }

    %{
      @envelope_key => Map.put(payload, "fingerprint", fingerprint(payload))
    }
  end

  @doc false
  @spec completion_encoding() :: map()
  def completion_encoding do
    @completion_encoding
  end

  @doc false
  @spec native_effect?(term()) :: boolean()
  def native_effect?(nil), do: false
  def native_effect?(_encoding), do: true

  @doc false
  @spec supported_native_effect?(term()) :: boolean()
  def supported_native_effect?(@completion_encoding), do: true
  def supported_native_effect?(_encoding), do: false

  @doc false
  @spec decode(map(), term()) :: {:ok, decoded()} | {:error, :malformed_jido_result_envelope}
  def decode(result, completion_encoding) when is_map(result) do
    case completion_encoding do
      nil -> {:ok, {:ordinary, result}}
      @completion_encoding -> decode_native_effect(result)
      _unsupported -> {:error, :malformed_jido_result_envelope}
    end
  end

  @doc false
  @spec public_result(map() | nil, term()) :: map() | nil
  def public_result(nil, _completion_encoding), do: nil

  def public_result(result, completion_encoding) when is_map(result) do
    case decode(result, completion_encoding) do
      {:ok, {:ordinary, output}} -> output
      {:ok, {:run_instruction, output, _plan}} -> output
      {:error, :malformed_jido_result_envelope} -> nil
    end
  end

  @doc false
  @spec reserved_output?(map()) :: boolean()
  def reserved_output?(output) when is_map(output) do
    Map.has_key?(output, @envelope_key)
  end

  defp decode_native_effect(result) do
    case Map.fetch(result, @envelope_key) do
      {:ok,
       %{
         "kind" => "run_instruction",
         "fingerprint" => fingerprint,
         "output" => output,
         "plan" => plan,
         "version" => @version
       } = envelope}
      when map_size(result) == 1 and map_size(envelope) == 5 and is_binary(fingerprint) and
             is_map(output) and is_map(plan) ->
        payload = Map.delete(envelope, "fingerprint")

        if fingerprint(payload) == fingerprint do
          {:ok, {:run_instruction, output, plan}}
        else
          {:error, :malformed_jido_result_envelope}
        end

      {:ok, _malformed} ->
        {:error, :malformed_jido_result_envelope}

      :error ->
        {:error, :malformed_jido_result_envelope}
    end
  end

  defp fingerprint(payload) do
    payload
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end
end
