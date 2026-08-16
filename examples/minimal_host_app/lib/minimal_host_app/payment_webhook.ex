defmodule MinimalHostApp.PaymentWebhook do
  @moduledoc """
  Host-owned boundary for verified payment-provider callbacks.

  A Phoenix controller can pass the untouched request body and signature header
  here after applying its normal request-size, rate-limit, and authorization
  controls. Jizoku receives the event only after signature and payload
  validation succeed.
  """

  @signature_prefix "sha256="

  @type delivery_result ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, term()}

  @spec deliver(Ecto.UUID.t(), binary(), binary(), binary()) :: delivery_result()
  def deliver(run_id, raw_body, signature, secret)
      when is_binary(run_id) and is_binary(raw_body) and is_binary(signature) and
             is_binary(secret) and byte_size(secret) > 0 do
    with :ok <- verify_signature(raw_body, signature, secret),
         {:ok, event} <- decode_event(raw_body) do
      Jizoku.signal_run(
        run_id,
        "payment.completed",
        %{payment_id: event.payment_id, status: event.status},
        correlation: event.payment_id,
        idempotency_key: "payment-provider:" <> event.event_id,
        metadata: %{source: "minimal_host_app.payment_webhook"}
      )
    end
  end

  def deliver(_run_id, _raw_body, _signature, _secret) do
    {:error, {:invalid_webhook, :invalid_request}}
  end

  defp verify_signature(raw_body, @signature_prefix <> encoded_signature, secret) do
    expected = :crypto.mac(:hmac, :sha256, secret, raw_body)

    case Base.decode16(encoded_signature, case: :mixed) do
      {:ok, received} ->
        if secure_compare(expected, received) do
          :ok
        else
          {:error, :invalid_webhook_signature}
        end

      :error ->
        {:error, :invalid_webhook_signature}
    end
  end

  defp verify_signature(_raw_body, _signature, _secret) do
    {:error, :invalid_webhook_signature}
  end

  defp decode_event(raw_body) do
    with {:ok, payload} when is_map(payload) <- Jason.decode(raw_body),
         {:ok, event_id} <- fetch_non_empty_string(payload, "event_id"),
         {:ok, payment_id} <- fetch_non_empty_string(payload, "payment_id"),
         {:ok, status} <- fetch_non_empty_string(payload, "status") do
      {:ok, %{event_id: event_id, payment_id: payment_id, status: status}}
    else
      _invalid -> {:error, {:invalid_webhook, :invalid_payload}}
    end
  end

  defp fetch_non_empty_string(payload, key) do
    case Map.get(payload, key) do
      value when is_binary(value) and byte_size(value) > 0 -> {:ok, value}
      _invalid -> {:error, key}
    end
  end

  defp secure_compare(left, right) when byte_size(left) == byte_size(right) do
    left
    |> :binary.bin_to_list()
    |> Enum.zip(:binary.bin_to_list(right))
    |> Enum.reduce(0, fn {left_byte, right_byte}, difference ->
      Bitwise.bor(difference, Bitwise.bxor(left_byte, right_byte))
    end)
    |> Kernel.==(0)
  end

  defp secure_compare(_left, _right) do
    false
  end
end
