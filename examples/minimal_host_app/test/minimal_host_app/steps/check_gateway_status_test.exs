defmodule MinimalHostApp.Steps.CheckGatewayStatusTest do
  use ExUnit.Case, async: true

  alias MinimalHostApp.Steps.CheckGatewayStatus
  alias MinimalHostApp.Workflows.PaymentRecovery

  test "includes durable attempt metadata from step context in gateway check output" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "GET", "/gateway", fn conn ->
      Plug.Conn.resp(conn, 200, "retry_required")
    end)

    context = %Jizoku.Step.Context{
      run_id: "run_123",
      workflow: PaymentRecovery,
      step: :check_gateway_status,
      attempt: 2,
      runnable_key: "run_123:check_gateway_status:2",
      idempotency_key: "run_123:check_gateway_status:attempt_789",
      claim_id: "claim_123",
      state: %{}
    }

    assert {:ok, %{gateway_check: gateway_check}} =
             CheckGatewayStatus.run(
               %{
                 invoice: %{id: "inv_456"},
                 gateway_url: "http://localhost:#{bypass.port}/gateway"
               },
               context
             )

    assert gateway_check.attempt == %{
             idempotency_key: "run_123:check_gateway_status:attempt_789",
             claim_id: "claim_123"
           }
  end

  test "includes durable attempt metadata in retry errors" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "GET", "/gateway", fn conn ->
      Plug.Conn.resp(conn, 503, "gateway_unavailable")
    end)

    assert {:retry, error} =
             CheckGatewayStatus.run(
               %{
                 invoice: %{id: "inv_456"},
                 gateway_url: "http://localhost:#{bypass.port}/gateway"
               },
               step_context()
             )

    assert error.kind == :http
    assert error.retryable? == true
    assert error.details.status == 503

    assert error.gateway_check.attempt == %{
             idempotency_key: "run_123:check_gateway_status:attempt_789",
             claim_id: "claim_123"
           }
  end

  defp step_context do
    %Jizoku.Step.Context{
      run_id: "run_123",
      workflow: PaymentRecovery,
      step: :check_gateway_status,
      attempt: 2,
      runnable_key: "run_123:check_gateway_status:2",
      idempotency_key: "run_123:check_gateway_status:attempt_789",
      claim_id: "claim_123",
      state: %{}
    }
  end
end
