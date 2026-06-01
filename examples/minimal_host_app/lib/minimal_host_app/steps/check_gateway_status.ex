defmodule MinimalHostApp.Steps.CheckGatewayStatus do
  @moduledoc """
  Example step that checks payment gateway state.
  """

  use SquidMesh.Step,
    name: :check_gateway_status,
    description: "Checks gateway state",
    input_schema: [
      invoice: [type: :map, required: true],
      gateway_url: [type: :string, required: true]
    ],
    output_schema: [
      gateway_check: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), SquidMesh.Step.Context.t()) ::
          {:ok, map()} | {:defer, map(), keyword()} | {:retry, map()}
  def run(%{invoice: invoice, gateway_url: gateway_url}, context) do
    case SquidMesh.Tools.invoke(SquidMesh.Tools.HTTP, %{method: :get, url: gateway_url}) do
      {:ok, %{payload: %{status: 202, body: body}}} ->
        {:defer,
         %{
           message: "gateway status is still pending",
           status: body,
           status_code: 202,
           invoice_id: invoice.id,
           attempt: attempt_metadata(context)
         }, schedule_in: 1}

      {:ok, result} ->
        {:ok,
         %{
           gateway_check: %{
             status: result.payload.body,
             invoice_id: invoice.id,
             status_code: result.payload.status,
             attempt: attempt_metadata(context)
           }
         }}

      {:error, error} ->
        {:retry,
         Map.put(SquidMesh.Tools.Error.to_map(error), :gateway_check, %{
           attempt: attempt_metadata(context)
         })}
    end
  end

  defp attempt_metadata(context) do
    %{
      idempotency_key: context.idempotency_key,
      claim_id: context.claim_id
    }
  end
end
