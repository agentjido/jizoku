defmodule MinimalHostApp.Workflows.JidoEmitWorkflow do
  @moduledoc """
  Battle-tests a raw Jido Emit directive through Squidie's durable outbox.
  """

  use Squidie.Workflow

  defmodule Publish do
    use Jido.Action,
      name: "publish_jido_order",
      description: "Publishes a durable sample order signal",
      schema: [order_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{order_id: order_id}, context) do
      {:ok, signal} =
        Jido.Signal.new("minimal_host.order.prepared", %{"order_id" => order_id},
          id: "#{context.run_id}:order-prepared",
          source: "/minimal-host/jido",
          subject: order_id
        )

      {:ok, %{jido_signal_prepared: true}, [Jido.Agent.Directive.emit(signal)]}
    end
  end

  workflow do
    trigger :manual do
      manual()

      payload do
        field :order_id, :string
      end
    end

    step :publish, Publish
    transition :publish, on: :ok, to: :complete
  end
end
