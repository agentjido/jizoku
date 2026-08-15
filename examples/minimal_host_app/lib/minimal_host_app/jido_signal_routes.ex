defmodule MinimalHostApp.JidoSignalRoutes do
  @moduledoc """
  Allowlisted domain-signal routes into Jizoku lifecycle commands.

  The resolver matches trusted signal types and chooses compiled workflow
  modules. Signal data can supply workflow input, but never module names or
  runtime routing options.
  """

  @behaviour Jizoku.Jido.SignalResolver

  @impl Jizoku.Jido.SignalResolver
  def resolve(%Jido.Signal{
        type: "minimal_host.dependency_recovery.requested",
        data: %{
          "account_id" => account_id,
          "attempt_id" => attempt_id,
          "invoice_id" => invoice_id
        }
      }) do
    {:ok,
     {:start_run, MinimalHostApp.Workflows.DependencyRecovery, :dependency_recovery,
      %{account_id: account_id, attempt_id: attempt_id, invoice_id: invoice_id}}}
  end

  def resolve(%Jido.Signal{}), do: {:error, :unsupported_domain_signal}
end
