defmodule BedrockMinimalHostApp.Application do
  @moduledoc """
  Supervision tree for the example host application.

  The example starts the Squidie Ecto repo plus an embedded Bedrock cluster
  and queue so workflow state and delivery state stay visibly separate.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children =
      Application.get_env(:bedrock_minimal_host_app, :runtime_children, default_children())

    opts = [strategy: :one_for_one, name: BedrockMinimalHostApp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @spec default_children() :: [Supervisor.child_spec()]
  defp default_children do
    [
      BedrockMinimalHostApp.Repo,
      {BedrockMinimalHostApp.BedrockCluster, []},
      {BedrockMinimalHostApp.JobQueue, concurrency: 5, batch_size: 10}
    ]
  end
end
