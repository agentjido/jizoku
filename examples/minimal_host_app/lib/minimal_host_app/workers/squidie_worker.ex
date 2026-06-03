defmodule MinimalHostApp.Workers.SquidieWorker do
  @moduledoc """
  Generic Oban delivery adapter for Squidie cron payloads.
  """

  use Oban.Worker, queue: :squidie, max_attempts: 1

  alias Squidie.Runtime.Runner

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"kind" => "cron"} = args}) do
    Runner.perform(args)
  end

  def perform(%Oban.Job{args: args}) do
    {:error, {:invalid_squidie_payload, args}}
  end
end
