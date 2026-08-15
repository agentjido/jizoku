defmodule MinimalHostApp.Workers.JizokuWorker do
  @moduledoc """
  Generic Oban delivery adapter for Jizoku cron payloads.
  """

  use Oban.Worker, queue: :jizoku, max_attempts: 1

  alias Jizoku.Runtime.Runner

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"kind" => "cron"} = args}) do
    Runner.perform(args)
  end

  def perform(%Oban.Job{args: args}) do
    {:error, {:invalid_jizoku_payload, args}}
  end
end
