defmodule MinimalHostApp.JizokuDeliveryAdapter do
  @moduledoc """
  Oban-backed Jizoku delivery adapter owned by the host app.
  """

  @behaviour Jizoku.Executor

  alias MinimalHostApp.Workers.JizokuWorker
  alias Jizoku.Executor.Payload

  @impl true
  def enqueue_cron(_config, workflow, trigger, opts) do
    changeset =
      workflow
      |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
      |> JizokuWorker.new(job_opts(opts))

    oban_name()
    |> Oban.insert(changeset)
    |> normalize_insert_result()
  end

  def queue do
    delivery_config()
    |> Keyword.get(:queue, :jizoku)
  end

  defp job_opts(opts) do
    [queue: queue()]
    |> maybe_put_schedule_in(Keyword.get(opts, :schedule_in))
  end

  defp oban_name do
    delivery_config()
    |> Keyword.get(:oban_name, Oban)
  end

  defp delivery_config do
    Application.get_env(:minimal_host_app, __MODULE__, [])
  end

  defp maybe_put_schedule_in(opts, schedule_in)
       when is_integer(schedule_in) and schedule_in > 0 do
    Keyword.put(opts, :schedule_in, schedule_in)
  end

  defp maybe_put_schedule_in(opts, _schedule_in), do: opts

  defp normalize_insert_result({:ok, job}), do: {:ok, metadata(job)}
  defp normalize_insert_result({:error, reason}), do: {:error, reason}
  defp normalize_insert_result(other), do: {:error, {:unexpected_insert_result, other}}

  defp metadata(%Oban.Job{} = job) do
    %{
      job_id: job.id,
      adapter: __MODULE__,
      queue: queue(),
      worker: job.worker,
      scheduled_at: job.scheduled_at
    }
  end
end
