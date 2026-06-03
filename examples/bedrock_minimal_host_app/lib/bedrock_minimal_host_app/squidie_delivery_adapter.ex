defmodule BedrockMinimalHostApp.SquidieDeliveryAdapter do
  @moduledoc """
  Bedrock-backed Squidie delivery adapter owned by the host app.
  """

  @behaviour Squidie.Executor

  alias Squidie.Executor.Payload

  @impl true
  def enqueue_cron(_config, workflow, trigger, opts) do
    workflow
    |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
    |> enqueue(opts)
  end

  def queue do
    Keyword.get(delivery_config(), :queue_id, "default")
  end

  defp enqueue(payload, opts) do
    queue_id = queue()
    topic = topic()

    # Bedrock owns job visibility, retries, and leases in this spike. Squidie
    # still owns workflow facts through its configured Ecto repo.
    case BedrockMinimalHostApp.JobQueue.enqueue(queue_id, topic, payload, job_opts(opts)) do
      {:ok, item} -> {:ok, metadata(item, queue_id, topic)}
      {:error, reason} -> {:error, reason}
      other -> {:error, {:unexpected_enqueue_result, other}}
    end
  end

  defp delivery_config do
    Application.get_env(:bedrock_minimal_host_app, __MODULE__, [])
  end

  defp job_opts(opts) do
    maybe_put_delay([], Keyword.get(opts, :schedule_in))
  end

  defp maybe_put_delay(opts, schedule_in)
       when is_integer(schedule_in) and schedule_in > 0 do
    Keyword.put(opts, :in, schedule_in)
  end

  defp maybe_put_delay(opts, _schedule_in), do: opts

  defp topic do
    Keyword.get(delivery_config(), :topic, "squidie:payload")
  end

  defp metadata(item, queue_id, topic) do
    %{
      item_id: item.id,
      adapter: __MODULE__,
      queue: queue_id,
      topic: topic,
      scheduled_at: item.vesting_time
    }
  end
end
