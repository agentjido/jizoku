defmodule BedrockMinimalHostApp.Jobs.JizokuPayload do
  @moduledoc """
  Bedrock job that delivers Jizoku delivery payloads back to the runtime.

  A `%{"kind" => "drain", "queue" => queue}` payload is the example app's
  explicit leased command for draining a journal queue that was not activated by
  the original delivery payload, such as a child workflow queue.
  """

  use Bedrock.JobQueue.Job,
    topic: "jizoku:payload",
    max_retries: 3,
    priority: 100

  alias Jizoku.ReadModel.Inspection.Snapshot
  alias Jizoku.Runtime.Runner

  @default_max_journal_attempts 50
  @default_journal_heartbeat_interval_ms 10_000

  @impl true
  def perform(%{"kind" => "drain", "queue" => queue}, _meta) when is_binary(queue) do
    drain_journal_attempts(0, queue)
  end

  def perform(%{"kind" => "drain"} = args, _meta) do
    {:error, {:invalid_runtime_payload, args}}
  end

  def perform(args, _meta) when is_map(args) do
    case Runner.perform(args) do
      :ok -> drain_journal_attempts(0, journal_queue(args))
      {:ok, %Snapshot{}} -> drain_journal_attempts(0, journal_queue(args))
      {:error, _reason} = error -> error
    end
  end

  defp drain_journal_attempts(count, queue) do
    if count >= max_journal_attempts() do
      {:error, :journal_drain_limit_exceeded}
    else
      case execute_next(queue) do
        {:ok, :none} -> :ok
        {:ok, %Snapshot{}} -> drain_journal_attempts(count + 1, queue)
        {:error, _reason} = error -> error
      end
    end
  end

  defp execute_next(nil), do: Jizoku.execute_next(journal_execute_options(nil))

  defp execute_next(queue) when is_binary(queue) do
    Jizoku.execute_next(journal_execute_options(queue))
  end

  defp journal_execute_options(queue) do
    [owner_id: "bedrock-minimal-host-app"]
    |> maybe_put_queue(queue)
    |> maybe_put_heartbeat_interval(journal_heartbeat_interval_ms())
  end

  defp maybe_put_queue(opts, nil), do: opts
  defp maybe_put_queue(opts, queue), do: Keyword.put(opts, :queue, queue)

  defp maybe_put_heartbeat_interval(opts, nil), do: opts

  defp maybe_put_heartbeat_interval(opts, heartbeat_interval_ms) do
    Keyword.put(opts, :heartbeat_interval_ms, heartbeat_interval_ms)
  end

  defp journal_queue(%{"queue" => queue}) when is_binary(queue), do: queue
  defp journal_queue(_args), do: nil

  defp max_journal_attempts do
    :bedrock_minimal_host_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:max_journal_attempts, @default_max_journal_attempts)
  end

  defp journal_heartbeat_interval_ms do
    :bedrock_minimal_host_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:journal_heartbeat_interval_ms, @default_journal_heartbeat_interval_ms)
  end
end
