defmodule BedrockMinimalHostApp.SquidieLeaseAdapter do
  @moduledoc """
  Bedrock-backed Squidie lease adapter owned by the host app.
  """

  @behaviour Squidie.Executor.Leases

  alias Bedrock.JobQueue.Payload, as: BedrockPayload
  alias Bedrock.JobQueue.Store
  alias BedrockMinimalHostApp.BedrockRepo
  alias BedrockMinimalHostApp.JobQueue
  alias Squidie.Executor.Leases.Claim

  @default_lease_duration_ms 30_000

  @impl true
  def claim(_config, queue_id, owner_id, opts)
      when is_binary(queue_id) and is_binary(owner_id) and is_list(opts) do
    transact(fn ->
      claims =
        queue_id
        |> visible_items(opts)
        |> Enum.reduce([], fn item, claims ->
          case claim_item(item, owner_id, opts) do
            {:ok, claim} -> [claim | claims]
            {:error, _reason} -> claims
          end
        end)
        |> Enum.reverse()

      {:ok, claims}
    end)
  end

  @impl true
  def heartbeat(_config, %Claim{backend_ref: lease} = claim, opts) when is_list(opts) do
    transact(fn ->
      lease_duration = Keyword.get(opts, :lease_duration_ms, @default_lease_duration_ms)
      heartbeat_opts = Keyword.take(opts, [:now])

      case Store.extend_lease(BedrockRepo, root(), lease, lease_duration, heartbeat_opts) do
        {:ok, updated_lease} -> {:ok, claim_from_lease(updated_lease, claim.payload)}
        {:error, reason} -> {:error, reason}
      end
    end)
  end

  @impl true
  def complete(_config, %Claim{backend_ref: lease}, opts) when is_list(opts) do
    transact(fn -> Store.complete(BedrockRepo, root(), lease) end)
  end

  @impl true
  def fail(_config, %Claim{backend_ref: lease}, _reason, opts) when is_list(opts) do
    transact(fn -> Store.requeue(BedrockRepo, root(), lease, opts) end)
  end

  defp visible_items(queue_id, opts) do
    Store.peek(BedrockRepo, root(), queue_id, limit: limit(opts), now: now(opts))
  end

  defp claim_item(item, owner_id, opts) do
    lease_duration = Keyword.get(opts, :lease_duration_ms, @default_lease_duration_ms)

    with {:ok, lease} <-
           Store.obtain_lease(BedrockRepo, root(), item, owner_id, lease_duration, now: now(opts)) do
      {:ok, claim_from_lease(lease)}
    end
  end

  defp limit(opts) do
    Keyword.get(opts, :limit, 1)
  end

  defp now(opts) do
    Keyword.get(opts, :now, System.system_time(:millisecond))
  end

  defp claim_from_lease(lease) do
    item = fetch_item!(lease)
    claim_from_lease(lease, decode_payload(item.payload))
  end

  defp claim_from_lease(lease, payload) do
    %Claim{
      id: lease.id,
      queue: lease.queue_id,
      item_id: lease.item_id,
      owner: lease.holder,
      lease_until: lease.expires_at,
      payload: payload,
      backend_ref: lease,
      metadata: %{item_key: lease.item_key}
    }
  end

  defp fetch_item!(lease) do
    keyspaces = Store.queue_keyspaces(root(), lease.queue_id)

    case BedrockRepo.get(keyspaces.items, lease.item_key) do
      nil -> raise "claimed Bedrock job item #{inspect(lease.item_id)} is missing"
      value -> :erlang.binary_to_term(value)
    end
  end

  defp decode_payload(payload) do
    case Jason.decode(payload) do
      {:ok, decoded} -> decoded
      {:error, _reason} -> BedrockPayload.decode(payload)
    end
  end

  defp transact(fun) when is_function(fun, 0) do
    case BedrockRepo.transact(fun, retry_limit: 3) do
      {:error, reason} -> {:error, reason}
      result -> result
    end
  end

  defp root do
    # The low-level lease Store API needs the same generated root keyspace used
    # by JobQueue.enqueue/4, otherwise claims and enqueued jobs use different
    # Bedrock keyspaces.
    Bedrock.JobQueue.Internal.root_keyspace(JobQueue)
  end
end
