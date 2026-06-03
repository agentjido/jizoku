defmodule BedrockMinimalHostApp.BedrockJobQueueStressTest do
  use ExUnit.Case, async: false

  alias BedrockMinimalHostApp.BedrockRepo
  alias BedrockMinimalHostApp.JobQueue
  alias BedrockMinimalHostApp.Jobs.StressProbe
  alias BedrockMinimalHostApp.Workflows.DailyDigest

  setup do
    StressProbe.reset!()

    queue_a = "tenant_a_#{System.unique_integer([:positive])}"
    queue_b = "tenant_b_#{System.unique_integer([:positive])}"

    delivery_config =
      Application.get_env(
        :bedrock_minimal_host_app,
        BedrockMinimalHostApp.SquidieDeliveryAdapter,
        []
      )

    Application.put_env(
      :bedrock_minimal_host_app,
      BedrockMinimalHostApp.SquidieDeliveryAdapter,
      Keyword.put(delivery_config, :queue_id, queue_a)
    )

    on_exit(fn ->
      Application.put_env(
        :bedrock_minimal_host_app,
        BedrockMinimalHostApp.SquidieDeliveryAdapter,
        delivery_config
      )
    end)

    {:ok, queue_a: queue_a, queue_b: queue_b}
  end

  describe "Bedrock job queue feature coverage" do
    test "routes topics and preserves queue isolation", %{queue_a: queue_a, queue_b: queue_b} do
      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{
                 "event" => "tenant_a_immediate"
               })

      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_b, "stress:probe", %{
                 "event" => "tenant_b_immediate"
               })

      assert :ok = drain_queue(queue_a)

      assert [%{queue_id: ^queue_a, event: "tenant_a_immediate"}] = StressProbe.events()
      assert %{pending_count: 1, processing_count: 0} = JobQueue.stats(queue_b)

      assert :ok = drain_queue(queue_b)

      assert [
               %{queue_id: ^queue_a, event: "tenant_a_immediate"},
               %{queue_id: ^queue_b, event: "tenant_b_immediate"}
             ] = StressProbe.events()
    end

    test "orders visible jobs by priority before enqueue order", %{queue_a: queue_a} do
      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{"event" => "low"}, priority: 200)

      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{"event" => "high"}, priority: 0)

      assert :ok = drain_queue(queue_a, limit: 2)

      assert [
               %{event: "high", priority: 0},
               %{event: "low", priority: 200}
             ] = StressProbe.events()
    end

    test "hides delayed jobs until their vesting time", %{queue_a: queue_a} do
      now = System.system_time(:millisecond)

      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{"event" => "later"},
                 in: 60_000,
                 now: now
               )

      assert [] = peek_visible(queue_a, now: now + 1_000)
      assert [_item] = peek_visible(queue_a, now: now + 60_000)
    end

    test "leases jobs, extends active leases, and makes expired claims visible again", %{
      queue_a: queue_a
    } do
      now = System.system_time(:millisecond)

      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{"event" => "leased"}, now: now)

      assert {:ok, [lease]} =
               dequeue(queue_a, holder: "worker_a", lease_duration: 1_000, now: now)

      assert %{pending_count: 0, processing_count: 1} = JobQueue.stats(queue_a)

      assert {:ok, extended_lease} = extend_lease(lease, 5_000, now: now + 500)
      assert extended_lease.expires_at > lease.expires_at
      assert [] = peek_visible(queue_a, now: now + 1_200)
      assert [_item] = peek_visible(queue_a, now: extended_lease.expires_at)
    end

    test "requeues failures with retry metadata and dead-letters exhausted jobs", %{
      queue_a: queue_a
    } do
      now = System.system_time(:millisecond)

      assert {:ok, _item_id} =
               JobQueue.enqueue(queue_a, "stress:probe", %{"event" => "retry"},
                 max_retries: 2,
                 now: now
               )

      assert {:ok, [lease]} = dequeue(queue_a, holder: "worker_a", now: now)
      assert {:ok, :requeued} = requeue(lease, now: now, base_delay: 1_000)
      assert %{pending_count: 1, processing_count: 0} = JobQueue.stats(queue_a)

      assert {:ok, [retry_lease]} = dequeue(queue_a, holder: "worker_a", now: now + 1_000)
      assert {:ok, :dead_lettered} = requeue(retry_lease, now: now + 1_000, base_delay: 1_000)
      assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue_a)
    end

    test "maps Squidie cron payloads into delayed Bedrock jobs", %{queue_a: queue_a} do
      intended_window = %{
        "start_at" => "2026-05-22T00:00:00Z",
        "end_at" => "2026-05-23T00:00:00Z"
      }

      assert {:ok, metadata} =
               BedrockMinimalHostApp.SquidieDeliveryAdapter.enqueue_cron(
                 %{},
                 DailyDigest,
                 :daily_digest,
                 signal_id: "daily-digest-2026-05-22",
                 intended_window: intended_window,
                 schedule_in: 60_000
               )

      assert %{
               adapter: BedrockMinimalHostApp.SquidieDeliveryAdapter,
               queue: ^queue_a,
               topic: "squidie:payload",
               scheduled_at: scheduled_at
             } = metadata

      assert [] = peek_visible(queue_a, now: scheduled_at - 1)

      [item] = peek_visible(queue_a, now: scheduled_at)
      payload = Jason.decode!(item.payload)

      assert %{
               "kind" => "cron",
               "workflow" => "Elixir.BedrockMinimalHostApp.Workflows.DailyDigest",
               "trigger" => "daily_digest",
               "signal_id" => "daily-digest-2026-05-22",
               "intended_window" => ^intended_window
             } = payload
    end
  end

  defp drain_queue(queue_id, opts \\ []) do
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    limit = Keyword.get(opts, :limit, 10)

    with {:ok, leases} <-
           dequeue(queue_id, holder: "drain", lease_duration: 30_000, now: now, limit: limit) do
      Enum.each(leases, fn lease ->
        item = leased_item(lease)
        assert :ok = StressProbe.perform_item(item)
        assert :ok = complete(lease)
      end)

      :ok
    end
  end

  defp peek_visible(queue_id, opts) do
    transact!(fn -> Bedrock.JobQueue.Store.peek(BedrockRepo, root(), queue_id, opts) end)
  end

  defp dequeue(queue_id, opts) do
    holder = Keyword.fetch!(opts, :holder)
    opts = Keyword.delete(opts, :holder)

    transact!(fn ->
      Bedrock.JobQueue.Store.dequeue(BedrockRepo, root(), queue_id, holder, opts)
    end)
  end

  defp extend_lease(lease, extension_ms, opts) do
    transact!(fn ->
      Bedrock.JobQueue.Store.extend_lease(BedrockRepo, root(), lease, extension_ms, opts)
    end)
  end

  defp complete(lease) do
    transact!(fn -> Bedrock.JobQueue.Store.complete(BedrockRepo, root(), lease) end)
  end

  defp requeue(lease, opts) do
    transact!(fn -> Bedrock.JobQueue.Store.requeue(BedrockRepo, root(), lease, opts) end)
  end

  defp leased_item(lease) do
    transact!(fn ->
      keyspaces = Bedrock.JobQueue.Store.queue_keyspaces(root(), lease.queue_id)

      # Store.dequeue/5 returns lease metadata, not the item payload. The lease
      # item_key points to the current storage key after Bedrock moved visibility
      # to the lease expiry time.
      keyspaces.items
      |> BedrockRepo.get(lease.item_key)
      |> :erlang.binary_to_term()
    end)
  end

  defp transact!(fun) when is_function(fun, 0) do
    case BedrockRepo.transact(fun, retry_limit: 3) do
      {:error, reason} -> flunk("Bedrock transaction failed: #{inspect(reason)}")
      result -> result
    end
  end

  defp root do
    # Low-level Store helpers must share the generated queue root, otherwise
    # they inspect a different keyspace than JobQueue.enqueue/4 writes to.
    Bedrock.JobQueue.Internal.root_keyspace(JobQueue)
  end
end
