defmodule BedrockMinimalHostApp.BedrockMultiNodeConsumerTest do
  use ExUnit.Case, async: false

  alias Bedrock.JobQueue.Consumer.Manager
  alias Bedrock.JobQueue.Lease
  alias Bedrock.JobQueue.Store
  alias BedrockMinimalHostApp.BedrockRepo
  alias BedrockMinimalHostApp.JobQueue

  defmodule BlockingProbe do
    use Bedrock.JobQueue.Job,
      topic: "multi_node:probe",
      max_retries: 1,
      priority: 100,
      timeout: 5_000

    @table __MODULE__

    def register(test_id, controller) do
      ensure_table!()
      true = :ets.insert(@table, {test_id, controller})
      :ok
    end

    def unregister(test_id) do
      ensure_table!()
      true = :ets.delete(@table, test_id)
      :ok
    end

    @impl true
    def perform(%{"test_id" => test_id}, meta) do
      run(test_id, meta)
    end

    def perform(%{test_id: test_id}, meta) do
      run(test_id, meta)
    end

    defp run(test_id, meta) do
      controller = controller!(test_id)
      send(controller, {:bedrock_job_started, test_id, self(), meta})

      receive do
        {:release_bedrock_job, ^test_id} ->
          :ok
      after
        5_000 ->
          {:error, :probe_timeout}
      end
    end

    defp controller!(test_id) do
      case :ets.lookup(@table, test_id) do
        [{^test_id, controller}] ->
          controller

        [] ->
          raise "missing blocking probe controller"
      end
    end

    defp ensure_table! do
      case :ets.whereis(@table) do
        :undefined ->
          :ets.new(@table, [:named_table, :public, :set])

        _table ->
          :ok
      end
    end
  end

  test "two consumers dispatch once, renew the lease, and complete from the stale claim" do
    suffix = System.unique_integer([:positive])
    test_id = "multi-node-consumer-#{suffix}"
    queue = "multi_node_#{suffix}"
    lease_duration = 900

    BlockingProbe.register(test_id, self())
    on_exit(fn -> BlockingProbe.unregister(test_id) end)

    manager_a = start_manager("node-a", suffix, lease_duration)
    manager_b = start_manager("node-b", suffix, lease_duration)

    assert {:ok, _item_id} =
             JobQueue.enqueue(queue, "multi_node:probe", %{"test_id" => test_id})

    notify_queue_ready([manager_a, manager_b], queue)

    assert_receive {:bedrock_job_started, ^test_id, worker_pid, meta}, 2_000
    refute_receive {:bedrock_job_started, ^test_id, _duplicate_pid, _duplicate_meta}, 100

    initial_lease = stored_lease!(queue, meta.item_id)
    assert %Lease{} = initial_lease

    extended_lease = await_extended_lease(queue, meta.item_id, initial_lease.expires_at, 2_000)
    assert extended_lease.expires_at > initial_lease.expires_at

    await_timestamp(initial_lease.expires_at + 50, 2_000)
    notify_queue_ready([manager_a, manager_b], queue)

    refute_receive {:bedrock_job_started, ^test_id, _duplicate_pid, _duplicate_meta}, 200
    assert %{pending_count: 0, processing_count: 1} = JobQueue.stats(queue)

    send(worker_pid, {:release_bedrock_job, test_id})

    assert :ok =
             await_condition(
               fn -> JobQueue.stats(queue) == %{pending_count: 0, processing_count: 0} end,
               2_000
             )

    assert stored_lease(queue, meta.item_id) == nil
  end

  test "ExAws remains compatible with the security-patched Hackney override" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "GET", "/health", fn conn ->
      Plug.Conn.resp(conn, 200, "ok")
    end)

    assert {:ok, %{status_code: 200, body: "ok"}} =
             ExAws.Request.Hackney.request(
               :get,
               "http://localhost:#{bypass.port}/health"
             )
  end

  defp start_manager(node_name, suffix, lease_duration) do
    pool_name = {:global, {__MODULE__, :pool, node_name, suffix}}
    manager_name = {:global, {__MODULE__, :manager, node_name, suffix}}

    start_supervised!(
      Supervisor.child_spec(
        {Task.Supervisor, name: pool_name, max_children: 1},
        id: pool_name
      )
    )

    start_supervised!(
      Supervisor.child_spec(
        {Manager,
         name: manager_name,
         repo: BedrockRepo,
         root: root(),
         workers: %{"multi_node:probe" => BlockingProbe},
         worker_pool: pool_name,
         concurrency: 1,
         batch_size: 1,
         lease_duration: lease_duration,
         queue_lease_duration: 300,
         holder_id: "#{node_name}-#{suffix}"},
        id: manager_name
      )
    )
  end

  defp notify_queue_ready(managers, queue) do
    Enum.each(managers, fn manager ->
      send(manager, {:queue_ready, queue})
    end)
  end

  defp await_extended_lease(queue, item_id, initial_expiry, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    await_value(
      fn ->
        case stored_lease(queue, item_id) do
          %Lease{expires_at: expires_at} = lease when expires_at > initial_expiry -> lease
          _lease -> nil
        end
      end,
      deadline,
      "Bedrock lease was not extended"
    )
  end

  defp await_timestamp(timestamp, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    await_condition(
      fn -> System.system_time(:millisecond) >= timestamp end,
      deadline,
      "original Bedrock lease did not elapse"
    )
  end

  defp await_condition(condition, timeout) when is_integer(timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_condition(condition, deadline, "condition did not become true")
  end

  defp await_condition(condition, deadline, message) do
    case await_value(fn -> if condition.(), do: :ok end, deadline, message) do
      :ok -> :ok
    end
  end

  defp await_value(fun, deadline, message) do
    case fun.() do
      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk(message)
        else
          retry_ref = make_ref()
          Process.send_after(self(), {:retry, retry_ref}, 10)
          assert_receive {:retry, ^retry_ref}, 500
          await_value(fun, deadline, message)
        end

      value ->
        value
    end
  end

  defp stored_lease!(queue, item_id) do
    case stored_lease(queue, item_id) do
      %Lease{} = lease ->
        lease

      nil ->
        flunk("missing Bedrock lease")
    end
  end

  defp stored_lease(queue, item_id) do
    transact!(fn ->
      keyspaces = Store.queue_keyspaces(root(), queue)

      case BedrockRepo.get(keyspaces.leases, item_id) do
        nil -> nil
        encoded_lease -> :erlang.binary_to_term(encoded_lease)
      end
    end)
  end

  defp transact!(fun) do
    case BedrockRepo.transact(fun, retry_limit: 3) do
      {:error, reason} ->
        flunk("Bedrock transaction failed: #{inspect(reason)}")

      result ->
        result
    end
  end

  defp root do
    Bedrock.JobQueue.Internal.root_keyspace(JobQueue)
  end
end
