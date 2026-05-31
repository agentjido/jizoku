defmodule BedrockMinimalHostApp.SquidMeshLeaseAdapterTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias BedrockMinimalHostApp.Jobs.SquidMeshPayload
  alias BedrockMinimalHostApp.JobQueue
  alias BedrockMinimalHostApp.Repo
  alias BedrockMinimalHostApp.RuntimeSignals
  alias BedrockMinimalHostApp.SquidMeshLeaseAdapter
  alias BedrockMinimalHostApp.WorkflowRuns
  alias BedrockMinimalHostApp.Workflows.DailyDigest
  alias SquidMesh.Executor.Payload
  alias SquidMesh.Runtime.Journal

  setup do
    :ok = Sandbox.checkout(Repo)
    cleanup_runtime_state()
    original_payload_config = Application.get_env(:bedrock_minimal_host_app, SquidMeshPayload, [])

    on_exit(fn ->
      Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, original_payload_config)
    end)

    queue = "lease_tenant_#{System.unique_integer([:positive])}"
    {:ok, queue: queue}
  end

  test "claims, heartbeats, and completes leased Squid Mesh payloads", %{queue: queue} do
    now = System.system_time(:millisecond)

    assert {:ok, _item_id} =
             JobQueue.enqueue(
               queue,
               "squid_mesh:payload",
               %{"kind" => "step", "run_id" => "run_123", "step" => "charge_card"},
               now: now
             )

    assert {:ok, [claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a",
               lease_duration_ms: 1_000,
               now: now
             )

    assert claim.queue == queue
    assert claim.owner == "worker_a"
    assert claim.lease_until == now + 1_000
    assert claim.payload == %{"kind" => "step", "run_id" => "run_123", "step" => "charge_card"}

    assert {:ok, []} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b",
               lease_duration_ms: 1_000,
               now: now + 500
             )

    assert {:ok, heartbeated_claim} =
             SquidMeshLeaseAdapter.heartbeat(%{}, claim,
               lease_duration_ms: 5_000,
               now: now + 500
             )

    assert heartbeated_claim.lease_until == now + 5_500
    assert heartbeated_claim.payload == claim.payload

    assert :ok = SquidMeshLeaseAdapter.complete(%{}, heartbeated_claim, [])
    assert {:ok, []} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_c", now: now + 5_500)
    assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue)
  end

  test "fails claims through backend retry and dead-letter policy", %{queue: queue} do
    now = System.system_time(:millisecond)

    assert {:ok, _item_id} =
             JobQueue.enqueue(
               queue,
               "squid_mesh:payload",
               %{"kind" => "step", "run_id" => "run_456", "step" => "capture_payment"},
               max_retries: 2,
               now: now
             )

    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", now: now)

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, claim, %{message: "gateway timeout"},
               base_delay: 1_000,
               now: now
             )

    assert {:ok, []} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 999)

    assert {:ok, [retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 1_000)

    assert retry_claim.payload == %{
             "kind" => "step",
             "run_id" => "run_456",
             "step" => "capture_payment"
           }

    assert {:ok, :dead_lettered} =
             SquidMeshLeaseAdapter.fail(%{}, retry_claim, %{message: "still failing"},
               base_delay: 1_000,
               now: now + 1_000
             )

    assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue)
  end

  test "executes a journal workflow while holding a Bedrock lease", %{queue: queue} do
    payload =
      Payload.cron(
        DailyDigest,
        :daily_digest,
        signal_id: "bedrock-lease-daily-digest-#{System.unique_integer([:positive])}"
      )

    assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", payload)
    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", [])

    assert :ok = SquidMeshPayload.perform(claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, claim, [])

    assert {:ok, [run]} = WorkflowRuns.list_daily_digest_runs()
    assert run.status == :completed

    assert {:ok, inspected_run} = WorkflowRuns.inspect_run(run.run_id)
    assert inspected_run.status == :completed
    assert inspected_run.context.digest_delivery.channel == "ops"
  end

  test "applies cancellation through the example app signal boundary" do
    queue = "bedrock-signal-cancel-#{System.unique_integer([:positive])}"

    with_squid_mesh_queue(queue, fn ->
      assert {:ok, started_run} =
               WorkflowRuns.start_cancellable_wait(%{account_id: "acct_bedrock_signal_cancel"})

      assert started_run.queue == queue
      assert started_run.status == :running
      assert [%{step: "wait_for_cancellation", status: :available}] = started_run.visible_attempts

      assert {:ok, signal} =
               SquidMesh.Runtime.Signal.cancel_run(started_run.run_id,
                 metadata: %{source: "bedrock_minimal_host_app.runtime_signals"},
                 idempotency_key: "bedrock-runtime-signal:cancel:#{started_run.run_id}"
               )

      assert {:ok, jido_signal} = RuntimeSignals.to_jido(signal)
      assert {:ok, cancelled_run} = RuntimeSignals.apply(jido_signal)

      assert cancelled_run.run_id == started_run.run_id
      assert cancelled_run.queue == queue
      assert cancelled_run.status == :cancelled
      assert cancelled_run.terminal?
      assert cancelled_run.visible_attempts == []

      assert [
               %{signal_type: "start_run"},
               %{
                 signal_type: "cancel_run",
                 metadata: %{source: "bedrock_minimal_host_app.runtime_signals"},
                 idempotency_key: "bedrock-runtime-signal:cancel:" <> _
               }
             ] = cancelled_run.command_history
    end)
  end

  test "applies manual control signals through the example app boundary" do
    queue = "bedrock-signal-manual-#{System.unique_integer([:positive])}"

    with_squid_mesh_queue(queue, fn ->
      assert {:ok, approval_run} =
               WorkflowRuns.start_manual_approval(%{account_id: "acct_bedrock_approve"})

      assert {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{status: :paused}} =
               SquidMesh.execute_next(owner_id: "bedrock-manual-approve-test")

      assert {:ok, approved_run} =
               WorkflowRuns.approve(approval_run.run_id, %{actor: "ops_bedrock"})

      assert [
               %{signal_type: "start_run"},
               %{signal_type: "approve_run", payload: %{run_id: approved_run_id}}
             ] = approved_run.command_history

      assert approved_run_id == approval_run.run_id

      assert {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{status: :completed}} =
               SquidMesh.execute_next(owner_id: "bedrock-manual-approve-complete-test")

      assert {:ok, rejection_run} =
               WorkflowRuns.start_manual_approval(%{account_id: "acct_bedrock_reject"})

      assert {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{status: :paused}} =
               SquidMesh.execute_next(owner_id: "bedrock-manual-reject-test")

      assert {:ok, rejected_run} =
               WorkflowRuns.reject(rejection_run.run_id, %{actor: "ops_bedrock"})

      assert [
               %{signal_type: "start_run"},
               %{signal_type: "reject_run", payload: %{run_id: rejected_run_id}}
             ] = rejected_run.command_history

      assert rejected_run_id == rejection_run.run_id

      assert {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{status: :completed}} =
               SquidMesh.execute_next(owner_id: "bedrock-manual-reject-complete-test")

      assert {:ok, pause_run} =
               WorkflowRuns.start_manual_pause(%{account_id: "acct_bedrock_resume"})

      assert {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{status: :paused}} =
               SquidMesh.execute_next(owner_id: "bedrock-manual-resume-test")

      assert {:ok, resumed_run} =
               WorkflowRuns.resume(pause_run.run_id, %{actor: "ops_bedrock"})

      assert [
               %{signal_type: "start_run"},
               %{signal_type: "resume_run", payload: %{run_id: resumed_run_id}}
             ] = resumed_run.command_history

      assert resumed_run_id == pause_run.run_id
    end)
  end

  test "returns a structured error when the signal target run is missing" do
    assert {:error, :not_found} = WorkflowRuns.cancel(Ecto.UUID.generate())
  end

  test "executes payment recovery through a Bedrock lease with runtime attempt metadata",
       %{queue: queue} do
    Sandbox.mode(Repo, {:shared, self()})

    bypass = Bypass.open()

    Bypass.expect_once(bypass, "GET", "/gateway", fn conn ->
      Process.sleep(250)
      Plug.Conn.resp(conn, 200, "retry_required")
    end)

    with_squid_mesh_queue(queue, fn ->
      Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload,
        journal_heartbeat_interval_ms: 50
      )

      assert {:ok, started_run} =
               WorkflowRuns.start_payment_recovery(%{
                 account_id: "acct_bedrock_payment",
                 invoice_id: "inv_bedrock_payment",
                 attempt_id: "attempt_bedrock_payment",
                 gateway_url: "http://localhost:#{bypass.port}/gateway"
               })

      assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", drain_payload(queue))
      assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_payment", [])

      assert :ok = SquidMeshPayload.perform(claim.payload, %{})
      assert :ok = SquidMeshLeaseAdapter.complete(%{}, claim, [])

      assert {:ok, completed_run} = WorkflowRuns.inspect_run(started_run.run_id)
      assert completed_run.status == :completed
      assert completed_run.context.gateway_check.status == "retry_required"
      assert completed_run.context.gateway_check.attempt.idempotency_key
      assert completed_run.context.gateway_check.attempt.claim_id
      refute Map.has_key?(completed_run.context.gateway_check.attempt, :claim_token)

      storage = {SquidMesh.Runtime.Journal.Storage.Ecto, repo: Repo}
      assert {:ok, dispatch_entries} = Journal.load_entries(storage, {:dispatch, queue})
      heartbeat_entries = Enum.filter(dispatch_entries, &(&1.type == :attempt_heartbeat))
      assert heartbeat_entries != []

      assert Enum.all?(heartbeat_entries, fn entry ->
               Map.has_key?(entry.data, :claim_token_hash) and
                 not Map.has_key?(entry.data, :claim_token)
             end)
    end)
  end

  test "keeps Bedrock payment recovery retry errors inspectable with attempt metadata",
       %{queue: queue} do
    bypass = Bypass.open()

    Bypass.expect(bypass, "GET", "/gateway", fn conn ->
      Plug.Conn.resp(conn, 503, "gateway_unavailable")
    end)

    with_squid_mesh_queue(queue, fn ->
      assert {:ok, started_run} =
               WorkflowRuns.start_payment_recovery(%{
                 account_id: "acct_bedrock_payment_retry",
                 invoice_id: "inv_bedrock_payment_retry",
                 attempt_id: "attempt_bedrock_payment_retry",
                 gateway_url: "http://localhost:#{bypass.port}/gateway"
               })

      Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 2)

      assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", drain_payload(queue))
      assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_payment_retry", [])

      assert {:error, :journal_drain_limit_exceeded} =
               SquidMeshPayload.perform(claim.payload, %{})

      assert {:ok, retrying_run} = WorkflowRuns.inspect_run(started_run.run_id)

      assert %{idempotency_key: _idempotency_key, claim_id: _claim_id} =
               failed_attempt =
               Enum.find(retrying_run.attempts, fn attempt ->
                 attempt.step == "check_gateway_status" and attempt.status == :failed
               end)

      refute Map.has_key?(failed_attempt, :claim_token)
    end)
  end

  test "executes a nested journal workflow through a Bedrock lease and drains child retry separately",
       %{queue: queue} do
    now = System.system_time(:millisecond)
    child_queue = "bedrock_nested_child"

    payload =
      Payload.cron(
        BedrockMinimalHostApp.Workflows.NestedInviteDelivery,
        :scheduled_nested_invite,
        signal_id: "bedrock-lease-nested-invite-#{System.unique_integer([:positive])}"
      )

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 1)

    assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", payload, now: now)
    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", now: now)

    assert {:error, :journal_drain_limit_exceeded} = SquidMeshPayload.perform(claim.payload, %{})

    assert {:ok, [parent_summary]} =
             SquidMesh.list_runs(workflow: BedrockMinimalHostApp.Workflows.NestedInviteDelivery)

    assert {:ok, retried_parent} =
             WorkflowRuns.inspect_run(parent_summary.run_id, include_history: true)

    assert retried_parent.status == :running

    assert [%{child_run_id: child_run_id, child_key: "invite_guest_bedrock"}] =
             retried_parent.child_runs

    assert {:ok, parent_graph} = SquidMesh.inspect_run_graph(parent_summary.run_id)
    parent_graph_map = SquidMesh.Runs.GraphInspection.to_map(parent_graph)

    assert [
             %{
               from: "start_nested_invite",
               to: ^child_run_id,
               type: :child_run,
               status: :linked,
               child_run_id: ^child_run_id,
               child_key: "invite_guest_bedrock",
               child_trigger: "deliver_invite",
               metadata: %{guest_id: "guest_bedrock"}
             }
           ] = parent_graph_map.child_links

    assert [%{step: "start_nested_invite", status: :retry_scheduled, attempt_number: 2}] =
             retried_parent.visible_attempts

    assert {:ok, child_run} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert child_run.status == :running
    assert [%{step: "deliver_invite", status: :available}] = child_run.visible_attempts

    Repo.delete_all("squid_mesh_journal_checkpoints")

    assert {:ok, reconstructed_parent} = WorkflowRuns.inspect_run(retried_parent.run_id)
    assert reconstructed_parent.child_runs == retried_parent.child_runs
    assert {:ok, reconstructed_parent_graph} = SquidMesh.inspect_run_graph(retried_parent.run_id)

    reconstructed_parent_graph_map =
      SquidMesh.Runs.GraphInspection.to_map(reconstructed_parent_graph)

    assert [%{from: "start_nested_invite", to: ^child_run_id, type: :child_run}] =
             reconstructed_parent_graph_map.child_links

    assert {:ok, reconstructed_waiting_child} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert reconstructed_waiting_child.parent_run == child_run.parent_run
    assert reconstructed_waiting_child.status == :running

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, claim, %{message: "parent drain limit"},
               base_delay: 1,
               now: now
             )

    assert {:ok, [parent_retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 1)

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 50)

    assert :ok = SquidMeshPayload.perform(parent_retry_claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, parent_retry_claim, [])

    assert {:ok, completed_parent} =
             WorkflowRuns.inspect_run(retried_parent.run_id, include_history: true)

    assert completed_parent.status == :completed
    assert completed_parent.context.invite_child.queue == child_queue
    assert completed_parent.context.invite_child.reused_after_retry? == true

    assert [
             {"start_nested_invite", :failed, false, 1},
             {"start_nested_invite", :completed, true, 2}
           ] =
             Enum.map(
               completed_parent.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             )

    assert [%{runnable_key: parent_runnable_key} | _remaining_parent_attempts] =
             completed_parent.attempts

    drain_payload = %{"kind" => "drain", "queue" => child_queue}
    child_drain_now = now + 2

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 1)

    assert {:ok, _item_id} =
             JobQueue.enqueue(queue, "squid_mesh:payload", drain_payload, now: child_drain_now)

    assert {:ok, [child_drain_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_child_a", now: child_drain_now)

    assert {:error, :journal_drain_limit_exceeded} =
             SquidMeshPayload.perform(child_drain_claim.payload, %{})

    assert {:ok, child_retrying} = WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert child_retrying.status == :running

    assert [%{step: "deliver_invite", status: :retry_scheduled, attempt_number: 2}] =
             child_retrying.visible_attempts

    Repo.delete_all("squid_mesh_journal_checkpoints")

    assert {:ok, reconstructed_retrying_child} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert reconstructed_retrying_child.visible_attempts == child_retrying.visible_attempts

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, child_drain_claim, %{message: "child drain limit"},
               base_delay: 1,
               now: child_drain_now
             )

    assert {:ok, [child_retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_child_b", now: child_drain_now + 1)

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 50)

    assert :ok = SquidMeshPayload.perform(child_retry_claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, child_retry_claim, [])

    assert {:ok, completed_child} = WorkflowRuns.inspect_run(child_run_id, queue: child_queue)
    assert completed_child.status == :completed

    assert {:ok, child_history} =
             WorkflowRuns.inspect_run(child_run_id,
               queue: child_queue,
               include_history: true
             )

    assert [
             {"deliver_invite", :failed, false, 1},
             {"deliver_invite", :completed, true, 2}
           ] =
             Enum.map(
               child_history.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             )

    assert child_history.parent_run == %{
             run_id: completed_parent.run_id,
             runnable_key: parent_runnable_key,
             step: "start_nested_invite",
             attempt: 1,
             child_key: "invite_guest_bedrock",
             metadata: %{guest_id: "guest_bedrock"}
           }

    Repo.delete_all("squid_mesh_journal_checkpoints")

    assert {:ok, terminal_reconstructed_parent} =
             WorkflowRuns.inspect_run(completed_parent.run_id)

    assert {:ok, terminal_reconstructed_child} =
             WorkflowRuns.inspect_run(child_run_id, queue: child_queue)

    assert terminal_reconstructed_parent.child_runs == completed_parent.child_runs
    assert terminal_reconstructed_child.parent_run == child_history.parent_run
  end

  test "requeues a leased journal payload after a bounded drain failure", %{queue: queue} do
    now = System.system_time(:millisecond)

    payload =
      Payload.cron(
        DailyDigest,
        :daily_digest,
        signal_id: "bedrock-lease-retry-daily-digest-#{System.unique_integer([:positive])}"
      )

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 1)

    assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", payload, now: now)
    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", now: now)

    assert {:error, :journal_drain_limit_exceeded} = SquidMeshPayload.perform(claim.payload, %{})

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, claim, %{message: "drain limit"},
               base_delay: 1,
               now: now
             )

    assert {:ok, [retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 1)

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 50)

    assert :ok = SquidMeshPayload.perform(retry_claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, retry_claim, [])

    assert {:ok, [run]} = WorkflowRuns.list_daily_digest_runs()
    assert run.status == :completed
  end

  defp cleanup_runtime_state do
    Repo.delete_all("squid_mesh_journal_entries")
    Repo.delete_all("squid_mesh_journal_checkpoints")
    Repo.delete_all("squid_mesh_journal_threads")
  end

  defp drain_payload(queue), do: %{"kind" => "drain", "queue" => queue}

  defp with_squid_mesh_queue(queue, fun) when is_function(fun, 0) do
    original_queue = Application.get_env(:squid_mesh, :queue)
    Application.put_env(:squid_mesh, :queue, queue)

    try do
      fun.()
    after
      if is_nil(original_queue) do
        Application.delete_env(:squid_mesh, :queue)
      else
        Application.put_env(:squid_mesh, :queue, original_queue)
      end
    end
  end
end
