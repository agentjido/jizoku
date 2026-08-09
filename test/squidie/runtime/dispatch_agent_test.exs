defmodule Squidie.Runtime.DispatchAgentTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.DispatchProtocol.Projection
  alias Squidie.Runtime.Journal

  @storage {ETS, table: :squidie_dispatch_agent_test}
  @run_id "run_123"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @claimed_at ~U[2026-05-15 00:00:20Z]
  @lease_until ~U[2026-05-15 00:01:00Z]
  @expired_at ~U[2026-05-15 00:02:00Z]

  defmodule TestNotifier do
    @spec notify_attempt_scheduled(map(), keyword()) :: :ok
    def notify_attempt_scheduled(attempt, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:attempt_wakeup, attempt})
      :ok
    end
  end

  defmodule FailingNotifier do
    @spec notify_attempt_scheduled(map(), keyword()) :: {:error, :offline}
    def notify_attempt_scheduled(_attempt, _opts), do: {:error, :offline}
  end

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "rebuilds a keyed dispatch agent from durable dispatch entries" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [scheduled_entry, claimed_entry])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert agent.id == "squidie.dispatch.default"
    assert agent.state.queue == "default"
    assert agent.state.thread_rev == 2
    assert %Projection{} = agent.state.projection
    assert DispatchAgent.visible_attempts(agent, @visible_at) == []

    assert [
             %{runnable_key: @runnable_key, claim_id: "claim_1", owner_id: "worker_1"}
           ] = DispatchAgent.expired_claims(agent, @expired_at)
  end

  test "partitioned dispatch agents have isolated collision-safe identities" do
    assert DispatchAgent.agent_id("default", nil) == "squidie.dispatch.default"

    acme_id = DispatchAgent.agent_id("shared.queue", "tenant.acme")
    globex_id = DispatchAgent.agent_id("shared.queue", "tenant_globex")

    refute acme_id == globex_id
    refute acme_id == DispatchAgent.agent_id("acme.shared.queue", "tenant")
  end

  test "rejects dispatch writes and checkpoints through a mismatched partition" do
    assert {:ok, acme_storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_acme")

    assert {:ok, globex_storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_globex")

    assert {:ok, agent} = DispatchAgent.rebuild(acme_storage, "default")

    assert {:error, {:partition_mismatch, :dispatch_agent}} =
             DispatchAgent.put_checkpoint(globex_storage, agent, updated_at: @visible_at)

    assert {:error, {:partition_mismatch, :dispatch_agent}} =
             DispatchAgent.schedule_attempts(
               globex_storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at
             )

    assert {:error, :not_found} =
             Journal.load_entries(globex_storage, {:dispatch, "default"})

    assert {:error, :not_found} =
             Journal.fetch_checkpoint(globex_storage, {:dispatch, "default"})
  end

  test "rebuilds an empty dispatch agent when the queue has no thread yet" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert agent.id == "squidie.dispatch.default"
    assert agent.state.queue == "default"
    assert agent.state.thread_rev == 0
    assert DispatchAgent.visible_attempts(agent, @visible_at) == []
    assert DispatchAgent.expired_claims(agent, @expired_at) == []
  end

  test "schedules missing runnable attempts with an optimistic dispatch-thread append" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: scheduled_agent, runnables: [%{runnable_key: @runnable_key}]}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at
             )

    assert scheduled_agent.state.thread_rev == 1

    assert [%{runnable_key: @runnable_key, status: :available}] =
             DispatchAgent.visible_attempts(scheduled_agent, @visible_at)

    assert {:ok, [scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert scheduled_entry.type == :attempt_scheduled
    assert scheduled_entry.data.runnable_key == @runnable_key
    assert scheduled_entry.data.queue == "default"
    assert scheduled_entry.data.occurred_at == @visible_at
  end

  test "emits live wakeup facts after scheduled attempts are durable" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: scheduled_agent, runnables: [%{runnable_key: @runnable_key}]}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at,
               notifier: TestNotifier,
               notifier_opts: [test_pid: self()]
             )

    assert_receive {:attempt_wakeup, attempt}

    assert attempt == %{
             run_id: @run_id,
             runnable_key: @runnable_key,
             queue: "default",
             visible_at: @visible_at
           }

    assert scheduled_agent.state.thread_rev == 2

    assert [
             %{runnable_key: @runnable_key, status: :available, wakeup_emitted?: true}
           ] = DispatchAgent.visible_attempts(scheduled_agent, @visible_at)

    assert {:ok, [scheduled_entry, wakeup_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert scheduled_entry.type == :attempt_scheduled
    assert wakeup_entry.type == :live_wakeup_emitted
    assert wakeup_entry.data.runnable_key == @runnable_key
    assert wakeup_entry.data.queue == "default"
    assert wakeup_entry.data.occurred_at == @visible_at
  end

  test "includes the durable partition in live wakeup hints" do
    assert {:ok, storage} =
             Squidie.Runtime.Journal.Storage.scope(@storage, "tenant_acme")

    assert {:ok, agent} = DispatchAgent.rebuild(storage, "default")

    assert {:ok, %{runnables: [%{runnable_key: @runnable_key}]}} =
             DispatchAgent.schedule_attempts(
               storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at,
               notifier: TestNotifier,
               notifier_opts: [test_pid: self()]
             )

    assert_receive {:attempt_wakeup,
                    %{
                      partition: "tenant_acme",
                      run_id: @run_id,
                      runnable_key: @runnable_key,
                      queue: "default",
                      visible_at: @visible_at
                    }}
  end

  test "keeps scheduled attempts recoverable when live wakeup notification is lost" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: scheduled_agent, runnables: [%{runnable_key: @runnable_key}]}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at,
               notifier: FailingNotifier
             )

    assert scheduled_agent.state.thread_rev == 1

    assert [
             %{runnable_key: @runnable_key, status: :available, wakeup_emitted?: false}
           ] = DispatchAgent.visible_attempts(scheduled_agent, @visible_at)

    assert {:ok, [scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert scheduled_entry.type == :attempt_scheduled
  end

  test "uses parent run id for wakeup attempts when planned runnable omits run id" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    runnable_without_run_id = Map.delete(planned_runnable(), :run_id)

    assert {:ok, %{agent: scheduled_agent, runnables: [%{runnable_key: @runnable_key}]}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [runnable_without_run_id],
               now: @visible_at,
               notifier: TestNotifier,
               notifier_opts: [test_pid: self()]
             )

    assert_receive {:attempt_wakeup,
                    %{
                      run_id: @run_id,
                      runnable_key: @runnable_key,
                      queue: "default",
                      visible_at: @visible_at
                    }}

    assert [
             %{runnable_key: @runnable_key, status: :available, wakeup_emitted?: true}
           ] = DispatchAgent.visible_attempts(scheduled_agent, @visible_at)
  end

  test "records known run ids idempotently on the dispatch queue" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: queued_agent, queued?: true}} =
             DispatchAgent.ensure_run_queued(@storage, agent, @run_id, now: @started_at)

    assert DispatchAgent.run_ids(queued_agent) == MapSet.new([@run_id])

    assert {:ok, %{agent: ^queued_agent, queued?: false}} =
             DispatchAgent.ensure_run_queued(@storage, queued_agent, @run_id, now: @visible_at)

    assert {:ok, [queued_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert queued_entry.type == :run_queued
    assert queued_entry.data.run_id == @run_id
    assert queued_entry.data.queue == "default"
    assert queued_entry.data.occurred_at == @started_at
  end

  test "treats already scheduled runnable attempts as idempotent" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: ^agent, runnables: []}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at
             )

    assert {:ok, [^scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "returns conflict when scheduling from a stale empty dispatch projection" do
    assert {:ok, first_agent} = DispatchAgent.rebuild(@storage, "default")
    assert {:ok, stale_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: scheduled_agent}} =
             DispatchAgent.schedule_attempts(
               @storage,
               first_agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at
             )

    assert scheduled_agent.state.thread_rev == 1

    assert {:error, :conflict} =
             DispatchAgent.schedule_attempts(
               @storage,
               stale_agent,
               @run_id,
               [planned_runnable()],
               now: @visible_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(entries, &(&1.type == :attempt_scheduled)) == 1
  end

  test "rejects planned runnables for a different queue before writing" do
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:error, {:wrong_queue, @runnable_key}} =
             DispatchAgent.schedule_attempts(
               @storage,
               agent,
               @run_id,
               [planned_runnable(queue: "priority")],
               now: @visible_at
             )

    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "priority"})
  end

  test "uses a current checkpoint instead of replaying the full dispatch thread" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])

    checkpoint_projection = %Projection{}

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               checkpoint_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert agent.state.projection == checkpoint_projection
    assert agent.state.thread_rev == thread.rev
  end

  test "upgrades dispatch checkpoints written before queued run ids existed" do
    assert {:ok, queued_entry} =
             DispatchProtocol.new_entry(:run_queued, %{
               run_id: @run_id,
               queue: "default",
               occurred_at: @started_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [queued_entry])

    baseline_projection = Map.delete(Projection.new(), :queued_run_ids)

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               baseline_projection,
               0,
               updated_at: @visible_at
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert agent.state.thread_rev == thread.rev
    assert DispatchAgent.run_ids(agent) == MapSet.new([@run_id])
  end

  test "rebuilds the full thread when a legacy checkpoint omitted a durable continuation fence" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, fence_entry} =
             DispatchProtocol.new_entry(:run_continuation_fenced, %{
               run_id: @run_id,
               successor_run_id: "run_456",
               continuation_key: "page-42",
               workflow: "MonitoringWorkflow",
               trigger: "continue",
               input: %{cursor: "page-42"},
               definition: :current,
               definition_fingerprint: "definition-fingerprint-v1",
               queue: "default",
               trace: %{
                 trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
                 span_id: "00f067aa0ba902b7"
               },
               occurred_at: @visible_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry, fence_entry])

    legacy_projection =
      [scheduled_entry]
      |> Projection.rebuild()
      |> Map.delete(:continuation_fences)

    refute Projection.checkpoint_compatible?(legacy_projection)

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               legacy_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert DispatchAgent.visible_attempts(agent, @visible_at) == []

    assert Projection.continuation_fence(agent.state.projection, @run_id).successor_run_id ==
             "run_456"
  end

  test "rebuilds a continuation repair hidden by a legacy checkpoint at the current revision" do
    fence_attrs = %{
      run_id: @run_id,
      successor_run_id: "run_456",
      continuation_key: "page-42",
      workflow: "MonitoringWorkflow",
      trigger: "continue",
      input: %{cursor: "page-42"},
      definition: :current,
      definition_fingerprint: "definition-fingerprint-v1",
      queue: "default",
      trace: %{
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
        span_id: "00f067aa0ba902b7"
      },
      occurred_at: @visible_at
    }

    assert {:ok, fence_entry} =
             DispatchProtocol.new_entry(:run_continuation_fenced, fence_attrs)

    assert {:ok, repair_entry} =
             DispatchProtocol.new_entry(:run_continuation_repaired, fence_attrs)

    assert {:ok, thread} = Journal.append_entries(@storage, [fence_entry, repair_entry])

    legacy_projection =
      [fence_entry]
      |> Projection.rebuild()
      |> Map.delete(:continuation_repairs)

    refute Projection.checkpoint_compatible?(legacy_projection)

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               legacy_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert DispatchAgent.continuation_fences(agent) == [fence_entry.data]
    assert DispatchAgent.pending_continuation_fences(agent) == []
    assert agent.state.thread_rev == thread.rev
  end

  test "upgrades legacy attempts nested in dispatch checkpoints without trace" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])
    %Projection{} = projection = Projection.rebuild([scheduled_entry])
    legacy_attempt = Map.delete(projection.attempts[@runnable_key], :trace)

    checkpoint_projection = %Projection{
      projection
      | attempts: %{@runnable_key => legacy_attempt}
    }

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               checkpoint_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert %ActionAttempt{trace: nil} = agent.state.projection.attempts[@runnable_key]
  end

  test "persists a checkpoint from the rebuilt dispatch agent state" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [scheduled_entry, claimed_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert :ok = DispatchAgent.put_checkpoint(@storage, agent, updated_at: @expired_at)

    assert {:ok,
            %{
              thread: {:dispatch, "default"},
              thread_rev: 2,
              projection: %Projection{} = checkpoint_projection,
              updated_at: @expired_at
            }} = Journal.fetch_checkpoint(@storage, {:dispatch, "default"})

    assert [%{runnable_key: @runnable_key}] =
             Projection.expired_claims(checkpoint_projection, @expired_at)
  end

  test "claims the next visible attempt with an optimistic dispatch-thread append" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok,
            %{
              agent: claimed_agent,
              attempt: %{
                runnable_key: @runnable_key,
                claim_id: "claim_2",
                owner_id: "worker_2",
                lease_until: ~U[2026-05-15 00:01:10Z]
              },
              claim_id: "claim_2",
              claim_token: "token_2",
              lease_until: ~U[2026-05-15 00:01:10Z]
            }} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               lease_for: 60,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert claimed_agent.state.thread_rev == 2
    assert DispatchAgent.visible_attempts(claimed_agent, @visible_at) == []

    assert {:ok, [^scheduled_entry, claimed_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert claimed_entry.type == :attempt_claimed
    assert claimed_entry.data.claim_id == "claim_2"
    assert claimed_entry.data.owner_id == "worker_2"
    assert claimed_entry.data.lease_until == ~U[2026-05-15 00:01:10Z]
    refute claimed_entry.data.claim_token_hash == "token_2"
    assert is_binary(claimed_entry.data.claim_token_hash)
  end

  test "redelivers an expired claim with a fresh claim fence" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [scheduled_entry, claimed_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok,
            %{
              agent: redelivered_agent,
              attempt: %{claim_id: "claim_2", owner_id: "worker_2"},
              claim_id: "claim_2",
              claim_token: "token_2",
              lease_until: ~U[2026-05-15 00:03:00Z]
            }} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @expired_at,
               lease_for: 60,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert redelivered_agent.state.thread_rev == 3
    assert DispatchAgent.expired_claims(redelivered_agent, @expired_at) == []

    assert [%{claim_id: "claim_2", owner_id: "worker_2"}] =
             DispatchAgent.expired_claims(redelivered_agent, ~U[2026-05-15 00:04:00Z])
  end

  test "does not claim when no attempt is visible or expired" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, :none} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @started_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:ok, [^scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "returns conflict for duplicate delivery from a stale competing claimer" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, first_agent} = DispatchAgent.rebuild(@storage, "default")
    assert {:ok, stale_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, attempt: %{claim_id: "claim_2"}}} =
             DispatchAgent.claim_next(@storage, first_agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert claimed_agent.state.thread_rev == 2

    assert {:error, :conflict} =
             DispatchAgent.claim_next(@storage, stale_agent, "worker_3",
               now: @visible_at,
               claim_id: "claim_3",
               claim_token: "token_3"
             )

    assert {:ok, [^scheduled_entry, claimed_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert claimed_entry.type == :attempt_claimed
    assert claimed_entry.data.claim_id == "claim_2"
  end

  test "does not claim when the run became terminal after dispatch rebuild" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :cancelled,
               occurred_at: @claimed_at
             })

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [run_terminal])

    assert {:ok, :none} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:ok, [^scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "returns conflict instead of claiming from a stale dispatch projection" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, wakeup_entry} =
             DispatchProtocol.new_entry(:live_wakeup_emitted, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               queue: "default",
               occurred_at: @claimed_at
             })

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")
    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [wakeup_entry], expected_rev: 1)

    assert {:error, :conflict} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:ok, [^scheduled_entry, ^wakeup_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "heartbeats the current claim and extends its durable lease" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok,
            %{
              agent: claimed_agent,
              claim_id: claim_id,
              claim_token: claim_token
            }} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2",
               lease_for: 60
             )

    assert {:ok,
            %{
              agent: heartbeat_agent,
              attempt: %{runnable_key: @runnable_key, lease_until: ~U[2026-05-15 00:02:20Z]},
              lease_until: ~U[2026-05-15 00:02:20Z]
            }} =
             DispatchAgent.heartbeat(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               now: @claimed_at,
               lease_for: 120
             )

    assert heartbeat_agent.state.thread_rev == 3

    assert {:ok, [^scheduled_entry, _claim_entry, heartbeat_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert heartbeat_entry.type == :attempt_heartbeat
    assert heartbeat_entry.data.claim_id == claim_id
    refute heartbeat_entry.data.claim_token_hash == claim_token
  end

  test "completes the current claim with a durable result" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    result = %{"status" => "captured"}

    assert {:ok,
            %{
              agent: completed_agent,
              attempt: %{runnable_key: @runnable_key, status: :completed, result: ^result}
            }} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               result,
               now: @claimed_at
             )

    assert completed_agent.state.thread_rev == 3

    assert [%{runnable_key: @runnable_key, result: ^result}] =
             DispatchAgent.completed_results(completed_agent)

    assert {:ok, [_scheduled_entry, _claim_entry, completed_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert completed_entry.type == :attempt_completed
    assert completed_entry.data.result == result
    refute completed_entry.data.claim_token_hash == claim_token
  end

  test "treats duplicate completion for the same claim and result as idempotent" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    result = %{"status" => "captured"}

    assert {:ok, %{agent: completed_agent, attempt: completed_attempt}} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               result,
               now: @claimed_at
             )

    assert {:ok, %{agent: ^completed_agent, attempt: ^completed_attempt}} =
             DispatchAgent.complete(
               @storage,
               completed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               result,
               now: @claimed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(entries, &(&1.type == :attempt_completed)) == 1
  end

  test "fails the current claim and schedules a retry attempt" do
    trace = %{
      trace_id: String.duplicate("a", 32),
      span_id: String.duplicate("b", 16),
      causation_id: @runnable_key
    }

    retry_trace = %{
      trace_id: trace.trace_id,
      span_id: String.duplicate("c", 16),
      parent_span_id: trace.span_id,
      causation_id: @runnable_key
    }

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs(trace: trace))

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    retry_key = "run_123:charge_card:2"
    retry_visible_at = ~U[2026-05-15 00:03:00Z]
    error = %{"code" => "gateway_timeout"}

    assert {:ok,
            %{
              agent: failed_agent,
              attempt: %{runnable_key: @runnable_key, status: :failed, error: ^error}
            }} =
             DispatchAgent.fail(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               error,
               now: @claimed_at,
               retry_runnable_key: retry_key,
               retry_visible_at: retry_visible_at,
               retry_trace: retry_trace
             )

    assert [
             %{
               runnable_key: ^retry_key,
               attempt_number: 2,
               status: :retry_scheduled,
               trace: ^retry_trace
             }
           ] =
             DispatchAgent.visible_attempts(failed_agent, retry_visible_at)

    assert {:ok, [^scheduled_entry, claim_entry, failed_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    assert failed_entry.type == :attempt_failed
    assert failed_entry.data.error == error
    assert failed_entry.data.trace == trace
    assert failed_entry.data.retry_runnable_key == retry_key
    assert failed_entry.data.retry_visible_at == retry_visible_at
    assert failed_entry.data.retry_trace == retry_trace

    assert Enum.all?([scheduled_entry, claim_entry, failed_entry], fn entry ->
             entry.data.trace == trace
           end)
  end

  test "rejects lifecycle appends with a stale claim token before writing" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    completed_event = [:squidie, :runtime, :attempt, :completed]
    Squidie.Test.TelemetryCapture.attach([completed_event])

    assert {:error, :stale_claim} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               "stale_token",
               %{"status" => "captured"},
               now: @claimed_at
             )

    assert {:ok, [_scheduled_entry, _claim_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})

    refute_receive {:telemetry_event, ^completed_event, _, _}
  end

  test "rejects lifecycle appends after the claim lease expired before writing" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               lease_for: 30,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:error, :expired_claim} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               %{"status" => "captured"},
               now: @expired_at
             )

    assert {:ok, [_scheduled_entry, _claim_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "rejects lifecycle appends when the run became terminal before writing" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :cancelled,
               occurred_at: @claimed_at
             })

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [run_terminal])

    assert {:error, :terminal_run} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               %{"status" => "captured"},
               now: @claimed_at
             )

    assert {:ok, [_scheduled_entry, _claim_entry]} =
             Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "returns conflict when lifecycle append uses a stale dispatch projection" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: claimed_agent, claim_id: claim_id, claim_token: claim_token}} =
             DispatchAgent.claim_next(@storage, agent, "worker_2",
               now: @visible_at,
               claim_id: "claim_2",
               claim_token: "token_2"
             )

    assert {:ok, stale_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok, %{agent: _heartbeat_agent}} =
             DispatchAgent.heartbeat(
               @storage,
               claimed_agent,
               @runnable_key,
               claim_id,
               claim_token,
               now: @claimed_at
             )

    assert {:error, :conflict} =
             DispatchAgent.complete(
               @storage,
               stale_agent,
               @runnable_key,
               claim_id,
               claim_token,
               %{"status" => "captured"},
               now: @claimed_at
             )

    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    refute Enum.any?(entries, &(&1.type == :attempt_completed))
  end

  test "replays entries newer than a stale dispatch checkpoint" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :cancelled,
               occurred_at: @expired_at
             })

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])

    checkpoint_projection = Projection.rebuild([scheduled_entry, run_terminal])

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               checkpoint_projection,
               thread.rev,
               updated_at: @visible_at
             )

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [claimed_entry], expected_rev: 1)

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert agent.state.thread_rev == 2
    assert DispatchAgent.expired_claims(agent, @expired_at) == []
  end

  test "loads run-thread applied overlays for attempts restored from checkpoint" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, completed_entry} =
             DispatchProtocol.new_entry(:attempt_completed, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               claim_id: "claim_1",
               claim_token_hash: "token_hash_1",
               queue: "default",
               result: %{"status" => "captured"},
               occurred_at: @claimed_at
             })

    assert {:ok, applied_entry} =
             DispatchProtocol.new_entry(:runnable_applied, %{
               run_id: @run_id,
               runnable_key: @runnable_key,
               result: %{"status" => "captured"},
               occurred_at: @claimed_at
             })

    dispatch_entries = [scheduled_entry, claimed_entry, completed_entry]
    assert {:ok, thread} = Journal.append_entries(@storage, dispatch_entries)

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               Projection.rebuild(dispatch_entries),
               thread.rev,
               updated_at: @claimed_at
             )

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [applied_entry])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert %{applied?: true} = Map.fetch!(agent.state.projection.attempts, @runnable_key)
  end

  test "fences dispatch work for runs with terminal run-thread entries" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, run_terminal} =
             DispatchProtocol.new_entry(:run_terminal, %{
               run_id: @run_id,
               status: :cancelled,
               occurred_at: @expired_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [scheduled_entry, claimed_entry])
    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [run_terminal])

    assert {:ok, agent} = DispatchAgent.rebuild(@storage, "default")

    assert DispatchAgent.visible_attempts(agent, @expired_at) == []
    assert DispatchAgent.expired_claims(agent, @expired_at) == []
  end

  test "returns an error when a related run thread has incompatible persisted entries" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])

    assert {:ok, _thread} =
             ETS.append_thread(
               Journal.thread_id({:run, @run_id}),
               [%{kind: :note, payload: %{}}],
               table: :squidie_dispatch_agent_test
             )

    assert {:error, {:invalid_journal_entry, 0, :missing_data}} =
             DispatchAgent.rebuild(@storage, "default")
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        idempotency_key: @idempotency_key,
        attempt_number: 1,
        queue: "default",
        step: "charge_card",
        input: %{"payment_id" => "pay_123"},
        visible_at: @visible_at,
        occurred_at: @started_at
      },
      Map.new(attrs)
    )
  end

  defp planned_runnable(attrs \\ %{}) do
    Map.delete(scheduled_attrs(attrs), :occurred_at)
  end

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: "claim_1",
        claim_token_hash: "token_hash_1",
        owner_id: "worker_1",
        queue: "default",
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp table_name(:checkpoints), do: :squidie_dispatch_agent_test_checkpoints
  defp table_name(:threads), do: :squidie_dispatch_agent_test_threads
  defp table_name(:thread_meta), do: :squidie_dispatch_agent_test_thread_meta

  defp cleanup_storage do
    Enum.each([:checkpoints, :threads, :thread_meta], fn suffix ->
      table = table_name(suffix)

      try do
        if :ets.whereis(table) != :undefined do
          :ets.delete(table)
        end
      rescue
        ArgumentError -> :ok
      end
    end)
  end
end
