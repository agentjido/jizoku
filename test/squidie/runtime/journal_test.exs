defmodule Squidie.Runtime.JournalTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Projection
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Checkpoint
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.RunCatalogProjection
  alias Squidie.Runtime.RunIndexProjection
  alias Squidie.Telemetry.CommitBuffer
  alias Squidie.Telemetry.JournalEvents
  alias Squidie.Test.TelemetryCapture

  defmodule LoadTrackingStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      ETS.get_checkpoint(key, delegate_opts(opts))
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      ETS.put_checkpoint(key, data, delegate_opts(opts))
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      ETS.delete_checkpoint(key, delegate_opts(opts))
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:storage_load_thread, thread_id})
      ETS.load_thread(thread_id, delegate_opts(opts))
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      send(Keyword.fetch!(opts, :test_pid), {:storage_append_opts, opts})
      ETS.append_thread(thread_id, entries, delegate_opts(opts))
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      ETS.delete_thread(thread_id, delegate_opts(opts))
    end

    defp delegate_opts(opts) do
      Keyword.delete(opts, :test_pid)
    end
  end

  @storage {ETS, table: :squidie_journal_test}
  @run_id "run_123"
  @second_run_id "run_456"
  @workflow "BillingWorkflow"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @claim_id "claim_1"
  @claim_token_hash "token_hash_1"
  @owner_id "worker_1"
  @queue "default"
  @started_at ~U[2026-05-14 00:00:00Z]
  @visible_at ~U[2026-05-14 00:00:10Z]
  @claimed_at ~U[2026-05-14 00:00:20Z]
  @lease_until ~U[2026-05-14 00:01:00Z]
  @completed_at ~U[2026-05-14 00:00:30Z]
  @trace %{
    trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
    span_id: "00f067aa0ba902b7",
    causation_id: "signal-123"
  }

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "appends runtime entries to Jido storage and rebuilds dispatch projections" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])
    assert thread.id == "squidie:dispatch:default"
    assert thread.rev == 1

    assert {:ok, restored_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert restored_entries == [scheduled_entry]
    assert {:ok, projection} = Journal.rebuild_dispatch_projection(@storage, "default")

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "normalizes journal storage behind a Squidie-owned boundary" do
    assert {:ok, %Storage{adapter: ETS, opts: [table: :squidie_journal_test]}} =
             Storage.normalize(@storage)

    assert {:ok, %Storage{} = storage} = Storage.normalize(@storage)

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(storage, [scheduled_entry])
    assert {:ok, [^scheduled_entry]} = Journal.load_entries(storage, {:dispatch, "default"})
  end

  test "scopes durable thread and checkpoint identities by partition while preserving legacy ids" do
    assert Journal.thread_id({:run, @run_id}) == "squidie:run:run_123"
    assert Journal.thread_id({:dispatch, "default"}) == "squidie:dispatch:default"

    assert Journal.thread_id({:run_index, @workflow}) ==
             "squidie:run_index:BillingWorkflow"

    assert Journal.thread_id({:run_catalog, "all"}) == "squidie:run_catalog:all"

    assert Journal.thread_id({:run, @run_id}, "tenant_acme") ==
             "squidie:partition:tenant_acme:run:run_123"

    assert Journal.thread_id({:dispatch, "default"}, "tenant_acme") ==
             "squidie:partition:tenant_acme:dispatch:default"

    assert Journal.thread_id({:run_index, @workflow}, "tenant_acme") ==
             "squidie:partition:tenant_acme:run_index:BillingWorkflow"

    assert Journal.thread_id({:run_catalog, "all"}, "tenant_acme") ==
             "squidie:partition:tenant_acme:run_catalog:all"

    refute Journal.thread_id({:run, @run_id}, "default") ==
             Journal.thread_id({:run, @run_id})

    assert {:ok, acme_storage} = Storage.scope(@storage, "tenant_acme")
    assert {:ok, globex_storage} = Storage.scope(@storage, "tenant_globex")
    assert {:ok, legacy_storage} = Storage.scope(@storage, nil)

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{id: "squidie:partition:tenant_acme:dispatch:default"}} =
             Journal.append_entries(acme_storage, [scheduled_entry])

    assert {:ok, [^scheduled_entry]} =
             Journal.load_entries(acme_storage, {:dispatch, "default"})

    assert {:error, :not_found} =
             Journal.load_entries(globex_storage, {:dispatch, "default"})

    assert {:error, :not_found} =
             Journal.load_entries(legacy_storage, {:dispatch, "default"})

    assert :ok =
             Journal.put_checkpoint(
               acme_storage,
               {:dispatch, "default"},
               %Projection{},
               1,
               updated_at: @visible_at
             )

    assert {:ok, %Checkpoint{thread_id: "squidie:partition:tenant_acme:dispatch:default"}} =
             Journal.fetch_checkpoint(acme_storage, {:dispatch, "default"})

    assert {:error, :not_found} =
             Journal.fetch_checkpoint(globex_storage, {:dispatch, "default"})
  end

  test "revalidates normalized journal storage structs" do
    malformed_storage = %Storage{adapter: String, opts: [], config: String}

    assert {:error, {:invalid_option, {:journal_storage, String}}} =
             Storage.normalize(malformed_storage)

    caller_config = %{path: "/tmp/ignored", token: "not-canonical"}

    storage = %Storage{
      adapter: ETS,
      opts: [table: :squidie_journal_test],
      config: caller_config
    }

    assert {:ok, %Storage{config: {ETS, [table: :squidie_journal_test]}}} =
             Storage.normalize(storage)
  end

  test "replays multiple dispatch entries in order and rebuilds final projection state" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, completed_entry} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    entries = [scheduled_entry, claimed_entry, completed_entry]

    assert {:ok, %{rev: 3}} = Journal.append_entries(@storage, entries)
    assert {:ok, ^entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    assert {:ok, projection} = Journal.rebuild_dispatch_projection(@storage, "default")

    assert Projection.visible_attempts(projection, @visible_at) == []

    assert [
             %{
               runnable_key: @runnable_key,
               status: :completed,
               result: %{"status" => "captured"}
             }
           ] = Projection.completed_results(projection)
  end

  test "loads thread metadata with decoded entries" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    entries = [scheduled_entry, claimed_entry]

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, entries)

    assert {:ok,
            %{
              thread: {:dispatch, "default"},
              thread_id: "squidie:dispatch:default",
              rev: 2,
              entries: ^entries
            }} = Journal.load_thread(@storage, {:dispatch, "default"})
  end

  test "appends run index entries and rebuilds run index projections" do
    assert {:ok, first_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: @workflow,
               queue: @queue,
               occurred_at: @started_at
             })

    assert {:ok, second_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @second_run_id,
               workflow: @workflow,
               queue: @queue,
               occurred_at: @completed_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [first_entry, second_entry])

    assert {:ok, %RunIndexProjection{} = projection} =
             Journal.rebuild_run_index_projection(@storage, @workflow)

    assert RunIndexProjection.run_ids(projection) == [@run_id, @second_run_id]
  end

  test "anchors run index rebuilds to the requested workflow" do
    misfiled_entry = %DispatchProtocol.Entry{
      type: :run_indexed,
      thread: {:run_index, @workflow},
      data: %{
        run_id: @second_run_id,
        workflow: "OtherWorkflow",
        queue: @queue
      },
      occurred_at: @started_at
    }

    assert {:ok, valid_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: @workflow,
               queue: @queue,
               occurred_at: @completed_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [misfiled_entry, valid_entry])

    assert {:ok, %RunIndexProjection{} = projection} =
             Journal.rebuild_run_index_projection(@storage, @workflow)

    assert RunIndexProjection.run_ids(projection) == [@run_id]

    assert [
             %{
               entry_type: :run_indexed,
               reason: :conflicting_workflow,
               run_id: @second_run_id,
               workflow: "OtherWorkflow",
               queue: @queue
             }
           ] = RunIndexProjection.anomalies(projection)
  end

  test "appends run catalog entries and rebuilds the global run catalog projection" do
    assert {:ok, first_entry} =
             DispatchProtocol.new_entry(:run_cataloged, %{
               run_id: @run_id,
               workflow: @workflow,
               queue: @queue,
               occurred_at: @started_at
             })

    assert {:ok, second_entry} =
             DispatchProtocol.new_entry(:run_cataloged, %{
               run_id: @second_run_id,
               workflow: "OtherWorkflow",
               queue: "other",
               occurred_at: @completed_at
             })

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, [first_entry, second_entry])

    assert {:ok, %RunCatalogProjection{} = projection} =
             Journal.rebuild_run_catalog_projection(@storage)

    assert RunCatalogProjection.run_ids(projection) == [@run_id, @second_run_id]

    assert RunCatalogProjection.runs(projection) == [
             %{
               run_id: @run_id,
               workflow: @workflow,
               queue: @queue,
               indexed_at: @started_at
             },
             %{
               run_id: @second_run_id,
               workflow: "OtherWorkflow",
               queue: "other",
               indexed_at: @completed_at
             }
           ]
  end

  test "returns an empty run catalog projection for absent catalog threads" do
    assert {:ok, %RunCatalogProjection{} = projection} =
             Journal.rebuild_run_catalog_projection(@storage)

    assert RunCatalogProjection.run_ids(projection) == []
    assert RunCatalogProjection.anomalies(projection) == []
  end

  test "returns an empty run index projection for absent index threads" do
    assert {:ok, %RunIndexProjection{} = projection} =
             Journal.rebuild_run_index_projection(@storage, @workflow)

    assert RunIndexProjection.workflow(projection) == @workflow
    assert RunIndexProjection.run_ids(projection) == []
    assert RunIndexProjection.anomalies(projection) == []
  end

  test "normalizes atom workflows when rebuilding run index projections" do
    workflow = Atom.to_string(__MODULE__)

    assert {:ok, index_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: @run_id,
               workflow: __MODULE__,
               queue: @queue,
               occurred_at: @started_at
             })

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [index_entry])

    assert {:ok, %RunIndexProjection{} = projection} =
             Journal.rebuild_run_index_projection(@storage, __MODULE__)

    assert RunIndexProjection.workflow(projection) == workflow
    assert RunIndexProjection.run_ids(projection) == [@run_id]
  end

  @tag :tmp_dir
  test "restores entries through file-backed Jido storage", %{tmp_dir: tmp_dir} do
    storage = {Jido.Storage.File, path: tmp_dir}

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(storage, [scheduled_entry])
    assert {:ok, [^scheduled_entry]} = Journal.load_entries(storage, {:dispatch, "default"})

    restored_storage = {Jido.Storage.File, path: tmp_dir}
    assert {:ok, projection} = Journal.rebuild_dispatch_projection(restored_storage, "default")

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "rejects stale optimistic appends with the current Jido thread revision" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert thread.rev == 1

    assert {:error, :conflict} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert {:ok, thread} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 1)

    assert thread.rev == 2
  end

  test "stores projection checkpoints with explicit applied thread revisions" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    projection = Projection.rebuild(entries)

    assert :ok =
             Journal.put_checkpoint(@storage, {:dispatch, "default"}, projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok,
            %Checkpoint{
              thread: {:dispatch, "default"},
              thread_id: "squidie:dispatch:default",
              thread_rev: 1,
              projection: ^projection,
              updated_at: @visible_at
            }} = Journal.fetch_checkpoint(@storage, {:dispatch, "default"})
  end

  test "returns structured not found errors for absent threads and checkpoints" do
    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "missing"})
    assert {:error, :not_found} = Journal.load_thread(@storage, {:dispatch, "missing"})
    assert {:error, :not_found} = Journal.fetch_checkpoint(@storage, {:dispatch, "missing"})
  end

  test "returns structured errors for incompatible persisted thread entries" do
    assert {:ok, _thread} =
             ETS.append_thread(
               Journal.thread_id({:dispatch, "default"}),
               [%{kind: :note, payload: %{}}],
               table: :squidie_journal_test
             )

    assert {:error, {:invalid_journal_entry, 0, :missing_data}} =
             Journal.load_thread(@storage, {:dispatch, "default"})
  end

  test "returns structured errors for invalid persisted timestamps" do
    assert {:ok, _thread} =
             ETS.append_thread(
               Journal.thread_id({:dispatch, "default"}),
               [
                 %{
                   kind: :attempt_scheduled,
                   at: "not-a-unix-millisecond",
                   payload: %{data: scheduled_attrs()}
                 }
               ],
               table: :squidie_journal_test
             )

    assert {:error, {:invalid_journal_entry, 0, :invalid_timestamp}} =
             Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "rejects appending entries that belong to different durable threads" do
    assert {:ok, run_entry} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: "BillingWorkflow",
               occurred_at: @started_at
             })

    assert {:ok, dispatch_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:error, {:mixed_threads, [{:run, @run_id}, {:dispatch, "default"}]}} =
             Journal.append_entries(@storage, [run_entry, dispatch_entry])
  end

  test "emits sanitized lifecycle points only after committed appends and in fact order" do
    events = [
      [:squidie, :runtime, :command, :received],
      [:squidie, :runtime, :run, :started],
      [:squidie, :runtime, :runnable, :planned],
      [:squidie, :runtime, :attempt, :scheduled],
      [:squidie, :runtime, :attempt, :claimed],
      [:squidie, :runtime, :attempt, :heartbeat],
      [:squidie, :runtime, :attempt, :completed]
    ]

    TelemetryCapture.attach(events)

    runnable =
      scheduled_attrs(trace: @trace)
      |> Map.delete(:occurred_at)
      |> Map.put(:input, %{password: "secret-sentinel"})

    run_entries = [
      entry!(:run_signal_received, %{
        run_id: @run_id,
        signal_type: :start_run,
        signal_id: "signal-123",
        trace: @trace,
        payload: %{password: "secret-sentinel"},
        metadata: %{token: "secret-sentinel"},
        occurred_at: @started_at
      }),
      entry!(:run_started, %{
        run_id: @run_id,
        workflow: @workflow,
        trace: @trace,
        input: %{password: "secret-sentinel"},
        occurred_at: @started_at
      }),
      entry!(:runnables_planned, %{
        run_id: @run_id,
        runnables: [runnable],
        occurred_at: @started_at
      })
    ]

    assert {:ok, %{rev: 3}} = Journal.append_entries(@storage, run_entries)

    assert_receive {:telemetry_event, [:squidie, :runtime, :command, :received],
                    %{count: 1, system_time: command_time}, command_metadata}

    assert is_integer(command_time)

    assert command_metadata == %{
             command_type: :start_run,
             workflow: @workflow,
             run_id: @run_id,
             signal_id: "signal-123",
             trace_id: @trace.trace_id,
             span_id: @trace.span_id,
             causation_id: @trace.causation_id
           }

    assert_receive {:telemetry_event, [:squidie, :runtime, :run, :started], %{count: 1},
                    started_metadata}

    assert started_metadata == %{
             workflow: @workflow,
             run_id: @run_id,
             trace_id: @trace.trace_id,
             span_id: @trace.span_id,
             causation_id: @trace.causation_id
           }

    assert_receive {:telemetry_event, [:squidie, :runtime, :runnable, :planned], %{count: 1},
                    planned_metadata}

    assert planned_metadata == %{
             workflow: @workflow,
             queue: @queue,
             step: "charge_card",
             attempt_number: 1,
             run_id: @run_id,
             runnable_key: @runnable_key,
             trace_id: @trace.trace_id,
             span_id: @trace.span_id,
             causation_id: @trace.causation_id
           }

    dispatch_entries = [
      entry!(:attempt_scheduled, scheduled_attrs(trace: @trace)),
      entry!(:attempt_claimed, claimed_attrs(trace: @trace)),
      entry!(:attempt_heartbeat, %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        owner_id: "secret-sentinel",
        queue: @queue,
        trace: @trace,
        lease_until: @lease_until,
        occurred_at: @claimed_at
      }),
      entry!(:attempt_completed, completed_attrs(trace: @trace))
    ]

    assert {:ok, %{rev: 4}} = Journal.append_entries(@storage, dispatch_entries)

    for event <- [:scheduled, :claimed, :heartbeat, :completed] do
      assert_receive {:telemetry_event, [:squidie, :runtime, :attempt, ^event], %{count: 1},
                      metadata}

      assert metadata.workflow == @workflow
      assert metadata.queue == @queue
      assert metadata.step == "charge_card"
      assert metadata.attempt_number == 1
      assert metadata.run_id == @run_id
      assert metadata.runnable_key == @runnable_key
      assert metadata.trace_id == @trace.trace_id
      refute inspect(metadata) =~ "secret-sentinel"
    end

    refute_receive {:telemetry_event, _, _, _}
  end

  test "emits committed lifecycle points without rereading durable threads" do
    storage =
      {LoadTrackingStorage, table: :squidie_journal_test, test_pid: self()}

    entry =
      entry!(:run_started, %{
        run_id: @run_id,
        workflow: @workflow,
        occurred_at: @started_at
      })

    assert {:ok, %{rev: 1}} =
             Journal.append_entries(storage, [entry],
               expected_rev: 0,
               telemetry_projection: Squidie.Runtime.WorkflowAgent.Projection.new()
             )

    assert_receive {:storage_append_opts, append_opts}
    refute Keyword.has_key?(append_opts, :telemetry_projection)
    refute_receive {:storage_load_thread, _thread_id}
  end

  test "emits failure and retry scheduling once without exposing errors" do
    failed_event = [:squidie, :runtime, :attempt, :failed]
    retry_event = [:squidie, :runtime, :attempt, :retry_scheduled]
    retry_key = "#{@run_id}:charge_card:2"

    retry_trace = %{
      trace_id: @trace.trace_id,
      span_id: "b7ad6b7169203331",
      parent_span_id: @trace.span_id,
      causation_id: @runnable_key
    }

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:run_started, %{
                 run_id: @run_id,
                 workflow: @workflow,
                 trace: @trace,
                 occurred_at: @started_at
               }),
               entry!(:runnables_planned, %{
                 run_id: @run_id,
                 runnables: [Map.delete(scheduled_attrs(trace: @trace), :occurred_at)],
                 occurred_at: @started_at
               })
             ])

    assert {:ok, _thread} =
             Journal.append_entries(@storage, [
               entry!(:attempt_scheduled, scheduled_attrs(trace: @trace)),
               entry!(:attempt_claimed, claimed_attrs(trace: @trace))
             ])

    assert {:ok, dispatch_projection} =
             Journal.rebuild_dispatch_projection(@storage, @queue)

    TelemetryCapture.attach([failed_event, retry_event])

    failed_entry =
      entry!(:attempt_failed, %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        queue: @queue,
        trace: @trace,
        error: %{message: "secret-sentinel", token: "secret-sentinel"},
        retry_runnable_key: retry_key,
        retry_visible_at: @completed_at,
        retry_trace: retry_trace,
        occurred_at: @completed_at
      })

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(@storage, [failed_entry],
               telemetry_projection: dispatch_projection
             )

    assert_receive {:telemetry_event, ^failed_event, %{count: 1}, failed_metadata}
    assert failed_metadata.runnable_key == @runnable_key
    refute Map.has_key?(failed_metadata, :retry_state)
    refute inspect(failed_metadata) =~ "secret-sentinel"

    assert_receive {:telemetry_event, ^retry_event, %{count: 1}, retry_metadata}
    assert retry_metadata.runnable_key == retry_key
    assert retry_metadata.retry_state == :retry
    assert retry_metadata.attempt_number == 2
    assert retry_metadata.trace_id == retry_trace.trace_id
    assert retry_metadata.span_id == retry_trace.span_id
    refute_receive {:telemetry_event, _, _, _}
  end

  test "maps committed run lifecycle facts to exact sanitized point schemas" do
    runnable = Map.delete(scheduled_attrs(trace: @trace), :occurred_at)

    run_entries = [
      entry!(:run_started, %{
        run_id: @run_id,
        workflow: @workflow,
        trace: @trace,
        occurred_at: @started_at
      }),
      entry!(:runnables_planned, %{
        run_id: @run_id,
        runnables: [runnable],
        occurred_at: @started_at
      })
    ]

    assert {:ok, _thread} = Journal.append_entries(@storage, run_entries)

    workflow_projection = Squidie.Runtime.WorkflowAgent.Projection.rebuild(run_entries)

    events = [
      [:squidie, :runtime, :runnable, :applied],
      [:squidie, :runtime, :manual, :paused],
      [:squidie, :runtime, :manual, :resolved],
      [:squidie, :runtime, :child, :started],
      [:squidie, :runtime, :dynamic_work, :recorded],
      [:squidie, :runtime, :run, :terminal]
    ]

    TelemetryCapture.attach(events)

    entries = [
      entry!(:runnable_applied, %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        trace: @trace,
        result: %{token: "secret-sentinel"},
        transition: %{on: "error", to: "manual_review"},
        occurred_at: @completed_at
      }),
      entry!(:manual_step_paused, %{
        run_id: @run_id,
        step: "charge_card",
        kind: "approval",
        trace: @trace,
        metadata: %{token: "secret-sentinel"},
        occurred_at: @completed_at
      }),
      entry!(:manual_step_resolved, %{
        run_id: @run_id,
        step: "charge_card",
        action: "approved",
        trace: @trace,
        result: %{token: "secret-sentinel"},
        metadata: %{actor: "secret-sentinel"},
        occurred_at: @completed_at
      }),
      entry!(:child_run_started, %{
        run_id: @run_id,
        child_run_id: @second_run_id,
        child_workflow: "ChildWorkflow",
        child_trigger: "manual",
        child_key: "child-1",
        origin: %{runnable_key: @runnable_key, step: "charge_card", attempt: 1},
        trace: @trace,
        metadata: %{token: "secret-sentinel"},
        occurred_at: @completed_at
      }),
      entry!(:dynamic_work_recorded, %{
        run_id: @run_id,
        dynamic_key: "fanout-1",
        origin: %{runnable_key: @runnable_key, step: "charge_card", attempt: 1},
        nodes: [%{id: "node-1", metadata: %{token: "secret-sentinel"}}],
        trace: @trace,
        metadata: %{token: "secret-sentinel"},
        occurred_at: @completed_at
      }),
      entry!(:run_terminal, %{
        run_id: @run_id,
        workflow: @workflow,
        status: :failed,
        trace: @trace,
        error: %{message: "secret-sentinel"},
        occurred_at: @completed_at
      })
    ]

    assert {:ok, %{rev: 8}} =
             Journal.append_entries(@storage, entries, telemetry_projection: workflow_projection)

    assert_receive {:telemetry_event, [:squidie, :runtime, :runnable, :applied], %{count: 1},
                    applied_metadata}

    assert applied_metadata.outcome == :error
    assert applied_metadata.step == "charge_card"
    assert applied_metadata.queue == @queue

    assert_receive {:telemetry_event, [:squidie, :runtime, :manual, :paused], %{count: 1},
                    paused_metadata}

    assert paused_metadata.kind == "approval"
    assert paused_metadata.step == "charge_card"

    assert_receive {:telemetry_event, [:squidie, :runtime, :manual, :resolved], %{count: 1},
                    resolved_metadata}

    assert resolved_metadata.action == "approved"
    assert resolved_metadata.step == "charge_card"

    assert_receive {:telemetry_event, [:squidie, :runtime, :child, :started], %{count: 1},
                    child_metadata}

    assert child_metadata.child_run_id == @second_run_id

    assert_receive {:telemetry_event, [:squidie, :runtime, :dynamic_work, :recorded], %{count: 1},
                    dynamic_metadata}

    assert dynamic_metadata.dynamic_key == "fanout-1"

    assert_receive {:telemetry_event, [:squidie, :runtime, :run, :terminal], %{count: 1},
                    terminal_metadata}

    assert terminal_metadata.status == :failed

    for metadata <- [
          applied_metadata,
          paused_metadata,
          resolved_metadata,
          child_metadata,
          dynamic_metadata,
          terminal_metadata
        ] do
      assert metadata.workflow == @workflow
      assert metadata.run_id == @run_id
      assert metadata.trace_id == @trace.trace_id
      refute inspect(metadata) =~ "secret-sentinel"
    end

    refute_receive {:telemetry_event, _, _, _}
  end

  test "conflicts checkpoints and projection rebuilds emit no lifecycle points" do
    event = [:squidie, :runtime, :attempt, :scheduled]
    TelemetryCapture.attach([event])

    scheduled_entry = entry!(:attempt_scheduled, scheduled_attrs(trace: @trace))

    assert {:ok, %{rev: 1}} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert_receive {:telemetry_event, ^event, %{count: 1}, _metadata}

    assert {:error, :conflict} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert {:ok, projection} = Journal.rebuild_dispatch_projection(@storage, @queue)

    assert :ok =
             Journal.put_checkpoint(@storage, {:dispatch, @queue}, projection, 1,
               updated_at: @visible_at
             )

    assert {:ok, %Checkpoint{}} = Journal.fetch_checkpoint(@storage, {:dispatch, @queue})
    refute_receive {:telemetry_event, _, _, _}
  end

  test "buffers sanitized committed intents until explicit flush and discards them on demand" do
    started_event = [:squidie, :runtime, :run, :started]
    planned_event = [:squidie, :runtime, :runnable, :planned]
    terminal_event = [:squidie, :runtime, :run, :terminal]
    TelemetryCapture.attach([started_event, planned_event, terminal_event])

    buffer = CommitBuffer.new()
    assert {:ok, storage} = Storage.put_commit_buffer(@storage, buffer)

    assert {:ok, %{rev: 2}} =
             Journal.append_entries(storage, [
               entry!(:run_started, %{
                 run_id: @run_id,
                 workflow: @workflow,
                 trace: @trace,
                 occurred_at: @started_at
               }),
               entry!(:runnables_planned, %{
                 run_id: @run_id,
                 runnables: [Map.delete(scheduled_attrs(trace: @trace), :occurred_at)],
                 occurred_at: @started_at
               })
             ])

    refute_receive {:telemetry_event, _, _, _}

    assert :ok = JournalEvents.flush(buffer)
    assert_receive {:telemetry_event, ^started_event, %{count: 1}, _metadata}
    assert_receive {:telemetry_event, ^planned_event, %{count: 1}, _metadata}
    refute_receive {:telemetry_event, _, _, _}

    discard_buffer = CommitBuffer.new()
    assert {:ok, discard_storage} = Storage.put_commit_buffer(@storage, discard_buffer)

    assert {:ok, %{rev: 3}} =
             Journal.append_entries(discard_storage, [
               entry!(:run_terminal, %{
                 run_id: @run_id,
                 status: :completed,
                 trace: @trace,
                 occurred_at: @completed_at
               })
             ])

    assert :ok = JournalEvents.discard(discard_buffer)
    refute_receive {:telemetry_event, ^terminal_event, _, _}
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        workflow: @workflow,
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

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        owner_id: @owner_id,
        queue: "default",
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp completed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        queue: "default",
        result: %{"status" => "captured"},
        occurred_at: @completed_at
      },
      Map.new(attrs)
    )
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp table_name(:checkpoints), do: :squidie_journal_test_checkpoints
  defp table_name(:threads), do: :squidie_journal_test_threads
  defp table_name(:thread_meta), do: :squidie_journal_test_thread_meta

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      table = table_name(suffix)
      delete_table_if_present(table)
    end
  end

  defp delete_table_if_present(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
