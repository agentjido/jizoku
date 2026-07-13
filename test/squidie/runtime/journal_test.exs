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
