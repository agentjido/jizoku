defmodule Squidie.Runtime.Journal.ContinuationStarterTest do
  use ExUnit.Case, async: false

  alias Jido.Storage.ETS
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.Spec
  alias Squidie.Workflow.SpecData

  defmodule RecordingStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.get_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.put_checkpoint(key, data, delegate_opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.load_thread(thread_id, delegate_opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      send(Keyword.fetch!(opts, :test_pid), {
        :storage_append,
        thread_id,
        Enum.map(entries, & &1.kind)
      })

      {adapter, delegate_opts} = delegate(opts)
      adapter.append_thread(thread_id, entries, delegate_opts)
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp delegate(opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)
      {adapter, delegate_opts ++ Keyword.drop(opts, [:delegate, :test_pid])}
    end
  end

  defmodule RecordCursor do
    use Jido.Action,
      name: "record_cursor",
      description: "Records a continuation cursor",
      schema: [cursor: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{cursor: cursor}, _context) do
      {:ok, %{cursor: cursor}}
    end
  end

  defmodule CursorWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :continue do
        manual()

        payload do
          field :cursor, :string
        end
      end

      trigger :resume do
        manual()

        payload do
          field :cursor, :string
        end
      end

      step :record_cursor, RecordCursor
      transition :record_cursor, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :squidie_continuation_starter_test}
  @run_id "11111111-1111-5111-8111-111111111111"
  @predecessor_run_id "00000000-0000-5000-8000-000000000000"
  @now ~U[2026-08-09 14:00:00Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "commits successor lineage in the initial run-thread append" do
    recording_storage = {RecordingStorage, delegate: @storage, test_pid: self()}

    assert {:ok, snapshot} = start_successor(journal_storage: recording_storage)
    assert snapshot.run_id == @run_id

    assert_receive {
      :storage_append,
      "squidie:run:#{@run_id}",
      [:run_signal_received, :run_started, :run_continued_from, :runnables_planned]
    }

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})

    assert Enum.map(entries, & &1.type) == [
             :run_signal_received,
             :run_started,
             :run_continued_from,
             :runnables_planned
           ]

    projection = Projection.rebuild(entries)

    assert Projection.continuation(projection) == %{
             continued_from: %{
               run_id: @predecessor_run_id,
               continuation_key: "page-42"
             },
             continued_to: nil
           }
  end

  test "repairs duplicate successor starts without duplicating lineage" do
    assert {:ok, first} = start_successor()
    before_duplicate = journal_state()
    assert {:ok, duplicate} = start_successor()
    assert duplicate.run_id == first.run_id
    assert journal_state() == before_duplicate

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(entries, &(&1.type == :run_continued_from)) == 1
  end

  test "repairs indexing and dispatch gaps after the successor run commits" do
    assert {:ok, first} = start_successor()

    delete_thread({:run_index, Definition.serialize_workflow(CursorWorkflow)})
    delete_thread({:run_catalog, "all"})
    delete_thread({:dispatch, "default"})

    assert {:ok, repaired} = start_successor()
    assert repaired.run_id == first.run_id
    assert [%{runnable_key: _runnable_key}] = repaired.visible_attempts

    assert {:ok, [_index_entry]} =
             Journal.load_entries(
               @storage,
               {:run_index, Definition.serialize_workflow(CursorWorkflow)}
             )

    assert {:ok, [_catalog_entry]} = Journal.load_entries(@storage, {:run_catalog, "all"})
    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.any?(dispatch_entries, &(&1.type == :attempt_scheduled))

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(run_entries, &(&1.type == :run_continued_from)) == 1
  end

  test "rejects conflicting lineage for an existing successor" do
    assert {:ok, _snapshot} = start_successor()
    before_conflict = journal_state()

    assert {:error, :conflict} =
             start_successor(predecessor_run_id: "22222222-2222-5222-8222-222222222222")

    assert journal_state() == before_conflict

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, @run_id})
    projection = Projection.rebuild(entries)

    assert Projection.continuation(projection).continued_from.run_id == @predecessor_run_id
  end

  test "rejects conflicting trigger identity for an existing successor" do
    assert {:ok, _snapshot} = start_successor()
    assert {:error, :conflict} = start_successor(trigger: :resume)
  end

  test "rejects malformed successor lineage before writing" do
    assert {:error, {:invalid_option, {:continuation_origin, :invalid}}} =
             start_successor(continuation_origin: %{predecessor_run_id: :invalid})

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @run_id})
  end

  test "rejects unpaired or malformed continuation identity before writing" do
    assert {:error, {:invalid_option, {:continuation_definition_identity, :invalid}}} =
             start_successor(omit_continuation_definition_identity: true)

    assert {:error, {:invalid_option, {:continuation_definition_identity, :invalid}}} =
             start_successor(omit_continuation_origin: true)

    assert {:error, {:invalid_option, {:continuation_definition_identity, :invalid}}} =
             start_successor(continuation_definition_identity: %{definition_fingerprint: nil})

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @run_id})
  end

  test "rejects definition drift before creating a missing successor" do
    assert {:error, {:continuation_definition_mismatch, :fingerprint}} =
             start_successor(
               continuation_definition_identity: %{
                 definition_version: nil,
                 definition_fingerprint: "stale-definition"
               }
             )

    assert {:error, :not_found} = Journal.load_entries(@storage, {:run, @run_id})
  end

  test "rejects reuse under a different definition version with the same fingerprint" do
    assert {:ok, _snapshot} = start_successor()
    {:ok, definition} = Definition.load(CursorWorkflow)

    assert {:error, :conflict} =
             start_successor(
               continuation_definition_identity: %{
                 definition_version: "v2",
                 definition_fingerprint: Definition.fingerprint(definition)
               }
             )
  end

  test "repairs a persisted successor after the current module definition drifts" do
    {:ok, current_definition} = Definition.load(CursorWorkflow)

    versioned_spec = %Spec{
      Spec.from_definition(CursorWorkflow, current_definition)
      | definition_version: "v1"
    }

    versioned_definition = SpecData.from_struct(versioned_spec)

    continuation_identity = %{
      definition_version: versioned_definition.definition_version,
      definition_fingerprint: Definition.fingerprint(versioned_definition)
    }

    opts = [
      journal_storage: @storage,
      queue: "default",
      run_id: @run_id,
      now: @now,
      continuation_origin: %{
        predecessor_run_id: @predecessor_run_id,
        continuation_key: "page-42"
      },
      continuation_definition_identity: continuation_identity
    ]

    assert {:ok, first} =
             Starter.start_spec_run(versioned_spec, :continue, %{cursor: "next"}, opts)

    before_duplicate = journal_state()

    assert {:ok, duplicate} =
             Starter.start_spec_run(versioned_spec, :continue, %{cursor: "next"}, opts)

    assert duplicate.run_id == first.run_id
    assert journal_state() == before_duplicate

    delete_thread({:run_index, Definition.serialize_workflow(CursorWorkflow)})
    delete_thread({:run_catalog, "all"})
    delete_thread({:dispatch, "default"})

    assert current_definition.definition_version == nil

    assert {:ok, repaired} =
             start_successor(continuation_definition_identity: continuation_identity)

    assert repaired.run_id == first.run_id
    assert repaired.definition_version == "v1"
    assert [%{runnable_key: _runnable_key}] = repaired.visible_attempts
  end

  test "rejects repairing an existing successor into another queue" do
    assert {:ok, _snapshot} = start_successor()
    before_conflict = Map.put(journal_state(), {:dispatch, "priority"}, {:error, :not_found})

    assert {:error, :conflict} = start_successor(queue: "priority")

    after_conflict =
      Map.put(
        journal_state(),
        {:dispatch, "priority"},
        Journal.load_entries(@storage, {:dispatch, "priority"})
      )

    assert after_conflict == before_conflict
  end

  test "scopes the same successor identity and lineage by partition" do
    globex_predecessor = "33333333-3333-5333-8333-333333333333"

    assert {:ok, _acme} = start_successor(partition: "tenant_acme")

    assert {:ok, _globex} =
             start_successor(
               partition: "tenant_globex",
               predecessor_run_id: globex_predecessor
             )

    assert {:ok, acme_storage} = Storage.scope(@storage, "tenant_acme")
    assert {:ok, globex_storage} = Storage.scope(@storage, "tenant_globex")

    assert {:ok, acme_entries} = Journal.load_entries(acme_storage, {:run, @run_id})
    assert {:ok, globex_entries} = Journal.load_entries(globex_storage, {:run, @run_id})

    assert Projection.continuation(Projection.rebuild(acme_entries)).continued_from.run_id ==
             @predecessor_run_id

    assert Projection.continuation(Projection.rebuild(globex_entries)).continued_from.run_id ==
             globex_predecessor

    assert {:ok, _duplicate} = start_successor(partition: "tenant_acme")

    assert {:error, :conflict} =
             start_successor(
               partition: "tenant_acme",
               predecessor_run_id: globex_predecessor
             )
  end

  defp start_successor(overrides \\ []) do
    trigger = Keyword.get(overrides, :trigger, :continue)
    storage = Keyword.get(overrides, :journal_storage, @storage)
    partition = Keyword.get(overrides, :partition)
    queue = Keyword.get(overrides, :queue, "default")

    continuation_origin =
      Keyword.get(overrides, :continuation_origin, %{
        predecessor_run_id: Keyword.get(overrides, :predecessor_run_id, @predecessor_run_id),
        continuation_key: "page-42"
      })

    {:ok, definition} = Definition.load(CursorWorkflow)

    continuation_definition_identity =
      Keyword.get(overrides, :continuation_definition_identity, %{
        definition_version: definition.definition_version,
        definition_fingerprint: Definition.fingerprint(definition)
      })

    opts =
      [
        journal_storage: storage,
        partition: partition,
        queue: queue,
        run_id: @run_id,
        now: @now,
        continuation_origin: continuation_origin,
        continuation_definition_identity: continuation_definition_identity
      ]
      |> maybe_delete_option(
        :continuation_origin,
        Keyword.get(overrides, :omit_continuation_origin, false)
      )
      |> maybe_delete_option(
        :continuation_definition_identity,
        Keyword.get(overrides, :omit_continuation_definition_identity, false)
      )

    Starter.start_continuation_from_intent(CursorWorkflow, trigger, %{cursor: "next"}, opts)
  end

  defp maybe_delete_option(opts, option, true) do
    Keyword.delete(opts, option)
  end

  defp maybe_delete_option(opts, _option, false) do
    opts
  end

  defp cleanup_storage do
    for table <- [
          :squidie_continuation_starter_test_checkpoints,
          :squidie_continuation_starter_test_threads,
          :squidie_continuation_starter_test_thread_meta
        ] do
      delete_table(table)
    end
  end

  defp journal_state do
    Map.new(
      [
        {:run, @run_id},
        {:run_index, Definition.serialize_workflow(CursorWorkflow)},
        {:run_catalog, "all"},
        {:dispatch, "default"}
      ],
      fn thread -> {thread, Journal.load_entries(@storage, thread)} end
    )
  end

  defp delete_thread(thread) do
    thread_id = Journal.thread_id(thread)
    opts = [table: :squidie_continuation_starter_test]
    :ok = ETS.delete_thread(thread_id, opts)
    :ok = ETS.delete_checkpoint(thread_id, opts)
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
