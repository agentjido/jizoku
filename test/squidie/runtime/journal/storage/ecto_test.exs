defmodule Squidie.Runtime.Journal.Storage.EctoTest do
  use Squidie.DataCase, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias Jido.Thread.Entry
  alias Squidie.Persistence.JournalCheckpoint
  alias Squidie.Persistence.JournalEntry
  alias Squidie.Persistence.JournalThread
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Storage

  defmodule BarrierStorage do
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
      wait_at_barrier(thread_id, opts)
      {adapter, delegate_opts} = delegate(opts)

      adapter.append_thread(
        thread_id,
        entries,
        Keyword.merge(delegate_opts, Keyword.take(opts, [:expected_rev]))
      )
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp wait_at_barrier(thread_id, opts) do
      target = Keyword.get(opts, :barrier_thread_id)
      ref = Keyword.get(opts, :barrier_ref)
      key = {__MODULE__, ref}

      if thread_id == target and is_nil(Process.get(key)) do
        Process.put(key, true)
        barrier_pid = Keyword.fetch!(opts, :barrier_pid)
        send(barrier_pid, {:append_ready, ref, self()})

        receive do
          {:append_release, ^ref} -> :ok
        after
          5_000 -> raise "append barrier timed out"
        end
      end
    end

    defp delegate(opts) do
      case Keyword.fetch!(opts, :delegate) do
        {adapter, delegate_opts} -> {adapter, delegate_opts}
        adapter when is_atom(adapter) -> {adapter, []}
      end
    end
  end

  defmodule MutationOriginAction do
    use Squidie.Step, name: :mutation_origin

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{origin: true}}
    end
  end

  defmodule MutationAddedAction do
    use Squidie.Step, name: :mutation_added

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{added: true}}
    end
  end

  defmodule MutationHoldAction do
    use Squidie.Step, name: :mutation_hold

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{hold: true}}
    end
  end

  defmodule MutationWorkflow do
    use Squidie.Workflow

    alias Squidie.Runtime.Journal.Storage.EctoTest.MutationHoldAction
    alias Squidie.Runtime.Journal.Storage.EctoTest.MutationOriginAction

    workflow do
      trigger :manual do
        manual()
      end

      step :origin, MutationOriginAction
      step :hold, MutationHoldAction

      transition :origin, on: :ok, to: :hold
      transition :hold, on: :ok, to: :complete
    end
  end

  @storage_adapter Squidie.Runtime.Journal.Storage.Ecto

  @storage {@storage_adapter, repo: Repo}
  @thread_id "squidie:dispatch:ecto-storage"
  @run_id "run_123"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @started_at ~U[2026-05-14 00:00:00Z]
  @visible_at ~U[2026-05-14 00:00:10Z]

  setup do
    Repo.delete_all(JournalCheckpoint)
    Repo.delete_all(JournalEntry)
    Repo.delete_all(JournalThread)

    :ok
  end

  test "appends and reloads Jido thread entries from Postgres" do
    first_entry = entry(:attempt_scheduled, %{run_id: @run_id})
    second_entry = entry(:attempt_claimed, %{run_id: @run_id})

    assert {:ok, %{id: @thread_id, rev: 1, entries: [stored_first]}} =
             @storage_adapter.append_thread(@thread_id, [first_entry], repo: Repo)

    assert stored_first.seq == 0
    assert stored_first.kind == :attempt_scheduled

    assert {:ok, %{rev: 2, entries: [^stored_first, stored_second]}} =
             @storage_adapter.append_thread(@thread_id, [second_entry], repo: Repo)

    assert stored_second.seq == 1

    assert {:ok, %{rev: 2, entries: [^stored_first, ^stored_second]}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "rejects stale expected revisions without appending" do
    assert {:ok, %{rev: 1}} =
             @storage_adapter.append_thread(@thread_id, [entry(:attempt_scheduled)], repo: Repo)

    assert {:error, :conflict} =
             @storage_adapter.append_thread(@thread_id, [entry(:attempt_claimed)],
               repo: Repo,
               expected_rev: 0
             )

    assert {:ok, %{rev: 1, entries: [_entry]}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "rejects one of two concurrent appends with the same expected revision" do
    parent = self()

    tasks =
      for kind <- [:attempt_scheduled, :attempt_claimed] do
        Task.async(fn ->
          Sandbox.allow(Repo, parent, self())

          @storage_adapter.append_thread(@thread_id, [entry(kind)],
            repo: Repo,
            expected_rev: 0
          )
        end)
      end

    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &match?({:ok, %{rev: 1}}, &1)) == 1
    assert Enum.count(results, &match?({:error, :conflict}, &1)) == 1

    assert {:ok, %{rev: 1, entries: [_entry]}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "allows only one public graph mutation writer at a shared semantic version" do
    run_id = "0190a4f1-0a7c-7cb1-80c5-b4f8b1d23999"
    now = ~U[2026-07-18 11:00:00Z]

    assert {:ok, _snapshot} =
             Squidie.start(MutationWorkflow, %{},
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               run_id: run_id,
               now: now
             )

    assert {:ok, _snapshot} =
             Squidie.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: "default",
               owner_id: "mutation-origin",
               now: now
             )

    parent = self()
    barrier_ref = make_ref()

    barrier_storage =
      {BarrierStorage,
       delegate: @storage,
       barrier_thread_id: Journal.thread_id({:run, run_id}),
       barrier_pid: parent,
       barrier_ref: barrier_ref}

    tasks =
      for suffix <- ["left", "right"] do
        Task.async(fn ->
          Sandbox.allow(Repo, parent, self())

          Squidie.apply_graph_mutation(
            run_id,
            mutation(suffix),
            graph_mutation_options(barrier_storage, now)
          )
        end)
      end

    task_pids =
      for _task <- tasks do
        assert_receive {:append_ready, ^barrier_ref, task_pid}, 5_000
        task_pid
      end

    Enum.each(task_pids, &send(&1, {:append_release, barrier_ref}))
    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &match?({:ok, %{status: :committed}}, &1)) == 1

    assert Enum.count(
             results,
             &match?(
               {:error, {:invalid_graph_mutation, {:expected_version, {:stale, 1}}}},
               &1
             )
           ) == 1

    assert {:ok, entries} = Journal.load_entries(@storage, {:run, run_id})
    assert Enum.count(entries, &(&1.type == :dynamic_graph_mutated)) == 1
  end

  test "deletes persisted threads and their entries" do
    assert {:ok, %{rev: 1}} =
             @storage_adapter.append_thread(@thread_id, [entry(:attempt_scheduled)], repo: Repo)

    assert :ok = @storage_adapter.delete_thread(@thread_id, repo: Repo)

    assert :not_found = @storage_adapter.load_thread(@thread_id, repo: Repo)
    refute Repo.exists?(from(entry in JournalEntry, where: entry.thread_id == ^@thread_id))
  end

  test "returns an error for corrupted persisted entry payloads" do
    now = DateTime.utc_now(:microsecond)

    insert_thread!(rev: 1, now: now)

    Repo.insert_all(JournalEntry, [
      %{
        id: Ecto.UUID.generate(),
        thread_id: @thread_id,
        seq: 0,
        entry: "not an external term",
        inserted_at: now,
        updated_at: now
      }
    ])

    assert {:error, {:invalid_journal_entry, 0, _reason}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "fails closed when a persisted entry would create a new atom" do
    now = DateTime.utc_now(:microsecond)
    insert_thread!(rev: 1, now: now)

    unknown_atom_name = unknown_atom_name()

    entry =
      :attempt_scheduled
      |> entry(%{run_id: @run_id})
      |> Map.put(:refs, %{squidie_thread: {:dispatch, "default"}})

    encoded_entry =
      entry
      |> :erlang.term_to_binary()
      |> String.replace("squidie_thread", unknown_atom_name)

    Repo.insert_all(JournalEntry, [
      %{
        id: Ecto.UUID.generate(),
        thread_id: @thread_id,
        seq: 0,
        entry: encoded_entry,
        inserted_at: now,
        updated_at: now
      }
    ])

    assert {:error, {:invalid_journal_entry, 0, _reason}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_atom_name) end
  end

  test "fails closed when a versioned persisted entry would create a new atom" do
    now = DateTime.utc_now(:microsecond)
    insert_thread!(rev: 1, now: now)

    unknown_atom_name = unknown_atom_name()

    Repo.insert_all(JournalEntry, [
      %{
        id: Ecto.UUID.generate(),
        thread_id: @thread_id,
        seq: 0,
        entry: versioned_binary({:atom, unknown_atom_name}),
        inserted_at: now,
        updated_at: now
      }
    ])

    assert {:error, {:invalid_journal_entry, 0, {:unknown_atom, ^unknown_atom_name}}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_atom_name) end
  end

  test "fails closed when thread rev diverges from persisted entries" do
    assert {:ok, %{rev: 1}} =
             @storage_adapter.append_thread(@thread_id, [entry(:attempt_scheduled)], repo: Repo)

    Repo.update_all(
      from(thread in JournalThread, where: thread.id == ^@thread_id),
      set: [rev: 2]
    )

    assert {:error, {:invalid_journal_thread, @thread_id, {:rev_mismatch, 2, 1}}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "fails closed when persisted entry sequences are not contiguous" do
    assert {:ok, %{rev: 3}} =
             @storage_adapter.append_thread(
               @thread_id,
               [entry(:attempt_scheduled), entry(:attempt_claimed), entry(:attempt_completed)],
               repo: Repo
             )

    Repo.delete_all(
      from(entry in JournalEntry, where: entry.thread_id == ^@thread_id and entry.seq == 1),
      []
    )

    Repo.update_all(
      from(thread in JournalThread, where: thread.id == ^@thread_id),
      set: [rev: 2]
    )

    assert {:error, {:invalid_journal_thread, @thread_id, {:seq_gap, [0, 2]}}} =
             @storage_adapter.load_thread(@thread_id, repo: Repo)
  end

  test "round-trips checkpoints by arbitrary key term" do
    key = {"squidie", :checkpoint, @thread_id}
    checkpoint = %{thread_rev: 1, status: :running}

    assert :not_found = @storage_adapter.get_checkpoint(key, repo: Repo)
    assert :ok = @storage_adapter.put_checkpoint(key, checkpoint, repo: Repo)
    assert {:ok, ^checkpoint} = @storage_adapter.get_checkpoint(key, repo: Repo)

    updated_checkpoint = %{checkpoint | status: :completed}
    assert :ok = @storage_adapter.put_checkpoint(key, updated_checkpoint, repo: Repo)
    assert {:ok, ^updated_checkpoint} = @storage_adapter.get_checkpoint(key, repo: Repo)

    assert :ok = @storage_adapter.delete_checkpoint(key, repo: Repo)
    assert :not_found = @storage_adapter.get_checkpoint(key, repo: Repo)
  end

  test "fails closed when checkpoint payload is not a versioned journal term" do
    key = {"squidie", :checkpoint, @thread_id}

    assert :ok = @storage_adapter.put_checkpoint(key, %{thread_rev: 1}, repo: Repo)

    Repo.update_all(
      from(checkpoint in JournalCheckpoint),
      set: [checkpoint: :erlang.term_to_binary(%{unexpected: :shape})]
    )

    assert {:error, :invalid_encoded_term} = @storage_adapter.get_checkpoint(key, repo: Repo)
  end

  test "fails closed when a versioned checkpoint contains an unknown atom" do
    key = {"squidie", :checkpoint, @thread_id}
    unknown_atom_name = unknown_atom_name()

    assert :ok = @storage_adapter.put_checkpoint(key, %{thread_rev: 1}, repo: Repo)

    Repo.update_all(
      from(checkpoint in JournalCheckpoint),
      set: [checkpoint: versioned_binary({:atom, unknown_atom_name})]
    )

    assert {:error, {:unknown_atom, ^unknown_atom_name}} =
             @storage_adapter.get_checkpoint(key, repo: Repo)

    assert_raise ArgumentError, fn -> String.to_existing_atom(unknown_atom_name) end
  end

  test "integrates with the Squidie journal boundary" do
    assert {:ok, %Storage{adapter: @storage_adapter, opts: [repo: Repo]}} =
             Storage.normalize(@storage)

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, [^scheduled_entry]} = Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "requires a repo option at the Squidie storage boundary" do
    assert {:error, {:invalid_option, {:journal_storage, @storage_adapter}}} =
             Storage.normalize(@storage_adapter)

    assert {:error, {:invalid_option, {:journal_storage, @storage_adapter}}} =
             Storage.normalize({@storage_adapter, []})

    assert {:error, {:invalid_option, {:journal_storage, @storage_adapter}}} =
             Storage.normalize({@storage_adapter, repo: String})
  end

  defp entry(kind, payload \\ %{}) do
    %Entry{
      id: nil,
      seq: 0,
      at: 0,
      kind: kind,
      payload: payload,
      refs: %{}
    }
  end

  defp scheduled_attrs do
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
    }
  end

  defp mutation(suffix) do
    %{
      mutation_id: "ecto-mutation-#{suffix}",
      expected_version: 0,
      origin: "origin",
      additions: [
        %{
          kind: :node,
          id: "node-#{suffix}",
          action: "added",
          input: %{},
          queue: "dynamic-#{suffix}"
        },
        %{
          kind: :edge,
          id: "origin-#{suffix}",
          from: "origin",
          to: "node-#{suffix}"
        }
      ],
      removals: []
    }
  end

  defp graph_mutation_options(storage, now) do
    [
      runtime: :journal,
      journal_storage: storage,
      queue: "default",
      now: now,
      limits: %{
        max_nodes_per_mutation: 10,
        max_edges_per_mutation: 10,
        max_active_nodes_per_run: 10,
        max_active_edges_per_run: 10
      },
      action_registry: %{"added" => MutationAddedAction}
    ]
  end

  defp insert_thread!(opts) do
    now = Keyword.fetch!(opts, :now)

    Repo.insert_all(JournalThread, [
      %{
        id: @thread_id,
        rev: Keyword.fetch!(opts, :rev),
        metadata: %{},
        created_at_ms: System.system_time(:millisecond),
        updated_at_ms: System.system_time(:millisecond),
        inserted_at: now,
        updated_at: now
      }
    ])
  end

  defp unknown_atom_name do
    unique = Integer.to_string(System.unique_integer([:positive]), 36)

    "zz_" <> String.pad_trailing(unique, 14, "x")
  end

  defp versioned_binary(encoded_value) do
    :erlang.term_to_binary({:squidie_ecto_term_v1, encoded_value})
  end
end
