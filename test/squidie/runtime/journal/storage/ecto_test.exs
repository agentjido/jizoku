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
