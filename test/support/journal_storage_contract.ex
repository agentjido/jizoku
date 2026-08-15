defmodule Jizoku.JournalStorageContract do
  @moduledoc false

  import ExUnit.Assertions

  alias Jido.Thread.Entry

  @contract_tests [
    {"reports missing threads and checkpoints", :missing},
    {"atomically appends ordered batches with contiguous revisions", :ordered_batch},
    {"rejects stale revisions without changing the thread", :stale_revision},
    {"serializes concurrent appends at one expected revision", :concurrent_revision},
    {"round-trips replacement and deletion of durable checkpoints", :checkpoint_lifecycle},
    {"deleting one thread is idempotent and preserves sibling state", :thread_deletion},
    {"rejects unsupported checkpoint keys without changing state", :unsupported_checkpoint_keys},
    {"rejects unsupported checkpoint and entry terms before writing", :unsupported_terms},
    {"rejects non-JSON thread metadata before writing", :unsupported_metadata}
  ]

  defmacro __using__(_opts) do
    tests =
      Enum.map(@contract_tests, fn {description, contract_case} ->
        quote do
          test unquote(description), context do
            Jizoku.JournalStorageContract.run(
              unquote(contract_case),
              contract_storage(context),
              &contract_run_task/1
            )
          end
        end
      end)

    quote do
      describe "Jido.Storage contract" do
        (unquote_splicing(tests))
      end
    end
  end

  @doc false
  @spec run(atom(), {module(), keyword()}, ((-> term()) -> term())) :: :ok
  def run(:missing, {adapter, opts}, _allow_task) do
    thread_id = contract_id("missing-thread")
    checkpoint_key = {:contract, contract_id("missing-checkpoint")}

    assert :not_found = adapter.load_thread(thread_id, opts)
    assert :not_found = adapter.get_checkpoint(checkpoint_key, opts)
    :ok
  end

  def run(:ordered_batch, {adapter, opts}, _allow_task) do
    thread_id = contract_id("ordered-batch")
    metadata = %{nested: %{state: :ready}, scope: :contract, version: 1}

    normalized_metadata = %{
      "nested" => %{"state" => "ready"},
      "scope" => "contract",
      "version" => 1
    }

    entries = [
      contract_entry("entry-1", :first, %{value: 1}),
      contract_entry("entry-2", :second, %{value: 2}),
      contract_entry("entry-3", :third, %{value: 3})
    ]

    assert {:ok, thread} =
             adapter.append_thread(
               thread_id,
               entries,
               Keyword.merge(opts, expected_rev: 0, metadata: metadata)
             )

    assert thread.id == thread_id
    assert thread.rev == 3
    assert thread.metadata == normalized_metadata
    assert Enum.map(thread.entries, & &1.seq) == [0, 1, 2]
    assert Enum.map(thread.entries, & &1.id) == ["entry-1", "entry-2", "entry-3"]
    assert Enum.map(thread.entries, & &1.kind) == [:first, :second, :third]
    assert Enum.map(thread.entries, & &1.at) == List.duplicate(1_725_000_000_000, 3)

    assert Enum.map(thread.entries, & &1.refs) ==
             List.duplicate(%{contract: "journal-storage"}, 3)

    assert Enum.map(thread.entries, & &1.payload) == [
             %{value: 1},
             %{value: 2},
             %{value: 3}
           ]

    assert {:ok, reloaded} = adapter.load_thread(thread_id, opts)
    assert reloaded == thread

    assert {:ok, appended} =
             adapter.append_thread(
               thread_id,
               [contract_entry("entry-4", :fourth, %{value: 4})],
               Keyword.put(opts, :expected_rev, 3)
             )

    assert appended.rev == 4
    assert appended.metadata == normalized_metadata
    assert Enum.take(appended.entries, 3) == thread.entries
    assert Enum.map(appended.entries, & &1.seq) == [0, 1, 2, 3]
    assert Enum.map(appended.entries, & &1.id) == ["entry-1", "entry-2", "entry-3", "entry-4"]
    assert [_first, _second, _third, fourth] = appended.entries
    assert fourth.kind == :fourth
    assert fourth.payload == %{value: 4}
    :ok
  end

  def run(:stale_revision, {adapter, opts}, _allow_task) do
    thread_id = contract_id("stale-revision")
    append_opts = Keyword.put(opts, :expected_rev, 0)

    assert {:ok, _thread} =
             adapter.append_thread(
               thread_id,
               [contract_entry("winner", :winner)],
               append_opts
             )

    assert {:ok, before_thread} = adapter.load_thread(thread_id, opts)

    assert {:error, :conflict} =
             adapter.append_thread(
               thread_id,
               [contract_entry("stale", :stale)],
               append_opts
             )

    assert {:ok, ^before_thread} = adapter.load_thread(thread_id, opts)
    :ok
  end

  def run(:concurrent_revision, {adapter, opts}, run_task) do
    thread_id = contract_id("concurrent-revision")
    parent = self()
    append_opts = Keyword.put(opts, :expected_rev, 0)

    tasks =
      for {id, kind} <- [{"left", :left}, {"right", :right}] do
        Task.async(fn ->
          run_concurrent_append(run_task, parent, adapter, thread_id, id, kind, append_opts)
        end)
      end

    task_pids = MapSet.new(tasks, & &1.pid)

    ready_pids =
      for _task <- tasks, into: MapSet.new() do
        assert_receive {:contract_task_ready, task_pid}, 5_000
        task_pid
      end

    assert ready_pids == task_pids
    Enum.each(tasks, &send(&1.pid, :append))

    results = Enum.map(tasks, &Task.await(&1, 5_000))

    assert Enum.count(results, &match?({:ok, %{rev: 1}}, &1)) == 1
    assert Enum.count(results, &match?({:error, :conflict}, &1)) == 1

    assert {:ok, %{rev: 1, entries: [stored_entry]}} = adapter.load_thread(thread_id, opts)

    assert {stored_entry.id, stored_entry.kind} in [
             {"left", :left},
             {"right", :right}
           ]

    :ok
  end

  def run(:checkpoint_lifecycle, {adapter, opts}, _allow_task) do
    key = {:contract, contract_id("checkpoint")}
    original = %{rev: 1, state: {:running, [1, 2.5, "three"]}}
    replacement = %{rev: 2, state: {:completed, [:ok]}}

    assert :ok = adapter.put_checkpoint(key, original, opts)
    assert {:ok, ^original} = adapter.get_checkpoint(key, opts)

    assert :ok = adapter.put_checkpoint(key, replacement, opts)
    assert {:ok, ^replacement} = adapter.get_checkpoint(key, opts)

    assert :ok = adapter.delete_checkpoint(key, opts)
    assert :not_found = adapter.get_checkpoint(key, opts)
    assert :ok = adapter.delete_checkpoint(key, opts)
    :ok
  end

  def run(:thread_deletion, {adapter, opts}, _allow_task) do
    deleted_id = contract_id("deleted-thread")
    sibling_id = contract_id("sibling-thread")
    checkpoint_key = {:contract, contract_id("preserved-checkpoint")}
    checkpoint = %{rev: 7, status: :running}

    assert {:ok, _thread} =
             adapter.append_thread(deleted_id, [contract_entry("deleted", :deleted)], opts)

    assert {:ok, _thread} =
             adapter.append_thread(sibling_id, [contract_entry("sibling", :sibling)], opts)

    assert :ok = adapter.put_checkpoint(checkpoint_key, checkpoint, opts)
    assert {:ok, sibling_before} = adapter.load_thread(sibling_id, opts)

    assert :ok = adapter.delete_thread(deleted_id, opts)
    assert :ok = adapter.delete_thread(deleted_id, opts)

    assert :not_found = adapter.load_thread(deleted_id, opts)
    assert {:ok, ^sibling_before} = adapter.load_thread(sibling_id, opts)
    assert {:ok, ^checkpoint} = adapter.get_checkpoint(checkpoint_key, opts)
    :ok
  end

  def run(:unsupported_checkpoint_keys, {adapter, opts}, _run_task) do
    safe_key = {:contract, contract_id("safe-checkpoint")}
    unsafe_key = self()
    checkpoint = %{status: :safe}

    assert :ok = adapter.put_checkpoint(safe_key, checkpoint, opts)

    assert {:error, {:unsupported_term, ^unsafe_key}} =
             adapter.get_checkpoint(unsafe_key, opts)

    assert {:error, {:unsupported_term, ^unsafe_key}} =
             adapter.put_checkpoint(unsafe_key, checkpoint, opts)

    assert {:error, {:unsupported_term, ^unsafe_key}} =
             adapter.delete_checkpoint(unsafe_key, opts)

    assert {:ok, ^checkpoint} = adapter.get_checkpoint(safe_key, opts)
    :ok
  end

  def run(:unsupported_terms, {adapter, opts}, _allow_task) do
    checkpoint_key = {:contract, contract_id("unsafe-checkpoint")}
    thread_id = contract_id("unsafe-thread")
    unsafe = self()
    safe_checkpoint = %{status: :safe}

    assert :ok = adapter.put_checkpoint(checkpoint_key, safe_checkpoint, opts)

    assert {:error, {:unsupported_term, ^unsafe}} =
             adapter.put_checkpoint(checkpoint_key, %{pid: unsafe}, opts)

    assert {:ok, ^safe_checkpoint} = adapter.get_checkpoint(checkpoint_key, opts)

    entries = [
      contract_entry("safe", :safe),
      contract_entry("unsafe", :unsafe, %{pid: unsafe})
    ]

    assert {:error, {:unsupported_term, ^unsafe}} =
             adapter.append_thread(thread_id, entries, opts)

    assert :not_found = adapter.load_thread(thread_id, opts)
    :ok
  end

  def run(:unsupported_metadata, {adapter, opts}, _run_task) do
    thread_id = contract_id("unsafe-metadata")
    unsafe = {:tuple, "not-json"}
    append_opts = Keyword.put(opts, :metadata, %{"unsafe" => unsafe})

    assert {:error, {:unsupported_term, ^unsafe}} =
             adapter.append_thread(
               thread_id,
               [contract_entry("safe", :safe)],
               append_opts
             )

    assert :not_found = adapter.load_thread(thread_id, opts)
    :ok
  end

  defp contract_id(label) do
    "#{label}-#{System.unique_integer([:positive, :monotonic])}"
  end

  defp run_concurrent_append(run_task, parent, adapter, thread_id, id, kind, append_opts) do
    run_task.(fn ->
      send(parent, {:contract_task_ready, self()})

      receive do
        :append ->
          adapter.append_thread(thread_id, [contract_entry(id, kind)], append_opts)
      end
    end)
  end

  defp contract_entry(id, kind, payload \\ %{}) do
    %Entry{
      id: id,
      seq: 0,
      at: 1_725_000_000_000,
      kind: kind,
      payload: payload,
      refs: %{contract: "journal-storage"}
    }
  end
end
