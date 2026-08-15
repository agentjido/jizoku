defmodule Jizoku.ReadModel.ListingTest do
  use ExUnit.Case, async: false

  alias Jizoku.ReadModel.Listing
  alias Jizoku.ReadModel.Listing.Summary
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal

  @storage {Jido.Storage.ETS, table: :jizoku_read_model_listing_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @queue "default"
  @runnable_key "run_123:charge_card:1"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @overdue_at ~U[2026-05-15 00:00:40Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "exposes the most urgent active deadline in run summaries" do
    deadline = deadline(started_at: @visible_at, within: 20_000, due_soon: 10_000)

    append_entries([
      run_cataloged(),
      run_started(),
      runnables_planned([planned_runnable(deadline: deadline)])
    ])

    assert {:ok, [%Summary{} = summary]} = Listing.list(@storage, [], now: @overdue_at)

    assert summary.run_id == @run_id
    assert summary.deadline.status == :overdue
    assert summary.deadline.step == "charge_card"
    assert summary.deadline.runnable_key == @runnable_key
    refute Map.has_key?(summary.deadline, :policy)
  end

  test "summarizes only the latest active attempt deadline per step" do
    old_deadline = deadline(started_at: @visible_at, within: 1_000, due_soon: 500)
    retry_key = "run_123:charge_card:2"
    retry_deadline = deadline(started_at: @overdue_at, within: 60_000, due_soon: 10_000)

    append_entries([
      run_cataloged(),
      run_started(),
      runnables_planned([
        planned_runnable(deadline: old_deadline),
        planned_runnable(
          runnable_key: retry_key,
          idempotency_key: retry_key,
          attempt_number: 2,
          deadline: retry_deadline
        )
      ])
    ])

    assert {:ok, [%Summary{} = summary]} = Listing.list(@storage, [], now: @overdue_at)

    assert summary.deadline.status == :on_time
    assert summary.deadline.runnable_key == retry_key
  end

  test "does not revive failed retry deadlines after a later attempt applied" do
    failed_deadline = deadline(started_at: @visible_at, within: 1_000, due_soon: 500)
    retry_key = "run_123:charge_card:2"
    retry_deadline = deadline(started_at: @visible_at, within: 60_000, due_soon: 10_000)
    successor_key = "run_123:notify_customer:1"
    successor_deadline = deadline(started_at: @overdue_at, within: 60_000, due_soon: 10_000)

    append_entries([
      run_cataloged(),
      run_started(),
      runnables_planned([
        planned_runnable(deadline: failed_deadline),
        planned_runnable(
          runnable_key: retry_key,
          idempotency_key: retry_key,
          attempt_number: 2,
          deadline: retry_deadline
        ),
        planned_runnable(
          runnable_key: successor_key,
          idempotency_key: successor_key,
          step: "notify_customer",
          deadline: successor_deadline
        )
      ]),
      runnable_applied(runnable_key: retry_key)
    ])

    assert {:ok, [%Summary{} = summary]} = Listing.list(@storage, [], now: @overdue_at)

    assert summary.deadline.status == :on_time
    assert summary.deadline.runnable_key == successor_key
  end

  test "does not expose escalation targets in list summaries" do
    deadline =
      deadline(
        started_at: @visible_at,
        within: 20_000,
        due_soon: 10_000,
        escalation: %{outcome: :host_callback, target: "internal-callback"}
      )

    append_entries([
      run_cataloged(),
      run_started(),
      runnables_planned([planned_runnable(deadline: deadline)])
    ])

    assert {:ok, [%Summary{} = summary]} = Listing.list(@storage, [], now: @overdue_at)

    assert summary.deadline.escalation == %{outcome: :host_callback}
  end

  test "suppresses deadline summaries for terminal runs" do
    deadline = deadline(started_at: @visible_at, within: 20_000, due_soon: 10_000)

    append_entries([
      run_cataloged(),
      run_started(),
      runnables_planned([planned_runnable(deadline: deadline)]),
      run_terminal(:cancelled)
    ])

    assert {:ok, [%Summary{} = summary]} = Listing.list(@storage, [], now: @overdue_at)

    assert summary.status == :cancelled
    assert summary.deadline == nil
  end

  defp append_entries(entries) do
    Enum.each(entries, fn entry ->
      assert {:ok, _thread} = Journal.append_entries(@storage, [entry])
    end)
  end

  defp run_cataloged do
    entry!(:run_cataloged, %{
      run_id: @run_id,
      workflow: @workflow,
      queue: @queue,
      occurred_at: @started_at
    })
  end

  defp run_started do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      occurred_at: @started_at
    })
  end

  defp runnables_planned(runnables) do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: runnables,
      occurred_at: @visible_at
    })
  end

  defp run_terminal(status) do
    entry!(:run_terminal, %{
      run_id: @run_id,
      status: status,
      occurred_at: @overdue_at
    })
  end

  defp runnable_applied(overrides) do
    entry!(:runnable_applied, %{
      run_id: @run_id,
      runnable_key: Keyword.fetch!(overrides, :runnable_key),
      result: %{"ok" => true},
      occurred_at: @overdue_at
    })
  end

  defp planned_runnable(overrides) do
    base = %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      idempotency_key: @runnable_key,
      attempt_number: 1,
      queue: @queue,
      step: "charge_card",
      input: %{"payment_id" => "pay_123"},
      visible_at: @visible_at
    }

    Map.merge(base, Map.new(overrides))
  end

  defp deadline(opts) do
    started_at = Keyword.fetch!(opts, :started_at)
    within = Keyword.fetch!(opts, :within)
    due_at = DateTime.add(started_at, within, :millisecond)
    due_soon = Keyword.get(opts, :due_soon)
    escalation = Keyword.get(opts, :escalation, :diagnostic)

    %{
      policy: %{within: within, due_soon: due_soon, escalation: escalation},
      started_at: started_at,
      due_at: due_at,
      due_soon_at: if(is_integer(due_soon), do: DateTime.add(due_at, -due_soon, :millisecond))
    }
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp table_name(:checkpoints), do: :jizoku_read_model_listing_test_checkpoints
  defp table_name(:threads), do: :jizoku_read_model_listing_test_threads
  defp table_name(:thread_meta), do: :jizoku_read_model_listing_test_thread_meta

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(table_name(suffix))
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
