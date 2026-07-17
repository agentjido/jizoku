defmodule Squidie.Runtime.DependencyOrderedDispatchTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Workflow.Definition

  defmodule DynamicAction do
    use Squidie.Step, name: :dynamic_action

    @impl Squidie.Step
    def run(input, _context) do
      {:ok, %{node: Map.get(input, :node)}}
    end
  end

  defmodule OriginAction do
    use Squidie.Step, name: :origin

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{origin: true}}
    end
  end

  defmodule Workflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :origin, OriginAction
      transition :origin, on: :ok, to: :complete
    end
  end

  @storage {Jido.Storage.ETS, table: :squidie_dependency_ordered_dispatch_test}
  @run_id "0190a4f1-0a7c-7cb1-80c5-b4f8b1d23001"
  @queue "dynamic"
  @now ~U[2026-07-17 14:00:00Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "selects only active ready dependency nodes and preserves legacy eager work" do
    append_run_entries(selector_entries())

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, @queue)

    assert ["left", "legacy", "right"] ==
             workflow_agent
             |> WorkflowAgent.pending_dispatches(dispatch_agent)
             |> Enum.map(& &1.step)
             |> Enum.sort()

    terminal_steps =
      workflow_agent
      |> WorkflowAgent.planned_runnables()
      |> Enum.filter(&Projection.terminal_runnable?(workflow_agent.state.projection, &1))
      |> Enum.map(& &1.step)

    assert "join" in terminal_steps
    assert "legacy" in terminal_steps
    refute "removed" in terminal_steps

    assert :ok = WorkflowAgent.put_checkpoint(@storage, workflow_agent, updated_at: @now)
    assert {:ok, checkpoint_agent} = WorkflowAgent.rebuild(@storage, @run_id)

    assert WorkflowAgent.pending_dispatches(checkpoint_agent, dispatch_agent) ==
             WorkflowAgent.pending_dispatches(workflow_agent, dispatch_agent)
  end

  test "schedules fan-in exactly once after the final predecessor is durably applied" do
    append_run_entries(fan_in_entries())

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, @queue)

    assert {:ok, %{runnables: initial}} =
             WorkflowAgent.schedule_pending_dispatches(
               @storage,
               workflow_agent,
               dispatch_agent,
               now: @now
             )

    assert Enum.map(initial, & &1.step) == ["left", "right"]
    assert scheduled_steps() == ["left", "right"]

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, @queue)

    assert {:ok, %{runnables: []}} =
             WorkflowAgent.schedule_pending_dispatches(
               @storage,
               workflow_agent,
               dispatch_agent,
               now: @now
             )

    assert {:ok, _snapshot} = execute_next("left-owner")
    assert scheduled_steps() == ["left", "right"]

    assert {:ok, _snapshot} = execute_next("right-owner")
    assert scheduled_steps() == ["join", "left", "right"]
    assert Enum.count(scheduled_steps(), &(&1 == "join")) == 1

    assert {:ok, _snapshot} = execute_next("join-owner")
    assert {:ok, terminal_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert Projection.terminal_status(terminal_agent.state.projection) == :completed
  end

  test "terminal cancellation suppresses ready dependency and retry dispatches" do
    terminal =
      entry!(:run_terminal, %{
        run_id: @run_id,
        status: :cancelled,
        occurred_at: @now
      })

    entries = Enum.reverse([terminal | Enum.reverse(selector_entries())])

    append_run_entries(entries)

    assert {:ok, workflow_agent} = WorkflowAgent.rebuild(@storage, @run_id)
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, @queue)
    assert WorkflowAgent.pending_dispatches(workflow_agent, dispatch_agent) == []
  end

  defp selector_entries do
    additions =
      fan_in_additions() ++
        [
          node_operation("removed"),
          node_operation("applied-dynamic"),
          edge("origin-removed", "origin", "removed"),
          edge("origin-applied", "origin", "applied-dynamic")
        ]

    mutation = mutation_entry("mutation-selector", 0, 1, additions, [])

    removal =
      mutation_entry(
        "mutation-remove",
        1,
        2,
        [],
        [%{kind: :edge, id: "origin-removed"}, %{kind: :node, id: "removed"}]
      )

    runnables = [
      dynamic_runnable("left", 2),
      dynamic_runnable("right"),
      dynamic_runnable("join"),
      dynamic_runnable("removed"),
      dynamic_runnable("applied-dynamic"),
      dynamic_runnable("legacy")
    ]

    base_entries() ++
      [
        mutation,
        removal,
        legacy_entry(),
        runnables_planned_entry(runnables),
        entry!(:runnable_applied, %{
          run_id: @run_id,
          runnable_key: "#{@run_id}:applied-dynamic:1",
          result: %{done: true},
          occurred_at: @now
        })
      ]
  end

  defp fan_in_entries do
    base_entries() ++
      [
        mutation_entry("mutation-fan-in", 0, 1, fan_in_additions(), []),
        runnables_planned_entry([
          dynamic_runnable("left"),
          dynamic_runnable("right"),
          dynamic_runnable("join")
        ])
      ]
  end

  defp base_entries do
    {:ok, definition} = Definition.load(Workflow)

    [
      entry!(:run_started, %{
        run_id: @run_id,
        workflow: Atom.to_string(Workflow),
        definition_version: definition.definition_version,
        definition_fingerprint: Definition.fingerprint(definition),
        occurred_at: @now
      }),
      runnables_planned_entry([origin_runnable()]),
      entry!(:runnable_applied, %{
        run_id: @run_id,
        runnable_key: "#{@run_id}:origin:1",
        result: %{origin: true},
        occurred_at: @now
      })
    ]
  end

  defp fan_in_additions do
    [
      node_operation("left"),
      node_operation("right"),
      node_operation("join"),
      edge("origin-left", "origin", "left"),
      edge("origin-right", "origin", "right"),
      edge("left-join", "left", "join"),
      edge("right-join", "right", "join")
    ]
  end

  defp mutation_entry(mutation_id, expected_version, result_version, additions, removals) do
    fingerprints =
      additions
      |> Enum.filter(&(&1.kind == :node))
      |> Map.new(&{&1.id, "intent-#{&1.id}"})

    entry!(:dynamic_graph_mutated, %{
      run_id: @run_id,
      mutation_id: mutation_id,
      expected_version: expected_version,
      result_version: result_version,
      origin: "origin",
      additions: additions,
      removals: removals,
      runnable_intent_fingerprints: fingerprints,
      occurred_at: @now
    })
  end

  defp legacy_entry do
    entry!(:dynamic_work_recorded, %{
      run_id: @run_id,
      dynamic_key: "legacy-work",
      origin: %{runnable_key: "#{@run_id}:origin:1", step: "origin", attempt: 1},
      nodes: [%{id: "legacy"}],
      occurred_at: @now
    })
  end

  defp runnables_planned_entry(runnables) do
    entry!(:runnables_planned, %{run_id: @run_id, runnables: runnables, occurred_at: @now})
  end

  defp origin_runnable do
    %{
      run_id: @run_id,
      runnable_key: "#{@run_id}:origin:1",
      idempotency_key: "#{@run_id}:origin:1",
      attempt_number: 1,
      queue: @queue,
      step: "origin",
      input: %{},
      visible_at: @now
    }
  end

  defp dynamic_runnable(node_id, attempt_number \\ 1) do
    runnable_key = "#{@run_id}:#{node_id}:#{attempt_number}"

    %{
      run_id: @run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: attempt_number,
      queue: @queue,
      step: node_id,
      input: %{node: node_id},
      visible_at: @now,
      recovery: %{
        irreversible?: true,
        compensatable?: false,
        replay: :manual_review_required,
        recovery: :manual_intervention,
        dynamic?: true,
        action: "dynamic"
      },
      dynamic?: true,
      dynamic_work: %{action: "dynamic", module: DynamicAction, action_opts: []},
      graph_mutation: %{
        mutation_id: "mutation-fan-in",
        node_id: node_id,
        intent_fingerprint: "intent-#{node_id}"
      }
    }
  end

  defp node_operation(id) do
    %{kind: :node, id: id, action: "dynamic", input: %{node: id}, queue: @queue}
  end

  defp edge(id, from, to) do
    %{kind: :edge, id: id, from: from, to: to}
  end

  defp execute_next(owner_id) do
    Squidie.execute_next(
      runtime: :journal,
      journal_storage: @storage,
      queue: @queue,
      owner_id: owner_id,
      now: @now
    )
  end

  defp scheduled_steps do
    {:ok, entries} = Journal.load_entries(@storage, {:dispatch, @queue})

    entries
    |> Enum.filter(&(&1.type == :attempt_scheduled))
    |> Enum.map(& &1.data.step)
    |> Enum.sort()
  end

  defp append_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp cleanup_storage do
    delete_table(:squidie_dependency_ordered_dispatch_test_checkpoints)
    delete_table(:squidie_dependency_ordered_dispatch_test_threads)
    delete_table(:squidie_dependency_ordered_dispatch_test_thread_meta)
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
