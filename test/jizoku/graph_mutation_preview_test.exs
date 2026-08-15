defmodule Jizoku.GraphMutationPreviewTest do
  use ExUnit.Case, async: false

  alias Jizoku.GraphMutation
  alias Jizoku.GraphMutation.Preview
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal

  defmodule OriginAction do
    use Jizoku.Step, name: :origin

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{origin: true}}
    end
  end

  defmodule HoldAction do
    use Jizoku.Step, name: :hold

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{hold: true}}
    end
  end

  defmodule AddedAction do
    use Jizoku.Step,
      name: :added,
      input_schema: [account_id: [type: :string, required: true]]

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{added: true}}
    end

    @spec persisted_action_opts(keyword()) :: keyword()
    def persisted_action_opts(opts) do
      Keyword.take(opts, [:policy])
    end
  end

  defmodule Workflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :origin, OriginAction
      step :hold, HoldAction

      transition :origin, on: :ok, to: :hold
      transition :hold, on: :ok, to: :complete
    end
  end

  @storage {Jido.Storage.ETS, table: :jizoku_graph_mutation_preview_test}
  @run_id "018f6f08-95c8-7ce2-a9d1-77a20ea8f001"
  @queue "default"
  @now ~U[2026-07-17 12:00:00Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)

    assert {:ok, _snapshot} =
             Jizoku.start(Workflow, %{},
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               run_id: @run_id,
               now: @now
             )

    assert {:ok, _result} =
             Jizoku.execute_next(
               runtime: :journal,
               journal_storage: @storage,
               queue: @queue,
               owner_id: "preview-test",
               now: @now
             )

    :ok
  end

  test "previews a graph mutation without changing run or dispatch threads" do
    run_before = load_thread!({:run, @run_id})
    dispatch_before = load_thread!({:dispatch, @queue})

    assert {:ok, %Preview{} = preview} =
             Jizoku.preview_graph_mutation(@run_id, mutation(), preview_options())

    assert preview.mutation_id == "mutation-preview"
    assert preview.expected_version == 0
    assert preview.base_version == 0
    assert preview.result_version == 1
    assert preview.status == :applicable
    refute preview.duplicate?
    assert preview.active_node_ids == ["child"]
    assert preview.ready_node_ids == ["child"]
    assert preview.blocked_node_ids == []
    assert preview.tombstoned_node_ids == []

    assert preview.applied_operations == [
             %{
               from: "origin",
               id: "origin-child",
               kind: :edge,
               operation: :add,
               to: "child"
             },
             %{action: "added", id: "child", kind: :node, operation: :add, queue: nil}
           ]

    assert load_thread!({:run, @run_id}) == run_before
    assert load_thread!({:dispatch, @queue}) == dispatch_before
  end

  test "returns exact duplicates without exposing private materialization data" do
    secret = "credential-value"
    options = preview_options(action_opts: [policy: "strict", credential: secret])

    assert {:ok, %Preview{} = first} =
             Jizoku.preview_graph_mutation(@run_id, mutation(), options)

    append_committed_mutation()
    run_before = load_thread!({:run, @run_id})
    duplicate_options = Keyword.put(options, :action_registry, %{})

    assert {:ok, %Preview{} = duplicate} =
             Jizoku.preview_graph_mutation(@run_id, mutation(), duplicate_options)

    assert duplicate.status == :duplicate
    assert duplicate.duplicate?
    assert duplicate.base_version == 1
    assert duplicate.result_version == 1
    assert duplicate.active_node_ids == ["child"]
    assert load_thread!({:run, @run_id}) == run_before
    refute inspect(first) =~ secret
    refute inspect(first) =~ inspect(AddedAction)
    refute inspect(first) =~ "account-123"
    refute inspect(first) =~ "trace"
    refute inspect(duplicate) =~ "account-123"
  end

  test "rejects unsupported options and redacts unsupported metadata values" do
    assert Jizoku.preview_graph_mutation(@run_id, mutation(), unknown: "secret") ==
             {:error, {:invalid_option, {:option, :unknown}}}

    invalid = Map.put(mutation(), :metadata, %{credential: "secret"})

    assert Jizoku.preview_graph_mutation(@run_id, invalid, preview_options()) ==
             {:error, {:invalid_graph_mutation, {:metadata, :unsupported}}}
  end

  defp mutation do
    %{
      mutation_id: "mutation-preview",
      expected_version: 0,
      origin: "origin",
      additions: [
        %{
          kind: :node,
          id: "child",
          action: "added",
          input: %{account_id: "account-123"}
        },
        %{kind: :edge, id: "origin-child", from: "origin", to: "child"}
      ],
      removals: []
    }
  end

  defp preview_options(overrides \\ []) do
    action_opts = Keyword.get(overrides, :action_opts, [])

    [
      runtime: :journal,
      journal_storage: @storage,
      queue: @queue,
      now: @now,
      limits: %{
        max_nodes_per_mutation: 10,
        max_edges_per_mutation: 10,
        max_active_nodes_per_run: 10,
        max_active_edges_per_run: 10
      },
      action_registry: %{
        "added" => %{module: AddedAction, enabled?: true, action_opts: action_opts}
      }
    ]
  end

  defp load_thread!(thread_id) do
    {:ok, thread} = Journal.load_thread(@storage, thread_id)
    thread
  end

  defp append_committed_mutation do
    {:ok, normalized} = GraphMutation.normalize(mutation())

    attrs =
      normalized
      |> GraphMutation.to_map()
      |> Map.merge(%{
        run_id: @run_id,
        result_version: 1,
        runnable_intent_fingerprints: %{"child" => "intent-fingerprint"},
        occurred_at: @now
      })

    {:ok, entry} = DispatchProtocol.new_entry(:dynamic_graph_mutated, attrs)
    {:ok, _thread} = Journal.append_entries(@storage, [entry])
  end

  defp cleanup_storage do
    delete_table(:jizoku_graph_mutation_preview_test_checkpoints)
    delete_table(:jizoku_graph_mutation_preview_test_threads)
    delete_table(:jizoku_graph_mutation_preview_test_thread_meta)
  end

  defp delete_table(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
