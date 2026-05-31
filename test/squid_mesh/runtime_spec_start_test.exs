defmodule SquidMesh.RuntimeSpecStartTest do
  use ExUnit.Case, async: false

  defmodule LoadInvoice do
    use SquidMesh.Step, name: :load_invoice

    @impl SquidMesh.Step
    def run(input, _context), do: {:ok, %{id: input.invoice_id}}
  end

  defmodule SendReminder do
    use SquidMesh.Step, name: :send_reminder

    @impl SquidMesh.Step
    def run(input, _context), do: {:ok, %{sent_invoice_id: input.invoice.id}}
  end

  @storage {Jido.Storage.ETS, table: :squid_mesh_runtime_spec_start_test}
  @run_id "00000000-0000-4000-8000-000000000254"
  @missing_action_run_id "00000000-0000-4000-8000-000000000255"

  test "starts and executes a validated runtime-authored spec" do
    registry = action_registry()
    spec = runtime_invoice_spec()

    assert :ok = SquidMesh.Workflow.validate_spec(spec, action_registry: registry)

    assert {:ok, snapshot} =
             SquidMesh.start_spec(spec, :manual, %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: @run_id
             )

    assert snapshot.run_id == @run_id
    assert snapshot.workflow == Atom.to_string(__MODULE__.RuntimeInvoiceReminder)
    assert snapshot.trigger == "manual"
    assert snapshot.definition_version == "runtime-spec-v1"

    assert {:ok, first} =
             SquidMesh.execute_next(journal_storage: @storage, owner_id: "worker")

    assert first.status in [:running, :waiting_for_dispatch]
    assert "send_reminder" in Enum.map(first.planned_runnables, &Map.fetch!(&1, :step))

    assert {:ok, completed} =
             SquidMesh.execute_next(journal_storage: @storage, owner_id: "worker")

    assert completed.status == :completed
    assert completed.terminal?

    assert {:ok, inspected} = SquidMesh.inspect_run(@run_id, journal_storage: @storage)
    assert inspected.status == :completed

    assert {:ok, graph} =
             SquidMesh.inspect_run_graph(@run_id,
               journal_storage: @storage,
               include_history: true
             )

    assert Enum.map(graph.nodes, & &1.id) == ["load_invoice", "send_reminder"]

    assert Enum.any?(
             graph.edges,
             &match?(%{from: "load_invoice", to: "send_reminder", type: :transition}, &1)
           )
  end

  test "starts specs whose top-level fields were decoded with string keys" do
    registry = action_registry()
    run_id = "00000000-0000-4000-8000-000000000257"

    spec = Map.new(runtime_invoice_spec(), fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:ok, snapshot} =
             SquidMesh.start_spec(spec, :manual, %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: run_id
             )

    assert snapshot.run_id == run_id
    assert snapshot.workflow == Atom.to_string(__MODULE__.RuntimeInvoiceReminder)
    assert [%{step: "load_invoice", status: :available}] = snapshot.visible_attempts
  end

  test "rejects unknown action keys before creating a run" do
    assert {:error, {:invalid_workflow_spec, errors}} =
             SquidMesh.start_spec(runtime_invoice_spec(), %{invoice_id: "inv_123"},
               action_registry: %{},
               journal_storage: @storage,
               run_id: @missing_action_run_id
             )

    assert Enum.any?(errors, &(&1.code == :unknown_action_key))

    assert {:error, :not_found} =
             SquidMesh.inspect_run(@missing_action_run_id, journal_storage: @storage)
  end

  test "rejects invalid named triggers without raising" do
    assert {:error, {:invalid_trigger, "manual"}} =
             SquidMesh.start_spec(runtime_invoice_spec(), "manual", %{invoice_id: "inv_123"},
               action_registry: action_registry(),
               journal_storage: @storage
             )
  end

  test "rejects runtime-authored spec replays explicitly" do
    registry = action_registry()
    run_id = "00000000-0000-4000-8000-000000000256"

    assert {:ok, _snapshot} =
             SquidMesh.start_spec(runtime_invoice_spec(), %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: run_id
             )

    assert {:error, {:invalid_replay_source, :runtime_spec}} =
             SquidMesh.replay(run_id, journal_storage: @storage)
  end

  defp action_registry do
    %{
      "billing.load_invoice" => LoadInvoice,
      "billing.send_reminder" => SendReminder
    }
  end

  defp runtime_invoice_spec do
    %{
      workflow: __MODULE__.RuntimeInvoiceReminder,
      definition_version: "runtime-spec-v1",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :invoice_id, type: :string, opts: []}]
        }
      ],
      payload: [%{name: :invoice_id, type: :string, opts: []}],
      steps: [
        %{name: :load_invoice, action: "billing.load_invoice", opts: [output: :invoice]},
        %{name: :send_reminder, action: "billing.send_reminder", opts: [input: [:invoice]]}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :send_reminder},
        %{from: :send_reminder, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }
  end
end
