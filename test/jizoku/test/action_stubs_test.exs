defmodule Jizoku.Test.ActionStubsTest do
  use ExUnit.Case, async: true

  alias Jizoku.Test
  alias Jizoku.Test.Storage

  defmodule RuntimeStubWorkflow do
  end

  defmodule DenyGuardrail do
    @spec validate_guardrail(term(), term()) :: {:error, map()}
    def validate_guardrail(_value, _context) do
      {:error, %{message: "blocked by policy"}}
    end
  end

  defmodule ModuleStep do
    use Jizoku.Step, name: :module_step

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{}}
    end
  end

  defmodule RealAction do
    use Jizoku.Step, name: :real_action

    @impl Jizoku.Step
    def run(%{invoice_id: invoice_id}, _context) do
      {:ok, %{id: invoice_id}}
    end

    @spec persisted_action_opts(keyword()) :: keyword()
    def persisted_action_opts(_opts) do
      []
    end
  end

  defmodule ModuleWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :done, Jizoku.Test.ActionStubsTest.ModuleStep
      transition :done, on: :ok, to: :complete
    end
  end

  @now ~U[2026-08-11 12:00:00.000000Z]

  test "executes deterministic stub result sequences through a runtime-authored spec" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_stubs: %{
                 "billing.action" => [
                   {:ok, %{id: "inv_123"}},
                   {:ok, %{confirmed_id: "inv_123"}}
                 ]
               },
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{invoice_id: "inv_123"})
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.invoice == %{id: "inv_123"}
    assert completed.context.confirmation == %{confirmed_id: "inv_123"}

    assert {:ok, calls} = Test.stub_calls(runtime, "billing.action")

    assert [
             %{
               attempt: 1,
               input: %{invoice_id: "inv_123"},
               replayed?: false,
               run_id: run_id,
               step: :load_invoice
             },
             %{
               attempt: 1,
               input: %{invoice: %{id: "inv_123"}},
               replayed?: false,
               run_id: run_id,
               step: :confirm_invoice
             }
           ] = calls

    assert run_id == run.run_id
  end

  test "restart replays an assigned result after completion is interrupted" do
    test_pid = self()

    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_stubs: %{
                 "billing.action" => [
                   {:ok, %{id: "inv_456"}},
                   {:ok, %{confirmed_id: "inv_456"}}
                 ]
               },
               now: @now
             )

    assert {:ok, run} = Test.start(runtime, %{invoice_id: "inv_456"})

    runtime = %{
      runtime
      | test_action_stub_after_consume: fn call ->
          send(test_pid, {:stub_result_assigned, call.runnable_key})

          receive do
            :release_completion -> :ok
          end
        end
    }

    drain = Task.async(fn -> Test.drain(runtime, run) end)
    Process.unlink(drain.pid)
    drain_ref = Process.monitor(drain.pid)

    assert_receive {:stub_result_assigned, first_runnable_key}
    Process.exit(drain.pid, :kill)
    assert_receive {:DOWN, ^drain_ref, :process, _pid, :killed}

    assert {:ok, restarted} = Test.restart_runtime(runtime)
    restarted = %{restarted | test_action_stub_after_consume: nil}

    on_exit(fn -> Test.stop_runtime(restarted) end)

    assert {:ok, calls} = Test.stub_calls(restarted, "billing.action")
    assert [%{replayed?: false, runnable_key: ^first_runnable_key}] = calls

    assert {:ok, claimed} = Test.inspect(restarted, run)
    assert [%{lease_until: lease_until, runnable_key: ^first_runnable_key}] = claimed.attempts

    advance_by = DateTime.diff(lease_until, @now, :microsecond)
    assert {:ok, ^lease_until} = Test.advance_time(restarted, advance_by, :microsecond)

    assert {:completed, completed} = Test.drain(restarted, run)
    assert completed.context.confirmation == %{confirmed_id: "inv_456"}

    assert {:ok, calls} = Test.stub_calls(restarted, "billing.action")

    assert [
             {:load_invoice, ^first_runnable_key, false},
             {:load_invoice, ^first_runnable_key, true},
             {:confirm_invoice, confirm_runnable_key, false}
           ] = Enum.map(calls, &{&1.step, &1.runnable_key, &1.replayed?})

    assert confirm_runnable_key != first_runnable_key
    assert confirm_runnable_key in completed.applied_runnable_keys
  end

  test "replays an assigned result for the same durable runnable identity" do
    results = [
      {:ok, %{sequence: 1}},
      {:ok, %{sequence: 2}}
    ]

    assert {:ok, storage} = Storage.start_link(self(), @now, %{"billing.action" => results})
    on_exit(fn -> Storage.stop(storage) end)

    first_call = %{
      attempt: 1,
      input: %{invoice_id: "inv_123"},
      run_id: "run-1",
      runnable_key: "runnable-1",
      step: :load_invoice
    }

    assert {:ok, {:ok, %{sequence: 1}}} =
             Storage.consume_action_stub(
               storage,
               "billing.action",
               {"run-1", "runnable-1"},
               first_call
             )

    assert {:ok, {:ok, %{sequence: 1}}} =
             Storage.consume_action_stub(
               storage,
               "billing.action",
               {"run-1", "runnable-1"},
               first_call
             )

    assert {:ok, {:ok, %{sequence: 2}}} =
             Storage.consume_action_stub(
               storage,
               "billing.action",
               {"run-1", "runnable-2"},
               %{first_call | runnable_key: "runnable-2", step: :confirm_invoice}
             )

    assert :ok = Storage.reserve_start(storage)
    assert :ok = Storage.commit_start(storage, Ecto.UUID.generate())
    assert {:ok, calls} = Storage.action_stub_calls(storage, "billing.action")
    assert Enum.map(calls, & &1.replayed?) == [false, true, false]
  end

  test "fails nonretryably when a result sequence is exhausted" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_stubs: %{"billing.action" => [{:ok, %{id: "inv_123"}}]}
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{invoice_id: "inv_123"})
    assert {:failed, failed} = Test.drain(runtime, run)
    assert failed.terminal_error.code == "test_action_stub_failed"
    assert failed.terminal_error.retryable? == false
  end

  test "rejects malformed stubs and unknown stub action keys before journal writes" do
    assert {:error, {:invalid_option, {:action_stubs, :invalid}}} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_stubs: %{"billing.action" => []}
             )

    assert {:error, {:invalid_workflow_spec, errors}} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_stubs: %{"billing.other" => [{:ok, %{}}]}
             )

    assert Enum.any?(errors, &(&1.code == :unknown_action_key))

    assert {:error, {:invalid_option, {:action_stubs, :duplicate_action_key}}} =
             Test.start_runtime(
               workflow: sequence_spec(),
               action_registry: %{"billing.action" => ModuleStep},
               action_stubs: %{"billing.action" => [{:ok, %{}}]}
             )
  end

  test "rejects stubs for module-authored workflows instead of silently ignoring them" do
    assert {:error, {:invalid_option, {:action_stubs, :requires_runtime_spec}}} =
             Test.start_runtime(
               workflow: Jizoku.Test.ActionStubsTest.ModuleWorkflow,
               action_stubs: %{"billing.action" => [{:ok, %{}}]}
             )
  end

  test "combines keyword action registries with stub entries and redacts registries from inspect" do
    registry = [billing_real: [module: RealAction, action_opts: [credential: "secret-token"]]]

    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: mixed_spec(),
               action_registry: registry,
               action_stubs: %{billing_stub: [{:ok, %{status: "delivered"}}]}
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    refute Kernel.inspect(runtime) =~ "secret-token"

    assert {:ok, run} = Test.start(runtime, %{invoice_id: "inv_mixed"})
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.invoice == %{id: "inv_mixed"}
    assert completed.context.confirmation == %{status: "delivered"}

    assert {:ok, [%{input: %{invoice: %{id: "inv_mixed"}}}]} =
             Test.stub_calls(runtime, :billing_stub)
  end

  test "keeps action guardrails in the normal start boundary" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: guarded_spec(),
               action_stubs: %{"billing.action" => [{:ok, %{id: "inv_999"}}]},
               guardrail_registry: %{"billing.policy" => DenyGuardrail}
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    before_start = persistence_state(runtime)

    assert {:error, {:invalid_workflow_spec, errors}} =
             Test.start(runtime, %{invoice_id: "inv_999"})

    assert Enum.any?(errors, &(&1.code == :guardrail_failed))
    assert {:error, :run_not_started} = Test.stub_calls(runtime, "billing.action")
    assert persistence_state(runtime) == before_start
  end

  defp sequence_spec do
    %{
      workflow: RuntimeStubWorkflow,
      definition_version: "test-stub-v1",
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
        %{
          name: :load_invoice,
          action: "billing.action",
          opts: [output: :invoice]
        },
        %{
          name: :confirm_invoice,
          action: "billing.action",
          opts: [input: [:invoice], output: :confirmation]
        }
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :confirm_invoice},
        %{from: :confirm_invoice, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }
  end

  defp guarded_spec do
    put_in(
      sequence_spec(),
      [:steps, Access.at(0), :opts, :guardrails],
      input: [[key: "billing.policy"]]
    )
  end

  defp mixed_spec do
    sequence_spec()
    |> put_in([:steps, Access.at(0), :action], :billing_real)
    |> put_in([:steps, Access.at(1), :action], :billing_stub)
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
