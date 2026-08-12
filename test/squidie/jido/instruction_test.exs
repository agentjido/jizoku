defmodule Squidie.Jido.InstructionTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.Journal
  alias Squidie.Test

  defmodule PrepareOrder do
    use Squidie.Step, name: :prepare_instruction_origin

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{prepared: true}}
    end
  end

  defmodule FinishOrder do
    use Squidie.Step, name: :finish_instruction_workflow

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{finished: true}}
    end
  end

  defmodule EnrichOrder do
    use Jido.Action,
      name: "enrich_order_instruction",
      description: "Enriches one order from a Jido instruction",
      schema: [order_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{order_id: order_id}, context) do
      {:ok,
       %{
         enriched_order_id: order_id,
         instruction_request_id: context.request_id,
         instruction_secret: Map.get(context, :secret)
       }}
    end
  end

  defmodule RetryEnrichment do
    use Jido.Action,
      name: "retry_enrichment_instruction",
      description: "Retries one instruction before completing",
      schema: [order_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_params, %{attempt: 1}) do
      {:error, %{code: "temporary_failure", message: "try again", retryable?: true}}
    end

    def run(%{order_id: order_id}, %{attempt: 2}) do
      {:ok, %{retried_order_id: order_id}}
    end
  end

  defmodule WaitingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :prepare, PrepareOrder
      step :wait, :wait, duration: 60_000
      step :finish, FinishOrder

      transition :prepare, on: :ok, to: :wait
      transition :wait, on: :ok, to: :finish
      transition :finish, on: :ok, to: :complete
    end
  end

  @now ~U[2026-08-12 13:00:00.000000Z]
  @registry %{
    "orders.enrich" => EnrichOrder,
    "orders.retry_enrichment" => RetryEnrichment
  }

  test "schedules an allowlisted Jido instruction as durable dynamic work" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run, origin} = start_with_origin(runtime)

    instruction =
      Jido.Instruction.new!(
        id: "instruction-123",
        action: EnrichOrder,
        params: %{order_id: "ord_123"},
        context: %{request_id: "req_123", secret: "instruction-secret"}
      )

    assert {:ok, scheduled} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin)
             )

    assert [dynamic] = scheduled.dynamic_work
    assert dynamic.dynamic_key == "jido-instruction:instruction-123"

    assert dynamic.origin == origin

    assert [%{"jido_instruction" => %{"id" => "instruction-123"}}] =
             Enum.map(dynamic.nodes, &Map.take(&1.metadata, ["jido_instruction"]))

    refute Map.has_key?(dynamic.metadata, :jido_instruction_id)
    assert dynamic.metadata["jido_instruction_id"] == "instruction-123"

    assert {:ok, %{entries: entries}} = Journal.load_thread(runtime.storage, {:run, run.run_id})

    assert %{"jido_instruction" => %{"id" => "instruction-123"}} =
             entries
             |> Enum.filter(&(&1.type == :runnables_planned))
             |> List.last()
             |> Map.fetch!(:data)
             |> Map.fetch!(:runnables)
             |> List.first()
             |> Map.fetch!(:dynamic_work)

    persisted = persistence_state(runtime)

    assert {:ok, duplicate} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin)
             )

    assert duplicate == scheduled
    assert persistence_state(runtime) == persisted

    before_loss = persistence_state(runtime)
    assert map_size(before_loss.checkpoints) > 0
    assert :ok = Test.delete_checkpoints(runtime)
    assert persistence_state(runtime) == %{before_loss | checkpoints: %{}}

    after_loss = persistence_state(runtime)

    assert {:ok, rebuilt_duplicate} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin)
             )

    assert rebuilt_duplicate.dynamic_work == scheduled.dynamic_work
    assert persistence_state(runtime) == after_loss

    assert {:ok, restarted_runtime} = Test.restart_runtime(runtime)
    on_exit(fn -> Test.stop_runtime(restarted_runtime) end)
    assert {:error, :runtime_stopped} = Test.inspect(runtime, run)

    assert {:ok, rebuilt} = Test.inspect(restarted_runtime, run)
    assert rebuilt.dynamic_work == scheduled.dynamic_work
    assert persistence_state(restarted_runtime).threads == before_loss.threads

    assert {:blocked, after_instruction} = Test.drain(restarted_runtime, run)
    assert after_instruction.context.enriched_order_id == "ord_123"
    assert after_instruction.context.instruction_request_id == "req_123"
    assert after_instruction.context.instruction_secret == "[REDACTED]"
    refute inspect(after_instruction) =~ "instruction-secret"

    assert {:ok, ~U[2026-08-12 13:01:00.000000Z]} =
             Test.advance_time(restarted_runtime, 60, :second)

    assert {:completed, completed} = Test.drain(restarted_runtime, run)

    dynamic_attempts =
      Enum.filter(completed.attempts, &(&1.step == "jido-instruction:instruction-123"))

    assert [%{status: :completed}] = dynamic_attempts
  end

  test "requires one enabled registry key for the instruction action" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run, origin} = start_with_origin(runtime)
    instruction = instruction()
    before = persistence_state(runtime)

    assert {:error, {:invalid_jido_instruction, {:action, :unknown_action_module}}} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin, action_registry: %{})
             )

    assert {:error, {:invalid_jido_instruction, {:action, :ambiguous_action_module}}} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime,
                 origin: origin,
                 action_registry: %{"one" => EnrichOrder, "two" => EnrichOrder}
               )
             )

    assert {:error, {:invalid_jido_instruction, {:action, :unknown_action_module}}} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime,
                 origin: origin,
                 action_registry: %{
                   "orders.enrich" => %{module: EnrichOrder, enabled?: false}
                 }
               )
             )

    assert persistence_state(runtime) == before
  end

  test "rejects a changed durable intent for the same instruction ID" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run, origin} = start_with_origin(runtime)
    instruction = instruction()

    assert {:ok, _scheduled} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin)
             )

    assert {:blocked, after_execution} = Test.drain(runtime, run)

    assert %{runnable_key: alternate_key, step: alternate_step, attempt_number: alternate_attempt} =
             Enum.find(
               after_execution.planned_runnables,
               &(&1.step == "jido-instruction:instruction-invalid")
             )

    assert alternate_key in after_execution.applied_runnable_keys

    alternate_origin = %{
      runnable_key: alternate_key,
      step: alternate_step,
      attempt: alternate_attempt
    }

    before = persistence_state(runtime)

    changed = [
      %{instruction | params: %{order_id: "changed"}},
      %{instruction | context: %{"request_id" => "changed"}},
      %{instruction | opts: [retry: [max_attempts: 2]]},
      %{instruction | action: RetryEnrichment}
    ]

    for candidate <- changed do
      assert {:error,
              {:invalid_dynamic_work,
               {:nodes, {:duplicate_existing_id, "jido-instruction:instruction-invalid"}}}} =
               Squidie.schedule_dynamic_work(
                 run.run_id,
                 candidate,
                 runtime_opts(runtime, origin: origin)
               )
    end

    assert {:error,
            {:invalid_dynamic_work,
             {:nodes, {:duplicate_existing_id, "jido-instruction:instruction-invalid"}}}} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: alternate_origin)
             )

    assert persistence_state(runtime) == before
  end

  test "maps the supported instruction retry option onto durable retries" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run, origin} = start_with_origin(runtime)

    instruction =
      Jido.Instruction.new!(
        id: "instruction-retry",
        action: RetryEnrichment,
        params: %{order_id: "ord_retry"},
        opts: [retry: [max_attempts: 2]]
      )

    assert {:ok, _scheduled} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction,
               runtime_opts(runtime, origin: origin)
             )

    assert {:blocked, snapshot} = Test.drain(runtime, run)
    assert snapshot.context.retried_order_id == "ord_retry"

    assert [
             %{attempt_number: 1, status: :failed},
             %{attempt_number: 2, status: :completed}
           ] =
             Enum.filter(
               snapshot.attempts,
               &(&1.step == "jido-instruction:instruction-retry")
             )
  end

  test "rejects unsafe context, reserved keys, and Jido-owned execution options" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run, origin} = start_with_origin(runtime)
    before = persistence_state(runtime)

    %Jido.Instruction{} = instruction = instruction()

    cases = [
      {%{instruction | id: ""}, {:id, :invalid}},
      {%{instruction | id: String.duplicate("x", 256)}, {:id, :invalid}},
      {%{instruction | context: %{unsafe: self()}}, {:context, :invalid}},
      {%{instruction | context: %{"run_id" => "override"}}, {:context, {:reserved_key, :run_id}}},
      {%{instruction | context: %{run_id: "override"}}, {:context, {:reserved_key, :run_id}}},
      {%{instruction | opts: [max_retries: 3]}, {:opts, :unsupported}},
      {%{instruction | params: %{unsafe: self()}}, {:params, :invalid}},
      {%{instruction | context: %{"value" => String.duplicate("x", 65_537)}},
       {:context, :invalid}}
    ]

    for {invalid, reason} <- cases do
      assert {:error, {:invalid_jido_instruction, ^reason}} =
               Squidie.schedule_dynamic_work(
                 run.run_id,
                 invalid,
                 runtime_opts(runtime, origin: origin)
               )
    end

    assert persistence_state(runtime) == before
  end

  test "requires one applied durable origin without mutating the run" do
    assert {:ok, runtime} = runtime()
    on_exit(fn -> Test.stop_runtime(runtime) end)
    assert {:ok, run} = Test.start(runtime, %{})
    before = persistence_state(runtime)

    assert {:error, {:invalid_jido_instruction, {:origin, :required}}} =
             Squidie.schedule_dynamic_work(run.run_id, instruction(), runtime_opts(runtime))

    assert {:error, {:invalid_dynamic_work, {:origin, :unapplied_runnable}}} =
             Squidie.schedule_dynamic_work(
               run.run_id,
               instruction(),
               runtime_opts(runtime,
                 origin: %{
                   runnable_key: hd(run.planned_runnable_keys),
                   step: "prepare",
                   attempt: 1
                 }
               )
             )

    assert persistence_state(runtime) == before
  end

  defp instruction do
    Jido.Instruction.new!(
      id: "instruction-invalid",
      action: EnrichOrder,
      params: %{order_id: "ord_invalid"},
      context: %{request_id: "req_invalid"}
    )
  end

  defp runtime do
    Test.start_runtime(
      workflow: WaitingWorkflow,
      action_registry: @registry,
      now: @now
    )
  end

  defp runtime_opts(runtime, overrides \\ []) do
    Keyword.merge(
      [
        runtime: :journal,
        read_model: :read_model,
        journal_storage: runtime.storage,
        queue: runtime.queue,
        partition: runtime.partition,
        now: @now,
        action_registry: @registry
      ],
      overrides
    )
  end

  defp start_with_origin(runtime) do
    with {:ok, run} <- Test.start(runtime, %{}),
         {:reached, snapshot} <-
           Test.execute_until(runtime, run, &(&1.applied_runnable_keys != []), max_steps: 1),
         %{runnable_key: runnable_key, step: step, attempt_number: attempt} <-
           Enum.find(snapshot.planned_runnables, &(&1.step == "prepare")) do
      {:ok, run, %{runnable_key: runnable_key, step: step, attempt: attempt}}
    end
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
