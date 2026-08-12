defmodule Squidie.Runtime.Journal.NativeRunInstructionTest do
  use ExUnit.Case, async: false

  alias Jido.Agent.Directive
  alias Jido.Storage.ETS
  alias Squidie.Runtime.AgentRecovery
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Commands.Starter
  alias Squidie.Runtime.Journal.Executor

  defmodule FaultStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts), do: delegate(:get_checkpoint, [key], opts)

    @impl Jido.Storage
    def put_checkpoint(key, data, opts), do: delegate(:put_checkpoint, [key, data], opts)

    @impl Jido.Storage
    def delete_checkpoint(key, opts), do: delegate(:delete_checkpoint, [key], opts)

    @impl Jido.Storage
    def load_thread(thread_id, opts), do: delegate(:load_thread, [thread_id], opts)

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      kinds = Enum.map(entries, & &1.kind)
      mode = Keyword.fetch!(opts, :fault_mode)
      key = {__MODULE__, Keyword.fetch!(opts, :fault_ref)}

      case fault_action(mode, kinds, key) do
        {:fail_before, reason} ->
          Process.put(key, true)
          {:error, reason}

        :corrupt_completion ->
          Process.put(key, true)

          corrupted_entries =
            Enum.map(entries, fn entry ->
              payload =
                put_in(
                  entry.payload,
                  [:data, :result, "__squidie_jido_result__", "fingerprint"],
                  "corrupted"
                )

              %{entry | payload: payload}
            end)

          {:ok, _thread} = delegate(:append_thread, [thread_id, corrupted_entries], opts)
          {:error, :conflict}

        :completion_barrier ->
          Process.put(key, true)
          owner = Keyword.fetch!(opts, :barrier_owner)
          send(owner, {:completion_ready, self()})

          receive do
            :release_completion -> delegate(:append_thread, [thread_id, entries], opts)
          end

        {:commit_then_fail, reason} ->
          Process.put(key, true)
          {:ok, _thread} = delegate(:append_thread, [thread_id, entries], opts)
          {:error, reason}

        :delegate ->
          delegate(:append_thread, [thread_id, entries], opts)
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts), do: delegate(:delete_thread, [thread_id], opts)

    defp fault_batch?(:completion_timeout, [:attempt_completed]), do: true
    defp fault_batch?(:completion_failure, [:attempt_completed]), do: true

    defp fault_batch?(:run_failure, [
           :runnable_applied,
           :dynamic_work_recorded,
           :runnables_planned
         ]),
         do: true

    defp fault_batch?(:run_conflict, [
           :runnable_applied,
           :dynamic_work_recorded,
           :runnables_planned
         ]),
         do: true

    defp fault_batch?(_mode, _kinds), do: false

    defp fault_action(mode, kinds, key) do
      if is_nil(Process.get(key)) do
        pending_fault_action(mode, kinds)
      else
        :delegate
      end
    end

    defp pending_fault_action(mode, kinds) do
      cond do
        mode in [:completion_failure, :run_failure] and fault_batch?(mode, kinds) ->
          {:fail_before, fault_result(mode)}

        mode == :corrupt_completion and kinds == [:attempt_completed] ->
          :corrupt_completion

        mode == :completion_barrier and kinds == [:attempt_completed] ->
          :completion_barrier

        fault_batch?(mode, kinds) ->
          {:commit_then_fail, fault_result(mode)}

        true ->
          :delegate
      end
    end

    defp fault_result(:completion_timeout), do: :injected_completion_timeout
    defp fault_result(:completion_failure), do: :injected_completion_failure
    defp fault_result(:run_failure), do: :injected_run_failure
    defp fault_result(:run_conflict), do: :conflict

    defp delegate(callback, args, opts) do
      {adapter, delegate_opts} = Keyword.fetch!(opts, :delegate)

      apply(
        adapter,
        callback,
        Enum.concat(args, [delegate_opts ++ Keyword.take(opts, [:expected_rev])])
      )
    end
  end

  defmodule Followup do
    use Jido.Action,
      name: "native_run_instruction_followup",
      description: "Completes work from a durable RunInstruction directive",
      schema: [value: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{value: value}, context) do
      {:ok, %{followup_value: value, followup_request_id: context.request_id}}
    end
  end

  defmodule RequestFollowup do
    use Jido.Action,
      name: "native_run_instruction_source",
      description: "Requests durable follow-up work",
      schema: []

    @impl Jido.Action
    def run(_input, context) do
      invocation_key = {__MODULE__, context.run_id}
      :persistent_term.put(invocation_key, :persistent_term.get(invocation_key, 0) + 1)

      instruction =
        Jido.Instruction.new!(
          id: "followup-1",
          action: Followup,
          params: %{value: "durable"},
          context: %{request_id: "request-1"}
        )

      {:ok, %{source_completed: true}, [Directive.run_instruction(instruction)]}
    end
  end

  defmodule Seed do
    use Squidie.Step, name: :native_run_instruction_seed

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{seeded: true}}
    end
  end

  defmodule RecoverCollision do
    use Squidie.Step, name: :native_run_instruction_collision_recovery

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{collision_recovered: true}}
    end
  end

  defmodule Workflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :source, RequestFollowup
      transition :source, on: :ok, to: :complete
    end
  end

  defmodule CollisionWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :seed, Seed
      step :source, RequestFollowup
      step :recover_collision, RecoverCollision

      transition :seed, on: :ok, to: :source
      transition :source, on: :ok, to: :complete
      transition :source, on: :error, to: :recover_collision
      transition :recover_collision, on: :ok, to: :complete
    end
  end

  @storage {ETS, table: :squidie_native_run_instruction_test}
  @run_id "22222222-2222-5222-8222-222222222222"
  @now ~U[2026-08-12 15:00:00.000000Z]
  @registry %{"native.followup" => Followup}

  setup do
    previous = Application.fetch_env(:squidie, :jido_effects)
    Application.put_env(:squidie, :jido_effects, :enabled)
    cleanup_storage()
    :persistent_term.erase({RequestFollowup, @run_id})

    on_exit(fn ->
      restore_activation(previous)
      :persistent_term.erase({RequestFollowup, @run_id})
      cleanup_storage()
    end)

    :ok
  end

  test "a committed completion is repaired without rerunning the source action" do
    assert {:ok, _started} = start_run()
    fault_storage = fault_storage(:completion_timeout)

    assert {:error, :injected_completion_timeout} = execute(fault_storage, "claim-1")
    assert invocation_count() == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(dispatch_entries, &(&1.type == :attempt_completed)) == 1

    assert %{data: %{result: result}} =
             Enum.find(dispatch_entries, &(&1.type == :attempt_completed))

    assert %{"__squidie_jido_result__" => %{"kind" => "run_instruction"}} = result

    assert %{data: completion_data} =
             Enum.find(dispatch_entries, &(&1.type == :attempt_completed))

    assert completion_data["completion_encoding"] ==
             %{"effect" => "run_instruction", "version" => 1}

    assert completion_data.execution_opts == []

    assert {:ok, dispatch_before_legacy_checkpoint} = DispatchAgent.rebuild(@storage, "default")

    current_projection = dispatch_before_legacy_checkpoint.state.projection

    legacy_attempts =
      Map.new(current_projection.attempts, fn {key, attempt} ->
        {key, Map.delete(attempt, "completion_encoding")}
      end)

    legacy_projection =
      current_projection
      |> Map.put(:attempts, legacy_attempts)
      |> Map.delete("squidie_dispatch_checkpoint_version")

    assert :ok =
             Journal.put_checkpoint(
               @storage,
               {:dispatch, "default"},
               legacy_projection,
               dispatch_before_legacy_checkpoint.state.thread_rev,
               updated_at: @now
             )

    assert {:ok, rebuilt_from_legacy_checkpoint} = DispatchAgent.rebuild(@storage, "default")

    assert [{_runnable_key, %ActionAttempt{} = legacy_rebuilt_attempt}] =
             Map.to_list(rebuilt_from_legacy_checkpoint.state.projection.attempts)

    assert ActionAttempt.completion_encoding(legacy_rebuilt_attempt) ==
             %{"effect" => "run_instruction", "version" => 1}

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :runnable_applied))
    refute Enum.any?(run_entries, &(&1.type == :dynamic_work_recorded))

    run_entries_before_recovery = run_entries
    assert {:ok, recovered} = AgentRecovery.recover(@storage, @run_id)
    assert recovered.applied_attempts == []

    assert {:ok, ^run_entries_before_recovery} =
             Journal.load_entries(@storage, {:run, @run_id})

    assert {:ok, _checkpoint} = Journal.fetch_checkpoint(@storage, {:run, @run_id})
    assert {:ok, _checkpoint} = Journal.fetch_checkpoint(@storage, {:dispatch, "default"})
    assert :ok = delete_checkpoint({:run, @run_id})
    assert :ok = delete_checkpoint({:dispatch, "default"})
    assert {:error, :not_found} = Journal.fetch_checkpoint(@storage, {:run, @run_id})
    assert {:error, :not_found} = Journal.fetch_checkpoint(@storage, {:dispatch, "default"})

    assert {:ok, rebuilt_dispatch} = DispatchAgent.rebuild(@storage, "default")

    assert [{_runnable_key, %ActionAttempt{} = rebuilt_attempt}] =
             Map.to_list(rebuilt_dispatch.state.projection.attempts)

    assert ActionAttempt.completion_encoding(rebuilt_attempt) ==
             %{"effect" => "run_instruction", "version" => 1}

    assert {:ok, ^run_entries_before_recovery} =
             Journal.load_entries(@storage, {:run, @run_id})

    Application.delete_env(:squidie, :jido_effects)

    assert {:ok, after_recovery} = execute(@storage, "claim-recovery")
    assert after_recovery.context.source_completed == true
    assert after_recovery.terminal? == false
    assert invocation_count() == 1

    assert {:ok, completed} =
             execute(@storage, "claim-followup", DateTime.add(@now, 1, :second))

    assert completed.terminal_status == :completed
    assert completed.context.followup_value == "durable"
    assert completed.context.followup_request_id == "request-1"
    assert invocation_count() == 1

    assert_native_batches_once()
  end

  test "an unknown successful run batch converges without duplicate effects" do
    assert {:ok, _started} = start_run()
    fault_storage = fault_storage(:run_conflict)

    assert {:ok, planned} = execute(fault_storage, "claim-1")
    assert planned.context.source_completed == true
    assert planned.terminal? == false
    assert invocation_count() == 1

    assert {:ok, completed} =
             execute(@storage, "claim-followup", DateTime.add(@now, 1, :second))

    assert completed.terminal_status == :completed
    assert completed.context.followup_value == "durable"
    assert invocation_count() == 1

    assert_native_batches_once()
  end

  test "fail-before append outcomes expose no effect and remain retryable" do
    assert {:ok, _started} = start_run()
    completion_failure = fault_storage(:completion_failure)

    assert {:error, :injected_completion_failure} = execute(completion_failure, "claim-1")
    assert invocation_count() == 1

    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    refute Enum.any?(dispatch_entries, &(&1.type == :attempt_completed))

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :dynamic_work_recorded))

    assert {:ok, planned} =
             execute(@storage, "claim-2", DateTime.add(@now, 301, :second))

    assert planned.terminal? == false
    assert invocation_count() == 2
    assert [dynamic] = planned.dynamic_work
    assert dynamic.dynamic_key == "jido-instruction:followup-1"

    cleanup_storage()
    :persistent_term.erase({RequestFollowup, @run_id})
    assert {:ok, _started} = start_run()
    run_failure = fault_storage(:run_failure)

    assert {:error, :injected_run_failure} = execute(run_failure, "claim-1")
    assert invocation_count() == 1

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries, &(&1.type == :dynamic_work_recorded))

    assert {:ok, repaired} = execute(@storage, "claim-repair")
    assert repaired.terminal? == false
    assert invocation_count() == 1
    assert [dynamic] = repaired.dynamic_work
    assert dynamic.dynamic_key == "jido-instruction:followup-1"
  end

  test "a node collision after completion preflight resolves through the error transition" do
    assert {:ok, _started} = start_run(CollisionWorkflow)
    assert {:ok, seeded} = execute(@storage, "claim-seed")

    seed_attempt = Enum.find(seeded.attempts, &(&1.step == "seed"))

    origin = %{
      runnable_key: seed_attempt.runnable_key,
      step: seed_attempt.step,
      attempt: seed_attempt.attempt_number
    }

    barrier_storage =
      {FaultStorage,
       delegate: @storage,
       fault_mode: :completion_barrier,
       fault_ref: make_ref(),
       barrier_owner: self()}

    source_task =
      Task.async(fn ->
        execute(barrier_storage, "claim-source", DateTime.add(@now, 1, :second))
      end)

    assert_receive {:completion_ready, source_pid}

    assert {:ok, _competing} =
             Squidie.schedule_dynamic_work(
               @run_id,
               followup_instruction(),
               journal_storage: @storage,
               action_registry: @registry,
               queue: "default",
               now: @now,
               origin: origin
             )

    send(source_pid, :release_completion)
    assert {:ok, resolved} = Task.await(source_task)
    assert resolved.context.seeded == true
    refute Map.has_key?(resolved.context, :source_completed)
    assert invocation_count() == 1

    assert {:ok, completed} = drain("collision")
    assert completed.status == :completed
    assert completed.context.collision_recovered == true
    assert completed.context.followup_value == "durable"
    refute Map.has_key?(completed.context, :source_completed)

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    assert Enum.count(run_entries, &(&1.type == :dynamic_work_recorded)) == 1
    assert Enum.count(run_entries, &(&1.type == :run_terminal)) == 1

    assert %{transition: %{"on" => "error", "to" => "recover_collision"}} =
             Enum.find(completed.attempts, &(&1.step == "source"))
  end

  test "a malformed durable effect remains suppressed and resolves as a sanitized failure" do
    assert {:ok, _started} = start_run(CollisionWorkflow)
    assert {:ok, _seeded} = execute(@storage, "claim-seed")
    corrupt_storage = fault_storage(:corrupt_completion)

    assert {:error, :conflicting_completion} =
             execute(corrupt_storage, "claim-source", DateTime.add(@now, 1, :second))

    assert invocation_count() == 1
    assert {:ok, run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})
    refute Enum.any?(run_entries_before, &(&1.type == :dynamic_work_recorded))
    refute Enum.any?(run_entries_before, &(&1.type == :run_terminal))

    assert {:ok, recovered} = AgentRecovery.recover(@storage, @run_id)
    assert recovered.applied_attempts == []
    assert {:ok, ^run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})

    assert {:ok, after_resolution} =
             execute(@storage, "claim-recover", DateTime.add(@now, 2, :second))

    assert after_resolution.terminal? == false
    assert invocation_count() == 1
    refute inspect(after_resolution) =~ "corrupted"
    assert after_resolution.dynamic_work == []

    assert {:ok, run_entries_after_resolution} =
             Journal.load_entries(@storage, {:run, @run_id})

    refute Enum.any?(run_entries_after_resolution, &(&1.type == :dynamic_work_recorded))

    assert {:ok, completed} = drain("malformed")
    assert completed.status == :completed
    assert completed.context.collision_recovered == true
    refute Map.has_key?(completed.context, :source_completed)
    refute Map.has_key?(completed.context, :followup_value)

    assert %{transition: %{"on" => "error", "to" => "recover_collision"}} =
             Enum.find(completed.attempts, &(&1.step == "source"))
  end

  test "an unsupported future effect encoding remains pending without run-thread writes" do
    assert {:ok, _started} = start_run()
    assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@storage, "default")

    assert {:ok,
            %{
              agent: claimed_agent,
              attempt: %ActionAttempt{runnable_key: runnable_key},
              claim_id: claim_id,
              claim_token: claim_token
            }} =
             DispatchAgent.claim_next(@storage, dispatch_agent, "future-effect-worker",
               now: @now,
               claim_id: "future-effect-claim",
               claim_token: "future-effect-token"
             )

    assert {:ok, _completion} =
             DispatchAgent.complete(
               @storage,
               claimed_agent,
               runnable_key,
               claim_id,
               claim_token,
               %{"future" => "encoded-effect"},
               now: @now,
               completion_encoding: %{"effect" => "run_instruction", "version" => 2}
             )

    assert {:ok, run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})

    assert {:error, :malformed_jido_result_envelope} =
             execute(@storage, "future-effect-recovery", DateTime.add(@now, 1, :second))

    assert {:ok, ^run_entries_before} = Journal.load_entries(@storage, {:run, @run_id})
    assert invocation_count() == 0
  end

  defp start_run(workflow \\ Workflow) do
    Starter.start_run(workflow, :manual, %{},
      journal_storage: @storage,
      run_id: @run_id,
      now: @now
    )
  end

  defp drain(suffix, attempts \\ 5)
  defp drain(_suffix, 0), do: {:error, :timeout}

  defp drain(suffix, attempts) do
    elapsed = 6 - attempts

    case execute(
           @storage,
           "claim-#{suffix}-#{attempts}",
           DateTime.add(@now, elapsed, :second)
         ) do
      {:ok, %{terminal?: true} = snapshot} -> {:ok, snapshot}
      {:ok, _snapshot} -> drain(suffix, attempts - 1)
      {:error, _reason} = error -> error
    end
  end

  defp followup_instruction do
    Jido.Instruction.new!(
      id: "followup-1",
      action: Followup,
      params: %{value: "durable"},
      context: %{request_id: "request-1"}
    )
  end

  defp execute(storage, claim_id, now \\ @now) do
    Executor.execute_next(
      runtime: :journal,
      journal_storage: storage,
      action_registry: @registry,
      now: now,
      finished_at: DateTime.add(now, 1, :second),
      owner_id: "native-run-instruction-worker",
      claim_id: claim_id,
      claim_token: claim_id <> "-token"
    )
  end

  defp fault_storage(mode) do
    {FaultStorage, delegate: @storage, fault_mode: mode, fault_ref: make_ref()}
  end

  defp invocation_count do
    :persistent_term.get({RequestFollowup, @run_id}, 0)
  end

  defp assert_native_batches_once do
    assert {:ok, dispatch_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert Enum.count(dispatch_entries, &(&1.type == :attempt_completed)) == 2

    assert {:ok, run_entries} = Journal.load_entries(@storage, {:run, @run_id})
    types = Enum.map(run_entries, & &1.type)

    assert Enum.count(types, &(&1 == :dynamic_work_recorded)) == 1
    assert Enum.count(types, &(&1 == :runnables_planned)) == 2
    assert Enum.count(types, &(&1 == :runnable_applied)) == 2
    assert Enum.count(types, &(&1 == :run_terminal)) == 1

    dynamic_index = Enum.find_index(types, &(&1 == :dynamic_work_recorded))

    assert Enum.slice(types, dynamic_index - 1, 3) == [
             :runnable_applied,
             :dynamic_work_recorded,
             :runnables_planned
           ]
  end

  defp restore_activation({:ok, value}) do
    Application.put_env(:squidie, :jido_effects, value)
  end

  defp restore_activation(:error) do
    Application.delete_env(:squidie, :jido_effects)
  end

  defp cleanup_storage do
    for table <- [
          :squidie_native_run_instruction_test_checkpoints,
          :squidie_native_run_instruction_test_threads,
          :squidie_native_run_instruction_test_thread_meta
        ] do
      if :ets.whereis(table) != :undefined do
        :ets.delete(table)
      end
    end
  rescue
    ArgumentError -> :ok
  end

  defp delete_checkpoint(thread) do
    {adapter, opts} = @storage

    adapter.delete_checkpoint(
      {"squidie", :checkpoint, Journal.thread_id(thread)},
      opts
    )
  end
end
