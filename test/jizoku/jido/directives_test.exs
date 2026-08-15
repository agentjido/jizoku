defmodule Jizoku.Jido.DirectivesTest do
  use ExUnit.Case, async: false

  alias Jido.Agent.Directive
  alias Jizoku.Runtime.Jido.Directives
  alias Jizoku.Runtime.Jido.Outbox
  alias Jizoku.Runtime.Jido.ResultEnvelope
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Test

  defmodule FollowupAction do
    use Jido.Action,
      name: "jido_directive_followup",
      description: "Executes work requested by a RunInstruction directive",
      schema: [value: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{value: value}, context) do
      {:ok,
       %{
         instruction_completed: value,
         instruction_request_id: context.request_id
       }}
    end
  end

  defmodule ExtrasAction do
    use Jido.Action,
      name: "jido_extras",
      description: "Returns Jido action extras",
      schema: [kind: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{kind: "reserved_output"}, _context) do
      {:ok, %{"__jizoku_jido_result__" => %{secret: "directive-secret"}}}
    end

    def run(%{kind: kind}, context) do
      if test_pid = :persistent_term.get({__MODULE__, context.run_id}, nil) do
        send(test_pid, {:extras_action_ran, context.run_id})
      end

      {:ok, %{accepted: true}, extras(kind)}
    end

    defp extras("emit") do
      [%Directive.Emit{signal: %{secret: "directive-secret"}}]
    end

    defp extras("emit_valid") do
      {:ok, signal} =
        Jido.Signal.new("sample.order.accepted", %{"order_id" => "order-1"},
          id: "signal-order-1",
          source: "/minimal_host/orders"
        )

      [%Directive.Emit{signal: signal}]
    end

    defp extras("emit_dispatch") do
      {:ok, signal} =
        Jido.Signal.new("sample.order.accepted", %{},
          id: "signal-order-dispatch",
          source: "/minimal_host/orders"
        )

      [%Directive.Emit{signal: signal, dispatch: {:pid, target: self()}}]
    end

    defp extras("run_instruction") do
      [run_instruction()]
    end

    defp extras("run_instruction_custom_result") do
      [%{run_instruction() | result_action: :custom_result}]
    end

    defp extras("run_instruction_meta") do
      [%{run_instruction() | meta: %{secret: "directive-secret"}}]
    end

    defp extras("error") do
      [%Directive.Error{error: %{secret: "directive-secret"}, context: :action}]
    end

    defp extras("custom") do
      [%{custom: "directive-secret"}]
    end

    defp extras("none") do
      []
    end

    defp run_instruction do
      instruction =
        Jido.Instruction.new!(
          id: "directive-followup",
          action: FollowupAction,
          params: %{value: "from-directive"},
          context: %{request_id: "req-directive"}
        )

      Directive.run_instruction(instruction)
    end
  end

  defmodule MalformedExtrasAction do
    use Jido.Action,
      name: "malformed_jido_extras",
      description: "Returns malformed Jido action extras",
      schema: []

    @impl Jido.Action
    def run(_input, _context) do
      {:ok, %{accepted: true}, %{secret: "directive-secret"}}
    end
  end

  defmodule RecoveryStep do
    use Jizoku.Step, name: :recover_directive_failure

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{recovered: true}}
    end
  end

  defmodule DenyOutput do
    @spec validate_guardrail(term(), map()) :: {:error, map()}
    def validate_guardrail(_value, context) do
      {:error, %{message: "blocked by output policy", placement: context.placement}}
    end
  end

  defmodule ReservedKeyStep do
    use Jizoku.Step, name: :reserved_key_native_step

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{"__jizoku_jido_result__" => %{application: "ordinary"}},
       jido: %{"effect" => "run_instruction", "version" => 1}}
    end
  end

  defmodule ExtrasWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction, retry: [max_attempts: 2]
      transition :jido_extras, on: :ok, to: :complete
    end
  end

  defmodule MalformedExtrasWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :jido_extras, MalformedExtrasAction
      transition :jido_extras, on: :ok, to: :complete
    end
  end

  defmodule ErrorTransitionWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction
      step :recover, RecoveryStep

      transition :jido_extras, on: :ok, to: :complete
      transition :jido_extras, on: :error, to: :recover
      transition :recover, on: :ok, to: :complete
    end
  end

  defmodule EmitTransitionWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction
      step :recover, RecoveryStep

      transition :jido_extras, on: :ok, to: :complete
      transition :jido_extras, on: :error, to: :recover
      transition :recover, on: :ok, to: :complete
    end
  end

  defmodule ReservedKeyWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :reserved_key, ReservedKeyStep
      transition :reserved_key, on: :ok, to: :complete
    end
  end

  defmodule GuardedInstructionWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction,
        guardrails: [output: [[key: "jido.output", policy: :route_error]]]

      step :recover, RecoveryStep

      transition :jido_extras, on: :error, to: :recover
      transition :recover, on: :ok, to: :complete
    end
  end

  @now ~U[2026-08-11 12:00:00.000000Z]

  setup do
    previous = Application.fetch_env(:jizoku, :jido_effects)
    previous_emit = Application.fetch_env(:jizoku, :jido_emit_effects)
    Application.put_env(:jizoku, :jido_effects, :enabled)
    Application.put_env(:jizoku, :jido_emit_effects, :enabled)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:jizoku, :jido_effects, value)
        :error -> Application.delete_env(:jizoku, :jido_effects)
      end

      case previous_emit do
        {:ok, value} -> Application.put_env(:jizoku, :jido_emit_effects, value)
        :error -> Application.delete_env(:jizoku, :jido_emit_effects)
      end
    end)
  end

  describe "normalize/1" do
    test "preserves the compatible empty extras result" do
      assert {:ok, []} = Directives.normalize([])
    end

    test "normalizes an error directive without exposing its contents" do
      directive = %Directive.Error{error: %{secret: "error-secret"}, context: :action}

      assert {:error,
              %{
                code: "jido_directive_error",
                directive_types: [:error],
                message: "Jido action returned an error directive",
                retryable?: false
              }} = Directives.normalize([directive])

      refute inspect(Directives.normalize([directive])) =~ "secret"
    end

    test "selects one run instruction for durable execution" do
      assert {:run_instruction, %Directive.RunInstruction{}} =
               Directives.normalize(extras_for("run_instruction"))
    end

    test "selects one emit directive for durable execution" do
      assert {:emit, %Directive.Emit{}} = Directives.normalize(extras_for("emit_valid"))
    end

    test "classifies unsupported and mixed directive types without exposing their contents" do
      extras = [
        %Directive.Emit{signal: %{secret: "emit-secret"}},
        %Directive.RunInstruction{
          instruction: %{secret: "instruction-secret"},
          result_action: :record_result,
          meta: %{}
        },
        %Directive.Error{error: %{secret: "error-secret"}, context: :action},
        %{custom: "custom-secret"}
      ]

      assert {:error,
              %{
                code: "unsupported_jido_directive",
                directive_types: [:emit, :run_instruction, :error, :unsupported],
                message: "Jido action directives are not supported",
                retryable?: false
              }} = Directives.normalize(extras)

      refute inspect(Directives.normalize(extras)) =~ "secret"
    end

    test "rejects malformed extras without exposing their contents" do
      assert {:error,
              %{
                code: "invalid_jido_action_extras",
                directive_types: [],
                message: "Jido action extras must be a list",
                retryable?: false
              }} = Directives.normalize(%{secret: "extras-secret"})

      refute inspect(Directives.normalize(%{secret: "extras-secret"})) =~ "secret"
    end
  end

  test "empty extras retain the raw Jido action success contract" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "none"})
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.accepted == true
  end

  test "the internal result envelope key is reserved from action output" do
    assert ResultEnvelope.reserved_output?(%{
             "__jizoku_jido_result__" => %{secret: "directive-secret"}
           })

    refute ResultEnvelope.reserved_output?(%{accepted: true})
  end

  test "a raw Jido action cannot return the internal result envelope key" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "reserved_output"})
    assert {:failed, failed} = Test.drain(runtime, run)

    assert failed.terminal_error == %{
             code: "reserved_jido_output_key",
             message: "Jido action output uses a reserved runtime key",
             retryable?: false
           }

    assert failed.context == %{}
    refute inspect(failed) =~ "directive-secret"
    refute Enum.any?(failed.attempts, &(&1.status == :completed))
  end

  test "the durable result envelope rejects changed output or plans" do
    envelope =
      ResultEnvelope.wrap_run_instruction(
        %{accepted: true},
        %{"dynamic_work" => %{dynamic_key: "one"}, "runnables" => [%{step: "one"}]}
      )

    completion_encoding = ResultEnvelope.completion_encoding()

    assert {:ok, {:run_instruction, %{accepted: true}, _plan}} =
             ResultEnvelope.decode(envelope, completion_encoding)

    changed =
      put_in(
        envelope,
        ["__jizoku_jido_result__", "output"],
        %{accepted: false, secret: "changed-secret"}
      )

    assert {:error, :malformed_jido_result_envelope} =
             ResultEnvelope.decode(changed, completion_encoding)

    assert ResultEnvelope.public_result(changed, completion_encoding) == nil

    changed_plan =
      put_in(
        envelope,
        ["__jizoku_jido_result__", "plan", "runnables"],
        [%{step: "changed-plan", secret: "changed-secret"}]
      )

    assert {:error, :malformed_jido_result_envelope} =
             ResultEnvelope.decode(changed_plan, completion_encoding)

    assert ResultEnvelope.public_result(changed_plan, completion_encoding) == nil

    assert {:ok, {:ordinary, ^changed}} = ResultEnvelope.decode(changed, nil)
    assert ResultEnvelope.public_result(changed, nil) == changed
  end

  test "the durable emit envelope rejects changed output or intent" do
    assert {:ok, signal} =
             Jido.Signal.new("sample.order.accepted", %{"order_id" => "order-1"},
               id: "signal-order-1",
               source: "/minimal_host/orders"
             )

    assert {:ok, intent} =
             Outbox.prepare(
               signal,
               "11111111-1111-5111-8111-111111111111",
               "11111111-1111-5111-8111-111111111111:emit:1"
             )

    encoded_intent = Outbox.encode_intent(intent)
    envelope = ResultEnvelope.wrap_emit(%{accepted: true}, encoded_intent)
    completion_encoding = ResultEnvelope.emit_completion_encoding()

    assert {:ok, {:emit, %{accepted: true}, ^encoded_intent}} =
             ResultEnvelope.decode(envelope, completion_encoding)

    changed =
      put_in(
        envelope,
        ["__jizoku_jido_result__", "intent", "route"],
        "changed"
      )

    assert {:error, :malformed_jido_result_envelope} =
             ResultEnvelope.decode(changed, completion_encoding)

    assert ResultEnvelope.public_result(changed, completion_encoding) == nil
  end

  test "native step output and options matching internal values remain ordinary" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ReservedKeyWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:completed, completed} = Test.drain(runtime, run)

    assert completed.context["__jizoku_jido_result__"] == %{application: "ordinary"}

    assert [%{result: %{"__jizoku_jido_result__" => %{application: "ordinary"}}}] =
             completed.attempts
  end

  test "a run instruction is durably planned and completed before a tail run terminates" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: ExtrasWorkflow,
               action_registry: %{"directive.followup" => FollowupAction},
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "run_instruction"})
    :persistent_term.put({ExtrasAction, run.run_id}, self())

    on_exit(fn -> :persistent_term.erase({ExtrasAction, run.run_id}) end)

    assert {:completed, completed} = Test.drain(runtime, run)
    assert_receive {:extras_action_ran, run_id}
    assert run_id == run.run_id
    assert completed.context.accepted == true
    assert completed.context.instruction_completed == "from-directive"
    assert completed.context.instruction_request_id == "req-directive"

    assert [dynamic_work] = completed.dynamic_work
    assert dynamic_work.dynamic_key == "jido-instruction:directive-followup"

    assert [dynamic_node] = dynamic_work.nodes

    assert dynamic_node.metadata["jido_instruction"] == %{
             "context" => %{request_id: "req-directive"},
             "id" => "directive-followup"
           }

    assert Enum.count(completed.attempts, &(&1.step == "jido_extras")) == 1

    assert Enum.count(
             completed.attempts,
             &(&1.step == "jido-instruction:directive-followup")
           ) == 1

    refute inspect(completed) =~ "__jizoku_jido_result__"

    before_loss = persistence_state(runtime)
    assert map_size(before_loss.checkpoints) > 0
    assert :ok = Test.delete_checkpoints(runtime)
    after_loss = persistence_state(runtime)
    assert after_loss == %{before_loss | checkpoints: %{}}

    assert {:ok, restarted} = Test.restart_runtime(runtime)
    on_exit(fn -> Test.stop_runtime(restarted) end)
    assert persistence_state(restarted) == after_loss

    assert {:completed, replayed} = Test.drain(restarted, run)
    assert replayed.context == completed.context
    assert persistence_state(restarted) == after_loss
    refute_receive {:extras_action_ran, _run_id}
  end

  test "an emit directive is atomically enqueued before terminal completion" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_valid"})
    :persistent_term.put({ExtrasAction, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({ExtrasAction, run.run_id}) end)

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.accepted == true
    assert_receive {:extras_action_ran, run_id}
    assert run_id == run.run_id

    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert [pending] = Outbox.pending(Projection.jido_outbox(agent.state.projection))
    assert pending["signal_id"] == "signal-order-1"
    assert pending["route"] == "default"
    assert pending["status"] == "pending"

    assert {:ok, run_entries} =
             Jizoku.Runtime.Journal.load_entries(runtime.storage, {:run, run.run_id})

    types = Enum.map(run_entries, & &1.type)
    applied_index = Enum.find_index(types, &(&1 == :runnable_applied))

    assert Enum.slice(types, applied_index, 3) == [
             :runnable_applied,
             :jido_signal_enqueued,
             :run_terminal
           ]

    assert Enum.count(types, &(&1 == :jido_signal_enqueued)) == 1
    refute inspect(completed) =~ "__jizoku_jido_result__"

    before_restart = persistence_state(runtime)
    assert {:ok, restarted} = Test.restart_runtime(runtime)
    on_exit(fn -> Test.stop_runtime(restarted) end)
    assert {:completed, replayed} = Test.drain(restarted, run)
    assert replayed.context == completed.context
    assert persistence_state(restarted) == before_restart
    refute_receive {:extras_action_ran, _run_id}
  end

  test "emit validation rejects runtime dispatch state before durable completion" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_dispatch"})
    assert {:failed, failed} = Test.drain(runtime, run)
    assert failed.applied_runnable_keys == []
    assert failed.terminal_error.code == "jido_effect_rejected"

    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert Outbox.items(Projection.jido_outbox(agent.state.projection)) == []
    refute inspect(persistence_state(runtime)) =~ "signal-order-dispatch"
  end

  test "emit emission is fail-closed until the fleet activation is enabled" do
    Application.delete_env(:jizoku, :jido_emit_effects)

    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_valid"})
    assert {:failed, failed} = Test.drain(runtime, run)

    assert failed.terminal_error == %{
             code: "jido_effect_rejected",
             message: "native Jido effect was rejected",
             retryable?: false
           }

    assert failed.applied_runnable_keys == []
    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert Outbox.items(Projection.jido_outbox(agent.state.projection)) == []
  end

  test "emit run-thread conflicts retry without rerunning or duplicating the signal" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_valid"})
    :persistent_term.put({ExtrasAction, run.run_id}, self())
    on_exit(fn -> :persistent_term.erase({ExtrasAction, run.run_id}) end)
    assert :ok = Test.inject_append_conflict(runtime, :run)

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.accepted == true
    assert_receive {:extras_action_ran, run_id}
    assert run_id == run.run_id
    refute_receive {:extras_action_ran, _run_id}

    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert [_pending] = Outbox.pending(Projection.jido_outbox(agent.state.projection))

    assert {:ok, run_entries} =
             Jizoku.Runtime.Journal.load_entries(runtime.storage, {:run, run.run_id})

    assert Enum.count(run_entries, &(&1.type == :jido_signal_enqueued)) == 1
  end

  test "an outbox collision after completion resolves through the ordinary error transition" do
    assert {:ok, runtime} =
             Test.start_runtime(workflow: EmitTransitionWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_valid"})
    [source_runnable_key] = run.planned_runnable_keys

    assert {:ok, conflicting_signal} =
             Jido.Signal.new("sample.order.accepted", %{"order_id" => "changed"},
               id: "signal-order-1",
               source: "/minimal_host/orders"
             )

    assert {:ok, conflicting_intent} =
             Outbox.prepare(conflicting_signal, run.run_id, source_runnable_key)

    assert {:ok, conflicting_entry} = Outbox.enqueue_entry(conflicting_intent, @now)

    assert {:ok, _thread} =
             Jizoku.Runtime.Journal.append_entries(runtime.storage, [conflicting_entry])

    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.recovered == true
    refute Map.has_key?(completed.context, :accepted)

    assert Enum.map(completed.attempts, &{&1.step, &1.status}) == [
             {"jido_extras", :completed},
             {"recover", :completed}
           ]

    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert [retained] = Outbox.pending(Projection.jido_outbox(agent.state.projection))
    assert retained["signal"]["data"] == %{"order_id" => "changed"}

    refute Enum.any?(
             Projection.anomalies(agent.state.projection),
             &(Map.get(&1, :component) == :jido_outbox)
           )
  end

  test "run instruction emission is fail-closed until the fleet activation is enabled" do
    Application.delete_env(:jizoku, :jido_effects)

    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: ExtrasWorkflow,
               action_registry: %{"directive.followup" => FollowupAction},
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "run_instruction"})
    assert {:failed, failed} = Test.drain(runtime, run)

    assert failed.terminal_error == %{
             code: "jido_effect_rejected",
             message: "native Jido effect was rejected",
             retryable?: false
           }

    assert failed.dynamic_work == []
    assert failed.applied_runnable_keys == []
    assert Enum.count(failed.attempts, &(&1.step == "jido_extras")) == 1
  end

  test "output guardrails reject a run instruction before completion or effect planning" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: GuardedInstructionWorkflow,
               action_registry: %{"directive.followup" => FollowupAction},
               guardrail_registry: %{"jido.output" => DenyOutput},
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "run_instruction"})
    assert {:completed, completed} = Test.drain(runtime, run)

    assert completed.context.recovered == true
    assert completed.dynamic_work == []

    assert [%{status: :failed, error: %{code: "guardrail_failed"}} | _rest] =
             completed.attempts

    refute Enum.any?(completed.attempts, &(&1.step == "jido-instruction:directive-followup"))
    refute inspect(completed) =~ "__jizoku_jido_result__"
  end

  test "output guardrails reject an emit before completion or outbox enqueue" do
    assert {:ok, runtime} =
             Test.start_runtime(
               workflow: GuardedInstructionWorkflow,
               guardrail_registry: %{"jido.output" => DenyOutput},
               now: @now
             )

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit_valid"})
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.recovered == true
    refute Map.has_key?(completed.context, :accepted)

    assert [%{status: :failed, error: %{code: "guardrail_failed"}} | _rest] =
             completed.attempts

    assert {:ok, agent} = WorkflowAgent.rebuild(runtime.storage, run.run_id)
    assert Outbox.items(Projection.jido_outbox(agent.state.projection)) == []
  end

  test "run instruction validation fails before completion without exposing directive data" do
    assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "run_instruction"})
    assert {:failed, failed} = Test.drain(runtime, run)

    assert failed.terminal_error == %{
             code: "jido_effect_rejected",
             message: "native Jido effect was rejected",
             retryable?: false
           }

    assert failed.dynamic_work == []
    refute inspect(persistence_state(runtime)) =~ "req-directive"
  end

  for kind <- ["run_instruction_custom_result", "run_instruction_meta"] do
    @invalid_run_instruction_kind kind

    test "#{kind} is rejected because Jizoku has no Jido agent callback state" do
      assert {:ok, runtime} =
               Test.start_runtime(
                 workflow: ExtrasWorkflow,
                 action_registry: %{"directive.followup" => FollowupAction},
                 now: @now
               )

      on_exit(fn -> Test.stop_runtime(runtime) end)

      assert {:ok, run} = Test.start(runtime, %{kind: @invalid_run_instruction_kind})
      assert {:failed, failed} = Test.drain(runtime, run)

      assert failed.terminal_error == %{
               code: "jido_effect_rejected",
               message: "native Jido effect was rejected",
               retryable?: false
             }

      assert failed.dynamic_work == []
      assert failed.applied_runnable_keys == []
      refute inspect(persistence_state(runtime)) =~ "directive-secret"
    end
  end

  test "ordinary error transitions may recover a native error directive" do
    assert {:ok, runtime} =
             Test.start_runtime(workflow: ErrorTransitionWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "error"})
    assert {:completed, completed} = Test.drain(runtime, run)
    assert completed.context.recovered == true
    refute Map.has_key?(completed.context, :accepted)

    assert Enum.map(completed.attempts, &{&1.step, &1.status}) == [
             {"jido_extras", :failed},
             {"recover", :completed}
           ]

    assert {:ok, timeline} = Test.timeline(runtime, run)
    assert Enum.count(timeline.events, &(&1.type == :attempt_failed)) == 1
    assert Enum.count(timeline.events, &(&1.type == :attempt_completed)) == 1
  end

  for {kind, directive_type, code, message} <- [
        {"error", :error, "jido_directive_error", "Jido action returned an error directive"},
        {"custom", :unsupported, "unsupported_jido_directive",
         "Jido action directives are not supported"}
      ] do
    @kind kind
    @directive_type directive_type
    @code code
    @message message

    test "#{kind} extras fail durably before applying the runnable" do
      assert {:ok, runtime} = Test.start_runtime(workflow: ExtrasWorkflow, now: @now)

      assert {:ok, run} = Test.start(runtime, %{kind: @kind})
      :persistent_term.put({ExtrasAction, run.run_id}, self())

      on_exit(fn ->
        :persistent_term.erase({ExtrasAction, run.run_id})
        Test.stop_runtime(runtime)
      end)

      assert {:failed, failed} = Test.drain(runtime, run)
      assert_receive {:extras_action_ran, run_id}
      assert run_id == run.run_id
      refute_receive {:extras_action_ran, _run_id}

      assert failed.applied_runnable_keys == []
      assert Enum.count(failed.attempts, &(&1.step == "jido_extras")) == 1

      assert failed.terminal_error == %{
               code: @code,
               message: @message,
               retryable?: false
             }

      assert {:ok, timeline} = Test.timeline(runtime, run)
      assert Enum.any?(timeline.events, &(&1.type == :attempt_failed))
      refute Enum.any?(timeline.events, &(&1.type == :runnable_applied))
      refute inspect(failed) =~ "directive-secret"
      refute inspect(timeline) =~ "directive-secret"

      before_loss = persistence_state(runtime)
      assert map_size(before_loss.checkpoints) > 0
      refute inspect(before_loss) =~ "directive-secret"
      assert :ok = Test.delete_checkpoints(runtime)
      after_loss = persistence_state(runtime)
      assert after_loss.checkpoints == %{}
      assert after_loss.threads == before_loss.threads

      assert {:ok, rebuilt} = Test.inspect(runtime, run)
      assert rebuilt.status == :failed
      assert rebuilt.applied_runnable_keys == []
      assert persistence_state(runtime) == after_loss

      assert {:ok, restarted} = Test.restart_runtime(runtime)
      on_exit(fn -> Test.stop_runtime(restarted) end)
      assert persistence_state(restarted) == after_loss

      assert {:failed, replayed} = Test.drain(restarted, run)
      assert replayed.terminal_error == failed.terminal_error
      assert replayed.applied_runnable_keys == []
      assert persistence_state(restarted) == after_loss
      refute_receive {:extras_action_ran, _run_id}

      assert {:error, normalization_error} = Directives.normalize(extras_for(@kind))
      assert normalization_error.code == @code

      if @code == "unsupported_jido_directive" do
        assert normalization_error.directive_types == [@directive_type]
      end
    end
  end

  test "malformed extras fail durably before applying the runnable" do
    assert {:ok, runtime} =
             Test.start_runtime(workflow: MalformedExtrasWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert {:failed, failed} = Test.drain(runtime, run)
    assert failed.applied_runnable_keys == []

    assert failed.terminal_error == %{
             code: "invalid_jido_action_extras",
             message: "Jido action extras must be a list",
             retryable?: false
           }

    refute inspect(failed) =~ "directive-secret"
  end

  defp extras_for("emit") do
    [%Directive.Emit{signal: %{}}]
  end

  defp extras_for("emit_valid") do
    {:ok, signal} =
      Jido.Signal.new("sample.order.accepted", %{"order_id" => "order-1"},
        id: "signal-order-1",
        source: "/minimal_host/orders"
      )

    [%Directive.Emit{signal: signal}]
  end

  defp extras_for("run_instruction") do
    instruction =
      Jido.Instruction.new!(
        id: "directive-followup",
        action: FollowupAction,
        params: %{value: "from-directive"}
      )

    [Directive.run_instruction(instruction)]
  end

  defp extras_for("error") do
    [%Directive.Error{error: %{}, context: :action}]
  end

  defp extras_for("custom") do
    [%{custom: true}]
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
