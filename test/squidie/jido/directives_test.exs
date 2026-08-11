defmodule Squidie.Jido.DirectivesTest do
  use ExUnit.Case, async: true

  alias Jido.Agent.Directive
  alias Squidie.Runtime.Jido.Directives
  alias Squidie.Test

  defmodule ExtrasAction do
    use Jido.Action,
      name: "jido_extras",
      description: "Returns Jido action extras",
      schema: [kind: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{kind: kind}, context) do
      if test_pid = :persistent_term.get({__MODULE__, context.run_id}, nil) do
        send(test_pid, {:extras_action_ran, context.run_id})
      end

      {:ok, %{accepted: true}, extras(kind)}
    end

    defp extras("emit") do
      [%Directive.Emit{signal: %{secret: "directive-secret"}}]
    end

    defp extras("run_instruction") do
      [
        %Directive.RunInstruction{
          instruction: %{secret: "directive-secret"},
          result_action: :record_result,
          meta: %{}
        }
      ]
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
    use Squidie.Step, name: :recover_directive_failure

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{recovered: true}}
    end
  end

  defmodule ExtrasWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction
      transition :jido_extras, on: :ok, to: :complete
    end
  end

  defmodule MalformedExtrasWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :jido_extras, MalformedExtrasAction
      transition :jido_extras, on: :ok, to: :complete
    end
  end

  defmodule ErrorTransitionWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :kind, :string
        end
      end

      step :jido_extras, ExtrasAction
      step :recover, RecoveryStep

      transition :jido_extras, on: :error, to: :recover
      transition :recover, on: :ok, to: :complete
    end
  end

  @now ~U[2026-08-11 12:00:00.000000Z]

  describe "normalize/1" do
    test "preserves the compatible empty extras result" do
      assert {:ok, []} = Directives.normalize([])
    end

    test "classifies supported future directive types without exposing their contents" do
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

  test "ordinary error transitions may recover an unsupported directive failure" do
    assert {:ok, runtime} =
             Test.start_runtime(workflow: ErrorTransitionWorkflow, now: @now)

    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{kind: "emit"})
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

  for {kind, directive_type} <- [
        {"emit", :emit},
        {"run_instruction", :run_instruction},
        {"error", :error},
        {"custom", :unsupported}
      ] do
    @kind kind
    @directive_type directive_type

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

      assert failed.applied_runnable_keys == []

      assert failed.terminal_error == %{
               code: "unsupported_jido_directive",
               message: "Jido action directives are not supported",
               retryable?: false
             }

      assert {:ok, timeline} = Test.timeline(runtime, run)
      assert Enum.any?(timeline.events, &(&1.type == :attempt_failed))
      refute Enum.any?(timeline.events, &(&1.type == :runnable_applied))
      refute inspect(failed) =~ "directive-secret"
      refute inspect(timeline) =~ "directive-secret"

      before_loss = persistence_state(runtime)
      assert map_size(before_loss.checkpoints) > 0
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

      assert {:error, %{directive_types: [@directive_type]}} =
               Directives.normalize(extras_for(@kind))
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

  defp extras_for("run_instruction") do
    [
      %Directive.RunInstruction{
        instruction: %{},
        result_action: :record_result,
        meta: %{}
      }
    ]
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
