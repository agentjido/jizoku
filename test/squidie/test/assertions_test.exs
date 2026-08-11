defmodule Squidie.Test.AssertionsTest do
  use ExUnit.Case, async: true

  alias Squidie.Test

  defmodule SuccessStep do
    use Squidie.Step, name: :success

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{done: true}}
    end
  end

  defmodule SuccessWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :success, Squidie.Test.AssertionsTest.SuccessStep
      transition :success, on: :ok, to: :complete
    end
  end

  defmodule FailureStep do
    use Squidie.Step, name: :failure

    @impl Squidie.Step
    def run(%{secret: secret}, _context) do
      {:error,
       %{
         code: "expected_failure",
         message: "failure included #{secret}"
       }}
    end
  end

  defmodule FailureWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :secret, :string, required: true
        end
      end

      step :failure, Squidie.Test.AssertionsTest.FailureStep
      transition :failure, on: :ok, to: :complete
    end
  end

  defmodule FirstStep do
    use Squidie.Step, name: :first

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{first: true}}
    end
  end

  defmodule SecondStep do
    use Squidie.Step, name: :second

    @impl Squidie.Step
    def run(_input, _context) do
      {:ok, %{second: true}}
    end
  end

  defmodule BoundedWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :first, Squidie.Test.AssertionsTest.FirstStep
      step :second, Squidie.Test.AssertionsTest.SecondStep
      transition :first, on: :ok, to: :second
      transition :second, on: :ok, to: :complete
    end
  end

  test "returns the matching durable snapshot" do
    assert {:ok, runtime} = Test.start_runtime(workflow: SuccessWorkflow)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    assert completed = Test.assert_status(runtime, run, :completed)
    assert completed.status == :completed
    assert completed.context.done
  end

  test "raises a concise assertion without timeline output by default" do
    assert {:ok, runtime} = Test.start_runtime(workflow: FailureWorkflow)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{secret: "top-secret-value"})

    error =
      assert_raise ExUnit.AssertionError, fn ->
        Test.assert_status(runtime, run, :completed)
      end

    assert error.message =~ "expected Squidie workflow status :completed, got :failed"
    assert error.message =~ "reason: :terminal"
    refute error.message =~ "timeline"
    refute error.message =~ "top-secret-value"
  end

  test "adds a redacted golden timeline when explicitly requested" do
    assert {:ok, runtime} = Test.start_runtime(workflow: FailureWorkflow)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{secret: "top-secret-value"})
    assert {:failed, _snapshot} = Test.drain(runtime, run)
    before_assertion = persistence_state(runtime)

    error =
      assert_raise ExUnit.AssertionError, fn ->
        Test.assert_status(runtime, run, :completed, diagnostics: :timeline)
      end

    assert error.message =~ "timeline (schema v1)"
    assert error.message =~ "attempt_failed step=failure runnable=runnable-1"
    assert error.message =~ "run_terminal"
    refute error.message =~ "top-secret-value"
    refute error.message =~ "failure included"
    assert persistence_state(runtime) == before_assertion
  end

  test "rejects malformed assertion options before execution" do
    assert {:ok, runtime} = Test.start_runtime(workflow: SuccessWorkflow)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})
    before_assertion = persistence_state(runtime)

    assert_raise ArgumentError, ~r/diagnostics/, fn ->
      Test.assert_status(runtime, run, :completed, diagnostics: :explanation)
    end

    assert persistence_state(runtime) == before_assertion
  end

  test "reports a redacted partial timeline when bounded execution is exhausted" do
    assert {:ok, runtime} = Test.start_runtime(workflow: BoundedWorkflow)
    on_exit(fn -> Test.stop_runtime(runtime) end)

    assert {:ok, run} = Test.start(runtime, %{})

    error =
      assert_raise ExUnit.AssertionError, fn ->
        Test.assert_status(runtime, run, :completed,
          diagnostics: :timeline,
          max_steps: 1
        )
      end

    assert error.message =~ "got :running"
    assert error.message =~ "reason: :execution_limit_reached"
    assert error.message =~ "timeline (schema v1)"
    assert error.message =~ "attempt_completed step=first runnable=runnable-1"
  end

  test "sanitizes failures that cannot return a snapshot or timeline" do
    assert {:ok, runtime} = Test.start_runtime(workflow: SuccessWorkflow)
    assert {:ok, run} = Test.start(runtime, %{})
    assert :ok = Test.stop_runtime(runtime)

    error =
      assert_raise ExUnit.AssertionError, fn ->
        Test.assert_status(runtime, run, :completed, diagnostics: :timeline)
      end

    assert error.message =~ "execution did not return a snapshot"
    assert error.message =~ "reason: :execution_error"
    assert error.message =~ "timeline unavailable"
    refute error.message =~ "runtime_stopped"
  end

  defp persistence_state(runtime) do
    runtime.storage_server
    |> :sys.get_state()
    |> Map.take([:checkpoints, :threads])
  end
end
