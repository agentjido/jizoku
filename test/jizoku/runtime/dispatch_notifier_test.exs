defmodule Jizoku.Runtime.DispatchNotifierTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.DispatchNotifier

  defmodule RaisingNotifier do
    @behaviour DispatchNotifier

    @impl DispatchNotifier
    def notify_attempt_scheduled(_attempt, _opts), do: raise("notifier failed")
  end

  defmodule ThrowingNotifier do
    @behaviour DispatchNotifier

    @impl DispatchNotifier
    def notify_attempt_scheduled(_attempt, _opts), do: throw(:notifier_failed)
  end

  @attempt %{
    run_id: "run_123",
    runnable_key: "run_123:charge_card:1",
    queue: "default",
    visible_at: ~U[2026-05-15 00:00:10Z]
  }

  test "returns a structured error when notifier does not implement the callback" do
    assert {:error, {:invalid_notifier, String}} =
             DispatchNotifier.notify_attempt_scheduled(String, @attempt, [])
  end

  test "noop notifier accepts scheduled attempt wakeups" do
    assert :ok =
             DispatchNotifier.notify_attempt_scheduled(
               DispatchNotifier.Noop,
               @attempt,
               []
             )
  end

  test "returns a structured error when notifier raises" do
    assert {:error,
            {:notifier_exception, RaisingNotifier, %RuntimeError{message: "notifier failed"}}} =
             DispatchNotifier.notify_attempt_scheduled(RaisingNotifier, @attempt, [])
  end

  test "returns a structured error when notifier throws" do
    assert {:error, {:notifier_throw, ThrowingNotifier, {:throw, :notifier_failed}}} =
             DispatchNotifier.notify_attempt_scheduled(ThrowingNotifier, @attempt, [])
  end

  test "returns structured errors for invalid notifier arguments" do
    assert {:error, {:invalid_notifier, "notifier"}} =
             DispatchNotifier.notify_attempt_scheduled("notifier", @attempt, [])

    assert {:error, {:invalid_attempt, :attempt}} =
             DispatchNotifier.notify_attempt_scheduled(DispatchNotifier.Noop, :attempt, [])

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             DispatchNotifier.notify_attempt_scheduled(DispatchNotifier.Noop, @attempt, :opts)
  end
end
