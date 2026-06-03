defmodule MinimalHostApp.Verification.RestartResilience do
  @moduledoc """
  Repeatable restart and deploy resilience checks for the example host app.

  This harness focuses on the runtime guarantees Squidie claims today:
  queued work, delayed work, and retrying work should resume correctly after
  Oban restarts because durable run state and step jobs live in Postgres.
  """

  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.WorkflowRuns

  @poll_attempts 60

  @spec run!() :: %{
          queued_run: Squidie.ReadModel.Inspection.Snapshot.t(),
          delayed_run: Squidie.ReadModel.Inspection.Snapshot.t(),
          retry_run: Squidie.ReadModel.Inspection.Snapshot.t(),
          paused_run: Squidie.ReadModel.Inspection.Snapshot.t()
        }
  def run! do
    RuntimeHarness.ensure_runtime_started()

    %{
      queued_run: verify_queued_run_restart!(),
      delayed_run: verify_delayed_run_restart!(),
      retry_run: verify_retry_run_restart!(),
      paused_run: verify_paused_run_restart!()
    }
  end

  @spec verify_queued_run_restart!() :: Squidie.ReadModel.Inspection.Snapshot.t()
  defp verify_queued_run_restart! do
    {gateway_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn _attempt -> RuntimeHarness.success_gateway_response("ok") end,
        2
      )

    try do
      attrs = %{
        account_id: "acct_resilience_queue",
        invoice_id: "inv_resilience_queue",
        attempt_id: "attempt_resilience_queue",
        gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
      }

      {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)

      :ok = RuntimeHarness.restart_oban!()
      :ok = RuntimeHarness.wait_for_execution()

      {:ok, completed_run} =
        RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)

      unless completed_run.status == :completed do
        raise "expected queued run to complete after Oban restart"
      end

      completed_run
    after
      RuntimeHarness.stop_gateway_server(gateway_pid)
    end
  end

  @spec verify_delayed_run_restart!() :: Squidie.ReadModel.Inspection.Snapshot.t()
  defp verify_delayed_run_restart! do
    {:ok, run} = WorkflowRuns.start_cancellable_wait(%{account_id: "acct_resilience_delay"})

    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "wait_for_cancellation")

    {:ok, delayed_run} = WorkflowRuns.inspect_run(run.run_id)

    unless delayed_run.status == :running and scheduled_step?(delayed_run, "record_delivery") do
      raise "expected delayed run to be waiting on the next step"
    end

    :ok = RuntimeHarness.restart_oban!()
    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "record_delivery")

    {:ok, completed_run} =
      RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)

    unless completed_run.status == :completed do
      raise "expected delayed run to complete after restart"
    end

    completed_run
  end

  @spec verify_retry_run_restart!() :: Squidie.ReadModel.Inspection.Snapshot.t()
  defp verify_retry_run_restart! do
    {:ok, run} =
      WorkflowRuns.start_retry_verification(%{
        attempt_id: "attempt_resilience_retry"
      })

    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "exercise_retry")

    {:ok, retrying_run} = WorkflowRuns.inspect_run(run.run_id)

    unless retrying_run.status == :running and scheduled_step?(retrying_run, "exercise_retry") do
      raise "expected retrying run before restart"
    end

    :ok = RuntimeHarness.restart_oban!()
    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "exercise_retry")

    {:ok, completed_run} =
      RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)

    {:ok, history_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)
    retry_attempts = Enum.filter(history_run.attempts, &(Map.get(&1, :step) == "exercise_retry"))

    unless completed_run.status == :completed and length(retry_attempts) == 2 do
      raise "expected retried run to complete with two attempts"
    end

    completed_run
  end

  @spec verify_paused_run_restart!() :: Squidie.ReadModel.Inspection.Snapshot.t()
  defp verify_paused_run_restart! do
    {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_resilience_pause"})
    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "wait_for_approval")

    {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    unless paused_run.status == :paused and manual_step?(paused_run, "wait_for_approval") do
      raise "expected paused run before restart"
    end

    :ok = RuntimeHarness.restart_oban!()

    {:ok, resumed_run} =
      WorkflowRuns.approve(
        run.run_id,
        %{actor: "ops_restart", comment: "approved", metadata: %{ticket: "RESTART-1"}}
      )

    unless resumed_run.status == :running and visible_step?(resumed_run, "record_approval") do
      raise "expected resumed manual approval run after restart"
    end

    :ok = RuntimeHarness.wait_for_execution()

    {:ok, completed_run} =
      RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)

    {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    unless completed_run.status == :completed and
             completed_run.context.approval.status == "approved" do
      raise "expected paused run to complete after restart and unblock"
    end

    unless completed_history.context.approval.actor == "ops_restart" and
             completed_history.context.approval.metadata == %{ticket: "RESTART-1"} do
      raise "expected approval context to survive restart"
    end

    completed_history
  end

  defp scheduled_step?(run, step) do
    Enum.any?(run.scheduled_attempts, &(Map.get(&1, :step) == step))
  end

  defp visible_step?(run, step) do
    Enum.any?(run.visible_attempts, &(Map.get(&1, :step) == step))
  end

  defp manual_step?(%{manual_state: %{step: step}}, step), do: true
  defp manual_step?(_run, _step), do: false
end
