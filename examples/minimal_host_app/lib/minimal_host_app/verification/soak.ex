defmodule MinimalHostApp.Verification.Soak do
  @moduledoc """
  Bounded soak and load validation for the example host app.

  This is intentionally not a benchmark. It is a repeatable verification pass
  that drives multiple durable runs through success, retry, replay, and
  cancellation paths to catch obvious stability regressions before broader
  production claims are made.
  """

  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.WorkflowRuns

  @terminal_poll_attempts 80
  @load_run_count 12
  @retry_run_count 3
  @cancellation_run_count 3

  @spec run!() :: map()
  def run! do
    RuntimeHarness.ensure_runtime_started()

    started_at = System.monotonic_time()

    {success_runs, gateway_pid} = run_success_batch!()

    try do
      replay_runs = run_replays!(success_runs)
      retry_runs = run_retry_batch!()
      cancelled_runs = run_cancellation_batch!()

      duration_ms =
        System.convert_time_unit(System.monotonic_time() - started_at, :native, :millisecond)

      %{
        successful_runs: length(success_runs),
        replayed_runs: length(replay_runs),
        retried_runs: length(retry_runs),
        cancelled_runs: length(cancelled_runs),
        duration_ms: duration_ms
      }
    after
      RuntimeHarness.stop_gateway_server(gateway_pid)
    end
  end

  @spec run_success_batch!() :: {[Squidie.ReadModel.Inspection.Snapshot.t()], pid()}
  defp run_success_batch! do
    {gateway_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn _attempt -> RuntimeHarness.success_gateway_response("ok") end,
        @load_run_count * 4
      )

    runs =
      1..@load_run_count
      |> Task.async_stream(
        fn index ->
          attrs = %{
            account_id: "acct_soak_success_#{index}",
            invoice_id: "inv_soak_success_#{index}",
            attempt_id: "attempt_soak_success_#{index}",
            gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
          }

          {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)
          run
        end,
        timeout: 15_000
      )
      |> Enum.map(fn {:ok, run} -> run end)

    :ok = RuntimeHarness.wait_for_execution()

    completed_runs =
      Enum.map(runs, fn run ->
        {:ok, completed_run} =
          RuntimeHarness.await_terminal_run(run.run_id, attempts: @terminal_poll_attempts)

        unless completed_run.status == :completed do
          raise "expected successful soak run to complete"
        end

        completed_run
      end)

    {completed_runs, gateway_pid}
  end

  @spec run_replays!([Squidie.ReadModel.Inspection.Snapshot.t()]) :: [
          Squidie.ReadModel.Inspection.Snapshot.t()
        ]
  defp run_replays!(successful_runs) do
    successful_runs
    |> Enum.take(2)
    |> Enum.map(fn run ->
      {:ok, replay_run} = WorkflowRuns.replay(run.run_id)
      :ok = RuntimeHarness.wait_for_execution()

      {:ok, completed_replay} =
        RuntimeHarness.await_terminal_run(replay_run.run_id, attempts: @terminal_poll_attempts)

      unless completed_replay.status == :completed and
               completed_replay.replayed_from_run_id == run.run_id do
        raise "expected replayed soak run to complete"
      end

      completed_replay
    end)
  end

  @spec run_retry_batch!() :: [Squidie.ReadModel.Inspection.Snapshot.t()]
  defp run_retry_batch! do
    1..@retry_run_count
    |> Enum.map(fn index ->
      run_retry_scenario!("attempt_soak_retry_#{index}")
    end)
  end

  @spec run_retry_scenario!(String.t()) :: Squidie.ReadModel.Inspection.Snapshot.t()
  defp run_retry_scenario!(attempt_id) do
    {:ok, run} = WorkflowRuns.start_retry_verification(%{attempt_id: attempt_id})

    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "exercise_retry")
    :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "exercise_retry")

    {:ok, completed_run} =
      RuntimeHarness.await_terminal_run(run.run_id, attempts: @terminal_poll_attempts)

    unless completed_run.status == :completed do
      raise "expected retry soak run to complete"
    end

    completed_run
  end

  @spec run_cancellation_batch!() :: [Squidie.ReadModel.Inspection.Snapshot.t()]
  defp run_cancellation_batch! do
    1..@cancellation_run_count
    |> Enum.map(fn index ->
      {:ok, run} = WorkflowRuns.start_cancellable_wait(%{account_id: "acct_soak_cancel_#{index}"})
      :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "wait_for_cancellation")
      {:ok, cancelling_run} = WorkflowRuns.cancel(run.run_id)

      unless cancelling_run.status == :cancelling do
        raise "expected cancellation soak run to enter cancelling"
      end

      :ok = RuntimeHarness.perform_scheduled_step!(run.run_id, "record_delivery")

      {:ok, cancelled_run} =
        RuntimeHarness.await_terminal_run(run.run_id, attempts: @terminal_poll_attempts)

      unless cancelled_run.status == :cancelled do
        raise "expected cancellation soak run to converge to cancelled"
      end

      cancelled_run
    end)
  end
end
