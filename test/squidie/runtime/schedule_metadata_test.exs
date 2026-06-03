defmodule Squidie.Runtime.ScheduleMetadataTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.ScheduleMetadata

  defmodule ScheduledDigestWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC", idempotency: :return_existing_run
      end

      step :deliver_digest, ScheduledDigestWorkflow.DeliverDigest
    end
  end

  test "adds an idempotency key when cron idempotency is enabled" do
    trigger = trigger_definition()

    assert {:ok, %{schedule: schedule}} =
             ScheduleMetadata.cron_context(ScheduledDigestWorkflow, trigger, %{
               "intended_window" => %{
                 "start_at" => "2026-05-16T09:00:00Z",
                 "end_at" => "2026-05-16T10:00:00Z"
               }
             })

    assert schedule.idempotency == :return_existing_run
    assert schedule.idempotency_key == schedule.signal_id
  end

  test "rejects idempotent cron starts without a scheduler identity" do
    trigger = trigger_definition()

    assert {:error, {:missing_schedule_idempotency_key, :scheduled_digest}} =
             ScheduleMetadata.cron_context(ScheduledDigestWorkflow, trigger, %{})
  end

  test "rejects non-string intended window bounds" do
    trigger = trigger_definition()

    assert {:error, {:invalid_schedule_intended_window, %{start_at: 123}}} =
             ScheduleMetadata.cron_context(ScheduledDigestWorkflow, trigger, %{
               "signal_id" => "digest-2026-05-16T09",
               "intended_window" => %{
                 "start_at" => 123,
                 "end_at" => "2026-05-16T10:00:00Z"
               }
             })
  end

  defp trigger_definition do
    ScheduledDigestWorkflow.workflow_definition()
    |> Squidie.Workflow.Definition.trigger(:scheduled_digest)
    |> elem(1)
  end
end
