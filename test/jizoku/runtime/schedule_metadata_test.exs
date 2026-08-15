defmodule Jizoku.Runtime.ScheduleMetadataTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.ScheduleMetadata

  defmodule ScheduledDigestWorkflow do
    use Jizoku.Workflow

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

  test "uses the explicit runtime receive time" do
    received_at = ~U[2026-08-11 14:30:15.123456Z]

    assert {:ok, %{schedule: schedule}} =
             ScheduleMetadata.cron_context(
               ScheduledDigestWorkflow,
               trigger_definition(),
               %{"signal_id" => "digest-window"},
               received_at
             )

    assert schedule.received_at == "2026-08-11T14:30:15.123456Z"
  end

  test "rejects a non-cron trigger without raising" do
    trigger = %{trigger_definition() | type: :manual}

    assert {:error, {:invalid_schedule_trigger_type, :manual}} =
             ScheduleMetadata.cron_context(
               ScheduledDigestWorkflow,
               trigger,
               %{},
               ~U[2026-08-11 14:30:15Z]
             )
  end

  test "rejects an invalid explicit receive time without misclassifying the trigger" do
    assert {:error, {:invalid_schedule_received_at, :expected_datetime}} =
             ScheduleMetadata.cron_context(
               ScheduledDigestWorkflow,
               trigger_definition(),
               %{"signal_id" => "digest-window"},
               :invalid
             )
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
    |> Jizoku.Workflow.Definition.trigger(:scheduled_digest)
    |> elem(1)
  end
end
