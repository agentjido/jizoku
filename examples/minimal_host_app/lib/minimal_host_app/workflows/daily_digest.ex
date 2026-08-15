defmodule MinimalHostApp.Workflows.DailyDigest do
  @moduledoc """
  Example workflow with manual and cron triggers sharing one graph.
  """

  use Jizoku.Workflow

  workflow do
    trigger :manual_digest do
      manual()

      payload do
        field :channel, :string
        field :digest_date, :string
      end
    end

    trigger :daily_digest do
      cron "@reboot", timezone: "Etc/UTC", idempotency: :return_existing_run

      payload do
        field :channel, :string, default: "ops"
        field :digest_date, :string, default: {:today, :iso8601}
      end
    end

    step :announce_digest, :log, message: "posting daily digest"
    step :record_digest_delivery, MinimalHostApp.Steps.RecordDigestDelivery

    transition :announce_digest, on: :ok, to: :record_digest_delivery
    transition :record_digest_delivery, on: :ok, to: :complete
  end
end
