defmodule BedrockMinimalHostApp.JobQueue do
  @moduledoc """
  Bedrock-backed job queue for Squidie delivery payloads and stress probes.
  """

  use Bedrock.JobQueue,
    otp_app: :bedrock_minimal_host_app,
    repo: BedrockMinimalHostApp.BedrockRepo,
    workers: %{
      "squidie:payload" => BedrockMinimalHostApp.Jobs.SquidiePayload,
      "stress:probe" => BedrockMinimalHostApp.Jobs.StressProbe
    }
end
