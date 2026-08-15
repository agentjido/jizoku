defmodule BedrockMinimalHostApp.JobQueue do
  @moduledoc """
  Bedrock-backed job queue for Jizoku delivery payloads and stress probes.
  """

  use Bedrock.JobQueue,
    otp_app: :bedrock_minimal_host_app,
    repo: BedrockMinimalHostApp.BedrockRepo,
    workers: %{
      "jizoku:payload" => BedrockMinimalHostApp.Jobs.JizokuPayload,
      "stress:probe" => BedrockMinimalHostApp.Jobs.StressProbe
    }
end
