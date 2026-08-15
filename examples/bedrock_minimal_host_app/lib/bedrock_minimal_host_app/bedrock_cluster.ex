defmodule BedrockMinimalHostApp.BedrockCluster do
  @moduledoc """
  Embedded Bedrock cluster used by the Bedrock-backed example app.
  """

  # Local spike storage only. Production hosts should configure durable
  # Bedrock paths or cluster topology instead of relying on the temp directory.
  use Bedrock.Cluster,
    otp_app: :bedrock_minimal_host_app,
    name: "bedrock_minimal_host_app",
    config: [
      capabilities: [:coordination, :log, :materializer],
      durability_mode: :relaxed,
      trace: [],
      coordinator: [path: Path.join(System.tmp_dir!(), "bedrock_minimal_host_app")],
      materializer: [path: Path.join(System.tmp_dir!(), "bedrock_minimal_host_app")],
      log: [path: Path.join(System.tmp_dir!(), "bedrock_minimal_host_app")]
    ]
end
