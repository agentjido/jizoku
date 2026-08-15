defmodule BedrockMinimalHostApp.Workflows.RetryVerification do
  @moduledoc """
  Example workflow used by the production-readiness harness to verify retries.
  """

  use Jizoku.Workflow

  workflow do
    trigger :retry_verification do
      manual()

      payload do
        field :attempt_id, :string
      end
    end

    step :exercise_retry, BedrockMinimalHostApp.Steps.FailOnce,
      retry: [max_attempts: 3, backoff: [type: :exponential, min: 1_000, max: 1_000]]

    transition :exercise_retry, on: :ok, to: :complete
  end
end
