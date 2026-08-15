defmodule BedrockMinimalHostApp.Workflows.ManualPause do
  @moduledoc """
  Example workflow that waits for an explicit operator resume.
  """

  use Jizoku.Workflow

  workflow do
    trigger :manual_pause do
      manual()

      payload do
        field :account_id, :string
      end
    end

    step :wait_for_resume, :pause
    step :record_resume, :log, message: "resume recorded", level: :info

    transition :wait_for_resume, on: :ok, to: :record_resume
    transition :record_resume, on: :ok, to: :complete
  end
end
