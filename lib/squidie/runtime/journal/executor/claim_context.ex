defmodule Squidie.Runtime.Journal.Executor.ClaimContext do
  @moduledoc false

  alias Jido.Agent
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt

  defstruct [:dispatch_agent, :workflow_agent, :attempt, :claim_id, :claim_token]

  @type t :: %__MODULE__{
          dispatch_agent: Agent.t(),
          workflow_agent: Agent.t(),
          attempt: ActionAttempt.t(),
          claim_id: String.t(),
          claim_token: String.t()
        }

  @doc """
  Builds an executor claim context from the dispatch and workflow agents.
  """
  @spec new(Agent.t(), Agent.t(), ActionAttempt.t(), String.t(), String.t()) :: t()
  def new(
        %Agent{} = dispatch_agent,
        %Agent{} = workflow_agent,
        %ActionAttempt{} = attempt,
        claim_id,
        claim_token
      )
      when is_binary(claim_id) and is_binary(claim_token) do
    %__MODULE__{
      dispatch_agent: dispatch_agent,
      workflow_agent: workflow_agent,
      attempt: attempt,
      claim_id: claim_id,
      claim_token: claim_token
    }
  end
end
