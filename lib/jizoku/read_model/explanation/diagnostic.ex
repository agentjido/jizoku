defmodule Jizoku.ReadModel.Explanation.Diagnostic do
  @moduledoc """
  Deterministic explanation built from a projection-backed inspection snapshot.

  It describes what the Jido-native journals prove right now and which runtime
  boundary would make forward progress, while leaving mutation to recovery or
  dispatch modules.
  """

  alias Jizoku.ReadModel.Inspection.Snapshot

  @type next_action ::
          :schedule_pending_dispatch
          | :apply_pending_result
          | :recover_expired_claim
          | :wait_for_worker_claim
          | :wait_until_attempt_visible
          | :wait_for_attempt_completion
          | :resolve_manual_step
          | :inspect_continuation_successor
          | :inspect_terminal_run
          | :wait_for_new_runnables
          | :inspect_dispatch_state
          | :restore_historical_workflow_version
          | :restore_exact_workflow_definition
          | :verify_workflow_histories
          | :replay_run_after_restore

  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          workflow: String.t() | nil,
          definition_version: String.t() | nil,
          queue: String.t(),
          status: atom(),
          reason: Snapshot.reason(),
          step: String.t() | nil,
          summary: String.t(),
          details: map(),
          next_actions: [next_action()],
          evidence: map()
        }

  @enforce_keys [
    :run_id,
    :workflow,
    :definition_version,
    :queue,
    :status,
    :reason,
    :step,
    :summary,
    :details,
    :next_actions,
    :evidence
  ]

  defstruct [
    :run_id,
    :partition,
    :workflow,
    :definition_version,
    :queue,
    :status,
    :reason,
    :step,
    :summary,
    :details,
    next_actions: [],
    evidence: %{}
  ]
end
