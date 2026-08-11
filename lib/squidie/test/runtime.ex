defmodule Squidie.Test.Runtime do
  @moduledoc false

  @derive {Inspect,
           except: [:action_registry, :guardrail_registry, :test_action_stub_after_consume]}
  @enforce_keys [:id, :owner, :workflow, :storage, :storage_server, :queue, :max_steps]
  defstruct [
    :id,
    :owner,
    :workflow,
    :storage,
    :storage_server,
    :queue,
    :partition,
    :max_steps,
    action_registry: %{},
    action_stub_keys: [],
    guardrail_registry: nil,
    test_action_stub_after_consume: nil
  ]

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          owner: pid(),
          workflow: module() | Squidie.Workflow.Spec.t() | map(),
          storage: Squidie.Runtime.Journal.Storage.config(),
          storage_server: pid(),
          queue: String.t(),
          partition: String.t() | nil,
          max_steps: pos_integer(),
          action_registry: map(),
          action_stub_keys: [Squidie.Workflow.ActionRegistry.action_key()],
          guardrail_registry: term() | nil,
          test_action_stub_after_consume: (map() -> :ok) | nil
        }
end
