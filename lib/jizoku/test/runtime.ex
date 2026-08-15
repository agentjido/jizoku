defmodule Jizoku.Test.Runtime do
  @moduledoc false

  @derive {Inspect,
           except: [
             :action_registry,
             :guardrail_registry,
             :jido_dispatch_routes,
             :test_action_stub_after_consume
           ]}
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
    jido_dispatch_routes: nil,
    test_action_stub_after_consume: nil
  ]

  @type t :: %__MODULE__{
          id: Ecto.UUID.t(),
          owner: pid(),
          workflow: module() | Jizoku.Workflow.Spec.t() | map(),
          storage: Jizoku.Runtime.Journal.Storage.config(),
          storage_server: pid(),
          queue: String.t(),
          partition: String.t() | nil,
          max_steps: pos_integer(),
          action_registry: map(),
          action_stub_keys: [Jizoku.Workflow.ActionRegistry.action_key()],
          guardrail_registry: term() | nil,
          jido_dispatch_routes: map() | nil,
          test_action_stub_after_consume: (map() -> :ok) | nil
        }
end
