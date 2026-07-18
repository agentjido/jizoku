defmodule Squidie.GraphMutation.Reconciliation do
  @moduledoc """
  Result of repairing dispatch state from a run's durable graph facts.
  """

  alias Jido.Agent

  @type t :: %__MODULE__{
          run_id: String.t(),
          graph_version: non_neg_integer(),
          status: :reconciled,
          repaired_queue_ids: [String.t()],
          scheduled_node_ids: [String.t()],
          warnings: [atom()]
        }

  @enforce_keys [:run_id, :graph_version, :status]
  defstruct [
    :run_id,
    :graph_version,
    :status,
    repaired_queue_ids: [],
    scheduled_node_ids: [],
    warnings: []
  ]

  @doc false
  @spec new(Agent.t(), [map()]) :: t()
  def new(workflow_agent, queues) when is_list(queues) do
    %__MODULE__{
      run_id: workflow_agent.state.run_id,
      graph_version: workflow_agent.state.projection.graph.version,
      status: :reconciled,
      repaired_queue_ids: repaired_queue_ids(queues),
      scheduled_node_ids: scheduled_node_ids(queues)
    }
  end

  defp repaired_queue_ids(queues) do
    queues
    |> Enum.filter(&(&1.run_queued? or &1.scheduled_runnables != []))
    |> Enum.map(& &1.queue)
    |> Enum.sort()
  end

  defp scheduled_node_ids(queues) do
    queues
    |> Enum.flat_map(& &1.scheduled_runnables)
    |> Enum.map(&Map.fetch!(&1, :step))
    |> Enum.sort()
  end
end
