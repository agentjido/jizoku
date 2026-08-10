defmodule Squidie.Runs.GraphInspection do
  @moduledoc """
  Compatibility graph inspection struct for workflow runs.

  `Squidie.Inspection.GraphInspection` is the canonical inspection namespace.
  This module preserves the original `Squidie.Runs.*` return shape for existing
  callers while delegating projection logic to the canonical implementation.
  """

  alias Squidie.Inspection
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.Runs.GraphInspection.Edge
  alias Squidie.Runs.GraphInspection.Node

  @type source :: :read_model

  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          workflow: module() | String.t() | nil,
          definition_version: String.t() | nil,
          source: source(),
          status: atom(),
          current_node_id: String.t() | nil,
          current_node_ids: [String.t()],
          terminal?: boolean(),
          nodes: [Node.t()],
          edges: [Edge.t()],
          child_runs: [map()],
          child_links: [map()],
          continuation: map(),
          continuation_links: [map()],
          dynamic_work: [map()],
          dynamic_work_overlays: [map()],
          graph_version: non_neg_integer(),
          graph_provenance: map(),
          active_node_ids: [String.t()],
          active_edge_ids: [String.t()],
          ready_node_ids: [String.t()],
          blocked_node_ids: [String.t()],
          tombstoned_node_ids: [String.t()],
          tombstoned_edge_ids: [String.t()],
          mutation_history: [map()],
          reconciliation_status: :not_required | :required | :completed | :unknown,
          anomalies: [map()]
        }

  @enforce_keys [:run_id, :workflow, :source, :status, :terminal?]

  defstruct [
    :run_id,
    :partition,
    :workflow,
    :definition_version,
    :source,
    :status,
    :current_node_id,
    :terminal?,
    graph_version: 0,
    graph_provenance: %{nodes: [], edges: []},
    active_node_ids: [],
    active_edge_ids: [],
    ready_node_ids: [],
    blocked_node_ids: [],
    tombstoned_node_ids: [],
    tombstoned_edge_ids: [],
    mutation_history: [],
    reconciliation_status: :not_required,
    child_runs: [],
    child_links: [],
    continuation: %{continued_from: nil, continued_to: nil},
    continuation_links: [],
    current_node_ids: [],
    nodes: [],
    edges: [],
    anomalies: [],
    dynamic_work: [],
    dynamic_work_overlays: []
  ]

  @doc """
  Builds compatibility graph inspection data from a run snapshot.
  """
  @spec from_snapshot(Snapshot.t(), keyword()) :: t()
  def from_snapshot(%Snapshot{} = snapshot, opts) when is_list(opts) do
    snapshot
    |> Inspection.GraphInspection.from_snapshot(opts)
    |> from_inspection_graph()
  end

  @doc """
  Converts graph inspection data into a stable map for host UI serializers.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = graph) do
    graph
    |> to_inspection_graph()
    |> Inspection.GraphInspection.to_map()
  end

  @doc """
  Converts a canonical inspection graph into the compatibility struct.
  """
  @spec from_inspection_graph(Inspection.GraphInspection.t()) :: t()
  def from_inspection_graph(%Inspection.GraphInspection{} = graph) do
    attrs =
      graph
      |> Map.from_struct()
      |> Map.update!(:nodes, &Enum.map(&1, fn node -> Node.from_inspection_node(node) end))
      |> Map.update!(:edges, &Enum.map(&1, fn edge -> Edge.from_inspection_edge(edge) end))

    struct!(__MODULE__, attrs)
  end

  @doc """
  Converts a compatibility graph into the canonical inspection struct.
  """
  @spec to_inspection_graph(t()) :: Inspection.GraphInspection.t()
  def to_inspection_graph(%__MODULE__{} = graph) do
    attrs =
      graph
      |> Map.from_struct()
      |> Map.update!(:nodes, &Enum.map(&1, fn node -> Node.to_inspection_node(node) end))
      |> Map.update!(:edges, &Enum.map(&1, fn edge -> Edge.to_inspection_edge(edge) end))

    struct!(Inspection.GraphInspection, attrs)
  end
end
