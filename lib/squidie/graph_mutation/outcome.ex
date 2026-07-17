defmodule Squidie.GraphMutation.Outcome do
  @moduledoc """
  Shared contract generator for graph mutation previews and reports.

  Outcome structs store only redacted operation summaries and stable mutation
  identity and version fields.
  """

  @doc """
  Defines a graph mutation outcome struct for the supplied statuses.
  """
  defmacro __using__(opts) do
    statuses = Keyword.fetch!(opts, :statuses)
    graph_state? = Keyword.get(opts, :graph_state?, false)

    status_type =
      Enum.reduce(statuses, fn status, union ->
        {:|, [], [status, union]}
      end)

    quote do
      alias Squidie.GraphMutation
      alias Squidie.GraphMutation.Operation

      @type status :: unquote(status_type)
      @type reconciliation :: :not_required | :required | :completed
      @type operation_summary :: map()

      @enforce_keys [
        :mutation_id,
        :expected_version,
        :base_version,
        :result_version,
        :status
      ]

      if unquote(graph_state?) do
        @type t :: %__MODULE__{
                mutation_id: String.t(),
                expected_version: non_neg_integer(),
                base_version: non_neg_integer(),
                result_version: non_neg_integer(),
                duplicate?: boolean(),
                status: status(),
                applied_operations: [operation_summary()],
                active_node_ids: [String.t()],
                ready_node_ids: [String.t()],
                blocked_node_ids: [String.t()],
                tombstoned_node_ids: [String.t()],
                reconciliation: reconciliation(),
                warnings: [atom()]
              }

        defstruct [
          :mutation_id,
          :expected_version,
          :base_version,
          :result_version,
          :status,
          duplicate?: false,
          applied_operations: [],
          active_node_ids: [],
          ready_node_ids: [],
          blocked_node_ids: [],
          tombstoned_node_ids: [],
          reconciliation: :not_required,
          warnings: []
        ]
      else
        @type t :: %__MODULE__{
                mutation_id: String.t(),
                expected_version: non_neg_integer(),
                base_version: non_neg_integer(),
                result_version: non_neg_integer(),
                duplicate?: boolean(),
                status: status(),
                applied_operations: [operation_summary()],
                reconciliation: reconciliation(),
                warnings: [atom()]
              }

        defstruct [
          :mutation_id,
          :expected_version,
          :base_version,
          :result_version,
          :status,
          duplicate?: false,
          applied_operations: [],
          reconciliation: :not_required,
          warnings: []
        ]
      end

      @doc """
      Builds an outcome from a normalized mutation and outcome attributes.
      """
      @spec new(GraphMutation.t(), keyword()) :: t()
      def new(%GraphMutation{} = mutation, attrs) when is_list(attrs) do
        attrs
        |> Map.new()
        |> Map.update(:applied_operations, [], &operation_summaries/1)
        |> Map.put(:mutation_id, mutation.mutation_id)
        |> Map.put(:expected_version, mutation.expected_version)
        |> then(&struct!(__MODULE__, &1))
      end

      @doc """
      Converts an outcome to a redacted plain map.
      """
      @spec to_map(t()) :: map()
      def to_map(%__MODULE__{} = outcome) do
        Map.from_struct(outcome)
      end

      defp operation_summaries(operations) do
        Enum.map(operations, fn {mode, operation} ->
          Operation.to_public_map(operation, mode)
        end)
      end
    end
  end
end
