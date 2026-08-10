defmodule Squidie.ReadModel.ContinuationChain do
  @moduledoc """
  Bounded, read-only traversal of durable continue-as-new lineage.

  Traversal loads at most `max_hops + 1` run projections and never follows
  child-run or replay lineage. The default limit is configured through
  `config :squidie, :continuation_history`.
  """

  alias Jido.Agent
  alias Squidie.ReadModel.HistoryPolicy
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  @type direction :: :backward | :forward
  @type run_summary :: %{
          required(:run_id) => String.t(),
          required(:definition_version) => String.t() | nil,
          required(:status) => atom(),
          required(:terminal_status) => atom() | nil,
          required(:thread_revision) => non_neg_integer(),
          required(:continued_from) => map() | nil,
          required(:continued_to) => map() | nil
        }
  @type t :: %__MODULE__{
          run_id: String.t(),
          partition: String.t() | nil,
          direction: direction(),
          max_hops: pos_integer(),
          hops: non_neg_integer(),
          truncated?: boolean(),
          runs: [run_summary()],
          warnings: [map()]
        }

  @enforce_keys [:run_id, :direction, :max_hops, :hops, :truncated?, :runs]
  defstruct [
    :run_id,
    :partition,
    :direction,
    :max_hops,
    :hops,
    :truncated?,
    runs: [],
    warnings: []
  ]

  @doc """
  Traverses continuation lineage from one run with an explicit upper bound.

  `:direction` defaults to `:backward`, which follows predecessor links.
  `:max_hops` defaults to the configured continuation-history limit.
  """
  @spec inspect(Squidie.Runtime.Journal.storage_config(), String.t(), keyword()) ::
          {:ok, t()} | {:error, term()}
  def inspect(storage, run_id, opts \\ [])

  def inspect(storage, run_id, opts) when is_binary(run_id) and is_list(opts) do
    with :ok <- validate_options(opts),
         {:ok, run_id} <- Options.thread_part(run_id, :run_id),
         {:ok, direction} <- direction(opts),
         {:ok, max_hops} <- max_hops(opts),
         {:ok, runs, truncated?} <- traverse(storage, run_id, direction, max_hops) do
      hops = max(length(runs) - 1, 0)

      {:ok,
       %__MODULE__{
         run_id: run_id,
         partition: Squidie.Runtime.Journal.Storage.partition(storage),
         direction: direction,
         max_hops: max_hops,
         hops: hops,
         truncated?: truncated?,
         runs: runs,
         warnings: chain_warnings(hops)
       }}
    end
  end

  def inspect(_storage, run_id, _opts) when is_binary(run_id) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  def inspect(_storage, _run_id, _opts) do
    {:error, {:invalid_option, {:run_id, :invalid}}}
  end

  @spec traverse(
          Squidie.Runtime.Journal.storage_config(),
          String.t(),
          direction(),
          non_neg_integer()
        ) :: {:ok, [run_summary()], boolean()} | {:error, term()}
  defp traverse(storage, run_id, direction, max_hops) do
    do_traverse(storage, run_id, direction, max_hops, %{}, [])
  end

  @spec do_traverse(
          Squidie.Runtime.Journal.storage_config(),
          String.t(),
          direction(),
          non_neg_integer(),
          %{optional(String.t()) => true},
          [run_summary()]
        ) :: {:ok, [run_summary()], boolean()} | {:error, term()}
  defp do_traverse(storage, run_id, direction, remaining_hops, seen, runs) do
    if Map.has_key?(seen, run_id) do
      {:error, {:continuation_cycle, run_id}}
    else
      with {:ok, %Agent{state: state}} <- WorkflowAgent.rebuild(storage, run_id) do
        continuation = Projection.continuation(state.projection)
        next_edge = next_edge(continuation, direction)
        run = run_summary(state, continuation)
        runs = [run | runs]

        continue_traversal(
          storage,
          next_edge,
          direction,
          remaining_hops,
          Map.put(seen, run_id, true),
          runs
        )
      end
    end
  end

  defp continue_traversal(_storage, nil, _direction, _remaining_hops, _seen, runs) do
    {:ok, Enum.reverse(runs), false}
  end

  defp continue_traversal(_storage, _next_edge, _direction, 0, _seen, runs) do
    {:ok, Enum.reverse(runs), true}
  end

  defp continue_traversal(storage, next_edge, direction, remaining_hops, seen, runs) do
    do_traverse(
      storage,
      next_edge.run_id,
      direction,
      remaining_hops - 1,
      seen,
      runs
    )
  end

  defp run_summary(state, continuation) do
    %{
      run_id: state.run_id,
      definition_version: state.projection.definition_version,
      status: Projection.status(state.projection),
      terminal_status: Projection.terminal_status(state.projection),
      thread_revision: state.thread_rev,
      continued_from: continuation.continued_from,
      continued_to: continuation.continued_to
    }
  end

  defp next_edge(continuation, :backward) do
    continuation.continued_from
  end

  defp next_edge(continuation, :forward) do
    continuation.continued_to
  end

  defp direction(opts) do
    case Keyword.get(opts, :direction, :backward) do
      direction when direction in [:backward, :forward] -> {:ok, direction}
      _invalid -> {:error, {:invalid_option, {:direction, :invalid}}}
    end
  end

  defp validate_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in [:direction, :max_hops])) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  defp max_hops(opts) do
    case Keyword.get(opts, :max_hops, HistoryPolicy.max_chain_hops()) do
      max_hops when is_integer(max_hops) and max_hops > 0 -> {:ok, max_hops}
      _invalid -> {:error, {:invalid_option, {:max_hops, :invalid}}}
    end
  end

  defp chain_warnings(hops) do
    threshold = HistoryPolicy.chain_warning_hops()

    if hops >= threshold do
      [%{code: :large_continuation_chain, hops: hops, threshold: threshold}]
    else
      []
    end
  end
end
