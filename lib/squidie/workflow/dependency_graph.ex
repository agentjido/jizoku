defmodule Squidie.Workflow.DependencyGraph do
  @moduledoc false

  @doc """
  Normalizes a step option list into dependency step names.
  """
  @spec dependency_list(keyword()) :: {:ok, [atom()]} | :absent | :error
  def dependency_list(opts) do
    case Keyword.fetch(opts, :after) do
      {:ok, []} ->
        :error

      {:ok, dependencies} when is_list(dependencies) ->
        if Enum.all?(dependencies, &is_atom/1) do
          {:ok, Enum.uniq(dependencies)}
        else
          :error
        end

      {:ok, _other} ->
        :error

      :error ->
        :absent
    end
  end

  @doc """
  Returns true when a dependency adjacency map has no cycle.
  """
  @spec acyclic?(map()) :: boolean()
  def acyclic?(adjacency) when is_map(adjacency) do
    {result, _state} =
      Enum.reduce_while(
        Map.keys(adjacency),
        {:ok, %{visiting: MapSet.new(), visited: MapSet.new()}},
        fn step_name, {:ok, state} ->
          case visit_dependency(step_name, adjacency, state) do
            {:ok, next_state} -> {:cont, {:ok, next_state}}
            {:error, :cycle} -> {:halt, {:error, :cycle}}
          end
        end
      )

    result == :ok
  end

  defp visit_dependency(step_name, adjacency, %{visited: visited} = state) do
    cond do
      MapSet.member?(visited, step_name) ->
        {:ok, state}

      MapSet.member?(state.visiting, step_name) ->
        {:error, :cycle}

      true ->
        state = %{state | visiting: MapSet.put(state.visiting, step_name)}

        adjacency
        |> Map.get(step_name, [])
        |> visit_dependencies(adjacency, state)
        |> case do
          {:ok, next_state} ->
            {:ok,
             %{
               next_state
               | visiting: MapSet.delete(next_state.visiting, step_name),
                 visited: MapSet.put(next_state.visited, step_name)
             }}

          {:error, :cycle} ->
            {:error, :cycle}
        end
    end
  end

  defp visit_dependencies(dependencies, adjacency, state) do
    Enum.reduce_while(dependencies, {:ok, state}, fn dependency, {:ok, acc} ->
      case visit_dependency(dependency, adjacency, acc) do
        {:ok, next_acc} -> {:cont, {:ok, next_acc}}
        {:error, :cycle} -> {:halt, {:error, :cycle}}
      end
    end)
  end
end
