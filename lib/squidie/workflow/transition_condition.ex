defmodule Squidie.Workflow.TransitionCondition do
  @moduledoc false

  @type json_value ::
          nil
          | boolean()
          | number()
          | String.t()
          | [json_value()]
          | %{optional(String.t()) => json_value()}
  @type operator :: :equals | :greater_than | :less_than
  @type t ::
          %{required(:path) => [atom()], required(:equals) => json_value()}
          | %{required(:path) => [atom()], required(:greater_than) => number()}
          | %{required(:path) => [atom()], required(:less_than) => number()}
  @type error :: :invalid_condition

  @operators [:equals, :greater_than, :less_than]

  @doc false
  @spec normalize(term()) :: {:ok, t()} | {:error, error()}
  def normalize(condition) when is_list(condition) or is_map(condition) do
    with {:ok, condition} <- condition_map(condition),
         true <- condition_keys_valid?(condition),
         {:ok, operator, expected} <- fetch_condition_operator(condition),
         true <- valid_operator_value?(operator, expected),
         {:ok, normalized_path} <- normalize_path(condition_path(condition)) do
      {:ok, Map.put(%{path: normalized_path}, operator, expected)}
    else
      _invalid -> {:error, :invalid_condition}
    end
  end

  def normalize(_condition), do: {:error, :invalid_condition}

  @doc false
  @spec normalize!(term()) :: t()
  def normalize!(condition) do
    case normalize(condition) do
      {:ok, normalized} -> normalized
      {:error, :invalid_condition} -> raise ArgumentError, "invalid transition condition"
    end
  end

  @doc false
  @spec matches?(map(), t() | map()) :: boolean()
  def matches?(context, condition) when is_map(context) do
    case normalize(condition) do
      {:ok, %{path: path, equals: expected}} ->
        match_fetched_value?(context, path, &(&1 == expected))

      {:ok, %{path: path, greater_than: expected}} ->
        match_fetched_value?(context, path, fn
          value when is_number(value) -> value > expected
          _value -> false
        end)

      {:ok, %{path: path, less_than: expected}} ->
        match_fetched_value?(context, path, fn
          value when is_number(value) -> value < expected
          _value -> false
        end)

      {:error, :invalid_condition} ->
        false
    end
  end

  def matches?(_context, _condition), do: false

  @doc false
  @spec serialize(t() | map() | nil) :: map() | nil
  def serialize(nil), do: nil

  def serialize(condition) do
    case normalize(condition) do
      {:ok, %{path: path, equals: expected}} ->
        %{"path" => Enum.map(path, &Atom.to_string/1), "equals" => expected}

      {:ok, %{path: path, greater_than: expected}} ->
        %{"path" => Enum.map(path, &Atom.to_string/1), "greater_than" => expected}

      {:ok, %{path: path, less_than: expected}} ->
        %{"path" => Enum.map(path, &Atom.to_string/1), "less_than" => expected}

      {:error, :invalid_condition} ->
        nil
    end
  end

  @doc false
  @spec deserialize(map() | nil) :: t() | nil
  def deserialize(nil), do: nil

  def deserialize(condition) when is_map(condition) do
    path = map_value(condition, :path)

    with true <- is_list(path),
         {:ok, normalized_path} <- deserialize_path(path),
         {:ok, condition} <- put_deserialized_path(condition, normalized_path),
         {:ok, normalized} <- normalize(condition) do
      normalized
    else
      _invalid -> nil
    end
  end

  def deserialize(_condition), do: nil

  defp match_fetched_value?(context, path, predicate) when is_function(predicate, 1) do
    case fetch_path(context, path) do
      {:ok, value} -> predicate.(value)
      :error -> false
    end
  end

  defp fetch_path(context, path) do
    Enum.reduce_while(path, {:ok, context}, fn segment, {:ok, current} ->
      case fetch_segment(current, segment) do
        {:ok, value} -> {:cont, {:ok, value}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp fetch_segment(%{} = map, segment) when is_atom(segment) do
    case Map.fetch(map, segment) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(map, Atom.to_string(segment))
    end
  end

  defp fetch_segment(_value, _segment), do: :error

  defp condition_map(%{} = condition), do: {:ok, condition}

  defp condition_map(condition) when is_list(condition) do
    if condition_list_keys_unique?(condition) do
      {:ok, Map.new(condition)}
    else
      {:error, :invalid_condition}
    end
  rescue
    ArgumentError -> {:error, :invalid_condition}
  end

  defp condition_list_keys_unique?(condition) do
    case Enum.reduce_while(condition, MapSet.new(), &track_condition_key/2) do
      %MapSet{} -> true
      _invalid -> false
    end
  end

  defp track_condition_key({key, _value}, seen) do
    case canonical_condition_key(key) do
      nil ->
        {:cont, seen}

      canonical_key ->
        if MapSet.member?(seen, canonical_key) do
          {:halt, :duplicate}
        else
          {:cont, MapSet.put(seen, canonical_key)}
        end
    end
  end

  defp track_condition_key(_entry, _seen), do: {:halt, :invalid}

  defp condition_path(condition), do: map_value(condition, :path)

  defp map_value(map, key, default \\ nil)

  defp map_value(map, key, default) when is_map(map) and is_atom(key) do
    value = Map.get(map, key)

    if is_nil(value) do
      Map.get(map, Atom.to_string(key), default)
    else
      value
    end
  end

  defp map_value(_map, _key, default), do: default

  defp condition_keys_valid?(condition) do
    path_key_count(condition) == 1 and Enum.all?(Map.keys(condition), &condition_key?/1)
  end

  defp path_key_count(condition) do
    Enum.count([:path, "path"], &Map.has_key?(condition, &1))
  end

  defp condition_key?(:path), do: true
  defp condition_key?("path"), do: true
  defp condition_key?(operator) when operator in @operators, do: true
  defp condition_key?("equals"), do: true
  defp condition_key?("greater_than"), do: true
  defp condition_key?("less_than"), do: true
  defp condition_key?(_key), do: false

  defp canonical_condition_key(:path), do: :path
  defp canonical_condition_key("path"), do: :path
  defp canonical_condition_key(operator) when operator in @operators, do: operator
  defp canonical_condition_key("equals"), do: :equals
  defp canonical_condition_key("greater_than"), do: :greater_than
  defp canonical_condition_key("less_than"), do: :less_than
  defp canonical_condition_key(_key), do: nil

  defp fetch_condition_operator(condition) do
    case condition_operators(condition) do
      [{operator, expected}] -> {:ok, operator, expected}
      _other -> {:error, :invalid_condition}
    end
  end

  defp condition_operators(condition) do
    Enum.flat_map(@operators, fn operator ->
      Enum.map(operator_values(operator, condition), &{operator, &1})
    end)
  end

  defp operator_values(operator, condition) do
    string_operator = Atom.to_string(operator)

    [operator, string_operator]
    |> Enum.filter(&Map.has_key?(condition, &1))
    |> Enum.map(&Map.fetch!(condition, &1))
  end

  defp valid_operator_value?(:equals, expected), do: json_value?(expected)

  defp valid_operator_value?(:greater_than, expected) do
    is_number(expected) and json_value?(expected)
  end

  defp valid_operator_value?(:less_than, expected) do
    is_number(expected) and json_value?(expected)
  end

  defp put_deserialized_path(condition, normalized_path) do
    cond do
      Map.has_key?(condition, :path) -> {:ok, %{condition | path: normalized_path}}
      Map.has_key?(condition, "path") -> {:ok, Map.put(condition, "path", normalized_path)}
      true -> {:error, :invalid_condition}
    end
  end

  defp json_value?(nil), do: true
  defp json_value?(value) when is_boolean(value), do: true
  defp json_value?(value) when is_binary(value), do: true
  defp json_value?(value) when is_number(value), do: true
  defp json_value?(values) when is_list(values), do: Enum.all?(values, &json_value?/1)

  defp json_value?(%{} = value) do
    Enum.all?(value, fn
      {key, nested_value} when is_binary(key) -> json_value?(nested_value)
      _other -> false
    end)
  end

  defp json_value?(_value), do: false

  defp normalize_path(path) do
    if is_list(path) and path != [] do
      deserialize_path(path)
    else
      :error
    end
  end

  defp deserialize_path(path) do
    result =
      Enum.reduce_while(path, {:ok, []}, fn segment, {:ok, acc} ->
        case deserialize_segment(segment) do
          {:ok, atom} -> {:cont, {:ok, [atom | acc]}}
          :error -> {:halt, :error}
        end
      end)

    case result do
      {:ok, path} -> {:ok, Enum.reverse(path)}
      :error -> :error
    end
  end

  defp deserialize_segment(segment) when is_atom(segment), do: {:ok, segment}

  defp deserialize_segment(segment) when is_binary(segment) do
    {:ok, String.to_existing_atom(segment)}
  rescue
    ArgumentError -> :error
  end

  defp deserialize_segment(_segment), do: :error
end
