defmodule Squidie.GraphMutation.Operation do
  @moduledoc """
  One normalized node or edge operation in a graph mutation.

  This contract validates transport shape only. Graph identity, topology,
  lifecycle, and limit validation belong to the runtime validator.
  """

  @type kind :: :node | :edge
  @type mode :: :addition | :removal
  @type public_mode :: :add | :remove

  @fields [:kind, :id, :action, :input, :queue, :from, :to]
  @field_names Map.new(@fields, &{Atom.to_string(&1), &1})
  @supported_fields %{
    {:addition, :node} => [:kind, :id, :action, :input, :queue],
    {:addition, :edge} => [:kind, :id, :from, :to],
    {:removal, :node} => [:kind, :id],
    {:removal, :edge} => [:kind, :id]
  }

  @type t :: %__MODULE__{
          kind: kind(),
          id: String.t(),
          action: String.t() | nil,
          input: map() | nil,
          queue: String.t() | nil,
          from: String.t() | nil,
          to: String.t() | nil
        }

  @enforce_keys [:kind, :id]
  defstruct [:kind, :id, :action, :input, :queue, :from, :to]

  @doc """
  Normalizes one operation for its addition or removal context.
  """
  @spec normalize(term(), mode()) :: {:ok, t()} | {:error, {term(), atom()}}
  def normalize(attrs, mode) when is_map(attrs) and mode in [:addition, :removal] do
    with {:ok, kind} <- kind(value(attrs, :kind)),
         :ok <- supported_fields(attrs, mode, kind),
         {:ok, id} <- required_binary(attrs, :id) do
      normalize_kind(attrs, mode, kind, id)
    end
  end

  def normalize(_attrs, _mode) do
    {:error, {:attrs, :invalid}}
  end

  @doc """
  Converts an operation to its deterministic persistence map.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = operation) do
    Map.take(operation, serialization_fields(operation))
  end

  @doc """
  Converts an operation to a redacted report summary.
  """
  @spec to_public_map(t(), public_mode()) :: map()
  def to_public_map(%__MODULE__{} = operation, mode) when mode in [:add, :remove] do
    operation
    |> to_map()
    |> Map.delete(:input)
    |> Map.put(:operation, mode)
  end

  @doc """
  Returns the ordered operation content used by mutation identity.
  """
  @spec canonical(t()) :: tuple()
  def canonical(%__MODULE__{kind: :node, action: nil} = operation) do
    {:node, operation.id}
  end

  def canonical(%__MODULE__{kind: :node} = operation) do
    {:node, operation.id, operation.action, operation.queue, canonical_value(operation.input)}
  end

  def canonical(%__MODULE__{kind: :edge, from: nil, to: nil} = operation) do
    {:edge, operation.id}
  end

  def canonical(%__MODULE__{kind: :edge} = operation) do
    {:edge, operation.id, operation.from, operation.to}
  end

  defp normalize_kind(_attrs, :removal, kind, id) do
    {:ok, %__MODULE__{kind: kind, id: id}}
  end

  defp normalize_kind(attrs, :addition, :node, id) do
    with {:ok, action} <- required_binary(attrs, :action),
         {:ok, input} <- input(value(attrs, :input, %{})),
         {:ok, queue} <- optional_binary(value(attrs, :queue)) do
      {:ok,
       %__MODULE__{
         kind: :node,
         id: id,
         action: action,
         input: input,
         queue: queue
       }}
    end
  end

  defp normalize_kind(attrs, :addition, :edge, id) do
    with {:ok, from} <- required_binary(attrs, :from),
         {:ok, to} <- required_binary(attrs, :to) do
      {:ok, %__MODULE__{kind: :edge, id: id, from: from, to: to}}
    end
  end

  defp kind(kind) do
    case kind do
      :node -> {:ok, :node}
      "node" -> {:ok, :node}
      :edge -> {:ok, :edge}
      "edge" -> {:ok, :edge}
      _invalid -> {:error, {:kind, :invalid}}
    end
  end

  defp supported_fields(attrs, mode, kind) do
    supported = Map.fetch!(@supported_fields, {mode, kind})

    case Enum.find(Map.keys(attrs), &unsupported_field?(&1, supported)) do
      nil -> :ok
      field -> {:error, {field_name(field), :unsupported}}
    end
  end

  defp unsupported_field?(field, supported) do
    field_name(field) not in supported
  end

  defp field_name(field) when is_binary(field) do
    Map.get(@field_names, field, field)
  end

  defp field_name(field) do
    field
  end

  defp serialization_fields(%__MODULE__{kind: :node, action: nil}) do
    [:kind, :id]
  end

  defp serialization_fields(%__MODULE__{kind: :node}) do
    [:kind, :id, :action, :input, :queue]
  end

  defp serialization_fields(%__MODULE__{kind: :edge, from: nil, to: nil}) do
    [:kind, :id]
  end

  defp serialization_fields(%__MODULE__{kind: :edge}) do
    [:kind, :id, :from, :to]
  end

  defp required_binary(attrs, field) do
    case value(attrs, field) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {field, :invalid}}
    end
  end

  defp optional_binary(nil) do
    {:ok, nil}
  end

  defp optional_binary(value) when is_binary(value) and value != "" do
    {:ok, value}
  end

  defp optional_binary(_value) do
    {:error, {:queue, :invalid}}
  end

  defp input(value) when is_map(value) do
    if stable_value?(value) do
      {:ok, value}
    else
      {:error, {:input, :invalid}}
    end
  end

  defp input(_value) do
    {:error, {:input, :invalid}}
  end

  defp canonical_value(value) when is_map(value) do
    value
    |> Enum.map(fn {key, nested_value} ->
      {canonical_value(key), canonical_value(nested_value)}
    end)
    |> Enum.sort()
    |> then(&{:map, &1})
  end

  defp canonical_value(value) when is_list(value) do
    {:list, Enum.map(value, &canonical_value/1)}
  end

  defp canonical_value(value)
       when is_atom(value) or is_binary(value) or is_integer(value) or is_float(value) do
    value
  end

  defp stable_value?(value) when is_map(value) do
    Enum.all?(value, fn {key, nested_value} ->
      stable_value?(key) and stable_value?(nested_value)
    end)
  end

  defp stable_value?(value) when is_list(value) do
    Enum.all?(value, &stable_value?/1)
  end

  defp stable_value?(value)
       when is_atom(value) or is_binary(value) or is_integer(value) or is_float(value) do
    true
  end

  defp stable_value?(_value) do
    false
  end

  defp value(map, field, default \\ nil) do
    Squidie.MapField.get(map, field, default)
  end
end
