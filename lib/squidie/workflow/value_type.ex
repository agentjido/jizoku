defmodule Squidie.Workflow.ValueType do
  @moduledoc false

  @doc """
  Returns true when a value matches a workflow-declared primitive type.
  """
  @spec matches?(term(), atom()) :: boolean()
  def matches?(value, :string), do: is_binary(value)
  def matches?(value, :integer), do: is_integer(value)
  def matches?(value, :float), do: is_float(value)
  def matches?(value, :boolean), do: is_boolean(value)
  def matches?(value, :map), do: is_map(value)
  def matches?(value, :list), do: is_list(value)
  def matches?(value, :atom), do: is_atom(value)
  def matches?(_value, _unknown_type), do: true
end
