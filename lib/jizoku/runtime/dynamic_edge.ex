defmodule Jizoku.Runtime.DynamicEdge do
  @moduledoc false

  @doc """
  Builds a dynamic workflow edge payload.
  """
  @spec attrs(String.t(), String.t(), String.t(), atom() | nil, atom() | nil) :: map()
  def attrs(id, from, to, type, status) do
    Map.new(id: id, from: from, to: to, type: type, status: status)
  end
end
