defmodule Squidie.Runtime.ManualAction do
  @moduledoc """
  Validation and serialization helpers for durable manual workflow actions.

  Pause resume, approval, and rejection flows all persist a small audit payload
  so the read model can reconstruct who acted and when.
  """

  @type attrs :: %{
          optional(:actor) => String.t() | map(),
          optional(:comment) => String.t(),
          optional(:metadata) => map()
        }
  @type type :: :resumed | :approved | :rejected
  @type persisted :: map()

  @doc false
  @spec validate(attrs(), keyword()) :: :ok | {:error, {:invalid_manual_action, map()}}
  def validate(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    attrs
    |> validation_errors(Keyword.get(opts, :require_actor, false))
    |> Enum.reverse()
    |> case do
      [] -> :ok
      [{field, reason} | _rest] -> {:error, {:invalid_manual_action, %{field => reason}}}
    end
  end

  @doc false
  @spec build(type(), attrs()) :: persisted()
  @spec build(type(), attrs(), DateTime.t()) :: persisted()
  def build(type, attrs, now \\ DateTime.utc_now(:microsecond))

  def build(type, attrs, %DateTime{} = now)
      when type in [:resumed, :approved, :rejected] and is_map(attrs) do
    %{
      "event" => Atom.to_string(type),
      "at" => DateTime.to_iso8601(now)
    }
    |> maybe_put("actor", Map.get(attrs, :actor))
    |> maybe_put("comment", Map.get(attrs, :comment))
    |> maybe_put("metadata", Map.get(attrs, :metadata))
  end

  defp validation_errors(attrs, required_actor?) do
    []
    |> validate_actor(attrs, required_actor?)
    |> validate_comment(attrs)
    |> validate_metadata(attrs)
  end

  defp validate_actor(errors, attrs, true) do
    if valid_actor?(Map.get(attrs, :actor)), do: errors, else: [{:actor, :required} | errors]
  end

  defp validate_actor(errors, attrs, false) do
    if Map.has_key?(attrs, :actor) and not valid_actor?(Map.get(attrs, :actor)) do
      [{:actor, :invalid} | errors]
    else
      errors
    end
  end

  defp validate_comment(errors, attrs) do
    if Map.has_key?(attrs, :comment) and not valid_comment?(Map.get(attrs, :comment)) do
      [{:comment, :string} | errors]
    else
      errors
    end
  end

  defp validate_metadata(errors, attrs) do
    if Map.has_key?(attrs, :metadata) and not is_map(Map.get(attrs, :metadata)) do
      [{:metadata, :map} | errors]
    else
      errors
    end
  end

  defp valid_actor?(actor),
    do: (is_binary(actor) and actor != "") or (is_map(actor) and map_size(actor) > 0)

  defp valid_comment?(comment), do: is_binary(comment) and comment != ""

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
