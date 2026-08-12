# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.CommandReceipt do
  @moduledoc false

  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.Entry

  @doc false
  @spec new(atom(), map(), DateTime.t()) :: {:ok, Entry.t()} | {:error, term()}
  def new(signal_type, attrs, %DateTime{} = now) when is_atom(signal_type) and is_map(attrs) do
    attrs =
      %{
        run_id: Map.fetch!(attrs, :run_id),
        signal_type: signal_type,
        payload: Map.get(attrs, :payload, %{}),
        metadata: Map.get(attrs, :metadata, %{}),
        occurred_at: now
      }
      |> maybe_put(:source, Map.get(attrs, :source))
      |> maybe_put(:signal_id, Map.get(attrs, :signal_id))
      |> maybe_put(:trace, Map.get(attrs, :trace))
      |> maybe_put(:idempotency_key, Map.get(attrs, :idempotency_key))
      |> maybe_put(:actor, Map.get(attrs, :actor))
      |> maybe_put(:comment, Map.get(attrs, :comment))

    DispatchProtocol.new_entry(:run_signal_received, attrs)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
