defmodule Jizoku.ReadModel.Listing.Cursor do
  @moduledoc """
  Encodes and validates opaque, query-bound run-listing cursors.

  Cursors are navigation state, not authorization. Hosts must still authorize
  the selected partition before calling the listing API.
  """

  @version 1
  @ttl_seconds 3_600

  @type position :: {integer(), String.t()}
  @type cursor_error ::
          {:invalid_cursor,
           :malformed | :expired | :query_mismatch | {:unsupported_version, term()}}

  @doc """
  Encodes a stable catalog position for one normalized query.
  """
  @spec encode(position(), String.t(), DateTime.t()) :: String.t()
  def encode({started_at_us, run_id}, query_fingerprint, %DateTime{} = now)
      when is_integer(started_at_us) and is_binary(run_id) and is_binary(query_fingerprint) do
    payload = %{
      "version" => @version,
      "started_at_us" => started_at_us,
      "run_id" => run_id,
      "query" => query_fingerprint,
      "expires_at_us" =>
        now
        |> DateTime.add(@ttl_seconds, :second)
        |> DateTime.to_unix(:microsecond)
    }

    payload
    |> Jason.encode!()
    |> Base.url_encode64(padding: false)
  end

  @doc """
  Decodes a cursor and rejects malformed, expired, version-mismatched, or
  query-mismatched values.
  """
  @spec decode(String.t(), String.t(), DateTime.t()) ::
          {:ok, position()} | {:error, cursor_error()}
  def decode(cursor, query_fingerprint, %DateTime{} = now)
      when is_binary(cursor) and is_binary(query_fingerprint) do
    with {:ok, encoded} <- Base.url_decode64(cursor, padding: false),
         {:ok, payload} <- Jason.decode(encoded) do
      validate_payload(payload, query_fingerprint, now)
    else
      :error -> malformed()
      {:error, _reason} -> malformed()
    end
  end

  def decode(_cursor, _query_fingerprint, %DateTime{}) do
    malformed()
  end

  @doc """
  Returns a stable fingerprint for normalized query state.
  """
  @spec query_fingerprint(term()) :: String.t()
  def query_fingerprint(query) do
    query
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp validate_payload(%{"version" => version}, _query_fingerprint, _now)
       when version != @version do
    {:error, {:invalid_cursor, {:unsupported_version, version}}}
  end

  defp validate_payload(
         %{
           "version" => @version,
           "started_at_us" => started_at_us,
           "run_id" => run_id,
           "query" => query,
           "expires_at_us" => expires_at_us
         },
         query_fingerprint,
         %DateTime{} = now
       )
       when is_integer(started_at_us) and is_binary(run_id) and run_id != "" and
              is_binary(query) and is_integer(expires_at_us) do
    cond do
      query != query_fingerprint ->
        {:error, {:invalid_cursor, :query_mismatch}}

      expires_at_us <= DateTime.to_unix(now, :microsecond) ->
        {:error, {:invalid_cursor, :expired}}

      true ->
        {:ok, {started_at_us, run_id}}
    end
  end

  defp validate_payload(_payload, _query_fingerprint, _now) do
    malformed()
  end

  defp malformed do
    {:error, {:invalid_cursor, :malformed}}
  end
end
