defmodule Jizoku.Runtime.Trace do
  @moduledoc """
  Durable trace context for Jizoku runtime commands and work.

  Trace contexts are persisted as plain atom-key maps. Identifiers follow the
  W3C Trace Context sizes and lowercase hexadecimal representation without
  depending on process-local tracing state.
  """

  @trace_id_bytes 16
  @span_id_bytes 8
  @max_causation_id_bytes 255
  @max_tracestate_bytes 512
  @fields [:trace_id, :span_id, :parent_span_id, :causation_id, :tracestate]

  @typedoc "A normalized, version-tolerant durable trace map."
  @type t :: %{
          required(:trace_id) => String.t(),
          required(:span_id) => String.t(),
          optional(:parent_span_id) => String.t(),
          optional(:causation_id) => String.t(),
          optional(:tracestate) => String.t()
        }

  @type error :: {:invalid_trace, term()}

  @doc "Creates a new root trace with W3C-compatible identifiers."
  @spec new_root(keyword()) :: {:ok, t()} | {:error, error()}
  def new_root(opts \\ []) do
    with {:ok, opts} <- keyword_options(opts),
         :ok <- supported_options(opts),
         {:ok, causation_id} <- causation_id(Keyword.get(opts, :causation_id)),
         {:ok, tracestate} <- tracestate(Keyword.get(opts, :tracestate)) do
      {:ok,
       %{trace_id: generate_id(@trace_id_bytes), span_id: generate_id(@span_id_bytes)}
       |> maybe_put(:causation_id, causation_id)
       |> maybe_put(:tracestate, tracestate)}
    end
  end

  @doc "Creates a durable child span from a normalized parent trace."
  @spec child_of(t() | map(), String.t() | nil) :: {:ok, t()} | {:error, error()}
  def child_of(parent, causation_id) do
    with {:ok, parent} <- normalize_parent(parent),
         {:ok, causation_id} <- causation_id(causation_id) do
      {:ok,
       %{
         trace_id: parent.trace_id,
         span_id: generate_id(@span_id_bytes),
         parent_span_id: parent.span_id
       }
       |> maybe_put(:causation_id, causation_id)
       |> maybe_put(:tracestate, parent[:tracestate])}
    end
  end

  @doc "Validates and canonicalizes atom- or string-key trace input."
  @spec normalize(term()) :: {:ok, t()} | {:error, error()}
  def normalize(trace) when is_map(trace) do
    with :ok <- supported_keys(trace),
         {:ok, trace_id} <- required_id(trace, :trace_id, 32),
         {:ok, span_id} <- required_id(trace, :span_id, 16),
         {:ok, parent_span_id} <- optional_id(trace, :parent_span_id, 16),
         {:ok, causation_id} <- optional_value(trace, :causation_id, &causation_id/1),
         {:ok, tracestate} <- optional_value(trace, :tracestate, &tracestate/1) do
      {:ok,
       %{trace_id: trace_id, span_id: span_id}
       |> maybe_put(:parent_span_id, parent_span_id)
       |> maybe_put(:causation_id, causation_id)
       |> maybe_put(:tracestate, tracestate)}
    end
  end

  def normalize(_trace), do: invalid(:trace, :expected_map)

  @doc "Returns whether a value is a valid normalized trace context."
  @spec valid?(term()) :: boolean()
  def valid?(trace) do
    match?({:ok, _trace}, normalize(trace))
  end

  defp normalize_parent(parent) do
    case normalize(parent) do
      {:ok, parent} -> {:ok, parent}
      {:error, _reason} -> invalid(:trace, :invalid)
    end
  end

  defp keyword_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts), do: {:ok, opts}, else: invalid(:options, :expected_keyword)
  end

  defp keyword_options(_opts), do: invalid(:options, :expected_keyword)

  defp supported_options(opts) do
    case Enum.find(Keyword.keys(opts), &(&1 not in [:causation_id, :tracestate])) do
      nil -> :ok
      _option -> invalid(:options, :unsupported)
    end
  end

  defp supported_keys(trace) do
    if Enum.all?(Map.keys(trace), &supported_key?/1) do
      :ok
    else
      invalid(:keys, :unsupported)
    end
  end

  defp supported_key?(key) when is_atom(key), do: key in @fields
  defp supported_key?(key) when is_binary(key), do: key in Enum.map(@fields, &Atom.to_string/1)
  defp supported_key?(_key), do: false

  defp required_id(trace, field, size) do
    case fetch_field(trace, field) do
      {:ok, value} -> validate_id(value, field, size)
      :error -> invalid(field, :missing)
      {:error, reason} -> {:error, reason}
    end
  end

  defp optional_id(trace, field, size) do
    case fetch_field(trace, field) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} -> validate_id(value, field, size)
      :error -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp optional_value(trace, field, validator) do
    case fetch_field(trace, field) do
      {:ok, value} -> validator.(value)
      :error -> {:ok, nil}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_field(trace, field) do
    string_field = Atom.to_string(field)

    case {Map.fetch(trace, field), Map.fetch(trace, string_field)} do
      {{:ok, value}, :error} -> {:ok, value}
      {:error, {:ok, value}} -> {:ok, value}
      {{:ok, value}, {:ok, value}} -> {:ok, value}
      {{:ok, _atom_value}, {:ok, _string_value}} -> invalid(field, :ambiguous)
      {:error, :error} -> :error
    end
  end

  defp validate_id(value, field, size)
       when is_binary(value) and byte_size(value) == size do
    if lowercase_hex?(value) and not all_zero?(value) do
      {:ok, value}
    else
      invalid(field, :invalid)
    end
  end

  defp validate_id(_value, field, _size), do: invalid(field, :invalid)

  defp causation_id(nil), do: {:ok, nil}

  defp causation_id(value) when is_binary(value) and value != "" do
    cond do
      byte_size(value) > @max_causation_id_bytes -> invalid(:causation_id, :too_long)
      not String.valid?(value) -> invalid(:causation_id, :invalid)
      true -> {:ok, value}
    end
  end

  defp causation_id(_value), do: invalid(:causation_id, :expected_non_empty_string)

  defp tracestate(nil), do: {:ok, nil}

  defp tracestate(value) when is_binary(value) and value != "" do
    cond do
      byte_size(value) > @max_tracestate_bytes -> invalid(:tracestate, :too_long)
      not String.valid?(value) -> invalid(:tracestate, :invalid)
      true -> {:ok, value}
    end
  end

  defp tracestate(_value), do: invalid(:tracestate, :expected_non_empty_string)

  defp lowercase_hex?(value), do: String.match?(value, ~r/\A[0-9a-f]+\z/)
  defp all_zero?(value), do: String.trim(value, "0") == ""

  defp generate_id(bytes) do
    bytes
    |> :crypto.strong_rand_bytes()
    |> Base.encode16(case: :lower)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp invalid(field, reason), do: {:error, {:invalid_trace, {field, reason}}}
end
