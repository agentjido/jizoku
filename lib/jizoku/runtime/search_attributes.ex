# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.SearchAttributes do
  @moduledoc """
  Validates the bounded, host-allowlisted operational attributes attached to a run.

  Search attributes are intentionally smaller than general workflow data. They
  support exact-match operational queries without turning run input, context,
  or results into an implicit search index.
  """

  @max_attributes 32
  @max_idempotency_key_bytes 512
  @max_key_bytes 64
  @max_string_bytes 256
  @max_list_items 16
  @max_encoded_bytes 4_096
  @min_integer -9_223_372_036_854_775_808
  @max_integer 9_223_372_036_854_775_807
  @fingerprint_prefix "v1:"
  @fingerprint_domain "jizoku.search_attributes"
  @key_pattern ~r/^[a-z][a-z0-9_.-]*$/
  @scalar_types [:string, :integer, :boolean]

  @type scalar_type :: :string | :integer | :boolean
  @type attribute_type :: scalar_type() | {:list, scalar_type()}
  @type schema :: %{optional(String.t()) => attribute_type()}
  @type attributes :: %{
          optional(String.t()) => String.t() | integer() | boolean() | [term()]
        }
  @type validation_error :: %{
          required(:code) => atom(),
          optional(:key) => String.t()
        }

  @doc """
  Validates and returns one canonical search-attribute map.

  Non-empty attributes require a host-owned schema. Values are never coerced,
  and validation errors include keys and codes without copying attribute values.
  """
  @spec normalize(map(), schema() | nil) ::
          {:ok, attributes()}
          | {:error,
             {:invalid_search_attributes, [validation_error()]}
             | {:invalid_search_attribute_schema, [validation_error()]}}
  def normalize(attributes, schema)

  def normalize(attributes, nil) when is_map(attributes) and map_size(attributes) == 0 do
    {:ok, %{}}
  end

  def normalize(attributes, nil) when is_map(attributes) do
    invalid_attributes([%{code: :schema_required}])
  end

  def normalize(attributes, schema) when is_map(attributes) and is_map(schema) do
    with {:ok, schema} <- validate_schema(schema),
         :ok <- validate_attribute_count(attributes),
         :ok <- validate_attributes(attributes, schema),
         :ok <- validate_encoded_size(attributes) do
      {:ok, attributes}
    end
  end

  def normalize(_attributes, _schema) do
    invalid_attributes([%{code: :expected_map}])
  end

  @doc """
  Validates a host-owned allowlist schema.
  """
  @spec validate_schema(term()) ::
          {:ok, schema()} | {:error, {:invalid_search_attribute_schema, [validation_error()]}}
  def validate_schema(schema) when is_map(schema) do
    errors =
      schema
      |> Enum.flat_map(&schema_errors/1)
      |> maybe_add_schema_count_error(schema)

    case errors do
      [] -> {:ok, schema}
      errors -> {:error, {:invalid_search_attribute_schema, errors}}
    end
  end

  def validate_schema(_schema) do
    {:error, {:invalid_search_attribute_schema, [%{code: :expected_map}]}}
  end

  @doc """
  Returns a stable digest for one normalized attribute patch.
  """
  @spec fingerprint(attributes()) :: String.t()
  def fingerprint(attributes) when is_map(attributes) do
    encoded =
      Jason.encode!([
        @fingerprint_domain,
        1,
        attributes
        |> Enum.sort()
        |> Enum.map(fn {key, value} -> [key, value] end)
      ])

    @fingerprint_prefix <> fingerprint_digest(encoded)
  end

  @doc false
  @spec fingerprint_matches?(attributes(), term()) :: boolean()
  def fingerprint_matches?(attributes, fingerprint)
      when is_map(attributes) and is_binary(fingerprint) do
    fingerprint == fingerprint(attributes) or fingerprint == legacy_fingerprint(attributes)
  end

  def fingerprint_matches?(_attributes, _fingerprint) do
    false
  end

  @doc false
  @spec valid_idempotency_key?(term()) :: boolean()
  def valid_idempotency_key?(value)
      when is_binary(value) and byte_size(value) > 0 and
             byte_size(value) <= @max_idempotency_key_bytes do
    String.valid?(value)
  end

  def valid_idempotency_key?(_value) do
    false
  end

  @doc """
  Returns whether journal-sourced attributes satisfy storage safety and limits.
  """
  @spec valid_persisted?(term()) :: boolean()
  def valid_persisted?(attributes) when is_map(attributes) do
    map_size(attributes) <= @max_attributes and
      Enum.all?(attributes, fn {key, value} ->
        valid_key?(key) and valid_persisted_value?(value)
      end) and
      encoded_size_valid?(attributes)
  end

  def valid_persisted?(_attributes) do
    false
  end

  defp schema_errors({key, type}) do
    []
    |> maybe_add_key_error(key)
    |> maybe_add_type_error(key, type)
  end

  defp maybe_add_schema_count_error(errors, schema) do
    if map_size(schema) > @max_attributes do
      [%{code: :too_many_keys} | errors]
    else
      errors
    end
  end

  defp maybe_add_key_error(errors, key) do
    if valid_key?(key) do
      errors
    else
      [%{code: :invalid_key} | errors]
    end
  end

  defp maybe_add_type_error(errors, key, type) do
    if valid_type?(type) do
      errors
    else
      [error_for_key(:invalid_type, key) | errors]
    end
  end

  defp validate_attribute_count(attributes) do
    if map_size(attributes) <= @max_attributes do
      :ok
    else
      invalid_attributes([%{code: :too_many_keys}])
    end
  end

  defp validate_attributes(attributes, schema) do
    errors = Enum.flat_map(attributes, &attribute_errors(&1, schema))

    case errors do
      [] -> :ok
      errors -> invalid_attributes(errors)
    end
  end

  defp attribute_errors({key, value}, schema) do
    cond do
      not valid_key?(key) ->
        [%{code: :invalid_key}]

      not Map.has_key?(schema, key) ->
        [%{code: :unknown_key, key: key}]

      true ->
        value_errors(key, value, Map.fetch!(schema, key))
    end
  end

  defp value_errors(key, value, :string) do
    cond do
      not is_binary(value) -> [%{code: :invalid_value_type, key: key}]
      byte_size(value) > @max_string_bytes -> [%{code: :value_too_large, key: key}]
      not String.valid?(value) -> [%{code: :invalid_value, key: key}]
      true -> []
    end
  end

  defp value_errors(key, value, :integer) do
    cond do
      not is_integer(value) -> [%{code: :invalid_value_type, key: key}]
      value < @min_integer or value > @max_integer -> [%{code: :value_out_of_range, key: key}]
      true -> []
    end
  end

  defp value_errors(key, value, :boolean) do
    if is_boolean(value) do
      []
    else
      [%{code: :invalid_value_type, key: key}]
    end
  end

  defp value_errors(key, value, {:list, type}) when is_list(value) do
    case bounded_list_status(value, @max_list_items) do
      :too_large ->
        [%{code: :list_too_large, key: key}]

      :improper ->
        [%{code: :invalid_value_type, key: key}]

      :within_limit ->
        value
        |> Enum.flat_map(&value_errors(key, &1, type))
        |> Enum.uniq()
    end
  end

  defp value_errors(key, _value, {:list, _type}) do
    [%{code: :invalid_value_type, key: key}]
  end

  defp validate_encoded_size(attributes) do
    if encoded_size_valid?(attributes) do
      :ok
    else
      invalid_attributes([%{code: :encoded_value_too_large}])
    end
  end

  defp encoded_size_valid?(attributes) do
    case Jason.encode(attributes) do
      {:ok, encoded} -> byte_size(encoded) <= @max_encoded_bytes
      {:error, _reason} -> false
    end
  end

  defp valid_persisted_value?(value)
       when is_binary(value) or is_integer(value) or is_boolean(value) do
    valid_persisted_scalar?(value)
  end

  defp valid_persisted_value?(value) when is_list(value) do
    bounded_list_status(value, @max_list_items) == :within_limit and
      Enum.all?(value, &valid_persisted_scalar?/1)
  end

  defp valid_persisted_value?(_value) do
    false
  end

  defp valid_persisted_scalar?(value) when is_binary(value) do
    byte_size(value) <= @max_string_bytes and String.valid?(value)
  end

  defp valid_persisted_scalar?(value) when is_integer(value) do
    value >= @min_integer and value <= @max_integer
  end

  defp valid_persisted_scalar?(value) when is_boolean(value) do
    true
  end

  defp valid_persisted_scalar?(_value) do
    false
  end

  defp valid_key?(key) when is_binary(key) and byte_size(key) <= @max_key_bytes do
    String.valid?(key) and Regex.match?(@key_pattern, key)
  end

  defp valid_key?(_key) do
    false
  end

  defp valid_type?(type) when type in @scalar_types do
    true
  end

  defp valid_type?({:list, type}) when type in @scalar_types do
    true
  end

  defp valid_type?(_type) do
    false
  end

  defp bounded_list_status([], _items_left) do
    :within_limit
  end

  defp bounded_list_status([_item | _remaining], 0) do
    :too_large
  end

  defp bounded_list_status([_item | remaining], items_left) when items_left > 0 do
    bounded_list_status(remaining, items_left - 1)
  end

  defp bounded_list_status(_improper_tail, _items_left) do
    :improper
  end

  defp legacy_fingerprint(attributes) do
    attributes
    |> Enum.sort()
    |> :erlang.term_to_binary()
    |> fingerprint_digest()
  end

  defp fingerprint_digest(encoded) do
    encoded
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp error_for_key(code, key) when is_binary(key) do
    %{code: code, key: key}
  end

  defp error_for_key(code, _key) do
    %{code: code}
  end

  defp invalid_attributes(errors) do
    {:error, {:invalid_search_attributes, errors}}
  end
end
