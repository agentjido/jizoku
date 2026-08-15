defmodule Jizoku.Step.Elixir do
  @moduledoc """
  Native action for invoking host-approved Elixir adapters from runtime specs.

  Hosts expose this module through the action registry and provide adapter
  definitions through registry-owned `action_opts`. Runtime-authored specs name
  stable adapter keys and params only; they never provide modules, functions, or
  executable code.
  """

  use Jizoku.Step,
    name: :elixir_action,
    description: "Runs a host-approved Elixir action",
    input_schema: [
      adapter: [type: :string, required: true],
      params: [type: :map, required: true]
    ],
    output_schema: [
      result: [type: :map, required: true]
    ]

  alias Jizoku.Step.Context

  @type adapter_key :: atom() | String.t()
  @type adapter_definition ::
          module()
          | {module(), atom()}
          | keyword()
          | %{optional(:module) => module(), optional(:function) => atom()}
          | %{optional(String.t()) => term()}

  @doc """
  Validates planned Elixir action input with host-owned adapter options.
  """
  @spec validate_action_input(term(), keyword()) :: :ok | {:error, map()}
  def validate_action_input(input, opts) when is_map(input) and is_list(opts) do
    with :ok <- validate_action_policy(opts),
         :ok <- validate_action_input_fields(input),
         {:ok, adapter} <- validate_adapter_key(field(input, :adapter)),
         {:ok, _params} <- validate_params(field(input, :params)),
         {:ok, _adapter} <- resolve_adapter(adapter, opts) do
      :ok
    end
  end

  def validate_action_input(_input, opts) when is_list(opts) do
    {:error, validation_error(%{input: "Elixir action input must be a map"})}
  end

  def validate_action_input(_input, _opts) do
    {:error, validation_error(%{opts: "validation options must be a keyword list"})}
  end

  @doc false
  @spec persisted_action_opts(keyword()) :: keyword()
  def persisted_action_opts(opts) when is_list(opts) do
    opts
    |> Keyword.delete(:adapters)
    |> Keyword.put(:adapters, persisted_adapters(Keyword.get(opts, :adapters, %{})))
  end

  def persisted_action_opts(_opts), do: []

  @impl Jizoku.Step
  def run(input, %Context{} = context) when is_map(input) do
    action_opts = action_opts(context)

    with :ok <- validate_action_input(input, action_opts),
         {:ok, adapter_key} <- validate_adapter_key(field(input, :adapter)),
         {:ok, params} <- validate_params(field(input, :params)),
         {:ok, adapter} <- resolve_adapter(adapter_key, action_opts) do
      invoke_adapter(adapter_key, adapter, params, context)
    end
  end

  def run(_input, _context) do
    {:error, validation_error(%{input: "Elixir action input must be a map"})}
  end

  defp invoke_adapter(adapter_key, adapter, params, %Context{} = context) do
    result =
      try do
        {:ok, do_invoke_adapter(adapter, params, context)}
      rescue
        exception in [
          ArgumentError,
          BadMapError,
          CaseClauseError,
          ErlangError,
          FunctionClauseError,
          KeyError,
          MatchError,
          RuntimeError,
          UndefinedFunctionError
        ] ->
          {:error, exception_error(adapter_key, exception)}
      catch
        kind, reason ->
          {:error, caught_error(adapter_key, kind, reason)}
      end

    case result do
      {:ok, result} -> normalize_adapter_result(result, adapter_key)
      {:error, error} -> {:error, error}
    end
  end

  defp do_invoke_adapter(%{module: module, function: function, arity: 2}, params, context) do
    apply(module, function, [params, context])
  end

  defp do_invoke_adapter(%{module: module, function: function, arity: 1}, params, _context) do
    apply(module, function, [params])
  end

  defp normalize_adapter_result({:ok, output}, _adapter_key) when is_map(output) do
    {:ok, %{result: output}}
  end

  defp normalize_adapter_result({:ok, _output}, _adapter_key) do
    {:error, validation_error(%{output: "adapter output must be a map"})}
  end

  defp normalize_adapter_result({:ok, _output, opts}, _adapter_key) when is_list(opts) do
    {:error, validation_error(%{output: "adapter execution opts are not supported"})}
  end

  defp normalize_adapter_result({:retry, reason}, adapter_key) do
    {:retry, adapter_error(reason, adapter_key, true)}
  end

  defp normalize_adapter_result({:error, reason}, adapter_key) do
    {:error, adapter_error(reason, adapter_key, false)}
  end

  defp normalize_adapter_result(_result, _adapter_key) do
    {:error,
     %{
       message: "Elixir action returned an invalid result",
       retryable?: false
     }}
  end

  defp validate_action_policy(opts) do
    case Keyword.get(opts, :adapters) do
      adapters when is_map(adapters) and map_size(adapters) > 0 ->
        :ok

      adapters when is_list(adapters) and adapters != [] ->
        if Keyword.keyword?(adapters) do
          :ok
        else
          {:error, validation_error(%{adapters: "adapters policy must be a map or keyword list"})}
        end

      _missing_or_invalid ->
        {:error, validation_error(%{adapters: "adapters policy is required"})}
    end
  end

  defp persisted_adapters(adapters) when is_map(adapters) do
    Map.new(adapters, fn {key, entry} -> {key, persisted_adapter_metadata(entry)} end)
  end

  defp persisted_adapters(adapters) when is_list(adapters) do
    if Keyword.keyword?(adapters) do
      Map.new(adapters, fn {key, entry} -> {key, persisted_adapter_metadata(entry)} end)
    else
      %{}
    end
  end

  defp persisted_adapters(_adapters), do: %{}

  defp persisted_adapter_metadata(entry) when is_list(entry) do
    if Keyword.keyword?(entry) do
      entry
      |> Map.new()
      |> persisted_adapter_metadata()
    else
      %{}
    end
  end

  defp persisted_adapter_metadata(entry) when is_map(entry) do
    %{}
    |> maybe_put_string_metadata(:display_name, map_value(entry, :display_name))
    |> maybe_put_string_metadata(:description, map_value(entry, :description))
    |> maybe_put_string_metadata(:category, map_value(entry, :category))
    |> maybe_put_boolean_metadata(:enabled?, map_value(entry, :enabled?))
  end

  defp persisted_adapter_metadata(_entry), do: %{}

  defp maybe_put_string_metadata(metadata, key, value) when is_binary(value) do
    Map.put(metadata, key, value)
  end

  defp maybe_put_string_metadata(metadata, _key, _value), do: metadata

  defp maybe_put_boolean_metadata(metadata, key, value) when is_boolean(value) do
    Map.put(metadata, key, value)
  end

  defp maybe_put_boolean_metadata(metadata, _key, _value), do: metadata

  defp validate_action_input_fields(input) do
    allowed_keys = [:adapter, :params, "adapter", "params"]

    case Enum.reject(Map.keys(input), &(&1 in allowed_keys)) do
      [] ->
        :ok

      fields ->
        field_list =
          fields
          |> Enum.map(&to_string/1)
          |> Enum.sort()
          |> Enum.join(", ")

        {:error,
         validation_error(%{
           input: "unsupported Elixir action input fields: #{field_list}"
         })}
    end
  end

  defp validate_adapter_key(adapter) when is_atom(adapter), do: {:ok, adapter}
  defp validate_adapter_key(adapter) when is_binary(adapter) and adapter != "", do: {:ok, adapter}

  defp validate_adapter_key(_adapter) do
    {:error, validation_error(%{adapter: "adapter must be a non-empty string or atom"})}
  end

  defp validate_params(params) when is_map(params), do: {:ok, params}

  defp validate_params(_params) do
    {:error, validation_error(%{params: "params must be a map"})}
  end

  defp resolve_adapter(adapter_key, opts) do
    opts
    |> Keyword.fetch!(:adapters)
    |> fetch_adapter(adapter_key)
    |> normalize_adapter(adapter_key)
  end

  defp fetch_adapter(adapters, adapter_key) when is_map(adapters) do
    Map.fetch(adapters, adapter_key)
  end

  defp fetch_adapter(adapters, adapter_key) when is_list(adapters) and is_atom(adapter_key) do
    Keyword.fetch(adapters, adapter_key)
  end

  defp fetch_adapter(_adapters, _adapter_key), do: :error

  defp normalize_adapter(:error, _adapter_key) do
    {:error, validation_error(%{adapter: "adapter is not approved"})}
  end

  defp normalize_adapter({:ok, entry}, adapter_key) do
    case adapter_enabled(entry) do
      {:ok, false} ->
        {:error, validation_error(%{adapter: "adapter is disabled"})}

      {:ok, true} ->
        normalize_enabled_adapter(entry, adapter_key)

      :error ->
        {:error, validation_error(%{adapter: "adapter definition is invalid"})}
    end
  end

  defp normalize_enabled_adapter(module, adapter_key) when is_atom(module) do
    normalize_enabled_adapter({module, :run}, adapter_key)
  end

  defp normalize_enabled_adapter({module, function}, _adapter_key)
       when is_atom(module) and is_atom(function) do
    cond do
      Code.ensure_loaded?(module) and function_exported?(module, function, 2) ->
        {:ok, adapter_descriptor(module, function, 2)}

      Code.ensure_loaded?(module) and function_exported?(module, function, 1) ->
        {:ok, adapter_descriptor(module, function, 1)}

      true ->
        {:error, validation_error(%{adapter: "adapter definition is invalid"})}
    end
  end

  defp normalize_enabled_adapter({_module, _function}, _adapter_key) do
    {:error, validation_error(%{adapter: "adapter definition is invalid"})}
  end

  defp normalize_enabled_adapter([module, function], adapter_key)
       when is_atom(module) and is_atom(function) do
    normalize_enabled_adapter({module, function}, adapter_key)
  end

  defp normalize_enabled_adapter(entry, adapter_key) when is_list(entry) do
    if Keyword.keyword?(entry) do
      normalize_enabled_adapter(
        {Keyword.get(entry, :module), Keyword.get(entry, :function, :run)},
        adapter_key
      )
    else
      {:error, validation_error(%{adapter: "adapter definition is invalid"})}
    end
  end

  defp normalize_enabled_adapter(entry, adapter_key) when is_map(entry) do
    normalize_enabled_adapter(
      {map_value(entry, :module), map_value(entry, :function, :run)},
      adapter_key
    )
  end

  defp normalize_enabled_adapter(_entry, _adapter_key) do
    {:error, validation_error(%{adapter: "adapter definition is invalid"})}
  end

  defp adapter_descriptor(module, function, arity) do
    Map.new(module: module, function: function, arity: arity)
  end

  defp adapter_enabled([module, function]) when is_atom(module) and is_atom(function) do
    {:ok, true}
  end

  defp adapter_enabled(entry) when is_list(entry) do
    if Keyword.keyword?(entry) do
      boolean_option(Keyword.get(entry, :enabled?, true))
    else
      {:ok, true}
    end
  end

  defp adapter_enabled(entry) when is_map(entry),
    do: boolean_option(map_value(entry, :enabled?, true))

  defp adapter_enabled(_entry), do: {:ok, true}

  defp boolean_option(value) when is_boolean(value), do: {:ok, value}
  defp boolean_option(_value), do: :error

  defp adapter_error(reason, adapter_key, retryable?) when is_map(reason) do
    reason
    |> Map.take([:message, :code, :details])
    |> Map.put_new(:message, "Elixir action failed")
    |> Map.put(:kind, :elixir_action)
    |> Map.put(:adapter, adapter_name(adapter_key))
    |> Map.put(:retryable?, retryable?)
  end

  defp adapter_error(reason, adapter_key, retryable?) when is_binary(reason) do
    adapter_error(%{message: reason}, adapter_key, retryable?)
  end

  defp adapter_error(_reason, adapter_key, retryable?) do
    adapter_error(%{message: "Elixir action failed"}, adapter_key, retryable?)
  end

  defp exception_error(adapter_key, exception) do
    %{
      message: "Elixir action execution failed",
      kind: :elixir_action,
      adapter: adapter_name(adapter_key),
      exception: Atom.to_string(exception.__struct__),
      retryable?: false
    }
  end

  defp caught_error(adapter_key, :error, exception) when is_exception(exception) do
    exception_error(adapter_key, exception)
  end

  defp caught_error(adapter_key, kind, _reason) do
    %{
      message: "Elixir action execution failed",
      kind: :elixir_action,
      adapter: adapter_name(adapter_key),
      caught: to_string(kind),
      retryable?: false
    }
  end

  defp validation_error(errors) do
    Jizoku.Step.ErrorPayload.validation_failed("Elixir action validation failed", errors)
  end

  defp action_opts(%Context{step_opts: step_opts}) when is_list(step_opts) do
    Keyword.get(step_opts, :action_opts, [])
  end

  defp action_opts(%Context{}), do: []

  defp adapter_name(adapter) when is_atom(adapter), do: Atom.to_string(adapter)
  defp adapter_name(adapter) when is_binary(adapter), do: adapter

  defp field(map, key) when is_map(map) and is_atom(key) do
    case Map.fetch(map, key) do
      {:ok, value} -> value
      :error -> Map.get(map, Atom.to_string(key))
    end
  end

  defp map_value(map, key, default \\ nil), do: Jizoku.MapField.get(map, key, default)
end
