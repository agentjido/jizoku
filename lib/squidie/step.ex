defmodule Squidie.Step do
  @moduledoc """
  Public contract for authoring native Squidie workflow steps.

  Native steps return Squidie values and receive a `Squidie.Step.Context`.
  The runtime adapts them into the internal Jido execution path, so authors do
  not need to depend on Jido for the common workflow-step path.
  """

  alias Squidie.Step.Context

  @type schema :: keyword(keyword())
  @type result ::
          {:ok, map()}
          | {:ok, map(), keyword()}
          | {:defer, term(), keyword()}
          | {:error, term()}
          | {:retry, term()}
          | {:retry, term(), keyword()}

  @callback run(input :: map(), context :: Context.t()) :: result()

  defmacro __using__(opts) do
    name = Keyword.fetch!(opts, :name)
    description = Keyword.get(opts, :description)
    input_schema = Keyword.get(opts, :input_schema, [])
    output_schema = Keyword.get(opts, :output_schema, [])

    quote bind_quoted: [
            name: name,
            description: description,
            input_schema: input_schema,
            output_schema: output_schema
          ] do
      @behaviour Squidie.Step

      @squidie_step_name name
      @squidie_step_description description
      @squidie_step_input_schema input_schema
      @squidie_step_output_schema output_schema

      # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
      @doc false
      def __squidie_step__(:contract), do: :squidie_step

      # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
      @doc false
      def __squidie_step__(:metadata) do
        %{
          contract: :squidie_step,
          name: @squidie_step_name,
          description: @squidie_step_description,
          input_schema: @squidie_step_input_schema,
          output_schema: @squidie_step_output_schema
        }
      end

      # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
      @doc false
      def __squidie_step__(:input_schema), do: @squidie_step_input_schema

      # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
      @doc false
      def __squidie_step__(:output_schema), do: @squidie_step_output_schema
    end
  end

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec native_step?(module()) :: boolean()
  def native_step?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__squidie_step__, 1) and
      module.__squidie_step__(:contract) == :squidie_step
  end

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec metadata(module()) :: map() | nil
  def metadata(module) when is_atom(module) do
    if native_step?(module), do: module.__squidie_step__(:metadata), else: nil
  end

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec validate_input(module(), map()) :: {:ok, map()} | {:error, map()}
  def validate_input(module, input) when is_atom(module) and is_map(input) do
    validate_schema(input, module.__squidie_step__(:input_schema), "input")
  end

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec validate_output(module(), map()) :: {:ok, map()} | {:error, map()}
  def validate_output(module, output) when is_atom(module) and is_map(output) do
    validate_schema(output, module.__squidie_step__(:output_schema), "output")
  end

  # credo:disable-next-line ExSlop.Check.Readability.DocFalseOnPublicFunction
  @doc false
  @spec normalize_result(result()) :: {:ok, map(), keyword()} | {:error, map()}
  def normalize_result({:ok, output}) when is_map(output), do: {:ok, output, []}

  def normalize_result({:ok, output, opts}) when is_map(output) and is_list(opts) do
    if Keyword.has_key?(opts, :defer) do
      {:error, invalid_defer_options_error(:reserved_success_option)}
    else
      {:ok, output, opts}
    end
  end

  def normalize_result({:defer, reason, opts}) when is_list(opts) do
    case defer_schedule_in(opts) do
      {:ok, schedule_in} ->
        {:ok, %{}, [defer: %{reason: normalize_defer_reason(reason)}, schedule_in: schedule_in]}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def normalize_result({:retry, reason}), do: {:error, retryable_error(reason)}

  def normalize_result({:retry, reason, opts}) when is_list(opts),
    do: {:error, retryable_error(reason, opts)}

  def normalize_result({:error, reason}), do: {:error, terminal_error(reason)}

  def normalize_result(other) do
    {:error,
     %{
       message: "native step returned an invalid result",
       result: inspect(other),
       retryable?: false
     }}
  end

  defp validate_schema(value, [], _target), do: {:ok, value}

  defp validate_schema(value, schema, target) when is_list(schema) do
    if valid_schema?(schema) do
      validate_schema_entries(value, schema, target)
    else
      invalid_schema_error(target)
    end
  end

  defp validate_schema(_value, _schema, target), do: invalid_schema_error(target)

  defp validate_schema_entries(value, schema, target) do
    errors =
      Enum.reduce(schema, %{}, fn {field, opts}, acc ->
        acc
        |> validate_required_field(value, field, opts, target)
        |> validate_field_type(value, field, opts, target)
      end)

    case errors do
      %{} = empty when map_size(empty) == 0 ->
        {:ok, value}

      errors ->
        {:error,
         %{
           message: "native step #{target} validation failed",
           validation_errors: errors,
           retryable?: false
         }}
    end
  end

  defp invalid_schema_error(target) do
    {:error,
     %{
       message: "native step #{target} schema is invalid",
       retryable?: false
     }}
  end

  defp valid_schema?(schema) do
    Enum.all?(schema, fn
      {field, opts} -> is_atom(field) and Keyword.keyword?(opts)
      _other -> false
    end)
  end

  defp validate_required_field(errors, value, field, opts, target) do
    required? = Keyword.get(opts, :required, false)

    if required? and not Map.has_key?(value, field) do
      Map.put(errors, field, "#{target} field is required")
    else
      errors
    end
  end

  defp validate_field_type(errors, value, field, opts, target) do
    case Map.fetch(value, field) do
      {:ok, field_value} ->
        type = Keyword.get(opts, :type, :any)

        if valid_type?(field_value, type) do
          errors
        else
          Map.put(errors, field, "#{target} field must be #{inspect(type)}")
        end

      :error ->
        errors
    end
  end

  defp valid_type?(_value, :any), do: true
  defp valid_type?(value, :atom), do: is_atom(value)
  defp valid_type?(value, :boolean), do: is_boolean(value)
  defp valid_type?(value, :float), do: is_float(value)
  defp valid_type?(value, :integer), do: is_integer(value)
  defp valid_type?(value, :list), do: is_list(value)
  defp valid_type?(value, :map), do: is_map(value)
  defp valid_type?(value, :string), do: is_binary(value)
  defp valid_type?(_value, _type), do: false

  defp defer_schedule_in(opts) do
    case Keyword.fetch(opts, :schedule_in) do
      {:ok, seconds} when is_integer(seconds) and seconds > 0 ->
        {:ok, seconds}

      {:ok, invalid} ->
        {:error, invalid_defer_options_error(%{schedule_in: invalid})}

      :error ->
        {:error, invalid_defer_options_error(:missing_schedule_in)}
    end
  end

  defp invalid_defer_options_error(details) do
    %{
      message: invalid_defer_options_message(details),
      details: details,
      retryable?: false
    }
  end

  defp invalid_defer_options_message(:reserved_success_option) do
    "native step deferred continuation must use {:defer, reason, opts}; :defer is reserved in success options"
  end

  defp invalid_defer_options_message(_details) do
    "native step deferred continuation requires a positive :schedule_in option"
  end

  defp normalize_defer_reason(%{} = reason), do: reason
  defp normalize_defer_reason(reason), do: %{message: inspect(reason)}

  defp retryable_error(reason, opts \\ []) do
    reason
    |> normalize_error()
    |> Map.put(:retryable?, true)
    |> maybe_put_retry_after(opts)
  end

  defp terminal_error(reason) do
    reason
    |> normalize_error()
    |> Map.put(:retryable?, false)
  end

  defp normalize_error(%{__struct__: module} = reason) do
    reason
    |> Map.from_struct()
    |> Map.delete(:__exception__)
    |> Map.put_new(:message, struct_message(reason))
    |> Map.put_new(:type, inspect(module))
  end

  defp normalize_error(%{} = reason), do: reason
  defp normalize_error(reason), do: %{message: inspect(reason)}

  defp struct_message(%{__exception__: true} = reason), do: Exception.message(reason)
  defp struct_message(reason), do: inspect(reason)

  defp maybe_put_retry_after(error, opts) do
    case Keyword.fetch(opts, :retry_after) do
      {:ok, retry_after} -> Map.put(error, :retry_after, retry_after)
      :error -> error
    end
  end
end
