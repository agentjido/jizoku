# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Jido.Directives do
  @moduledoc false

  alias Jido.Agent.Directive

  @type directive_type :: :emit | :error | :run_instruction | :unsupported

  @type compatibility_error :: %{
          required(:code) => String.t(),
          required(:message) => String.t(),
          required(:retryable?) => false,
          optional(:directive_types) => [directive_type()]
        }

  @doc false
  @spec normalize(term()) :: {:ok, []} | {:error, compatibility_error()}
  def normalize([]) do
    {:ok, []}
  end

  def normalize([%Directive.Error{}]) do
    compatibility_error(
      "jido_directive_error",
      "Jido action returned an error directive",
      [:error]
    )
  end

  def normalize(extras) when is_list(extras) do
    compatibility_error(
      "unsupported_jido_directive",
      "Jido action directives are not supported",
      Enum.map(extras, &directive_type/1)
    )
  end

  def normalize(_extras) do
    compatibility_error(
      "invalid_jido_action_extras",
      "Jido action extras must be a list",
      []
    )
  end

  defp compatibility_error(code, message, directive_types) do
    {:error,
     %{
       code: code,
       directive_types: directive_types,
       message: message,
       retryable?: false
     }}
  end

  defp directive_type(%Directive.Emit{}) do
    :emit
  end

  defp directive_type(%Directive.Error{}) do
    :error
  end

  defp directive_type(%Directive.RunInstruction{}) do
    :run_instruction
  end

  defp directive_type(_directive) do
    :unsupported
  end
end
