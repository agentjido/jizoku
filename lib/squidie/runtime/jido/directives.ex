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

  def normalize(extras) when is_list(extras) do
    {:error,
     %{
       code: "unsupported_jido_directive",
       directive_types: Enum.map(extras, &directive_type/1),
       message: "Jido action directives are not supported",
       retryable?: false
     }}
  end

  def normalize(_extras) do
    {:error,
     %{
       code: "invalid_jido_action_extras",
       message: "Jido action extras must be a list",
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
