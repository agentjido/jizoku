# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Test.StubAction do
  @moduledoc false

  use Squidie.Step, name: :squidie_test_stub

  alias Squidie.Step.Context
  alias Squidie.Step.ErrorPayload
  alias Squidie.Test.Storage

  @impl Squidie.Step
  def run(input, %Context{} = context) do
    with {:ok, action_key} <- action_option(context, :action_key),
         {:ok, storage_server} <- action_option(context, :storage_server),
         {:ok, result} <-
           Storage.consume_action_stub(
             storage_server,
             action_key,
             invocation_key(context),
             call(input, context)
           ),
         :ok <- run_after_consume_hook(context, call(input, context)) do
      result
    else
      {:error, reason} ->
        {:error, ErrorPayload.terminal("test_action_stub_failed", stub_error_message(reason))}
    end
  end

  @doc false
  @spec persisted_action_opts(keyword()) :: keyword()
  def persisted_action_opts(opts) when is_list(opts) do
    Keyword.take(opts, [:action_key])
  end

  def persisted_action_opts(_opts) do
    []
  end

  defp action_option(%Context{step_opts: step_opts}, key) when is_list(step_opts) do
    step_opts
    |> Keyword.get(:action_opts, [])
    |> Keyword.fetch(key)
  end

  defp invocation_key(%Context{} = context) do
    {context.run_id, context.runnable_key}
  end

  defp run_after_consume_hook(context, call) do
    case action_option(context, :after_consume) do
      {:ok, hook} when is_function(hook, 1) -> hook.(call)
      :error -> :ok
      {:ok, _invalid} -> {:error, :invalid_action_stub_hook}
    end
  end

  defp call(input, %Context{} = context) do
    %{
      attempt: context.attempt,
      input: input,
      run_id: context.run_id,
      runnable_key: context.runnable_key,
      step: context.step
    }
  end

  defp stub_error_message(:action_stub_sequence_exhausted) do
    "test action stub result sequence is exhausted"
  end

  defp stub_error_message(:unknown_action_stub) do
    "test action stub is not configured"
  end

  defp stub_error_message(_reason) do
    "test action stub could not provide a result"
  end
end
