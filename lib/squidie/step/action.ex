defmodule Squidie.Step.Action do
  @moduledoc """
  Internal Jido action adapter for native Squidie steps.

  Native step modules expose the public `Squidie.Step` contract. This adapter
  preserves the runtime's Jido execution path by validating native input,
  converting the Jido context into `Squidie.Step.Context`, normalizing native
  return values, and validating native output before the workflow runtime
  persists the result.
  """

  use Jido.Action,
    name: "squidie_native_step",
    description: "Internal adapter for native Squidie steps"

  alias Squidie.Step
  alias Squidie.Step.Context

  @impl Jido.Action
  def run(%{step: step, input: input}, context) when is_atom(step) and is_map(input) do
    with {:ok, input} <- Step.validate_input(step, input),
         {:ok, result} <- run_native_step(step, input, Context.from_map(context)),
         {:ok, output, opts} <- Step.normalize_result(result),
         {:ok, output} <- maybe_validate_output(step, output, opts) do
      {:ok, output, opts}
    end
  end

  def run(_params, _context) do
    {:error,
     %{
       message: "native step adapter received invalid params",
       retryable?: false
     }}
  end

  defp run_native_step(step, input, context) do
    {:ok, step.run(input, context)}
  catch
    :error, reason ->
      exception = Exception.normalize(:error, reason, __STACKTRACE__)

      {:error,
       %{
         code: "step_exception",
         message: "step execution failed",
         exception: inspect(exception.__struct__),
         origin: exception_origin(__STACKTRACE__),
         retryable?: false
       }}
  end

  defp exception_origin([{module, function, arity_or_args, metadata} | _stacktrace]) do
    maybe_put_origin_line(
      %{}
      |> Map.put(:module, inspect(module))
      |> Map.put(:function, Atom.to_string(function))
      |> Map.put(:arity, stacktrace_arity(arity_or_args)),
      Keyword.get(metadata, :line)
    )
  end

  defp exception_origin(_stacktrace), do: nil

  defp stacktrace_arity(arity) when is_integer(arity), do: arity
  defp stacktrace_arity(args) when is_list(args), do: length(args)

  defp maybe_put_origin_line(origin, line) when is_integer(line), do: Map.put(origin, :line, line)
  defp maybe_put_origin_line(origin, _line), do: origin

  defp maybe_validate_output(step, output, opts) when is_list(opts) do
    if Keyword.has_key?(opts, :defer) do
      {:ok, output}
    else
      Step.validate_output(step, output)
    end
  end
end
