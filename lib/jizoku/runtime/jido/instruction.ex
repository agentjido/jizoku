# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Jido.Instruction do
  @moduledoc false

  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Workflow.ActionRegistry

  @max_id_bytes 255
  @max_term_bytes 65_536
  @reserved_context_keys [
    :run_id,
    :partition,
    :workflow,
    :step,
    :step_opts,
    :attempt,
    :runnable_key,
    :idempotency_key,
    :claim_id,
    :trace,
    :state
  ]

  @type error ::
          {:invalid_jido_instruction,
           {:id, :invalid}
           | {:action, ActionRegistry.action_validation_error()}
           | {:origin, :required | :invalid}
           | {:params, :invalid}
           | {:context, :invalid | {:reserved_key, atom()}}
           | {:opts, :invalid | :unsupported}}

  @doc false
  @spec dynamic_work(Jido.Instruction.t(), map(), ActionRegistry.registry()) ::
          {:ok, map()} | {:error, error()}
  def dynamic_work(%Jido.Instruction{} = instruction, origin, registry) do
    with {:ok, id} <- instruction_id(instruction.id),
         {:ok, action_key} <- action_key(instruction.action, registry),
         :ok <- origin(origin),
         :ok <- safe_map(instruction.params, :params),
         :ok <- safe_context(instruction.context),
         {:ok, retry} <- instruction_opts(instruction.opts) do
      node_id = "jido-instruction:" <> id

      {:ok,
       %{
         dynamic_key: node_id,
         origin: origin,
         nodes: [
           %{
             id: node_id,
             action: action_key,
             input: instruction.params,
             retry: retry,
             metadata: %{
               "jido_instruction" => %{
                 "id" => id,
                 "context" => instruction.context
               }
             }
           }
         ],
         metadata: %{"jido_instruction_id" => id},
         status: :scheduled
       }}
    end
  end

  def dynamic_work(_instruction, _origin, _registry) do
    {:error, {:invalid_jido_instruction, {:id, :invalid}}}
  end

  defp origin(origin) when is_map(origin), do: :ok
  defp origin(_origin), do: invalid(:origin, :invalid)

  defp instruction_id(id)
       when is_binary(id) and id != "" and byte_size(id) <= @max_id_bytes do
    if String.valid?(id), do: {:ok, id}, else: invalid(:id, :invalid)
  end

  defp instruction_id(_id), do: invalid(:id, :invalid)

  defp action_key(action, registry) do
    case ActionRegistry.action_key_for_module(action, registry) do
      {:ok, key} -> {:ok, key}
      {:error, reason} -> invalid(:action, reason)
    end
  end

  defp safe_map(value, field) when is_map(value) do
    if Options.storage_safe_value?(value) and bounded?(value) do
      :ok
    else
      invalid(field, :invalid)
    end
  end

  defp safe_map(_value, field), do: invalid(field, :invalid)

  defp safe_context(context) do
    with :ok <- safe_map(context, :context) do
      case Enum.find(@reserved_context_keys, &has_context_key?(context, &1)) do
        nil -> :ok
        key -> invalid(:context, {:reserved_key, key})
      end
    end
  end

  defp has_context_key?(context, key) do
    Map.has_key?(context, key) or Map.has_key?(context, Atom.to_string(key))
  end

  defp instruction_opts([]), do: {:ok, nil}

  defp instruction_opts(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and Keyword.keys(opts) == [:retry] do
      opts
      |> Keyword.fetch!(:retry)
      |> retry_option()
    else
      invalid(:opts, :unsupported)
    end
  end

  defp instruction_opts(_opts), do: invalid(:opts, :invalid)

  defp retry_option(retry) when is_list(retry) do
    if Keyword.keyword?(retry), do: retry_option(Map.new(retry)), else: invalid(:opts, :invalid)
  end

  defp retry_option(retry) when is_map(retry) do
    if Options.storage_safe_value?(retry) and bounded?(retry) do
      {:ok, retry}
    else
      invalid(:opts, :invalid)
    end
  end

  defp retry_option(_retry), do: invalid(:opts, :invalid)

  defp bounded?(value) do
    value
    |> :erlang.term_to_binary([:deterministic])
    |> byte_size()
    |> Kernel.<=(@max_term_bytes)
  end

  defp invalid(field, reason), do: {:error, {:invalid_jido_instruction, {field, reason}}}
end
