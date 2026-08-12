# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Signal.JidoResolver do
  @moduledoc false

  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.Signal.JidoAdapter

  @type error ::
          {:invalid_option, {:jido_signal_resolver, :invalid}}
          | {:jido_signal_rejected, atom()}
          | {:jido_signal_resolver_failed, :exception | :throw | :exit}
          | {:invalid_jido_signal_command, atom()}

  @doc false
  @spec option(keyword()) :: {:ok, module() | nil} | {:error, error()}
  def option(opts) when is_list(opts) do
    case Keyword.fetch(opts, :jido_signal_resolver) do
      :error -> {:ok, nil}
      {:ok, resolver} -> validate_resolver(resolver)
    end
  end

  @doc false
  @spec resolve(module(), Jido.Signal.t()) :: {:ok, Signal.t()} | {:error, term()}
  def resolve(resolver, %Jido.Signal{} = signal) when is_atom(resolver) do
    with {:ok, envelope} <- JidoAdapter.domain_envelope(signal) do
      resolve(resolver, signal, envelope)
    end
  end

  @doc false
  @spec resolve(module(), Jido.Signal.t(), JidoAdapter.domain_envelope()) ::
          {:ok, Signal.t()} | {:error, term()}
  def resolve(resolver, %Jido.Signal{} = signal, envelope)
      when is_atom(resolver) and is_map(envelope) do
    signal = %{signal | jido_dispatch: nil}

    with {:ok, command} <- invoke(resolver, signal),
         {:ok, %Signal{} = runtime_signal} <- command_signal(command, envelope) do
      {:ok, %Signal{runtime_signal | source: envelope.source}}
    end
  end

  defp validate_resolver(resolver) when is_atom(resolver) and not is_boolean(resolver) do
    if Code.ensure_loaded?(resolver) and function_exported?(resolver, :resolve, 1) do
      {:ok, resolver}
    else
      invalid_resolver()
    end
  end

  defp validate_resolver(_resolver), do: invalid_resolver()

  defp invalid_resolver do
    {:error, {:invalid_option, {:jido_signal_resolver, :invalid}}}
  end

  defp invoke(resolver, signal) do
    normalize_result(resolver.resolve(signal))
  catch
    :error, _reason -> {:error, {:jido_signal_resolver_failed, :exception}}
    :throw, _reason -> {:error, {:jido_signal_resolver_failed, :throw}}
    :exit, _reason -> {:error, {:jido_signal_resolver_failed, :exit}}
  end

  defp normalize_result({:ok, command}), do: {:ok, command}

  defp normalize_result({:error, reason}) when is_atom(reason) do
    {:error, {:jido_signal_rejected, reason}}
  end

  defp normalize_result(_result) do
    {:error, {:invalid_jido_signal_command, :invalid_resolver_result}}
  end

  defp command_signal({:start_run, workflow, trigger, input}, envelope)
       when is_atom(workflow) and not is_boolean(workflow) and
              (is_atom(trigger) or is_nil(trigger)) and is_map(input) do
    Signal.start_run(workflow, trigger, input, envelope_options(envelope))
  end

  defp command_signal({:cancel_run, run_id}, envelope) do
    Signal.cancel_run(run_id, envelope_options(envelope))
  end

  defp command_signal({:resume_run, run_id, attributes}, envelope) when is_map(attributes) do
    Signal.resume_run(run_id, attributes, envelope_options(envelope))
  end

  defp command_signal({:approve_run, run_id, attributes}, envelope) when is_map(attributes) do
    Signal.approve_run(run_id, attributes, envelope_options(envelope))
  end

  defp command_signal({:reject_run, run_id, attributes}, envelope) when is_map(attributes) do
    Signal.reject_run(run_id, attributes, envelope_options(envelope))
  end

  defp command_signal({:replay_run, run_id, allow_irreversible}, envelope)
       when is_boolean(allow_irreversible) do
    Signal.replay_run(
      run_id,
      Keyword.put(envelope_options(envelope), :allow_irreversible, allow_irreversible)
    )
  end

  defp command_signal(_command, _envelope) do
    {:error, {:invalid_jido_signal_command, :unsupported}}
  end

  defp envelope_options(envelope) do
    [
      id: envelope.id,
      trace: envelope.trace,
      metadata: envelope_metadata(envelope),
      occurred_at: envelope.occurred_at,
      idempotency_key: envelope.identity_key
    ]
  end

  defp envelope_metadata(envelope) do
    %{
      "jido" => maybe_put(%{"type" => envelope.type}, "subject", envelope.subject)
    }
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
