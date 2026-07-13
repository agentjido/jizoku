defmodule Squidie.Runtime.DispatchNotifier do
  @moduledoc """
  Boundary for live dispatch wakeups emitted after durable scheduling.

  Wakeups are hints only. Durable dispatch entries remain the source of truth,
  so a notifier failure must not roll back or hide a scheduled attempt.
  """

  @type attempt :: %{
          required(:run_id) => String.t(),
          required(:runnable_key) => String.t(),
          required(:queue) => String.t(),
          required(:visible_at) => DateTime.t(),
          optional(:partition) => String.t()
        }

  @callback notify_attempt_scheduled(attempt(), keyword()) :: :ok | {:error, term()}

  @doc """
  Emits a live wakeup hint for a scheduled attempt through the configured notifier.
  """
  @spec notify_attempt_scheduled(module(), attempt(), keyword()) :: :ok | {:error, term()}
  def notify_attempt_scheduled(notifier, attempt, opts)
      when is_atom(notifier) and is_map(attempt) and is_list(opts) do
    case Code.ensure_loaded(notifier) do
      {:module, ^notifier} ->
        if function_exported?(notifier, :notify_attempt_scheduled, 2) do
          safe_notify_attempt_scheduled(notifier, attempt, opts)
        else
          {:error, {:invalid_notifier, notifier}}
        end

      {:error, _reason} ->
        {:error, {:invalid_notifier, notifier}}
    end
  end

  def notify_attempt_scheduled(notifier, _attempt, _opts) when not is_atom(notifier),
    do: {:error, {:invalid_notifier, notifier}}

  def notify_attempt_scheduled(_notifier, attempt, _opts) when not is_map(attempt),
    do: {:error, {:invalid_attempt, attempt}}

  def notify_attempt_scheduled(_notifier, _attempt, opts) when not is_list(opts),
    do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp safe_notify_attempt_scheduled(notifier, attempt, opts) do
    notifier.notify_attempt_scheduled(attempt, opts)
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
      {:error, {:notifier_exception, notifier, exception}}
  catch
    kind, reason -> {:error, {:notifier_throw, notifier, {kind, reason}}}
  end
end

defmodule Squidie.Runtime.DispatchNotifier.Noop do
  @moduledoc false

  @behaviour Squidie.Runtime.DispatchNotifier

  @impl Squidie.Runtime.DispatchNotifier
  def notify_attempt_scheduled(_attempt, _opts), do: :ok
end
