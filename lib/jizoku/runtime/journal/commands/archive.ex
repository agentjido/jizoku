defmodule Jizoku.Runtime.Journal.Commands.Archive do
  @moduledoc false

  alias Jido.Agent
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @append_retries 8
  @max_reason_bytes 256

  @type archive_error ::
          :not_found
          | {:invalid_option, term()}
          | {:invalid_transition, atom(), :archiving | :unarchiving}
          | term()

  @doc "Archives one terminal workflow run with a bounded operational reason."
  @spec archive(Ecto.UUID.t(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, archive_error()}
  def archive(run_id, opts) when is_list(opts) do
    with {:ok, reason} <- reason(opts) do
      change(:archive, run_id, reason, opts)
    end
  end

  def archive(_run_id, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  @doc "Restores one archived terminal workflow run to normal listing visibility."
  @spec unarchive(Ecto.UUID.t(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, archive_error()}
  def unarchive(run_id, opts) when is_list(opts) do
    change(:unarchive, run_id, nil, opts)
  end

  def unarchive(_run_id, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp change(action, run_id, reason, opts) do
    with {:ok, run_id} <- Options.uuid_run_id(run_id),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts) do
      command = %{
        action: action,
        run_id: run_id,
        reason: reason,
        storage: storage,
        queue: queue,
        now: now
      }

      do_change(command, @append_retries)
    end
  end

  defp do_change(command, retries_left) do
    with {:ok, %Agent{state: %{projection: %Projection{} = projection, thread_rev: thread_rev}}} <-
           WorkflowAgent.rebuild(command.storage, command.run_id),
         {:ok, transition} <- transition(command, projection) do
      persist_transition(command, projection, thread_rev, transition, retries_left)
    end
  end

  defp transition(%{action: :archive, reason: reason}, %Projection{} = projection) do
    with :ok <- require_terminal(projection, :archiving) do
      case Projection.archive(projection) do
        nil -> {:ok, :append}
        %{reason: ^reason} -> {:ok, :unchanged}
        %{reason: _other_reason} -> {:error, {:invalid_transition, :archived, :archiving}}
      end
    end
  end

  defp transition(%{action: :unarchive}, %Projection{} = projection) do
    with :ok <- require_terminal(projection, :unarchiving) do
      if Projection.archived?(projection), do: {:ok, :append}, else: {:ok, :unchanged}
    end
  end

  defp require_terminal(%Projection{} = projection, transition) do
    if Projection.terminal?(projection) do
      :ok
    else
      {:error, {:invalid_transition, Projection.status(projection), transition}}
    end
  end

  defp persist_transition(command, _projection, _thread_rev, :unchanged, _retries_left) do
    snapshot(command)
  end

  defp persist_transition(command, projection, thread_rev, :append, retries_left) do
    with {:ok, entry} <- entry(command) do
      case Journal.append_entries(command.storage, [entry],
             expected_rev: thread_rev,
             telemetry_projection: projection
           ) do
        {:ok, _thread} ->
          snapshot(command)

        {:error, :conflict} when retries_left > 0 ->
          do_change(command, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp entry(%{action: :archive} = command) do
    DispatchProtocol.new_entry(:run_archived, %{
      run_id: command.run_id,
      reason: command.reason,
      occurred_at: command.now
    })
  end

  defp entry(%{action: :unarchive} = command) do
    DispatchProtocol.new_entry(:run_unarchived, %{
      run_id: command.run_id,
      occurred_at: command.now
    })
  end

  defp snapshot(command) do
    Inspection.snapshot(
      command.storage,
      command.run_id,
      queue: command.queue,
      now: command.now
    )
  end

  defp reason(opts) do
    case Keyword.get(opts, :reason) do
      reason when is_binary(reason) -> normalize_reason(reason)
      _missing_or_invalid -> {:error, {:invalid_option, {:reason, :invalid}}}
    end
  end

  defp normalize_reason(reason) do
    normalized = String.trim(reason)

    if normalized != "" and String.valid?(normalized) and
         byte_size(normalized) <= @max_reason_bytes and not String.contains?(normalized, <<0>>) do
      {:ok, normalized}
    else
      {:error, {:invalid_option, {:reason, :invalid}}}
    end
  end
end
