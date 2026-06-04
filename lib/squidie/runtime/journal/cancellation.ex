defmodule Squidie.Runtime.Journal.Cancellation do
  @moduledoc """
  Journal-backed workflow cancellation.

  Cancellation appends a terminal run fact to the run thread. Dispatch
  projections already overlay terminal run facts from run threads, so stale
  claims and later completions are fenced by the same durable source of truth.
  The configured queue selects the returned dispatch projection; cancellation
  itself is scoped by the globally unique run id.
  """

  alias Jido.Agent
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.CommandReceipt
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Signal
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection

  @run_append_retries 25
  @terminal_statuses [:completed, :failed, :cancelled]

  @type cancel_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:invalid_transition, atom(), :cancelling}
          | term()

  @doc """
  Cancels a journal-backed workflow run.
  """
  @spec cancel(String.t(), keyword()) :: {:ok, Inspection.Snapshot.t()} | {:error, cancel_error()}
  def cancel(run_id, opts \\ [])

  def cancel(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, _workflow_agent} <- cancel_or_repair(storage, run_id, now, @run_append_retries) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def cancel(_run_id, _opts), do: {:error, :invalid_run_id}

  @doc false
  @spec apply_signal(Signal.t(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, cancel_error() | {:invalid_signal, term()}}
  def apply_signal(
        %Signal{
          type: :cancel_run,
          payload: %{run_id: run_id},
          occurred_at: %DateTime{} = now
        } = signal,
        opts
      )
      when is_binary(run_id) and is_list(opts) do
    with {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, _workflow_agent} <-
           cancel_or_repair(storage, signal_command(run_id, now, signal), @run_append_retries) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def apply_signal(%Signal{type: :cancel_run}, _opts),
    do: {:error, {:invalid_signal, :cancel_run}}

  def apply_signal(%Signal{type: type}, _opts), do: {:error, {:unsupported_signal, type}}
  def apply_signal(_signal, _opts), do: {:error, :invalid_signal}

  defp cancel_or_repair(_storage, _command, 0), do: {:error, :conflict}

  defp cancel_or_repair(
         storage,
         %{run_id: run_id, occurred_at: %DateTime{} = now} = command,
         retries_left
       ) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id) do
      cancel_or_ignore_duplicate(storage, workflow_agent, command, now, retries_left)
    end
  end

  defp cancel_or_repair(storage, run_id, %DateTime{} = now, retries_left) do
    cancel_or_repair(storage, signal_command(run_id, now, nil), retries_left)
  end

  defp append_cancellation(
         storage,
         workflow_agent,
         entries,
         %{occurred_at: %DateTime{} = now} = command,
         retries_left
       )
       when is_list(entries) do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          _checkpoint_result =
            WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: now)

          {:ok, workflow_agent}
        end

      {:error, :conflict} ->
        retry_command = %{command | run_id: workflow_agent.state.run_id}
        cancel_or_repair(storage, retry_command, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp cancellable?(storage, %Agent{
         agent_module: WorkflowAgent,
         state: %{projection: %Projection{} = projection}
       }) do
    status = Projection.status(projection)

    if status in @terminal_statuses do
      {:error, {:invalid_transition, status, :cancelling}}
    else
      linked_children_started?(storage, projection)
    end
  end

  defp cancel_or_ignore_duplicate(storage, workflow_agent, command, now, retries_left) do
    if duplicate_cancel_signal?(workflow_agent, command) do
      {:ok, workflow_agent}
    else
      cancel_workflow_agent(storage, workflow_agent, command, now, retries_left)
    end
  end

  defp cancel_workflow_agent(storage, workflow_agent, command, %DateTime{} = now, retries_left) do
    with :ok <- cancellable?(storage, workflow_agent),
         {:ok, command_receipt} <- cancel_command_receipt(command),
         {:ok, terminal_entry} <- run_terminal_entry(command.run_id, :cancelled, now) do
      append_cancellation(
        storage,
        workflow_agent,
        [command_receipt, terminal_entry],
        command,
        retries_left
      )
    end
  end

  defp linked_children_started?(storage, %Projection{} = projection) do
    projection
    |> Projection.child_runs()
    |> Enum.reduce_while(:ok, fn child_run, :ok ->
      case child_started?(storage, child_run) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp child_started?(storage, child_run) do
    case child_run_id(child_run) do
      run_id when is_binary(run_id) -> load_child_thread(storage, run_id)
      _missing_or_invalid -> child_starting_error()
    end
  end

  defp load_child_thread(storage, run_id) do
    case Journal.load_thread(storage, {:run, run_id}) do
      {:ok, _thread} -> :ok
      {:error, :not_found} -> child_starting_error()
      {:error, _reason} = error -> error
    end
  end

  defp child_starting_error do
    {:error, {:invalid_transition, :child_starting, :cancelling}}
  end

  defp duplicate_cancel_signal?(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}},
         %{signal: %Signal{idempotency_key: idempotency_key}}
       )
       when is_binary(idempotency_key) do
    Projection.status(projection) == :cancelled and
      Enum.any?(Projection.command_history(projection), fn command ->
        Map.get(command, :signal_type) == "cancel_run" and
          Map.get(command, :idempotency_key) == idempotency_key
      end)
  end

  defp duplicate_cancel_signal?(_workflow_agent, _command), do: false

  defp child_run_id(child_run) when is_map(child_run) do
    map_value(child_run, :child_run_id)
  end

  defp child_run_id(_child_run), do: nil

  defp map_value(map, key, default \\ nil)

  defp map_value(map, key, default) when is_map(map) and is_atom(key) do
    value = Map.get(map, key)

    if is_nil(value) do
      Map.get(map, Atom.to_string(key), default)
    else
      value
    end
  end

  defp map_value(_map, _key, default), do: default

  defp rebuild_workflow_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _workflow_agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp run_terminal_entry(run_id, status, %DateTime{} = now) do
    DispatchProtocol.new_entry(:run_terminal, %{
      run_id: run_id,
      status: status,
      occurred_at: now
    })
  end

  defp cancel_command_receipt(%{signal: %Signal{} = signal}) do
    run_id = Map.fetch!(signal.payload, :run_id)

    CommandReceipt.new(
      :cancel_run,
      %{
        run_id: run_id,
        payload: signal.payload,
        metadata: signal.metadata,
        idempotency_key: signal.idempotency_key
      },
      signal.occurred_at
    )
  end

  defp cancel_command_receipt(%{run_id: run_id, occurred_at: %DateTime{} = now}) do
    CommandReceipt.new(:cancel_run, %{run_id: run_id, payload: %{run_id: run_id}}, now)
  end

  defp signal_command(run_id, %DateTime{} = now, signal) do
    %{run_id: run_id, occurred_at: now, signal: signal}
  end

  defp run_id(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, :invalid_run_id}
    end
  end

  defp journal_storage(opts) do
    opts
    |> Keyword.get(:journal_storage)
    |> Options.storage()
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp now(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end
end
