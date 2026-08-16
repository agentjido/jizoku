defmodule Jizoku.Runtime.Journal.Commands.SearchAttributes do
  @moduledoc false

  alias Jido.Agent
  alias Jizoku.ReadModel.Inspection
  alias Jizoku.Runtime.DispatchProtocol
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Options
  alias Jizoku.Runtime.WorkflowAgent
  alias Jizoku.Runtime.WorkflowAgent.Projection

  @append_retries 8

  @doc """
  Validates and appends one idempotent search-attribute patch to a run thread.
  """
  @spec update(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, term()}
  def update(run_id, changes, opts)
      when is_map(changes) and is_list(opts) do
    schema = Keyword.get(opts, :search_attribute_schema)

    with {:ok, run_id} <- Options.uuid_run_id(run_id),
         {:ok, storage} <- Options.storage_from_opts(opts),
         {:ok, queue} <- Options.queue_from_opts(opts),
         {:ok, now} <- Options.now_from_opts(opts),
         {:ok, idempotency_key} <- idempotency_key(opts),
         {:ok, changes} <- Jizoku.Runtime.SearchAttributes.normalize(changes, schema) do
      command = %{
        storage: storage,
        run_id: run_id,
        queue: queue,
        changes: changes,
        schema: schema,
        fingerprint: Jizoku.Runtime.SearchAttributes.fingerprint(changes),
        idempotency_key: idempotency_key,
        now: now
      }

      do_update(command, @append_retries)
    end
  end

  def update(_run_id, _changes, _opts) do
    {:error, {:invalid_option, {:search_attributes, :invalid}}}
  end

  defp do_update(command, retries_left) do
    with {:ok, %Agent{state: %{projection: %Projection{} = projection, thread_rev: thread_rev}}} <-
           WorkflowAgent.rebuild(command.storage, command.run_id) do
      case Projection.search_attribute_update_fingerprint(
             projection,
             command.idempotency_key
           ) do
        fingerprint when fingerprint == command.fingerprint ->
          snapshot(command)

        nil ->
          append_new_update(command, projection, thread_rev, retries_left)

        _conflicting_fingerprint ->
          {:error, {:idempotency_conflict, command.idempotency_key}}
      end
    end
  end

  defp append_new_update(command, projection, thread_rev, retries_left) do
    merged_attributes =
      projection
      |> Projection.search_attributes()
      |> Map.merge(command.changes)

    with {:ok, _merged_attributes} <-
           Jizoku.Runtime.SearchAttributes.normalize(merged_attributes, command.schema) do
      append_update(command, thread_rev, retries_left)
    end
  end

  defp append_update(command, thread_rev, retries_left) do
    with {:ok, entry} <-
           DispatchProtocol.new_entry(:search_attributes_updated, %{
             run_id: command.run_id,
             changes: command.changes,
             fingerprint: command.fingerprint,
             idempotency_key: command.idempotency_key,
             occurred_at: command.now
           }) do
      case Journal.append_entries(command.storage, [entry], expected_rev: thread_rev) do
        {:ok, _thread} ->
          snapshot(command)

        {:error, :conflict} when retries_left > 0 ->
          do_update(command, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp snapshot(command) do
    Inspection.snapshot(
      command.storage,
      command.run_id,
      queue: command.queue,
      now: command.now
    )
  end

  defp idempotency_key(opts) do
    case Keyword.get(opts, :idempotency_key) do
      value when is_binary(value) ->
        if Jizoku.Runtime.SearchAttributes.valid_idempotency_key?(value),
          do: {:ok, value},
          else: {:error, {:invalid_option, {:idempotency_key, :invalid}}}

      _invalid ->
        {:error, {:invalid_option, {:idempotency_key, :invalid}}}
    end
  end
end
