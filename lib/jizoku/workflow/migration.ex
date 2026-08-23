defmodule Jizoku.Workflow.Migration do
  @moduledoc """
  Host-owned contract for one explicit workflow-definition migration.

  Migration callbacks run only while issuing the command. Their normalized,
  bounded result is persisted so journal replay never calls migration code.
  Callbacks must be deterministic and free of external side effects because an
  optimistic append conflict may cause the command to evaluate them again.
  """

  alias Jizoku.Runtime.Journal.Options

  @max_key_bytes 128
  @max_context_bytes 65_536

  @type state :: %{
          required(:context) => map(),
          required(:manual_state) => map(),
          required(:source_version) => String.t(),
          required(:source_fingerprint) => String.t(),
          required(:target_version) => String.t(),
          required(:target_fingerprint) => String.t()
        }
  @type result :: %{
          required(:context) => map(),
          optional(:manual_step) => atom() | String.t()
        }
  @type contract :: %{
          required(:module) => module(),
          required(:key) => String.t(),
          required(:source_version) => String.t(),
          required(:target_version) => String.t()
        }

  @callback key() :: String.t()
  @callback source_version() :: String.t()
  @callback target_version() :: String.t()
  @callback migrate(state()) :: {:ok, result()} | {:error, term()}

  @doc """
  Loads and validates a migration module's durable identity contract.
  """
  @spec contract(term()) :: {:ok, contract()} | {:error, term()}
  def contract(module) when is_atom(module) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         :ok <- required_callbacks(module),
         {:ok, key} <- identifier(module.key(), :key),
         {:ok, source_version} <- identifier(module.source_version(), :source_version),
         {:ok, target_version} <- identifier(module.target_version(), :target_version),
         :ok <- distinct_versions(source_version, target_version) do
      {:ok,
       %{
         module: module,
         key: key,
         source_version: source_version,
         target_version: target_version
       }}
    else
      {:error, {:invalid_workflow_migration, _reason}} = error -> error
      _missing -> {:error, {:invalid_workflow_migration, :invalid_module}}
    end
  end

  def contract(_module) do
    {:error, {:invalid_workflow_migration, :invalid_module}}
  end

  @doc """
  Evaluates a migration callback and validates its bounded persisted result.
  """
  @spec apply(contract(), state()) :: {:ok, result()} | {:error, term()}
  def apply(%{module: module}, state) when is_atom(module) and is_map(state) do
    with result <- invoke(module, state),
         {:ok, context} <- migrated_context(result),
         {:ok, manual_step} <- migrated_manual_step(result, state),
         :ok <- bounded_context(context) do
      {:ok, %{context: context, manual_step: manual_step}}
    end
  end

  def apply(_contract, _state) do
    {:error, {:invalid_workflow_migration_result, :invalid}}
  end

  defp required_callbacks(module) do
    callbacks = [:key, :source_version, :target_version, :migrate]

    if Enum.all?(callbacks, &function_exported?(module, &1, callback_arity(&1))) do
      :ok
    else
      {:error, {:invalid_workflow_migration, :missing_callback}}
    end
  end

  defp callback_arity(:migrate) do
    1
  end

  defp callback_arity(_callback) do
    0
  end

  defp identifier(value, field)
       when is_binary(value) and value != "" and byte_size(value) <= @max_key_bytes do
    if String.valid?(value) do
      {:ok, value}
    else
      {:error, {:invalid_workflow_migration, field}}
    end
  end

  defp identifier(_value, field) do
    {:error, {:invalid_workflow_migration, field}}
  end

  defp distinct_versions(version, version) do
    {:error, {:invalid_workflow_migration, :same_version}}
  end

  defp distinct_versions(_source_version, _target_version) do
    :ok
  end

  defp invoke(module, state) do
    module.migrate(state)
  catch
    :error, %{__struct__: exception_module} ->
      {:error, {:callback_exception, exception_module}}

    kind, _reason ->
      {:error, {:callback_caught, kind}}
  end

  defp migrated_context({:ok, %{context: context}})
       when is_map(context) and not is_struct(context) do
    {:ok, context}
  end

  defp migrated_context({:ok, _result}) do
    {:error, {:invalid_workflow_migration_result, :context}}
  end

  defp migrated_context({:error, reason}) do
    {:error, {:workflow_migration_failed, reason}}
  end

  defp migrated_context(_result) do
    {:error, {:invalid_workflow_migration_result, :return}}
  end

  defp migrated_manual_step({:ok, result}, state) when is_map(result) do
    step = Map.get(result, :manual_step, get_in(state, [:manual_state, :step]))

    case step do
      value when is_atom(value) -> {:ok, value}
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {:invalid_workflow_migration_result, :manual_step}}
    end
  end

  defp bounded_context(context) do
    if Options.storage_safe_value?(context) and
         byte_size(:erlang.term_to_binary(context)) <= @max_context_bytes do
      :ok
    else
      {:error, {:invalid_workflow_migration_result, :context_bounds}}
    end
  end
end
