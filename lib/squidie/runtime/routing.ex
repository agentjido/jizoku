defmodule Squidie.Runtime.Routing do
  @moduledoc """
  Resolves public runtime/read-model choices into journal boundary options.

  The public `Squidie` facade owns workflow operations. This module owns the
  lower-level routing rules that turn host configuration and explicit overrides
  into validated runtime, read-model, and journal option sets.
  """

  alias Squidie.Config
  alias Squidie.Runtime.Journal.Options
  alias Squidie.Runtime.Journal.Storage

  @read_models [:read_model]
  @runtimes [:journal]
  @projection_snapshot_options [:queue, :now]
  @projection_list_options [:queue, :now]
  @journal_start_options [:runtime, :journal_storage, :queue, :partition, :now, :run_id]
  @journal_spec_start_options [
    :runtime,
    :journal_storage,
    :queue,
    :partition,
    :now,
    :run_id,
    :action_registry,
    :guardrail_registry
  ]
  @journal_child_start_options [
    :runtime,
    :journal_storage,
    :queue,
    :partition,
    :now,
    :child_key,
    :metadata
  ]
  @journal_control_options [:runtime, :journal_storage, :queue, :partition, :now]
  @journal_execute_options [
    :runtime,
    :journal_storage,
    :queue,
    :partition,
    :owner_id,
    :lease_for,
    :heartbeat_interval_ms,
    :now,
    :action_registry,
    :guardrail_registry
  ]
  @journal_dynamic_work_options [
    :runtime,
    :read_model,
    :journal_storage,
    :queue,
    :partition,
    :now,
    :repo,
    :action_registry
  ]

  @doc """
  Resolves the configured read model from explicit overrides or app config.
  """
  @spec read_model(keyword()) :: {:ok, :read_model} | {:error, term()}
  def read_model(overrides) when is_list(overrides) do
    with :ok <- validate_keyword_options(overrides) do
      configured_read_model(overrides)
    end
  end

  def read_model(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  @doc """
  Resolves the configured runtime from explicit overrides or app config.
  """
  @spec runtime(keyword()) :: {:ok, :journal} | {:error, term()}
  def runtime(overrides) when is_list(overrides) do
    with :ok <- validate_keyword_options(overrides) do
      configured_runtime(overrides)
    end
  end

  def runtime(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  @doc """
  Resolves the journal storage adapter from overrides or inferred config.
  """
  @spec journal_storage(keyword()) :: {:ok, term()} | {:error, term()}
  def journal_storage(overrides) do
    with {:ok, storage, partition} <- configured_storage_and_partition(overrides),
         {:ok, storage} <- Options.storage(storage) do
      Squidie.Runtime.Journal.Storage.scope(storage, partition)
    end
  end

  @doc """
  Builds journal projection options used by single-run inspection.
  """
  @spec projection_snapshot_options(keyword()) :: keyword()
  def projection_snapshot_options(overrides) do
    configured_journal_options(overrides, @projection_snapshot_options)
  end

  @doc """
  Builds journal options for module-authored workflow starts.
  """
  @spec journal_start_options(keyword()) :: keyword()
  def journal_start_options(overrides) do
    configured_journal_options(overrides, @journal_start_options)
  end

  @doc """
  Builds journal options for runtime-authored workflow spec starts.
  """
  @spec journal_spec_start_options(keyword()) :: keyword()
  def journal_spec_start_options(overrides) do
    configured_journal_options(overrides, @journal_spec_start_options)
  end

  @doc """
  Builds journal options for child workflow starts.
  """
  @spec journal_child_start_options(keyword()) :: keyword()
  def journal_child_start_options(overrides) do
    overrides
    |> configured_journal_options(@journal_child_start_options)
    |> child_partition_override(overrides)
  end

  @doc """
  Builds journal options for control commands such as cancel, resume, and replay.
  """
  @spec journal_control_options(keyword()) :: keyword()
  def journal_control_options(overrides) do
    configured_journal_options(overrides, @journal_control_options)
  end

  @doc """
  Builds journal options for claiming and executing visible work.
  """
  @spec journal_execute_options(keyword()) :: keyword()
  def journal_execute_options(overrides) do
    configured_journal_options(overrides, @journal_execute_options)
  end

  @doc """
  Builds journal projection options used by run listing.
  """
  @spec journal_list_options(keyword()) :: keyword()
  def journal_list_options(overrides) do
    configured_journal_options(overrides, @projection_list_options)
  end

  @doc """
  Validates public child-start overrides before they reach the journal boundary.
  """
  @spec public_child_start_options(keyword()) :: :ok | {:error, term()}
  def public_child_start_options(opts) do
    validate_public_options(opts, @journal_child_start_options)
  end

  @doc """
  Validates public dynamic-work overrides before they reach the journal boundary.
  """
  @spec public_dynamic_work_options(keyword()) :: :ok | {:error, term()}
  def public_dynamic_work_options(opts) do
    validate_public_options(opts, @journal_dynamic_work_options)
  end

  @doc """
  Validates public execution overrides.
  """
  @spec public_execute_options(keyword() | term()) :: :ok | {:error, term()}
  def public_execute_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        :ok

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in @journal_execute_options)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  @doc """
  Rejects internal-only start options from public manual-start APIs.
  """
  @spec reject_public_start_options(keyword()) :: :ok | {:error, term()}
  def reject_public_start_options(overrides) do
    cond do
      Keyword.has_key?(overrides, :context) ->
        {:error, {:invalid_option, :context}}

      Keyword.has_key?(overrides, :initial_context) ->
        {:error, {:invalid_option, :initial_context}}

      true ->
        :ok
    end
  end

  defp validate_public_options(opts, keys) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in keys)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  defp validate_keyword_options(overrides) do
    if Keyword.keyword?(overrides) do
      :ok
    else
      {:error, {:invalid_option, {:opts, :invalid}}}
    end
  end

  defp configured_read_model(overrides) do
    case Keyword.fetch(overrides, :read_model) do
      {:ok, read_model} when read_model in @read_models ->
        {:ok, read_model}

      {:ok, _read_model} ->
        {:error, {:invalid_option, {:read_model, :invalid}}}

      :error ->
        load_configured_read_model(overrides)
    end
  end

  defp load_configured_read_model(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{read_model: read_model}} -> {:ok, read_model}
      {:error, _reason} = error -> error
    end
  end

  defp configured_runtime(overrides) do
    case Keyword.fetch(overrides, :runtime) do
      {:ok, runtime} when runtime in @runtimes ->
        {:ok, runtime}

      {:ok, _runtime} ->
        {:error, {:invalid_option, {:runtime, :invalid}}}

      :error ->
        load_configured_runtime(overrides)
    end
  end

  defp load_configured_runtime(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{runtime: runtime}} -> {:ok, runtime}
      {:error, _reason} = error -> error
    end
  end

  defp configured_journal_options(overrides, keys) do
    case load_config_for_journal_options(overrides) do
      {:ok, %Config{} = config} ->
        [
          runtime: :journal,
          journal_storage: config.journal_storage,
          queue: config.queue
        ]
        |> maybe_put_configured_partition(config.partition)
        |> Keyword.merge(Keyword.take(overrides, keys))
        |> Keyword.take(keys)

      {:error, _reason} ->
        Keyword.take(overrides, keys)
    end
  end

  defp load_config_for_journal_options(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{} = config} ->
        {:ok, config}

      {:error, _reason} ->
        overrides
        |> Keyword.delete(:journal_storage)
        |> config_routing_overrides()
        |> Config.load()
    end
  end

  defp config_routing_overrides(overrides) do
    overrides
    |> Keyword.take([
      :repo,
      :runtime,
      :read_model,
      :journal_storage,
      :partition
    ])
    |> preserve_explicit_storage_partition(overrides)
  end

  defp configured_storage_and_partition(overrides) do
    case Keyword.fetch(overrides, :journal_storage) do
      {:ok, storage} ->
        partition =
          overrides
          |> config_routing_overrides()
          |> Keyword.get(:partition, Application.get_env(:squidie, :partition))

        {:ok, storage, partition}

      :error ->
        configured_storage_from_config(overrides)
    end
  end

  defp configured_storage_from_config(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{} = config} ->
        {:ok, config.journal_storage, Keyword.get(overrides, :partition, config.partition)}

      {:error, {:missing_config, [:journal_storage]}} ->
        {:ok, nil, Keyword.get(overrides, :partition)}

      {:error, _reason} = error ->
        error
    end
  end

  defp maybe_put_configured_partition(opts, nil), do: opts

  defp maybe_put_configured_partition(opts, partition),
    do: Keyword.put(opts, :partition, partition)

  defp child_partition_override(opts, overrides) do
    case Keyword.fetch(overrides, :partition) do
      {:ok, partition} -> Keyword.put(opts, :partition, partition)
      :error -> Keyword.delete(opts, :partition)
    end
  end

  defp preserve_explicit_storage_partition(config_overrides, overrides) do
    case {Keyword.fetch(overrides, :partition), Keyword.fetch(overrides, :journal_storage)} do
      {:error, {:ok, storage}} ->
        put_storage_partition(config_overrides, Storage.partition(storage))

      {_partition, _storage} ->
        config_overrides
    end
  end

  defp put_storage_partition(config_overrides, nil), do: config_overrides

  defp put_storage_partition(config_overrides, partition),
    do: Keyword.put(config_overrides, :partition, partition)
end
