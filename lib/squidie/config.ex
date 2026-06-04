defmodule Squidie.Config do
  @moduledoc """
  Loads and validates host application configuration for Squidie.

  This contract is intentionally small so application teams only configure the
  runtime boundary once, while workflow authors stay focused on declarative
  workflow definitions and public API usage.
  """

  alias Squidie.Runtime.Journal.Options

  @type runtime :: :journal
  @type read_model :: :read_model
  @type raw_config :: [
          repo: module() | nil,
          runtime: runtime(),
          read_model: read_model(),
          journal_storage: term(),
          queue: atom() | String.t()
        ]
  @type t :: %__MODULE__{
          repo: module() | nil,
          runtime: runtime(),
          read_model: read_model(),
          journal_storage: Squidie.Runtime.Journal.Storage.t() | nil,
          queue: String.t()
        }

  defstruct [
    :repo,
    :journal_storage,
    runtime: :journal,
    read_model: :read_model,
    queue: "default"
  ]

  @default_runtime :journal
  @default_read_model :read_model
  @default_queue "default"
  @runtimes [:journal]
  @read_models [:read_model]

  @type config_error :: {:missing_config, [atom()]} | {:invalid_config, keyword()}

  @doc """
  Loads Squidie configuration from the host application environment.

  Optional overrides are merged after application configuration so tests and
  embedding applications can supply runtime-specific repositories without
  mutating global application state.
  """
  @spec load(keyword()) :: {:ok, t()} | {:error, config_error()}
  def load(overrides \\ []) do
    config =
      :squidie
      |> Application.get_all_env()
      |> Keyword.merge(overrides)

    with {:ok, runtime} <- validate_runtime(Keyword.get(config, :runtime, @default_runtime)),
         {:ok, read_model} <-
           validate_read_model(Keyword.get(config, :read_model, @default_read_model)),
         {:ok, queue} <- validate_queue(Keyword.get(config, :queue, @default_queue)),
         {:ok, journal_storage} <- validate_journal_storage(config, runtime, read_model) do
      {:ok,
       %__MODULE__{
         repo: Keyword.get(config, :repo),
         runtime: runtime,
         read_model: read_model,
         journal_storage: journal_storage,
         queue: queue
       }}
    end
  end

  @doc """
  Loads configuration or raises an `ArgumentError` with the validation details.
  """
  @spec load!(keyword()) :: t()
  def load!(overrides \\ []) do
    case load(overrides) do
      {:ok, config} ->
        config

      {:error, {:missing_config, keys}} ->
        raise ArgumentError, missing_config_message(keys)

      {:error, {:invalid_config, details}} ->
        details =
          Enum.map_join(details, ", ", fn {key, value} -> "#{inspect(key)}=#{inspect(value)}" end)

        raise ArgumentError,
              "invalid Squidie configuration: #{details}"
    end
  end

  defp missing_config_message([:repo]) do
    "missing Squidie configuration keys: :repo. " <>
      "Public start APIs infer journal storage from `config :squidie, repo: MyApp.Repo` when `journal_storage:` is not passed explicitly. " <>
      "Set that config globally or pass an explicit `journal_storage:` override from the host boundary."
  end

  defp missing_config_message(keys) do
    keys = Enum.map_join(keys, ", ", &inspect/1)
    "missing Squidie configuration keys: #{keys}"
  end

  defp validate_runtime(runtime) when runtime in @runtimes, do: {:ok, runtime}

  defp validate_runtime(runtime) do
    {:error, {:invalid_config, [runtime: runtime]}}
  end

  defp validate_read_model(read_model) when read_model in @read_models, do: {:ok, read_model}

  defp validate_read_model(read_model) do
    {:error, {:invalid_config, [read_model: read_model]}}
  end

  defp validate_queue(queue) do
    case Options.queue(queue) do
      {:ok, queue} ->
        {:ok, queue}

      {:error, {:invalid_option, {:queue, :invalid}}} ->
        {:error, {:invalid_config, [queue: :invalid]}}
    end
  end

  defp validate_journal_storage(config, :journal, :read_model) do
    validate_required_journal_storage(config)
  end

  defp validate_required_journal_storage(config) do
    case Keyword.fetch(config, :journal_storage) do
      {:ok, nil} ->
        {:error, {:missing_config, [:journal_storage]}}

      {:ok, storage} ->
        case Options.storage(storage) do
          {:ok, storage} ->
            {:ok, storage}

          {:error, {:invalid_option, {:journal_storage, reason}}} ->
            {:error, {:invalid_config, [journal_storage: reason]}}
        end

      :error ->
        infer_journal_storage(config)
    end
  end

  defp infer_journal_storage(config) do
    case Keyword.fetch(config, :repo) do
      {:ok, repo} when is_atom(repo) ->
        case Options.storage({Squidie.Runtime.Journal.Storage.Ecto, repo: repo}) do
          {:ok, storage} ->
            {:ok, storage}

          {:error, {:invalid_option, {:journal_storage, reason}}} ->
            {:error, {:invalid_config, [journal_storage: reason]}}
        end

      {:ok, _invalid_repo} ->
        {:error, {:invalid_config, [repo: :invalid]}}

      _missing_repo ->
        {:error, {:missing_config, [:repo]}}
    end
  end
end
