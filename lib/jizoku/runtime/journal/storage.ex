# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Journal.Storage do
  @moduledoc """
  Normalized storage boundary for journal-backed runtime state.

  Jizoku's journal runtime uses Jido storage adapters today, but runtime
  modules should depend on one Jizoku-owned boundary. This struct carries a
  validated adapter and options while preserving the public storage config shape
  accepted by `runtime: :journal`.
  """

  @enforce_keys [:adapter, :opts, :config]
  defstruct [:adapter, :opts, :config, :commit_buffer, partition: nil]

  @storage_callbacks [
    get_checkpoint: 2,
    put_checkpoint: 3,
    delete_checkpoint: 2,
    load_thread: 2,
    append_thread: 3,
    delete_thread: 2
  ]

  @type config :: module() | {module(), keyword()}
  @type t :: %__MODULE__{
          adapter: module(),
          opts: keyword(),
          config: config(),
          commit_buffer: Jizoku.Telemetry.CommitBuffer.t() | nil,
          partition: String.t() | nil
        }

  @doc false
  @spec scope(t() | config(), String.t() | nil) ::
          {:ok, t()} | {:error, {:invalid_option, term()}}
  def scope(storage, partition) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage),
         {:ok, partition} <- Jizoku.Runtime.Partition.normalize(partition) do
      {:ok, %__MODULE__{storage | partition: partition}}
    end
  end

  @doc false
  @spec partition(t() | config()) :: String.t() | nil
  def partition(%__MODULE__{partition: partition}), do: partition
  def partition(_storage), do: nil

  @doc false
  @spec commit_buffer(t() | config()) :: Jizoku.Telemetry.CommitBuffer.t() | nil
  def commit_buffer(%__MODULE__{commit_buffer: commit_buffer}), do: commit_buffer
  def commit_buffer(_storage), do: nil

  @doc false
  @spec put_commit_buffer(t() | config(), Jizoku.Telemetry.CommitBuffer.t()) ::
          {:ok, t()} | {:error, {:invalid_option, term()}}
  def put_commit_buffer(storage, %Jizoku.Telemetry.CommitBuffer{} = commit_buffer) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage) do
      {:ok, %__MODULE__{storage | commit_buffer: commit_buffer}}
    end
  end

  @doc false
  @spec normalize(term()) :: {:ok, t()} | {:error, {:invalid_option, term()}}
  def normalize(
        %__MODULE__{
          adapter: module,
          opts: opts,
          partition: partition,
          commit_buffer: commit_buffer
        } = storage
      )
      when is_atom(module) and is_list(opts) do
    validate_storage_module(
      module,
      storage,
      storage_config(module, opts),
      opts,
      partition,
      commit_buffer
    )
  end

  def normalize(%__MODULE__{} = storage), do: invalid_storage(storage)

  def normalize(nil), do: {:error, {:invalid_option, {:journal_storage, nil}}}

  def normalize(storage) when is_atom(storage) do
    validate_storage_module(storage, storage, storage, [], nil, nil)
  end

  def normalize({module, opts} = storage) when is_atom(module) and is_list(opts) do
    validate_storage_module(module, storage, storage, opts, nil, nil)
  end

  def normalize(storage), do: invalid_storage(storage)

  @doc false
  @spec append_thread(t() | config(), String.t(), [Jido.Thread.Entry.t()], keyword()) ::
          {:ok, Jido.Thread.t()} | {:error, term()}
  def append_thread(storage, thread_id, entries, opts)
      when is_binary(thread_id) and is_list(entries) and is_list(opts) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage) do
      storage.adapter.append_thread(thread_id, entries, Keyword.merge(storage.opts, opts))
    end
  end

  @doc false
  @spec fetch_thread(t() | config(), String.t()) :: {:ok, Jido.Thread.t()} | {:error, term()}
  def fetch_thread(storage, thread_id) when is_binary(thread_id) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage) do
      Jido.Storage.fetch_thread(storage.adapter, thread_id, storage.opts)
    end
  end

  @doc false
  @spec put_checkpoint(t() | config(), term(), term()) :: :ok | {:error, term()}
  def put_checkpoint(storage, key, checkpoint) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage) do
      storage.adapter.put_checkpoint(key, checkpoint, storage.opts)
    end
  end

  @doc false
  @spec fetch_checkpoint(t() | config(), term()) :: {:ok, term()} | {:error, term()}
  def fetch_checkpoint(storage, key) do
    with {:ok, %__MODULE__{} = storage} <- normalize(storage) do
      Jido.Storage.fetch_checkpoint(storage.adapter, key, storage.opts)
    end
  end

  defp validate_storage_module(module, storage, config, opts, partition, commit_buffer) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- storage_callbacks?(module),
         :ok <- validate_storage_options(module, opts),
         :ok <- validate_commit_buffer(commit_buffer),
         {:ok, partition} <- Jizoku.Runtime.Partition.normalize(partition) do
      {:ok,
       %__MODULE__{
         adapter: module,
         opts: opts,
         config: config,
         partition: partition,
         commit_buffer: commit_buffer
       }}
    else
      {:error, {:invalid_option, {:partition, :invalid}}} = error -> error
      _error -> invalid_storage(storage)
    end
  end

  defp storage_config(module, []), do: module
  defp storage_config(module, opts), do: {module, opts}

  defp storage_callbacks?(module) do
    Enum.all?(@storage_callbacks, fn {name, arity} ->
      function_exported?(module, name, arity)
    end)
  end

  defp validate_storage_options(Jido.Storage.File, opts) do
    case Keyword.get(opts, :path) do
      path when is_binary(path) and path != "" -> :ok
      _invalid -> :error
    end
  end

  defp validate_storage_options(Jido.Storage.Redis, opts) do
    case Keyword.get(opts, :command_fn) do
      command_fn when is_function(command_fn, 1) -> :ok
      _invalid -> :error
    end
  end

  defp validate_storage_options(Jizoku.Runtime.Journal.Storage.Ecto, opts) do
    case Keyword.get(opts, :repo) do
      repo when is_atom(repo) and not is_nil(repo) ->
        if Code.ensure_loaded?(repo) and function_exported?(repo, :transaction, 1) do
          :ok
        else
          :error
        end

      _invalid ->
        :error
    end
  end

  defp validate_storage_options(_module, _opts), do: :ok

  defp validate_commit_buffer(nil), do: :ok
  defp validate_commit_buffer(%Jizoku.Telemetry.CommitBuffer{}), do: :ok
  defp validate_commit_buffer(_commit_buffer), do: :error

  defp invalid_storage(%__MODULE__{adapter: module}) when is_atom(module) do
    {:error, {:invalid_option, {:journal_storage, module}}}
  end

  defp invalid_storage({module, _opts}) when is_atom(module) do
    {:error, {:invalid_option, {:journal_storage, module}}}
  end

  defp invalid_storage(module) when is_atom(module) do
    {:error, {:invalid_option, {:journal_storage, module}}}
  end

  defp invalid_storage(_storage) do
    {:error, {:invalid_option, {:journal_storage, :invalid}}}
  end
end
