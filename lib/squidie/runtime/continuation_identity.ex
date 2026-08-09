defmodule Squidie.Runtime.ContinuationIdentity do
  @moduledoc false

  alias Squidie.Runtime.DeterministicIdentity
  alias Squidie.Runtime.Journal.Options

  @domain "squidie-continuation-v1"
  @fields [
    :partition,
    :predecessor_run_id,
    :continuation_key,
    :workflow,
    :trigger,
    :definition_version,
    :definition_fingerprint
  ]

  @type attrs :: %{
          required(:partition) => String.t() | nil,
          required(:predecessor_run_id) => Ecto.UUID.t(),
          required(:continuation_key) => String.t(),
          required(:workflow) => String.t(),
          required(:trigger) => String.t(),
          required(:definition_version) => String.t() | nil,
          required(:definition_fingerprint) => String.t()
        }
  @type error :: {:invalid_continuation_identity, atom()}

  @doc false
  @spec successor_run_id(attrs() | term()) :: {:ok, Ecto.UUID.t()} | {:error, error()}
  def successor_run_id(attrs) when is_map(attrs) do
    with {:ok, partition} <- required_partition(attrs),
         {:ok, predecessor_run_id} <- required_uuid(attrs, :predecessor_run_id),
         {:ok, continuation_key} <- required_thread_part(attrs, :continuation_key),
         {:ok, workflow} <- required_binary(attrs, :workflow),
         {:ok, trigger} <- required_binary(attrs, :trigger),
         {:ok, definition_version} <- optional_binary(attrs, :definition_version),
         {:ok, definition_fingerprint} <- required_binary(attrs, :definition_fingerprint) do
      successor_run_id =
        DeterministicIdentity.uuid([
          @domain,
          optional_part(partition),
          predecessor_run_id,
          continuation_key,
          workflow,
          trigger,
          optional_part(definition_version),
          definition_fingerprint
        ])

      {:ok, successor_run_id}
    end
  end

  def successor_run_id(_attrs) do
    invalid(:invalid)
  end

  defp required_partition(attrs) do
    with {:ok, partition} <- fetch(attrs, :partition),
         {:ok, partition} <- Options.partition(partition) do
      {:ok, partition}
    else
      _invalid -> invalid(:partition)
    end
  end

  defp required_uuid(attrs, field) do
    with {:ok, value} <- fetch(attrs, field),
         {:ok, value} <- Options.uuid_run_id(value) do
      {:ok, value}
    else
      _invalid -> invalid(field)
    end
  end

  defp required_thread_part(attrs, field) do
    with {:ok, value} <- fetch(attrs, field),
         {:ok, value} <- Options.thread_part(value, field) do
      {:ok, value}
    else
      _invalid -> invalid(field)
    end
  end

  defp required_binary(attrs, field) do
    case fetch(attrs, field) do
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> invalid(field)
    end
  end

  defp optional_binary(attrs, field) do
    case fetch(attrs, field) do
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> invalid(field)
    end
  end

  defp fetch(attrs, field) do
    if field in @fields do
      Map.fetch(attrs, field)
    else
      :error
    end
  end

  defp optional_part(nil) do
    "nil"
  end

  defp optional_part(value) when is_binary(value) do
    "value:" <> value
  end

  defp invalid(field) do
    {:error, {:invalid_continuation_identity, field}}
  end
end
