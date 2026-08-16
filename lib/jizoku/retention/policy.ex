defmodule Jizoku.Retention.Policy do
  @moduledoc """
  Trusted host hook for adding legal-hold or export requirements to retention.

  The configured policy may only add a block. It cannot waive Jizoku's
  intrinsic runtime, lineage, reconciliation, or archive checks.
  """

  alias Jizoku.ReadModel.Inspection.Snapshot

  @type context :: %{partition: String.t() | nil, now: DateTime.t()}
  @type decision :: :allow | {:block, atom()}

  @callback evaluate(Snapshot.t(), context(), term()) :: decision()

  @doc false
  @spec evaluate(Snapshot.t(), context()) :: :allow | {:block, atom()} | {:error, term()}
  def evaluate(%Snapshot{} = snapshot, context) when is_map(context) do
    case Application.get_env(:jizoku, :retention_policy) do
      nil -> :allow
      module when is_atom(module) -> invoke(module, snapshot, context, [])
      {module, opts} when is_atom(module) -> invoke(module, snapshot, context, opts)
      _invalid -> {:error, :invalid_retention_policy}
    end
  end

  defp invoke(module, snapshot, context, opts) do
    if Code.ensure_loaded?(module) and function_exported?(module, :evaluate, 3) do
      case module.evaluate(snapshot, context, opts) do
        :allow -> :allow
        {:block, reason} when is_atom(reason) -> {:block, reason}
        _invalid -> {:error, :invalid_retention_policy_decision}
      end
    else
      {:error, :invalid_retention_policy}
    end
  catch
    :error, _reason -> {:error, :retention_policy_failed}
    :exit, _reason -> {:error, :retention_policy_failed}
    :throw, _reason -> {:error, :retention_policy_failed}
  end
end
