defmodule Jizoku.Runtime.ContinuationActivation do
  @moduledoc false

  @config_key :continuation_fences

  @doc false
  @spec ensure_enabled() :: :ok | {:error, :continuation_fence_not_activated}
  def ensure_enabled do
    case Application.get_env(:jizoku, @config_key, :disabled) do
      :enabled -> :ok
      _disabled_or_invalid -> {:error, :continuation_fence_not_activated}
    end
  end
end
