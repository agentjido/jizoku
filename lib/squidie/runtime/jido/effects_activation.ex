defmodule Squidie.Runtime.Jido.EffectsActivation do
  @moduledoc false

  @config_key :jido_effects

  @doc false
  @spec ensure_enabled() :: :ok | {:error, :jido_effects_not_activated}
  def ensure_enabled do
    case Application.get_env(:squidie, @config_key, :disabled) do
      :enabled -> :ok
      _disabled_or_invalid -> {:error, :jido_effects_not_activated}
    end
  end
end
