# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Jido.EffectsActivation do
  @moduledoc false

  @config_key :jido_effects
  @emit_config_key :jido_emit_effects

  @doc false
  @spec ensure_enabled() :: :ok | {:error, :jido_effects_not_activated}
  def ensure_enabled do
    case Application.get_env(:squidie, @config_key, :disabled) do
      :enabled -> :ok
      _disabled_or_invalid -> {:error, :jido_effects_not_activated}
    end
  end

  @doc false
  @spec ensure_emit_enabled() :: :ok | {:error, :jido_emit_effects_not_activated}
  def ensure_emit_enabled do
    case Application.get_env(:squidie, @emit_config_key, :disabled) do
      :enabled -> :ok
      _disabled_or_invalid -> {:error, :jido_emit_effects_not_activated}
    end
  end
end
