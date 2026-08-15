defmodule Jizoku.ReadModel.HistoryPolicy do
  @moduledoc """
  Host-configured thresholds for durable run and continuation-chain history.

  Public read models use this policy to classify run-thread size and choose the
  default bound for explicit continuation-chain traversal.
  """

  @default_run_warning_threshold 5_000
  @default_run_critical_threshold 20_000
  @default_chain_warning_hops 25
  @default_max_chain_hops 100

  @type level :: :normal | :warning | :critical
  @type summary :: %{
          required(:thread_revision) => non_neg_integer(),
          required(:level) => level(),
          required(:warning_threshold) => pos_integer(),
          required(:critical_threshold) => pos_integer()
        }

  @doc "Returns the configured size classification for one run thread."
  @spec summary(non_neg_integer()) :: summary()
  def summary(thread_revision) when is_integer(thread_revision) and thread_revision >= 0 do
    warning_threshold =
      configured_positive(:run_warning_threshold, @default_run_warning_threshold)

    critical_threshold =
      configured_greater(
        :run_critical_threshold,
        @default_run_critical_threshold,
        warning_threshold
      )

    %{
      thread_revision: thread_revision,
      level: history_level(thread_revision, warning_threshold, critical_threshold),
      warning_threshold: warning_threshold,
      critical_threshold: critical_threshold
    }
  end

  @doc "Returns the hop count at which continuation-chain warnings begin."
  @spec chain_warning_hops() :: pos_integer()
  def chain_warning_hops do
    configured_positive(:chain_warning_hops, @default_chain_warning_hops)
  end

  @doc "Returns the default upper bound for continuation-chain traversal."
  @spec max_chain_hops() :: pos_integer()
  def max_chain_hops do
    configured_positive(:max_chain_hops, @default_max_chain_hops)
  end

  defp history_level(thread_revision, _warning_threshold, critical_threshold)
       when thread_revision >= critical_threshold do
    :critical
  end

  defp history_level(thread_revision, warning_threshold, _critical_threshold)
       when thread_revision >= warning_threshold do
    :warning
  end

  defp history_level(_thread_revision, _warning_threshold, _critical_threshold) do
    :normal
  end

  defp configured_positive(key, default) do
    case Keyword.get(config(), key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp configured_greater(key, default, lower_bound) do
    case Keyword.get(config(), key, default) do
      value when is_integer(value) and value > lower_bound -> value
      _invalid -> max(default, lower_bound + 1)
    end
  end

  defp config do
    case Application.get_env(:jizoku, :continuation_history, []) do
      config when is_list(config) -> config
      _invalid -> []
    end
  end
end
