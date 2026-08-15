defmodule Jizoku.ReadModel.HistoryPolicyTest do
  use ExUnit.Case, async: false

  alias Jizoku.ReadModel.HistoryPolicy

  setup do
    previous = Application.fetch_env(:jizoku, :continuation_history)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:jizoku, :continuation_history, value)
        :error -> Application.delete_env(:jizoku, :continuation_history)
      end
    end)

    :ok
  end

  test "classifies run history against host thresholds" do
    Application.put_env(:jizoku, :continuation_history,
      run_warning_threshold: 3,
      run_critical_threshold: 5,
      chain_warning_hops: 2,
      max_chain_hops: 7
    )

    assert HistoryPolicy.summary(2).level == :normal
    assert HistoryPolicy.summary(3).level == :warning
    assert HistoryPolicy.summary(4).level == :warning

    assert HistoryPolicy.summary(5) == %{
             thread_revision: 5,
             level: :critical,
             warning_threshold: 3,
             critical_threshold: 5
           }

    assert HistoryPolicy.summary(6).level == :critical

    assert HistoryPolicy.chain_warning_hops() == 2
    assert HistoryPolicy.max_chain_hops() == 7
  end

  test "invalid configuration falls back to bounded defaults" do
    Application.put_env(:jizoku, :continuation_history,
      run_warning_threshold: 0,
      run_critical_threshold: 1,
      chain_warning_hops: :many,
      max_chain_hops: nil
    )

    assert HistoryPolicy.summary(0) == %{
             thread_revision: 0,
             level: :normal,
             warning_threshold: 5_000,
             critical_threshold: 20_000
           }

    assert HistoryPolicy.chain_warning_hops() == 25
    assert HistoryPolicy.max_chain_hops() == 100
  end
end
