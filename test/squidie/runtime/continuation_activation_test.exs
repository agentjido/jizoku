defmodule Squidie.Runtime.ContinuationActivationTest do
  use ExUnit.Case, async: false

  alias Squidie.Runtime.ContinuationActivation

  setup do
    previous = Application.fetch_env(:squidie, :continuation_fences)
    Application.delete_env(:squidie, :continuation_fences)

    on_exit(fn -> restore_env(previous) end)

    :ok
  end

  test "is disabled when the host has not activated continuation fences" do
    assert ContinuationActivation.ensure_enabled() ==
             {:error, :continuation_fence_not_activated}
  end

  test "accepts the explicit host activation value" do
    Application.put_env(:squidie, :continuation_fences, :enabled)

    assert :ok = ContinuationActivation.ensure_enabled()
  end

  test "fails closed for disabled or invalid host values" do
    for value <- [:disabled, true, false, nil, "enabled", %{enabled: true}] do
      Application.put_env(:squidie, :continuation_fences, value)

      assert ContinuationActivation.ensure_enabled() ==
               {:error, :continuation_fence_not_activated}
    end
  end

  defp restore_env({:ok, value}) do
    Application.put_env(:squidie, :continuation_fences, value)
  end

  defp restore_env(:error) do
    Application.delete_env(:squidie, :continuation_fences)
  end
end
