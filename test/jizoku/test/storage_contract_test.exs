defmodule Jizoku.Test.StorageContractTest do
  use ExUnit.Case, async: true
  use Jizoku.JournalStorageContract

  alias Jizoku.Test.Storage

  @now ~U[2026-08-11 12:00:00Z]

  setup do
    {:ok, server} = Storage.start_link(self(), @now)
    on_exit(fn -> Storage.stop(server) end)

    {:ok, storage: {Storage, server: server}}
  end

  defp contract_storage(%{storage: storage}) do
    storage
  end

  defp contract_run_task(fun) do
    fun.()
  end

  test "ignores unrelated process messages", %{storage: {Storage, server: server}} do
    send(server, :unrelated_message)

    assert {:ok, @now} = Storage.now(server)
  end

  test "does not disguise an abnormal storage crash as a stopped runtime", %{
    storage: {Storage, server: server}
  } do
    Process.unlink(server)
    :sys.replace_state(server, fn _state -> %{} end)

    assert {{{:badkey, :now, %{}}, _stacktrace}, {GenServer, :call, _args}} =
             catch_exit(Storage.now(server))
  end
end
