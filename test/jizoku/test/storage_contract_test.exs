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
end
