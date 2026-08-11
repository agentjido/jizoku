defmodule Squidie.Runtime.Journal.Storage.EctoContractTest do
  use ExUnit.Case, async: false
  use Squidie.JournalStorageContract

  alias Ecto.Adapters.SQL.Sandbox
  alias Squidie.Persistence.JournalCheckpoint
  alias Squidie.Persistence.JournalEntry
  alias Squidie.Persistence.JournalThread
  alias Squidie.Test.Repo

  setup_all do
    :ok = Sandbox.mode(Repo, :auto)

    on_exit(fn ->
      clean_storage()
      Sandbox.mode(Repo, :manual)
    end)

    :ok
  end

  setup do
    clean_storage()
    {:ok, storage: {Squidie.Runtime.Journal.Storage.Ecto, repo: Repo}}
  end

  defp contract_storage(%{storage: storage}) do
    storage
  end

  defp contract_run_task(fun) do
    Repo.checkout(fun)
  end

  defp clean_storage do
    Repo.delete_all(JournalCheckpoint)
    Repo.delete_all(JournalEntry)
    Repo.delete_all(JournalThread)
  end
end
