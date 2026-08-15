defmodule Jizoku.Runtime.Journal.Storage.EctoContractTest do
  use ExUnit.Case, async: false
  use Jizoku.JournalStorageContract

  alias Ecto.Adapters.SQL.Sandbox
  alias Jizoku.Persistence.JournalCheckpoint
  alias Jizoku.Persistence.JournalEntry
  alias Jizoku.Persistence.JournalThread
  alias Jizoku.Test.Repo

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
    {:ok, storage: {Jizoku.Runtime.Journal.Storage.Ecto, repo: Repo}}
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
