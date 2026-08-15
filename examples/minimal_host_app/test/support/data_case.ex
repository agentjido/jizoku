defmodule MinimalHostApp.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate
  use Oban.Testing, repo: MinimalHostApp.Repo

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      alias MinimalHostApp.Repo

      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import MinimalHostApp.DataCase
    end
  end

  setup tags do
    original_queue = Application.get_env(:jizoku, :queue)
    queue = "minimal-host-app-test-#{System.unique_integer([:positive])}"
    Application.put_env(:jizoku, :queue, queue)

    on_exit(fn ->
      case original_queue do
        nil -> Application.delete_env(:jizoku, :queue)
        queue -> Application.put_env(:jizoku, :queue, queue)
      end
    end)

    pid = Sandbox.start_owner!(MinimalHostApp.Repo, shared: not tags[:async])
    cleanup_runtime_state()
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  defp cleanup_runtime_state do
    MinimalHostApp.Repo.delete_all("jizoku_journal_entries")
    MinimalHostApp.Repo.delete_all("jizoku_journal_checkpoints")
    MinimalHostApp.Repo.delete_all("jizoku_journal_threads")
    MinimalHostApp.Repo.delete_all("local_ledger_entries")
  end
end
