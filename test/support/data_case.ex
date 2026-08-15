defmodule Jizoku.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      alias Jizoku.Test.Repo

      import ExUnit.Assertions
      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import Jizoku.DataCase
    end
  end

  setup tags do
    pid = Sandbox.start_owner!(Jizoku.Test.Repo, shared: not tags[:async])
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  @spec errors_on(Ecto.Changeset.t()) :: map()
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _whole, key ->
        opts
        |> Keyword.get(String.to_existing_atom(key), key)
        |> to_string()
      end)
    end)
  end
end
