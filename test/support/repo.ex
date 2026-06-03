defmodule Squidie.Test.Repo do
  @moduledoc false

  use Ecto.Repo, otp_app: :squidie, adapter: Ecto.Adapters.Postgres
end
