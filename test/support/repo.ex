defmodule Jizoku.Test.Repo do
  @moduledoc false

  use Ecto.Repo, otp_app: :jizoku, adapter: Ecto.Adapters.Postgres
end
