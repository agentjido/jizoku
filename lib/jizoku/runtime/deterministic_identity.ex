# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.DeterministicIdentity do
  @moduledoc false

  import Bitwise, only: [band: 2, bor: 2]

  @doc false
  @spec encode_parts([String.t()]) :: binary()
  def encode_parts(parts) when is_list(parts) do
    parts
    |> Enum.map(fn part -> [Integer.to_string(byte_size(part)), ":", part] end)
    |> Enum.intersperse("|")
    |> IO.iodata_to_binary()
  end

  @doc false
  @spec uuid([String.t()]) :: Ecto.UUID.t()
  def uuid(parts) when is_list(parts) do
    <<a::32, b::16, c::16, d::16, e::48, _rest::binary>> =
      parts
      |> encode_parts()
      |> then(&:crypto.hash(:sha256, &1))

    version = bor(band(c, 0x0FFF), 0x5000)
    variant = bor(band(d, 0x3FFF), 0x8000)
    uuid_binary = <<a::32, b::16, version::16, variant::16, e::48>>
    {:ok, uuid} = Ecto.UUID.load(uuid_binary)
    uuid
  end
end
