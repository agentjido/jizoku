defmodule Squidie.Runtime.Journal.Options do
  @moduledoc """
  Validates public options that reach journal-backed runtime threads.

  Journal-backed start, inspection, and explanation all cross the same storage
  boundary: caller-provided storage config is normalized into a Squidie
  journal storage struct, and caller-provided run or queue identifiers become
  part of journal thread ids. This module keeps that validation in one place so
  those public APIs fail with structured, redacted errors before invalid values
  can reach an adapter or file-backed thread path.

  The validation is intentionally about runtime safety, not compatibility. It
  accepts the current Jido storage adapter shape through a Squidie-owned
  boundary, validates the built-in adapters whose required options can otherwise
  raise, and keeps thread id components to a conservative portable character
  set.
  """

  @thread_part_pattern ~r/^[A-Za-z0-9][A-Za-z0-9_.-]*$/
  @doc """
  Validates a journal storage config before a public API calls the adapter.

  Invalid configs return structured, redacted option errors. Built-in adapters
  that raise for missing required options are checked here first. Treat
  `journal_storage` as trusted host configuration, not request-derived input.
  """
  @spec storage(term()) ::
          {:ok, Squidie.Runtime.Journal.Storage.t()} | {:error, {:invalid_option, term()}}
  def storage(storage), do: Squidie.Runtime.Journal.Storage.normalize(storage)

  @doc """
  Normalizes and validates a dispatch queue name for use in journal thread ids.

  Atoms are converted to strings. Queue names must be non-empty and use the
  conservative portable thread-id character set enforced by this module.
  """
  @spec queue(term()) :: {:ok, String.t()} | {:error, {:invalid_option, {:queue, :invalid}}}
  def queue(queue \\ "default")

  def queue(queue) when is_atom(queue),
    do: validate_thread_part(Atom.to_string(queue), :queue)

  def queue(queue) when is_binary(queue), do: validate_thread_part(queue, :queue)
  def queue(_queue), do: invalid_option(:queue)

  @doc """
  Validates a caller-provided journal thread-id component.

  Use this for public identifiers, such as run ids in projection reads, that are
      not required to be UUIDs but still become part of a Jido thread id.
  """
  @spec thread_part(term(), atom()) :: {:ok, String.t()} | {:error, {:invalid_option, term()}}
  def thread_part(value, field) when is_binary(value) and is_atom(field) do
    validate_thread_part(value, field)
  end

  def thread_part(_value, field) when is_atom(field) do
    invalid_option(field)
  end

  @doc """
  Validates and canonicalizes a public run id that must be a UUID.

  Journal starts use UUID run ids so caller-provided ids are safe as stable
  workflow thread identifiers and duplicate-start fences.
  """
  @spec uuid(term()) :: {:ok, String.t()} | {:error, {:invalid_option, {:run_id, :invalid}}}
  def uuid(value) when is_binary(value) do
    case Ecto.UUID.cast(value) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> invalid_option(:run_id)
    end
  end

  def uuid(_value), do: invalid_option(:run_id)

  defp validate_thread_part("", field) do
    invalid_option(field)
  end

  defp validate_thread_part(value, field) do
    if Regex.match?(@thread_part_pattern, value) do
      {:ok, value}
    else
      invalid_option(field)
    end
  end

  defp invalid_option(field), do: {:error, {:invalid_option, {field, :invalid}}}
end
