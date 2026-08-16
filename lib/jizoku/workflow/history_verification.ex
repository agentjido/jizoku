defmodule Jizoku.Workflow.HistoryVerification do
  @moduledoc "Verifies sanitized golden histories against host-registered workflow versions."

  alias Jizoku.Workflow.VersionRegistry

  @schema_version 1
  @error_fields [
    :code,
    :workflow,
    :requested_version,
    :available_versions,
    :persisted_definition_fingerprint,
    :resolved_definition_fingerprint
  ]

  @type fixture :: %{
          required(:workflow) => module(),
          required(:definition_version) => String.t(),
          required(:definition_fingerprint) => String.t(),
          required(:golden_history) => map()
        }

  @doc """
  Verifies that every golden-history fixture resolves to the exact registered
  implementation and fingerprint needed by its durable run.

  Workflow modules come only from the trusted fixture and registry terms.
  Strings inside golden histories are compared as data and are never converted
  to modules or atoms.
  """
  @spec verify([fixture()], VersionRegistry.registry()) ::
          {:ok, map()} | {:error, map() | term()}
  def verify(fixtures, registry) when is_list(fixtures) do
    with :ok <- VersionRegistry.validate(registry) do
      histories = Enum.map(fixtures, &verify_fixture(&1, registry))
      report = report(histories)

      if report.incompatible == 0 do
        {:ok, report}
      else
        {:error, report}
      end
    end
  end

  def verify(_fixtures, _registry) do
    {:error, report([invalid_fixture([:fixtures])])}
  end

  defp verify_fixture(fixture, registry) when is_map(fixture) do
    fields = invalid_fields(fixture)

    case fields do
      [] -> resolve_fixture(fixture, registry)
      fields -> invalid_fixture(fields)
    end
  end

  defp verify_fixture(_fixture, _registry) do
    invalid_fixture([:fixture])
  end

  defp resolve_fixture(fixture, registry) do
    workflow = value(fixture, :workflow)
    version = value(fixture, :definition_version)
    fingerprint = value(fixture, :definition_fingerprint)
    golden = value(fixture, :golden_history)

    base = %{
      workflow: inspect(workflow),
      definition_version: version,
      definition_fingerprint: fingerprint,
      event_count: length(value(golden, :events))
    }

    case VersionRegistry.resolve(workflow, version, fingerprint, registry) do
      {:ok, _workflow, _definition} ->
        Map.put(base, :status, :verified)

      {:error, reason} ->
        base
        |> Map.put(:status, :incompatible)
        |> Map.put(:error, safe_error(reason))
    end
  end

  defp invalid_fields(fixture) do
    workflow = value(fixture, :workflow)
    golden = value(fixture, :golden_history)

    []
    |> invalid_unless(:workflow, is_atom(workflow))
    |> invalid_unless(:definition_version, non_empty_binary?(value(fixture, :definition_version)))
    |> invalid_unless(
      :definition_fingerprint,
      non_empty_binary?(value(fixture, :definition_fingerprint))
    )
    |> invalid_unless(:golden_history, valid_golden?(golden, workflow))
    |> Enum.reverse()
  end

  defp valid_golden?(golden, workflow) when is_map(golden) do
    events = value(golden, :events)

    value(golden, :schema_version) == @schema_version and
      is_list(events) and events != [] and
      (not is_atom(workflow) or value(golden, :workflow) == Atom.to_string(workflow))
  end

  defp valid_golden?(_golden, _workflow) do
    false
  end

  defp report(histories) do
    total = length(histories)
    incompatible = Enum.count(histories, &(&1.status == :incompatible))

    %{
      schema_version: @schema_version,
      total: total,
      verified: total - incompatible,
      incompatible: incompatible,
      histories: histories
    }
  end

  defp invalid_fixture(fields) do
    %{
      status: :incompatible,
      error: %{code: "invalid_history_fixture", fields: fields}
    }
  end

  defp safe_error(reason) when is_map(reason) do
    Map.take(reason, @error_fields)
  end

  defp safe_error(_reason) do
    %{code: "history_verification_failed"}
  end

  defp invalid_unless(fields, _field, true) do
    fields
  end

  defp invalid_unless(fields, field, false) do
    [field | fields]
  end

  defp non_empty_binary?(value) do
    is_binary(value) and value != ""
  end

  defp value(map, key) when is_map(map) do
    Jizoku.MapField.get(map, key)
  end
end
