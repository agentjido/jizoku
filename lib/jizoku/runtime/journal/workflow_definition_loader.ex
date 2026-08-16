# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Journal.WorkflowDefinitionLoader do
  @moduledoc false

  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.WorkflowAgent.Projection
  alias Jizoku.Workflow.Definition
  alias Jizoku.Workflow.Spec
  alias Jizoku.Workflow.VersionRegistry

  @doc false
  @spec load(Journal.storage_config(), String.t(), String.t()) ::
          {:ok, module(), Definition.t()} | {:error, term()}
  def load(storage, run_id, workflow_name)
      when is_binary(run_id) and is_binary(workflow_name) do
    load(storage, run_id, workflow_name, configured_version_options())
  end

  @doc false
  @spec load(Journal.storage_config(), String.t(), String.t(), keyword()) ::
          {:ok, module(), Definition.t()} | {:error, term()}
  def load(storage, run_id, workflow_name, opts)
      when is_binary(run_id) and is_binary(workflow_name) and is_list(opts) do
    with {:ok, persisted} <- persisted_definition_metadata(storage, run_id) do
      case definition_source(persisted) do
        :runtime_spec ->
          load_runtime_spec(persisted)

        _module ->
          load_module_definition(workflow_name, persisted, opts)
      end
    end
  end

  @doc false
  @spec runtime_spec_run?(Journal.storage_config(), String.t()) ::
          {:ok, boolean()} | {:error, term()}
  def runtime_spec_run?(storage, run_id) when is_binary(run_id) do
    with {:ok, persisted} <- persisted_definition_metadata(storage, run_id) do
      {:ok, definition_source(persisted) == :runtime_spec}
    end
  end

  defp load_runtime_spec(persisted) do
    with {:ok, spec} <- persisted_spec(persisted),
         definition <- definition_from_spec(spec),
         :ok <- Spec.validate(spec),
         :ok <- validate_definition_fingerprint(definition, persisted) do
      {:ok, spec.workflow, definition}
    end
  end

  defp load_module_definition(workflow_name, persisted, opts) do
    with {:ok, workflow, current_definition} <- Definition.load_serialized(workflow_name) do
      case validate_definition_fingerprint(current_definition, persisted) do
        :ok ->
          {:ok, workflow, current_definition}

        {:error, _reason} = current_error ->
          load_historical_definition(workflow, persisted, opts, current_error)
      end
    end
  end

  defp load_historical_definition(workflow, persisted, opts, current_error) do
    case Keyword.fetch(opts, :workflow_versions) do
      {:ok, registry} ->
        VersionRegistry.resolve(
          workflow,
          Map.get(persisted, :definition_version),
          Map.get(persisted, :definition_fingerprint),
          registry
        )

      :error ->
        current_error
    end
  end

  defp persisted_definition_metadata(storage, run_id) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, run_id}) do
      projection = Projection.rebuild(entries)

      metadata =
        entries
        |> Enum.find_value(&run_started_metadata/1)
        |> effective_definition_metadata(projection)

      {:ok, metadata || %{definition_version: nil, definition_fingerprint: nil}}
    end
  end

  defp run_started_metadata(%{type: :run_started, data: data}) do
    %{
      definition_source: metadata_value(data, :definition_source),
      definition_spec: metadata_value(data, :definition_spec)
    }
  end

  defp run_started_metadata(_entry) do
    nil
  end

  defp effective_definition_metadata(metadata, %Projection{} = projection)
       when is_map(metadata) do
    metadata
    |> Map.put(:definition_version, projection.definition_version)
    |> Map.put(:definition_fingerprint, projection.definition_fingerprint)
  end

  defp effective_definition_metadata(_metadata, _projection) do
    nil
  end

  defp validate_definition_fingerprint(definition, persisted) do
    case persisted do
      %{definition_fingerprint: nil} ->
        {:error, Definition.incompatible_definition_error(definition, persisted)}

      %{definition_fingerprint: fingerprint} ->
        if fingerprint == Definition.fingerprint(definition) do
          :ok
        else
          {:error, Definition.incompatible_definition_error(definition, persisted)}
        end
    end
  end

  defp definition_source(%{definition_source: :runtime_spec}), do: :runtime_spec
  defp definition_source(%{definition_source: "runtime_spec"}), do: :runtime_spec
  defp definition_source(_persisted), do: :module

  defp persisted_spec(%{definition_spec: %Spec{} = spec}), do: {:ok, spec}

  defp persisted_spec(%{definition_spec: spec}) when is_map(spec) do
    {:ok, struct(Spec, spec_field_values(spec))}
  end

  defp persisted_spec(_persisted) do
    {:error, {:invalid_runtime_definition, :missing_definition_spec}}
  end

  defp definition_from_spec(%Spec{} = spec), do: Jizoku.Workflow.SpecData.from_struct(spec)

  defp spec_field_values(spec) when is_map(spec),
    do: Jizoku.Workflow.SpecData.struct_fields(spec)

  defp metadata_value(data, key) when is_map(data) and is_atom(key) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end

  defp configured_version_options do
    case Application.fetch_env(:jizoku, :workflow_versions) do
      {:ok, registry} -> [workflow_versions: registry]
      :error -> []
    end
  end
end
