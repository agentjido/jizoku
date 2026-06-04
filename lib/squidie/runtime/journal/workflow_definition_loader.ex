# credo:disable-for-next-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Journal.WorkflowDefinitionLoader do
  @moduledoc false

  alias Squidie.Runtime.Journal
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.Spec

  @spec_fields [
    :workflow,
    :definition_version,
    :triggers,
    :payload,
    :steps,
    :transitions,
    :retries,
    :entry_steps,
    :initial_step,
    :entry_step
  ]

  @doc false
  @spec load(Journal.storage_config(), String.t(), String.t()) ::
          {:ok, module(), Definition.t()} | {:error, term()}
  def load(storage, run_id, workflow_name)
      when is_binary(run_id) and is_binary(workflow_name) do
    with {:ok, persisted} <- persisted_definition_metadata(storage, run_id) do
      case definition_source(persisted) do
        :runtime_spec ->
          load_runtime_spec(persisted)

        _module ->
          load_module_definition(workflow_name, persisted)
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

  defp load_module_definition(workflow_name, persisted) do
    with {:ok, workflow, definition} <- Definition.load_serialized(workflow_name),
         :ok <- validate_definition_fingerprint(definition, persisted) do
      {:ok, workflow, definition}
    end
  end

  defp persisted_definition_metadata(storage, run_id) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, run_id}) do
      metadata =
        Enum.find_value(entries, fn
          %{type: :run_started, data: data} ->
            %{
              definition_version: metadata_value(data, :definition_version),
              definition_fingerprint: metadata_value(data, :definition_fingerprint),
              definition_source: metadata_value(data, :definition_source),
              definition_spec: metadata_value(data, :definition_spec)
            }

          _entry ->
            nil
        end)

      {:ok, metadata || %{definition_version: nil, definition_fingerprint: nil}}
    end
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

  defp definition_from_spec(%Spec{} = spec), do: Map.from_struct(spec)

  defp spec_field_values(spec) when is_map(spec) do
    Map.new(@spec_fields, fn field ->
      {field, spec_field_value(spec, field)}
    end)
  end

  defp spec_field_value(spec, field) do
    case Map.fetch(spec, field) do
      {:ok, value} -> value
      :error -> Map.get(spec, Atom.to_string(field))
    end
  end

  defp metadata_value(data, key) when is_map(data) and is_atom(key) do
    Map.get(data, key) || Map.get(data, Atom.to_string(key))
  end
end
