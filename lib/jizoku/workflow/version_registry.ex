defmodule Jizoku.Workflow.VersionRegistry do
  @moduledoc """
  Validates and resolves host-owned historical workflow implementations.

  Registry entries come from trusted application configuration. Persisted
  workflow or version strings are lookup values only and never become modules.
  """

  alias Jizoku.Workflow.Definition

  @type registry :: %{optional(module()) => %{optional(String.t()) => module()}}
  @type validation_error :: %{
          required(:code) => atom(),
          optional(:workflow) => String.t(),
          optional(:configured_version) => String.t(),
          optional(:definition_version) => String.t() | nil,
          optional(:implementation) => String.t()
        }

  @doc """
  Validates every configured workflow, version, and implementation.
  """
  @spec validate(term()) :: :ok | {:error, {:invalid_workflow_versions, [validation_error()]}}
  def validate(registry) when is_map(registry) do
    errors =
      registry
      |> Enum.sort_by(fn {workflow, _versions} -> inspect(workflow) end)
      |> Enum.flat_map(fn {workflow, versions} ->
        validate_workflow_versions(workflow, versions)
      end)

    case errors do
      [] -> :ok
      errors -> {:error, {:invalid_workflow_versions, errors}}
    end
  end

  def validate(_registry) do
    {:error, {:invalid_workflow_versions, [%{code: :invalid_registry}]}}
  end

  @doc """
  Resolves one configured definition and verifies its precise fingerprint.

  The returned workflow module remains the stable durable identity. Historical
  implementation modules are execution details and are never exposed through
  step context or persisted as a replacement workflow identity.
  """
  @spec resolve(module(), String.t() | nil, String.t(), term()) ::
          {:ok, module(), Definition.t()} | {:error, term()}
  def resolve(workflow, version, persisted_fingerprint, registry)
      when is_atom(workflow) and is_binary(persisted_fingerprint) do
    with :ok <- validate(registry),
         {:ok, versions} <- workflow_versions(registry, workflow),
         {:ok, implementation} <- implementation(versions, workflow, version),
         {:ok, definition} <- Definition.load(implementation),
         :ok <- exact_fingerprint(definition, workflow, version, persisted_fingerprint) do
      {:ok, workflow, definition}
    end
  end

  def resolve(workflow, version, persisted_fingerprint, _registry) do
    {:error,
     %{
       code: "invalid_workflow_version_request",
       workflow: inspect(workflow),
       requested_version: version,
       persisted_fingerprint: persisted_fingerprint
     }}
  end

  @doc false
  @spec fetch(module(), String.t(), term()) ::
          {:ok, module(), Definition.t()} | {:error, term()}
  def fetch(workflow, version, registry) when is_atom(workflow) and is_binary(version) do
    with :ok <- validate(registry),
         {:ok, versions} <- workflow_versions(registry, workflow),
         {:ok, implementation} <- implementation(versions, workflow, version),
         {:ok, definition} <- Definition.load(implementation) do
      {:ok, workflow, definition}
    end
  end

  def fetch(workflow, version, _registry) do
    {:error,
     %{
       code: "invalid_workflow_version_request",
       workflow: inspect(workflow),
       requested_version: version
     }}
  end

  defp validate_workflow_versions(workflow, versions)
       when is_atom(workflow) and is_map(versions) do
    versions
    |> Enum.sort_by(fn {version, _implementation} -> inspect(version) end)
    |> Enum.flat_map(fn {version, implementation} ->
      validate_implementation(workflow, version, implementation)
    end)
  end

  defp validate_workflow_versions(workflow, _versions) when is_atom(workflow) do
    [%{code: :invalid_versions, workflow: inspect(workflow)}]
  end

  defp validate_workflow_versions(workflow, _versions) do
    [%{code: :invalid_workflow, workflow: inspect(workflow)}]
  end

  defp validate_implementation(workflow, version, implementation)
       when is_binary(version) and version != "" and is_atom(implementation) do
    case Definition.load(implementation) do
      {:ok, %{definition_version: ^version}} ->
        []

      {:ok, %{definition_version: definition_version}} ->
        [
          %{
            code: :definition_version_mismatch,
            workflow: inspect(workflow),
            implementation: inspect(implementation),
            configured_version: version,
            definition_version: definition_version
          }
        ]

      {:error, _reason} ->
        [
          %{
            code: :invalid_implementation,
            workflow: inspect(workflow),
            implementation: inspect(implementation),
            configured_version: version
          }
        ]
    end
  end

  defp validate_implementation(workflow, version, implementation) do
    [
      %{
        code: :invalid_implementation,
        workflow: inspect(workflow),
        implementation: inspect(implementation),
        configured_version: version
      }
    ]
  end

  defp workflow_versions(registry, workflow) do
    case Map.fetch(registry, workflow) do
      {:ok, versions} -> {:ok, versions}
      :error -> unavailable(workflow, nil, [])
    end
  end

  defp implementation(versions, workflow, version) when is_binary(version) do
    case Map.fetch(versions, version) do
      {:ok, implementation} -> {:ok, implementation}
      :error -> unavailable(workflow, version, Map.keys(versions))
    end
  end

  defp implementation(versions, workflow, version) do
    unavailable(workflow, version, Map.keys(versions))
  end

  defp unavailable(workflow, version, available_versions) do
    {:error,
     %{
       code: "workflow_version_unavailable",
       workflow: inspect(workflow),
       requested_version: version,
       available_versions: Enum.sort(available_versions)
     }}
  end

  defp exact_fingerprint(definition, workflow, version, persisted_fingerprint) do
    resolved_fingerprint = Definition.fingerprint(definition)

    if resolved_fingerprint == persisted_fingerprint do
      :ok
    else
      {:error,
       %{
         code: "workflow_version_fingerprint_mismatch",
         workflow: inspect(workflow),
         requested_version: version,
         persisted_definition_fingerprint: persisted_fingerprint,
         resolved_definition_fingerprint: resolved_fingerprint
       }}
    end
  end
end
