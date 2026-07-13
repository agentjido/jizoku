defmodule Squidie.Operations.Collector do
  @moduledoc """
  Rebuilds a bulk, read-only operational view of runs and dispatch queues.

  The collector loads each run and queue projection once so status and doctor
  reports can aggregate durable facts without repeated per-run queue rebuilds.
  """

  alias Jido.Agent
  alias Squidie.Config
  alias Squidie.MapField
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchAgent.State
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Storage
  alias Squidie.Runtime.RunCatalogProjection
  alias Squidie.Runtime.WorkflowAgent

  @enforce_keys [:config, :now, :runs, :queues, :catalog_anomalies]
  defstruct [:config, :now, :runs, :queues, :catalog_anomalies]

  @type run :: %{
          required(:run_id) => String.t(),
          required(:partition) => String.t() | nil,
          required(:workflow) => String.t(),
          required(:queue) => String.t(),
          required(:status) => atom(),
          required(:terminal?) => boolean(),
          required(:manual_state) => map() | nil,
          required(:planned_runnables) => [map()],
          required(:applied_runnable_keys) => MapSet.t(String.t()),
          required(:anomalies) => [map()]
        }

  @type queue :: %{
          required(:queue) => String.t(),
          required(:partition) => String.t() | nil,
          required(:projection) => DispatchProtocol.Projection.t(),
          required(:attempts) => [Squidie.Runtime.DispatchProtocol.ActionAttempt.t()]
        }

  @type t :: %__MODULE__{
          config: Config.t(),
          now: DateTime.t(),
          runs: [run()],
          queues: %{required(String.t()) => queue()},
          catalog_anomalies: [map()]
        }

  @spec collect(keyword()) :: {:ok, t()} | {:error, term()}
  @doc "Collects operational projections using optional Squidie configuration and `:now` overrides."
  def collect(overrides \\ [])

  def collect(overrides) when is_list(overrides) do
    :ok = ensure_projection_modules_loaded()
    collect_from_storage(overrides)
  end

  def collect(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp collect_from_storage(overrides) do
    do_collect(overrides)
  rescue
    _exception in [ArgumentError, DBConnection.ConnectionError, Ecto.QueryError, Postgrex.Error] ->
      {:error, :storage_unavailable}
  catch
    :exit, _reason -> {:error, :storage_unavailable}
  end

  defp do_collect(overrides) do
    {now, config_overrides} = Keyword.pop(overrides, :now, DateTime.utc_now())

    with :ok <- validate_now(now),
         {:ok, %Config{} = config} <- Config.load(config_overrides),
         {:ok, %RunCatalogProjection{} = catalog} <-
           Journal.rebuild_run_catalog_projection(config.journal_storage),
         catalog_runs = RunCatalogProjection.runs(catalog),
         :ok <- ensure_workflow_modules_loaded(catalog_runs),
         {:ok, runs} <- collect_runs(config.journal_storage, catalog_runs),
         {:ok, queues} <- collect_queues(config, runs) do
      {:ok,
       %__MODULE__{
         config: config,
         now: now,
         runs: runs,
         queues: queues,
         catalog_anomalies: RunCatalogProjection.anomalies(catalog)
       }}
    end
  end

  @spec pending_dispatches(run(), queue()) :: [map()]
  @doc "Returns planned runnables that are missing from the selected queue projection."
  def pending_dispatches(run, queue) do
    case {Map.get(run, :partition), Map.get(queue, :partition)} do
      {partition, partition} -> matching_partition_pending_dispatches(run, queue)
      {_run_partition, _queue_partition} -> []
    end
  end

  defp matching_partition_pending_dispatches(run, queue) do
    dispatched_keys = DispatchProtocol.Projection.attempt_runnable_keys(queue.projection)

    Enum.reject(run.planned_runnables, fn runnable ->
      key = runnable_key(runnable)

      runnable_queue(runnable, run.queue) != queue.queue or
        MapSet.member?(dispatched_keys, key) or
        MapSet.member?(run.applied_runnable_keys, key)
    end)
  end

  @spec pending_results(run(), queue()) :: [Squidie.Runtime.DispatchProtocol.ActionAttempt.t()]
  @doc "Returns completed queue attempts that have not been applied to the run projection."
  def pending_results(run, queue) do
    case {Map.get(run, :partition), Map.get(queue, :partition)} do
      {partition, partition} -> matching_partition_pending_results(run, queue)
      {_run_partition, _queue_partition} -> []
    end
  end

  defp matching_partition_pending_results(run, queue) do
    queue.projection
    |> DispatchProtocol.Projection.completed_results()
    |> Enum.filter(fn attempt ->
      attempt.run_id == run.run_id and
        Enum.any?(run.planned_runnables, &(runnable_key(&1) == attempt.runnable_key)) and
        not MapSet.member?(run.applied_runnable_keys, attempt.runnable_key)
    end)
  end

  defp collect_runs(storage, catalog_runs) do
    result =
      Enum.reduce_while(catalog_runs, {:ok, []}, fn catalog_run, {:ok, runs} ->
        collect_run(storage, catalog_run, runs)
      end)

    case result do
      {:ok, runs} -> {:ok, Enum.reverse(runs)}
      {:error, _reason} = error -> error
    end
  end

  defp collect_run(storage, catalog_run, runs) do
    case WorkflowAgent.rebuild(storage, catalog_run.run_id) do
      {:ok, %Agent{state: %{projection: %WorkflowAgent.Projection{} = projection}}} ->
        run = %{
          run_id: catalog_run.run_id,
          partition: Storage.partition(storage),
          workflow: catalog_run.workflow,
          queue: catalog_run.queue,
          status: WorkflowAgent.Projection.status(projection),
          terminal?: WorkflowAgent.Projection.terminal?(projection),
          manual_state: WorkflowAgent.Projection.manual_state(projection),
          planned_runnables: WorkflowAgent.Projection.planned_runnables(projection),
          applied_runnable_keys: WorkflowAgent.Projection.applied_runnable_keys(projection),
          anomalies: WorkflowAgent.Projection.anomalies(projection)
        }

        {:cont, {:ok, [run | runs]}}

      {:error, reason} ->
        {:halt, {:error, {:run_projection_failed, catalog_run.run_id, reason}}}
    end
  end

  defp collect_queues(%Config{} = config, runs) do
    queues =
      runs
      |> Enum.map(& &1.queue)
      |> then(&[config.queue | &1])
      |> Enum.uniq()
      |> Enum.sort()

    Enum.reduce_while(queues, {:ok, %{}}, fn queue_name, {:ok, collected} ->
      case DispatchAgent.rebuild(config.journal_storage, queue_name) do
        {:ok, %Agent{state: %State{projection: %DispatchProtocol.Projection{} = projection}}} ->
          queue = %{
            queue: queue_name,
            partition: config.partition,
            projection: projection,
            attempts:
              projection.attempts
              |> Map.values()
              |> Enum.sort_by(& &1.runnable_key)
          }

          {:cont, {:ok, Map.put(collected, queue_name, queue)}}

        {:error, reason} ->
          {:halt, {:error, {:dispatch_projection_failed, queue_name, reason}}}
      end
    end)
  end

  defp runnable_key(runnable), do: MapField.get(runnable, :runnable_key, "")

  defp runnable_queue(runnable, default) do
    case MapField.get(runnable, :queue) do
      queue when is_binary(queue) -> queue
      queue when is_atom(queue) and not is_nil(queue) -> Atom.to_string(queue)
      _missing -> default
    end
  end

  defp validate_now(%DateTime{}), do: :ok
  defp validate_now(_invalid), do: {:error, {:invalid_option, {:now, :invalid}}}

  defp ensure_projection_modules_loaded do
    modules =
      Application.spec(:squidie, :modules) ||
        [
          Squidie.Runtime.DispatchProtocol.Entry,
          Squidie.Runtime.DispatchProtocol.Projection,
          Squidie.Runtime.RunCatalogProjection,
          Squidie.Runtime.WorkflowAgent.Projection
        ]

    Enum.each(modules, &Code.ensure_loaded!/1)
    :ok
  end

  defp ensure_workflow_modules_loaded(catalog_runs) do
    application_module_sets()
    |> Enum.filter(&contains_catalog_workflow?(&1, catalog_runs))
    |> Enum.each(fn modules -> Enum.each(modules, &Code.ensure_loaded/1) end)

    :ok
  end

  defp application_module_sets do
    Enum.map(Application.loaded_applications(), fn {application, _description, _version} ->
      Application.spec(application, :modules) || []
    end)
  end

  defp contains_catalog_workflow?(modules, catalog_runs) do
    module_names = MapSet.new(modules, &Atom.to_string/1)
    Enum.any?(catalog_runs, &MapSet.member?(module_names, &1.workflow))
  end
end
