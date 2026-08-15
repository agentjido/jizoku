# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Squidie.Runtime.Jido.OutboxDelivery do
  @moduledoc false

  alias Jido.Signal.Dispatch
  alias Squidie.ReadModel.Inspection
  alias Squidie.Runtime.Jido.Outbox
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.RunCatalogProjection
  alias Squidie.Runtime.WorkflowAgent
  alias Squidie.Runtime.WorkflowAgent.Projection
  alias Squidie.Telemetry.Emitter

  @ack_retries 25
  @max_routes 32
  @max_deliveries 100

  @type routes :: %{optional(String.t()) => Dispatch.dispatch_configs()}
  @type delivery_error ::
          {:invalid_option, {:jido_dispatch_routes, :invalid}}
          | {:jido_signal_delivery_failed,
             %{
               required(:run_id) => String.t(),
               required(:signal_id) => String.t(),
               required(:outbox_id) => String.t(),
               required(:route) => String.t(),
               required(:reason) => atom()
             }}
          | term()

  @doc false
  @spec routes(term()) :: {:ok, routes() | nil} | {:error, delivery_error()}
  def routes(nil) do
    {:ok, nil}
  end

  def routes(routes) when is_map(routes) and map_size(routes) <= @max_routes do
    if Enum.all?(routes, &valid_route?/1) do
      {:ok, routes}
    else
      invalid_routes()
    end
  end

  def routes(_routes) do
    invalid_routes()
  end

  @doc false
  @spec deliver_run(Journal.storage_config(), String.t(), routes(), DateTime.t()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, delivery_error()}
  def deliver_run(storage, run_id, routes, %DateTime{} = now)
      when is_binary(run_id) and is_map(routes) do
    with {:ok, queue} <- run_queue(storage, run_id),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, _workflow_agent} <-
           deliver_pending(storage, workflow_agent, routes, now, @max_deliveries) do
      Inspection.snapshot(storage, run_id,
        queue: queue,
        now: now
      )
    end
  end

  @doc false
  @spec reconcile_next(Journal.storage_config(), String.t(), routes(), DateTime.t()) ::
          {:ok, Inspection.Snapshot.t() | :none} | {:error, delivery_error()}
  def reconcile_next(storage, queue, routes, %DateTime{} = now)
      when is_binary(queue) and is_map(routes) do
    with {:ok, runs} <- catalog_runs(storage) do
      runs
      |> Enum.filter(&(Map.get(&1, :queue) == queue))
      |> reconcile_runs(storage, routes, now)
    end
  end

  defp deliver_pending(_storage, workflow_agent, _routes, _now, 0) do
    case pending_items(workflow_agent) do
      [] -> {:ok, workflow_agent}
      _pending -> {:error, {:jido_signal_delivery_failed, delivery_limit_error(workflow_agent)}}
    end
  end

  defp deliver_pending(storage, workflow_agent, routes, now, remaining) do
    case pending_items(workflow_agent) do
      [] ->
        {:ok, workflow_agent}

      [item | _rest] ->
        with {:ok, delivery_now} <- delivery_time(item, now),
             {:ok, signal, route_config} <- delivery(item, routes),
             :ok <- dispatch(item, signal, route_config),
             {:ok, workflow_agent} <-
               acknowledge(storage, workflow_agent, item, delivery_now) do
          deliver_pending(storage, workflow_agent, routes, now, remaining - 1)
        end
    end
  end

  defp delivery(item, routes) do
    route = Map.get(item, "route")

    with {:ok, route_config} <- Map.fetch(routes, route),
         {:ok, signal} <- Outbox.decode_signal(Map.get(item, "signal")) do
      {:ok, signal, route_config}
    else
      :error -> delivery_error(item, :route_not_configured)
      {:error, _reason} -> delivery_error(item, :invalid_persisted_signal)
    end
  end

  defp dispatch(item, signal, route_config) do
    metadata = delivery_metadata(item)

    try do
      case Emitter.span([:squidie, :runtime, :jido_signal, :deliver], metadata, fn ->
             Dispatch.dispatch(signal, route_config)
           end) do
        :ok -> :ok
        {:error, _reason} -> delivery_error(item, :dispatch_failed)
        _unexpected -> delivery_error(item, :invalid_dispatch_result)
      end
    catch
      _kind, _reason -> delivery_error(item, :dispatch_exception)
    end
  end

  defp acknowledge(storage, workflow_agent, item, now) do
    acknowledge(storage, workflow_agent, item, now, @ack_retries)
  end

  defp acknowledge(_storage, _workflow_agent, item, _now, 0) do
    delivery_error(item, :acknowledgement_conflict)
  end

  defp acknowledge(storage, workflow_agent, item, now, retries_left) do
    case current_item(workflow_agent, item) do
      {:ok, %{"status" => "delivered"}} ->
        {:ok, workflow_agent}

      {:ok, %{"status" => "pending"} = current} ->
        with {:ok, intent} <- Outbox.intent_from_item(current),
             {:ok, entry} <- Outbox.acknowledge_entry(intent, now) do
          append_ack(storage, workflow_agent, item, entry, now, retries_left)
        else
          {:error, _reason} -> delivery_error(item, :invalid_persisted_signal)
        end

      _missing_or_changed ->
        delivery_error(item, :outbox_state_changed)
    end
  end

  defp append_ack(storage, workflow_agent, item, entry, now, retries_left) do
    case Journal.append_entries(storage, [entry],
           expected_rev: workflow_agent.state.thread_rev,
           telemetry_projection: workflow_agent.state.projection
         ) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, Map.get(item, "run_id"))

      {:error, _reason} = error ->
        repair_ack(storage, workflow_agent, item, now, retries_left, error)
    end
  end

  defp repair_ack(storage, _workflow_agent, item, now, retries_left, append_error) do
    case WorkflowAgent.rebuild(storage, Map.get(item, "run_id")) do
      {:ok, rebuilt} ->
        case current_item(rebuilt, item) do
          {:ok, %{"status" => "delivered"}} ->
            {:ok, rebuilt}

          {:ok, %{"status" => "pending"}} when retries_left > 1 ->
            acknowledge(storage, rebuilt, item, now, retries_left - 1)

          _not_repaired ->
            append_error
        end

      {:error, _reason} ->
        append_error
    end
  end

  defp current_item(workflow_agent, item) do
    workflow_agent.state.projection
    |> Projection.jido_outbox()
    |> Outbox.fetch_projected_item(Map.get(item, "outbox_id"))
  end

  defp pending_run(storage, run_id) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id) do
      {:ok, pending_items(workflow_agent) != []}
    end
  end

  defp pending_items(workflow_agent) do
    workflow_agent.state.projection
    |> Projection.jido_outbox()
    |> Outbox.pending()
  end

  defp valid_route?({route, config}) when is_binary(route) and route != "" do
    byte_size(route) <= 255 and String.valid?(route) and valid_dispatch_config?(config) and
      valid_dispatch_options?(config)
  end

  defp valid_route?(_route) do
    false
  end

  defp valid_dispatch_config?({adapter, opts}) do
    is_atom(adapter) and is_list(opts)
  end

  defp valid_dispatch_config?(configs) when is_list(configs) do
    configs != []
  end

  defp valid_dispatch_config?(_config) do
    false
  end

  defp valid_dispatch_options?(config) do
    match?({:ok, _}, Dispatch.validate_opts(config))
  catch
    _kind, _reason -> false
  end

  defp delivery_time(%{"enqueued_at" => %DateTime{} = enqueued_at}, %DateTime{} = now) do
    delivery_now =
      case DateTime.compare(now, enqueued_at) do
        :lt -> enqueued_at
        _same_or_later -> now
      end

    {:ok, delivery_now}
  end

  defp delivery_time(item, _now) do
    delivery_error(item, :invalid_persisted_signal)
  end

  defp delivery_metadata(item) do
    %{
      run_id: Map.get(item, "run_id"),
      signal_id: Map.get(item, "signal_id"),
      outbox_id: Map.get(item, "outbox_id"),
      route: Map.get(item, "route")
    }
  end

  defp delivery_error(item, reason) do
    {:error,
     {:jido_signal_delivery_failed, Map.merge(delivery_metadata(item), %{reason: reason})}}
  end

  defp delivery_limit_error(workflow_agent) do
    pending = pending_items(workflow_agent)
    item = List.first(pending) || %{}

    item
    |> delivery_metadata()
    |> Map.put(:reason, :delivery_limit_reached)
  end

  defp run_queue(storage, run_id) do
    with {:ok, runs} <- catalog_runs(storage),
         %{queue: queue} <- Enum.find(runs, &(Map.get(&1, :run_id) == run_id)) do
      {:ok, queue}
    else
      nil -> {:error, :run_not_cataloged}
      {:error, _reason} = error -> error
    end
  end

  defp reconcile_runs(runs, storage, routes, now) do
    Enum.reduce_while(runs, {:ok, :none}, fn run, _none ->
      case pending_run(storage, run.run_id) do
        {:ok, true} -> {:halt, deliver_run(storage, run.run_id, routes, now)}
        {:ok, false} -> {:cont, {:ok, :none}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp catalog_runs(storage) do
    with {:ok, catalog} <- Journal.rebuild_run_catalog_projection(storage) do
      case RunCatalogProjection.anomalies(catalog) do
        [] -> {:ok, RunCatalogProjection.runs(catalog)}
        anomalies -> {:error, {:run_catalog_anomalies, anomalies}}
      end
    end
  end

  defp invalid_routes do
    {:error, {:invalid_option, {:jido_dispatch_routes, :invalid}}}
  end
end
