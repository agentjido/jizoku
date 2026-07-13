defmodule Squidie.Operations.Doctor do
  @moduledoc """
  Builds read-only operational diagnostics with direct operator next actions.

  Doctor reports durable projection and schema state. It never reclaims work,
  applies results, schedules dispatches, or runs migrations.
  """

  alias Squidie.Config
  alias Squidie.Operations.Collector
  alias Squidie.Operations.SchemaCheck
  alias Squidie.Runtime.DispatchProtocol

  @schema_version 1

  @spec report(keyword()) :: {:ok, map()} | {:error, term()}
  @doc "Returns versioned, JSON-safe diagnostics using optional configuration and `:now` overrides."
  def report(overrides \\ [])

  def report(overrides) when is_list(overrides) do
    {now, config_overrides} = Keyword.pop(overrides, :now, DateTime.utc_now())

    case now do
      %DateTime{} -> {:ok, build_report(now, config_overrides)}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  def report(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  @doc "Returns whether a doctor report contains behind or incompatible schema drift."
  @spec drift?(map()) :: boolean()
  def drift?(%{checks: checks}) do
    Enum.any?(checks, &(&1.id == :schema and &1.details.status in [:behind, :incompatible]))
  end

  defp build_report(now, config_overrides) do
    {partition, checks} =
      case Config.load(config_overrides) do
        {:ok, %Config{} = config} ->
          {config.partition, configured_checks(config, now, config_overrides)}

        {:error, reason} ->
          {nil, [configuration_failure(reason)]}
      end

    summary = Enum.frequencies_by(checks, & &1.status)

    %{
      schema_version: @schema_version,
      generated_at: DateTime.to_iso8601(now),
      partition: partition,
      healthy: not Enum.any?(checks, &(&1.status == :fail)),
      summary: %{
        pass: Map.get(summary, :pass, 0),
        warn: Map.get(summary, :warn, 0),
        fail: Map.get(summary, :fail, 0)
      },
      checks: checks
    }
  end

  defp configured_checks(config, now, config_overrides) do
    config_check = configuration_check()
    schema_check = schema_check(SchemaCheck.check(config))

    runtime_checks =
      case Collector.collect(Keyword.put(config_overrides, :now, now)) do
        {:ok, %Collector{} = collector} -> runtime_checks(collector)
        {:error, _reason} -> [runtime_unavailable()]
      end

    [config_check, schema_check | runtime_checks]
  end

  defp runtime_checks(%Collector{} = collector) do
    [
      expired_claims_check(collector),
      overdue_attempts_check(collector),
      terminal_pending_facts_check(collector),
      manual_intervention_check(collector),
      projection_anomalies_check(collector)
    ]
  end

  defp configuration_check do
    misplaced =
      [:heartbeat_interval_ms, :lease_for]
      |> Enum.filter(&match?({:ok, _value}, Application.fetch_env(:squidie, &1)))
      |> Enum.sort()

    case misplaced do
      [] ->
        check(:configuration, :pass, "Squidie runtime configuration is valid.", [], %{})

      misplaced ->
        check(
          :configuration,
          :warn,
          "Unsupported worker options are configured globally and will be ignored.",
          [:move_heartbeat_options_to_execute_next_worker_calls],
          %{unsupported_global_options: misplaced}
        )
    end
  end

  defp configuration_failure(reason) do
    check(
      :configuration,
      :fail,
      configuration_message(reason),
      [:configure_squidie_repo_and_queue],
      %{reason: configuration_reason(reason)}
    )
  end

  defp schema_check(result) do
    {status, message} =
      case result.status do
        :current ->
          {:pass, "The host database matches Squidie's required schema baseline."}

        :not_applicable ->
          {:pass, "Schema drift is not applicable to the configured storage adapter."}

        :behind ->
          {:fail, "The host database is behind Squidie's required schema baseline."}

        :incompatible ->
          {:fail, "The host database has incompatible Squidie schema objects."}

        :unavailable ->
          {:fail, "Squidie could not inspect the host database schema."}
      end

    check(:schema, status, message, result.next_actions, result)
  end

  defp expired_claims_check(%Collector{} = collector) do
    findings =
      collector.queues
      |> Enum.flat_map(fn {queue_name, queue} ->
        queue.projection
        |> DispatchProtocol.Projection.expired_claims(collector.now)
        |> Enum.map(&%{queue: queue_name, run_id: &1.run_id, runnable_key: &1.runnable_key})
      end)
      |> Enum.sort_by(&{&1.queue, &1.run_id, &1.runnable_key})

    finding_check(
      :expired_claims,
      findings,
      :fail,
      "No expired claims were found.",
      "Expired claims require recovery by a current worker.",
      [:recover_expired_claim]
    )
  end

  defp overdue_attempts_check(%Collector{} = collector) do
    findings =
      collector.queues
      |> Enum.flat_map(fn {queue_name, queue} ->
        queue.attempts
        |> Enum.filter(fn attempt ->
          attempt.status in [:available, :retry_scheduled] and
            DateTime.compare(attempt.visible_at, collector.now) == :lt and
            not MapSet.member?(queue.projection.terminal_runs, attempt.run_id)
        end)
        |> Enum.map(fn attempt ->
          %{
            queue: queue_name,
            run_id: attempt.run_id,
            runnable_key: attempt.runnable_key,
            overdue_seconds: max(DateTime.diff(collector.now, attempt.visible_at, :second), 0)
          }
        end)
      end)
      |> Enum.sort_by(&{&1.queue, &1.run_id, &1.runnable_key})

    finding_check(
      :overdue_attempts,
      findings,
      :warn,
      "No overdue scheduled attempts were found.",
      "Scheduled attempts are visible but have not been claimed.",
      [:inspect_worker_capacity]
    )
  end

  defp terminal_pending_facts_check(%Collector{} = collector) do
    findings =
      collector.runs
      |> Enum.filter(& &1.terminal?)
      |> Enum.flat_map(fn run ->
        queue = Map.fetch!(collector.queues, run.queue)
        dispatches = Collector.pending_dispatches(run, queue)
        results = Collector.pending_results(run, queue)

        case {dispatches, results} do
          {[], []} ->
            []

          {_dispatches, _results} ->
            [
              %{
                run_id: run.run_id,
                workflow: run.workflow,
                queue: run.queue,
                pending_dispatch_count: length(dispatches),
                pending_result_count: length(results)
              }
            ]
        end
      end)
      |> Enum.sort_by(& &1.run_id)

    finding_check(
      :terminal_pending_facts,
      findings,
      :fail,
      "Terminal runs have no pending dispatch or result facts.",
      "Terminal runs retain pending dispatch or result facts for inspection.",
      [:inspect_terminal_run]
    )
  end

  defp manual_intervention_check(%Collector{} = collector) do
    findings =
      collector.runs
      |> Enum.filter(&(not &1.terminal? and not is_nil(&1.manual_state)))
      |> Enum.map(&%{run_id: &1.run_id, workflow: &1.workflow, queue: &1.queue})
      |> Enum.sort_by(& &1.run_id)

    finding_check(
      :manual_intervention,
      findings,
      :warn,
      "No runs are waiting for manual action.",
      "Runs are waiting for manual action.",
      [:resolve_manual_step]
    )
  end

  defp projection_anomalies_check(%Collector{} = collector) do
    queue_anomalies =
      Enum.flat_map(collector.queues, fn {queue_name, queue} ->
        Enum.map(DispatchProtocol.Projection.anomalies(queue.projection), fn anomaly ->
          %{scope: :queue, queue: queue_name, reason: anomaly.reason}
        end)
      end)

    run_anomalies =
      Enum.flat_map(collector.runs, fn run ->
        Enum.map(run.anomalies, fn anomaly ->
          %{scope: :run, run_id: run.run_id, reason: anomaly.reason}
        end)
      end)

    catalog_anomalies =
      Enum.map(collector.catalog_anomalies, fn anomaly ->
        %{scope: :catalog, reason: anomaly.reason}
      end)

    findings = Enum.sort_by(catalog_anomalies ++ run_anomalies ++ queue_anomalies, &inspect/1)

    finding_check(
      :projection_anomalies,
      findings,
      :warn,
      "No durable projection anomalies were found.",
      "Durable journal projections contain anomalies.",
      [:inspect_dispatch_state]
    )
  end

  defp runtime_unavailable do
    check(
      :runtime_state,
      :fail,
      "Squidie could not rebuild operational runtime projections.",
      [:verify_repo_connectivity_and_schema],
      %{}
    )
  end

  defp finding_check(id, [], _failure_status, pass_message, _failure_message, _actions) do
    check(id, :pass, pass_message, [], %{count: 0, findings: []})
  end

  defp finding_check(id, findings, failure_status, _pass_message, failure_message, actions) do
    check(id, failure_status, failure_message, actions, %{
      count: length(findings),
      findings: findings
    })
  end

  defp check(id, status, message, next_actions, details) do
    %{id: id, status: status, message: message, next_actions: next_actions, details: details}
  end

  defp configuration_message({:missing_config, keys}) do
    "Missing Squidie configuration: #{Enum.map_join(keys, ", ", &inspect/1)}."
  end

  defp configuration_message({:invalid_config, details}) do
    keys = Enum.map_join(Keyword.keys(details), ", ", &inspect/1)
    "Invalid Squidie configuration: #{keys}."
  end

  defp configuration_reason({:missing_config, keys}), do: %{type: :missing_config, keys: keys}

  defp configuration_reason({:invalid_config, details}) do
    %{type: :invalid_config, keys: Keyword.keys(details)}
  end
end
