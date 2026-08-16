defmodule MinimalHostApp.Smoke do
  @moduledoc """
  Repeatable smoke-test entrypoint for the example host app.
  """

  import Ecto.Query, only: [from: 2]

  alias MinimalHostApp.Cron
  alias MinimalHostApp.Repo
  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.RuntimeSignals
  alias MinimalHostApp.Steps
  alias MinimalHostApp.Verification.WorkflowEvolution
  alias MinimalHostApp.Verification.WorkflowMigration
  alias MinimalHostApp.WorkflowRuns
  alias MinimalHostApp.Workers.JizokuWorker
  alias Jizoku.Executor.Payload
  alias Jizoku.Runtime.Journal
  alias Jizoku.Runtime.Journal.Storage.Ecto, as: JournalStorage
  alias Jizoku.Runtime.Runner
  alias Jizoku.Runtime.Signal
  alias Jizoku.Runtime.Signal.JidoAdapter
  alias Jizoku.Runtime.Trace

  @poll_attempts 20
  @journal_run_attempts 10
  @journal_run_queue_prefix "minimal-host-app-journal-smoke"
  @journal_run_storage {Jizoku.Runtime.Journal.Storage.Ecto, repo: Repo}

  defmodule FaultInjectingStorage do
    @moduledoc false
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.get_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def put_checkpoint(key, data, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.put_checkpoint(key, data, delegate_opts)
    end

    @impl Jido.Storage
    def delete_checkpoint(key, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_checkpoint(key, delegate_opts)
    end

    @impl Jido.Storage
    def load_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.load_thread(thread_id, delegate_opts)
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      if thread_id == Keyword.get(opts, :fail_append_thread_id) do
        {:error, :append_failed}
      else
        {adapter, delegate_opts} = delegate(opts)

        adapter.append_thread(
          thread_id,
          entries,
          Keyword.merge(delegate_opts, Keyword.take(opts, [:expected_rev]))
        )
      end
    end

    @impl Jido.Storage
    def delete_thread(thread_id, opts) do
      {adapter, delegate_opts} = delegate(opts)
      adapter.delete_thread(thread_id, delegate_opts)
    end

    defp delegate(opts) do
      case Keyword.fetch!(opts, :delegate) do
        {adapter, delegate_opts} -> {adapter, delegate_opts}
        adapter when is_atom(adapter) -> {adapter, []}
      end
    end
  end

  @spec run!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run! do
    RuntimeHarness.ensure_runtime_started()
    reset_runtime_state!()

    {server_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn _attempt -> RuntimeHarness.success_gateway_response("retry_required") end,
        1
      )

    attrs = %{
      account_id: "acct_demo",
      invoice_id: "inv_demo",
      attempt_id: "attempt_demo",
      gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
    }

    try do
      with {:ok, run} <- WorkflowRuns.start_payment_recovery(attrs),
           :ok <- RuntimeHarness.wait_for_execution(),
           {:ok, inspected_run} <-
             RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
           {:ok, graph} <- Jizoku.inspect_run_graph(run.run_id) do
        IO.puts("started run #{run.run_id} for #{inspect(run.workflow)}")

        unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
          raise "unexpected smoke result"
        end

        unless selected_gateway_success_route?(graph) do
          raise "unexpected payment recovery conditional route"
        end

        inspected_run
      else
        {:error, reason} ->
          raise "smoke test failed: #{inspect(reason)}"
      end
    after
      RuntimeHarness.stop_gateway_server(server_pid)
    end
  end

  defp selected_gateway_success_route?(graph) do
    graph
    |> Jizoku.Runs.GraphInspection.to_map()
    |> Map.fetch!(:edges)
    |> Enum.any?(fn
      %{
        from: "check_gateway_status",
        to: "notify_customer",
        outcome: :ok,
        selected?: true,
        condition: %{path: [:gateway_check, :status_code], greater_than: 199}
      } ->
        true

      _edge ->
        false
    end)
  end

  @spec run_all!() :: %{
          payment_recovery: Jizoku.ReadModel.Inspection.Snapshot.t(),
          deferred_payment_recovery: Jizoku.ReadModel.Inspection.Snapshot.t(),
          dependency_recovery: Jizoku.ReadModel.Inspection.Snapshot.t(),
          manual_approval: Jizoku.ReadModel.Inspection.Snapshot.t(),
          manual_digest: Jizoku.ReadModel.Inspection.Snapshot.t(),
          local_ledger_checkout: Jizoku.ReadModel.Inspection.Snapshot.t(),
          local_ledger_rollback: Jizoku.ReadModel.Inspection.Snapshot.t(),
          saga_checkout: Jizoku.ReadModel.Inspection.Snapshot.t(),
          nested_invite_delivery: Jizoku.ReadModel.Inspection.Snapshot.t(),
          nested_invite_child: Jizoku.ReadModel.Inspection.Snapshot.t(),
          journal_run: Jizoku.ReadModel.Inspection.Snapshot.t(),
          recurring_cursor: %{
            predecessor: Jizoku.ReadModel.Inspection.Snapshot.t(),
            successor: Jizoku.ReadModel.Inspection.Snapshot.t(),
            chain: Jizoku.ReadModel.ContinuationChain.t()
          },
          journal_recovery: Jizoku.ReadModel.Inspection.Snapshot.t(),
          journal_cancellation: Jizoku.ReadModel.Inspection.Snapshot.t(),
          journal_replay: Jizoku.ReadModel.Inspection.Snapshot.t(),
          journal_command_signals: %{
            start: Jizoku.ReadModel.Inspection.Snapshot.t(),
            replay: Jizoku.ReadModel.Inspection.Snapshot.t()
          },
          dynamic_work_inspection: map(),
          graph_mutation_inspection: map(),
          journal_cron_digest: Jizoku.ReadModel.Inspection.Snapshot.t(),
          command_signals: map(),
          jido_command_signals: map(),
          action_registry: Jizoku.Workflow.Spec.t(),
          editor_spec_graph: map(),
          editor_action_registry_graph: map(),
          editor_spec_diff: map(),
          workflow_evolution: Jizoku.ReadModel.Inspection.Snapshot.t(),
          workflow_migration: Jizoku.ReadModel.Inspection.Snapshot.t(),
          daily_digest: Jizoku.ReadModel.Inspection.Snapshot.t()
        }
  def run_all! do
    action_registry = run_action_registry_validation!()
    editor_spec_graph = run_editor_spec_round_trip!()
    editor_action_registry_graph = run_editor_action_registry_preview!()
    editor_spec_diff = run_editor_spec_diff!()
    workflow_evolution = WorkflowEvolution.run!()
    workflow_migration = WorkflowMigration.run!()
    payment_recovery = run!()
    deferred_payment_recovery = run_deferred_payment_recovery!()
    dependency_recovery = run_dependency_recovery!()
    manual_approval = run_manual_approval!()
    manual_digest = run_manual_digest!()
    {local_ledger_checkout, local_ledger_rollback} = run_local_ledger_checkout!()
    saga_checkout = run_saga_checkout!()
    {nested_invite_delivery, nested_invite_child} = run_nested_invite_delivery!()
    journal_run = run_journal_run!()
    recurring_cursor = run_recurring_cursor!()
    journal_recovery = run_journal_recovery!()
    journal_cancellation = run_journal_cancellation!()
    journal_replay = run_journal_replay!()
    journal_command_signals = run_journal_command_signals!()
    dynamic_work_inspection = run_dynamic_work_inspection!()
    graph_mutation_inspection = run_graph_mutation_inspection!()
    journal_cron_digest = run_journal_cron_digest!()
    command_signals = run_signal_construction!()
    jido_command_signals = run_jido_signal_adapter!(command_signals)
    existing_daily_digest_run_ids = daily_digest_run_ids()

    with :ok <- run_cron_digest(),
         {:ok, cron_run} <-
           await_daily_digest_run(existing_daily_digest_run_ids, @poll_attempts) do
      unless cron_run.status == :completed and cron_run.trigger == "daily_digest" do
        raise "unexpected cron smoke result"
      end

      %{
        payment_recovery: payment_recovery,
        deferred_payment_recovery: deferred_payment_recovery,
        dependency_recovery: dependency_recovery,
        manual_approval: manual_approval,
        manual_digest: manual_digest,
        local_ledger_checkout: local_ledger_checkout,
        local_ledger_rollback: local_ledger_rollback,
        saga_checkout: saga_checkout,
        nested_invite_delivery: nested_invite_delivery,
        nested_invite_child: nested_invite_child,
        journal_run: journal_run,
        recurring_cursor: recurring_cursor,
        journal_recovery: journal_recovery,
        journal_cancellation: journal_cancellation,
        journal_replay: journal_replay,
        journal_command_signals: journal_command_signals,
        dynamic_work_inspection: dynamic_work_inspection,
        graph_mutation_inspection: graph_mutation_inspection,
        journal_cron_digest: journal_cron_digest,
        command_signals: command_signals,
        jido_command_signals: jido_command_signals,
        action_registry: action_registry,
        editor_spec_graph: editor_spec_graph,
        editor_action_registry_graph: editor_action_registry_graph,
        editor_spec_diff: editor_spec_diff,
        workflow_evolution: workflow_evolution,
        workflow_migration: workflow_migration,
        daily_digest: cron_run
      }
    else
      {:error, reason} ->
        raise "cron smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_deferred_payment_recovery!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_deferred_payment_recovery! do
    RuntimeHarness.ensure_runtime_started()

    {server_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn
          1 -> RuntimeHarness.accepted_gateway_response("pending")
          _attempt -> RuntimeHarness.success_gateway_response("settled")
        end,
        2
      )

    attrs = %{
      account_id: "acct_deferred_demo",
      invoice_id: "inv_deferred_demo",
      attempt_id: "attempt_deferred_demo",
      gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
    }

    try do
      with {:ok, run} <- WorkflowRuns.start_payment_recovery(attrs),
           :ok <- RuntimeHarness.perform_scheduled_step!(run.run_id, "load_invoice"),
           :ok <- RuntimeHarness.perform_scheduled_step!(run.run_id, "check_gateway_status"),
           {:ok, deferred_run} <- WorkflowRuns.inspect_run(run.run_id),
           :ok <- ensure_deferred_gateway_run(deferred_run),
           {:ok, graph} <- Jizoku.inspect_run_graph(run.run_id),
           :ok <- ensure_deferred_gateway_graph(graph),
           {:ok, diagnostic} <- Jizoku.explain_run(run.run_id),
           :ok <- ensure_deferred_gateway_explanation(diagnostic),
           :ok <- RuntimeHarness.perform_scheduled_step!(run.run_id, "check_gateway_status"),
           {:ok, completed_run} <-
             RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
        unless completed_run.status == :completed and
                 completed_run.context.gateway_check.status == "settled" do
          raise "unexpected deferred payment recovery result"
        end

        completed_run
      else
        {:error, reason} ->
          raise "deferred payment recovery smoke test failed: #{inspect(reason)}"
      end
    after
      RuntimeHarness.stop_gateway_server(server_pid)
    end
  end

  defp ensure_deferred_gateway_run(%Jizoku.ReadModel.Inspection.Snapshot{} = run) do
    with :running <- run.status,
         :deferred_continuation <- run.reason,
         [%{step: "check_gateway_status", deferred: %{reason: %{status_code: 202}}}] <-
           run.scheduled_attempts do
      :ok
    else
      _unexpected -> {:error, :unexpected_deferred_gateway_run}
    end
  end

  defp ensure_deferred_gateway_graph(%Jizoku.Runs.GraphInspection{} = graph) do
    nodes = Map.new(graph.nodes, &{&1.id, &1})

    case Map.fetch(nodes, "check_gateway_status") do
      {:ok, %{status: :deferred, current?: true}} -> :ok
      _unexpected -> {:error, :unexpected_deferred_gateway_graph}
    end
  end

  defp ensure_deferred_gateway_explanation(
         %Jizoku.ReadModel.Explanation.Diagnostic{} = diagnostic
       ) do
    case diagnostic do
      %{reason: :deferred_continuation, next_actions: [:wait_until_attempt_visible]} ->
        :ok

      _unexpected ->
        {:error, :unexpected_deferred_gateway_explanation}
    end
  end

  @doc """
  Builds the internal command signal taxonomy from the host-app boundary.
  """
  @spec run_signal_construction!() :: %{atom() => Signal.t()}
  def run_signal_construction! do
    run_id = Ecto.UUID.generate()
    occurred_at = DateTime.utc_now(:second)

    cron_input = %{
      "signal_id" => smoke_cron_signal_id(),
      "intended_window" => %{
        "start_at" => "2026-05-26T12:00:00Z",
        "end_at" => "2026-05-26T13:00:00Z"
      }
    }

    with {:ok, start_run} <-
           Signal.start_run(
             MinimalHostApp.Workflows.PaymentRecovery,
             :payment_recovery,
             %{
               account_id: "acct_signal_smoke",
               invoice_id: "inv_signal_smoke",
               attempt_id: "attempt_signal_smoke",
               gateway_url: "http://127.0.0.1/signal-smoke"
             },
             metadata: %{source: :minimal_host_app_smoke},
             occurred_at: occurred_at,
             idempotency_key: "minimal-host-app:signal-smoke:start"
           ),
         {:ok, start_cron} <-
           Signal.start_cron(
             MinimalHostApp.Workflows.DailyDigest,
             :daily_digest,
             cron_input,
             metadata: %{source: :minimal_host_app_smoke},
             occurred_at: occurred_at
           ),
         {:ok, approve_run} <-
           Signal.approve_run(run_id, %{actor: "ops_smoke"}, occurred_at: occurred_at),
         {:ok, reject_run} <-
           Signal.reject_run(run_id, %{reason: "signal smoke"}, occurred_at: occurred_at),
         {:ok, resume_run} <-
           Signal.resume_run(run_id, %{actor: "ops_smoke"}, occurred_at: occurred_at),
         {:ok, cancel_run} <- Signal.cancel_run(run_id, occurred_at: occurred_at),
         {:ok, replay_run} <-
           Signal.replay_run(run_id, allow_irreversible: true, occurred_at: occurred_at) do
      signals = %{
        start_run: start_run,
        start_cron: start_cron,
        approve_run: approve_run,
        reject_run: reject_run,
        resume_run: resume_run,
        cancel_run: cancel_run,
        replay_run: replay_run
      }

      signal_types =
        signals
        |> Map.values()
        |> Enum.map(fn %Signal{type: type} -> type end)
        |> Enum.sort()

      unless signal_types == [
               :approve_run,
               :cancel_run,
               :reject_run,
               :replay_run,
               :resume_run,
               :start_cron,
               :start_run
             ] do
        raise "unexpected command signal smoke result"
      end

      signal_id = Map.fetch!(cron_input, "signal_id")

      unless match?(
               %Signal{
                 type: :start_run,
                 payload: %{
                   workflow: "Elixir.MinimalHostApp.Workflows.PaymentRecovery",
                   trigger: "payment_recovery"
                 }
               },
               start_run
             ) do
        raise "unexpected start run command signal"
      end

      unless match?(
               %Signal{
                 type: :start_cron,
                 payload: %{
                   workflow: "Elixir.MinimalHostApp.Workflows.DailyDigest",
                   trigger: "daily_digest"
                 },
                 idempotency_key: ^signal_id
               },
               start_cron
             ) do
        raise "unexpected cron command signal"
      end

      signals
    else
      {:error, reason} ->
        raise "command signal smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Adapts command signals to Jido envelopes from the host-app boundary.
  """
  @spec run_jido_signal_adapter!(%{atom() => Signal.t()}) :: %{atom() => Jido.Signal.t()}
  def run_jido_signal_adapter!(signals) when is_map(signals) do
    Enum.reduce(signals, %{}, fn {name, signal}, acc ->
      with {:ok, jido_signal} <- JidoAdapter.to_jido(signal),
           {:ok, ^signal} <- JidoAdapter.from_jido(jido_signal) do
        Map.put(acc, name, jido_signal)
      else
        {:error, reason} ->
          raise "Jido command signal adapter smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Round-trips a compiled workflow spec through the visual-editor JSON contract.
  """
  @spec run_editor_spec_round_trip!() :: map()
  def run_editor_spec_round_trip! do
    with {:ok, spec} <- Jizoku.Workflow.to_spec(MinimalHostApp.Workflows.PaymentRecovery),
         editor_map <- Jizoku.Workflow.EditorSpec.to_map(spec),
         {:ok, json} <- Jason.encode(editor_map),
         {:ok, round_tripped} <- Jason.decode(json),
         :ok <- Jizoku.Workflow.EditorSpec.validate_map(round_tripped),
         {:ok, graph} <- Jizoku.Workflow.EditorSpec.preview_graph(round_tripped) do
      unless Enum.map(graph["nodes"], & &1["id"]) == [
               "load_invoice",
               "check_gateway_status",
               "issue_gateway_credit",
               "notify_customer"
             ] do
        raise "unexpected editor spec graph nodes"
      end

      graph
    else
      {:error, reason} ->
        raise "editor spec round-trip smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Compares a visual-editor draft against its source workflow spec.
  """
  @spec run_editor_spec_diff!() :: map()
  def run_editor_spec_diff! do
    with {:ok, spec} <- Jizoku.Workflow.to_spec(MinimalHostApp.Workflows.PaymentRecovery),
         editor_map <- Jizoku.Workflow.EditorSpec.to_map(spec),
         draft <- editor_diff_draft(editor_map),
         {:ok, json} <- Jason.encode(draft),
         {:ok, round_tripped} <- Jason.decode(json),
         {:ok, diff} <-
           Jizoku.Workflow.EditorSpec.diff(spec, round_tripped,
             action_registry: payment_action_registry()
           ) do
      unless diff["summary"]["nodes_added"] == 1 and
               diff["summary"]["edges_added"] == 2 and
               diff["summary"]["edges_removed"] == 1 and
               match?([%{"id" => "archive_invoice"}], diff["nodes"]["added"]) do
        raise "unexpected editor spec diff"
      end

      diff
    else
      {:error, reason} ->
        raise "editor spec diff smoke test failed: #{inspect(reason)}"
    end
  end

  defp editor_diff_draft(editor_map) do
    editor_map
    |> put_in(["steps"], editor_map["steps"] ++ [editor_archive_step()])
    |> Map.update!("transitions", fn transitions ->
      transitions
      |> Enum.map(fn
        %{"from" => "notify_customer", "on" => "ok"} = transition ->
          %{transition | "to" => "archive_invoice"}

        transition ->
          transition
      end)
      |> Kernel.++([%{"from" => "archive_invoice", "on" => "ok", "to" => "complete"}])
    end)
  end

  defp editor_archive_step do
    %{
      "name" => "archive_invoice",
      "action" => "payment.archive_invoice",
      "opts" => %{}
    }
  end

  @doc """
  Validates visual-editor JSON action keys through the host registry.
  """
  @spec run_editor_action_registry_preview!() :: map()
  def run_editor_action_registry_preview! do
    registry = payment_action_registry()

    with editor_map <- Jizoku.Workflow.EditorSpec.to_map(action_registry_spec()),
         {:ok, json} <- Jason.encode(editor_map),
         {:ok, round_tripped} <- Jason.decode(json),
         :ok <-
           Jizoku.Workflow.EditorSpec.validate_map(round_tripped,
             action_registry: registry
           ),
         {:ok, graph} <-
           Jizoku.Workflow.EditorSpec.preview_graph(round_tripped,
             action_registry: registry
           ) do
      unless Enum.map(graph["nodes"], &{&1["id"], &1["action"]}) == [
               {"load_invoice", "payment.load_invoice"},
               {"notify_customer", "payment.notify_customer"}
             ] do
        raise "unexpected editor action registry graph"
      end

      graph
    else
      {:error, reason} ->
        raise "editor action registry smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Validates a runtime-authored spec through host-owned action keys.
  """
  @spec run_action_registry_validation!() :: Jizoku.Workflow.Spec.t()
  def run_action_registry_validation! do
    registry = payment_action_registry()

    with :ok <-
           Jizoku.Workflow.validate_spec(action_registry_spec(), action_registry: registry),
         {:ok, resolved_spec} <-
           Jizoku.Workflow.resolve_spec_actions(action_registry_spec(),
             action_registry: registry
           ) do
      unless Enum.map(resolved_spec.steps, &{&1.name, &1.module, &1.metadata.action}) == [
               {:load_invoice, Steps.LoadInvoice, "payment.load_invoice"},
               {:notify_customer, Steps.NotifyCustomer, "payment.notify_customer"}
             ] do
        raise "unexpected action registry smoke result"
      end

      resolved_spec
    else
      {:error, reason} ->
        raise "action registry smoke test failed: #{inspect(reason)}"
    end
  end

  defp payment_action_registry do
    %{
      "payment.load_invoice" => Steps.LoadInvoice,
      "payment.notify_customer" => Steps.NotifyCustomer,
      "payment.archive_invoice" => Steps.NotifyCustomer
    }
  end

  defp action_registry_spec do
    %Jizoku.Workflow.Spec{
      workflow: MinimalHostApp.RuntimeAuthoredPaymentRecovery,
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [
            %{name: :account_id, type: :string, opts: []},
            %{name: :invoice_id, type: :string, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :account_id, type: :string, opts: []},
        %{name: :invoice_id, type: :string, opts: []}
      ],
      steps: [
        %{name: :load_invoice, action: "payment.load_invoice", opts: []},
        %{name: :notify_customer, action: "payment.notify_customer", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :notify_customer},
        %{from: :notify_customer, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }
  end

  @spec run_dependency_recovery!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_dependency_recovery! do
    attrs = %{
      account_id: "acct_dependency_demo",
      invoice_id: "inv_dependency_demo",
      attempt_id: "attempt_dependency_demo"
    }

    with {:ok, run} <- WorkflowRuns.start_dependency_recovery(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true) do
      unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
        raise "unexpected dependency recovery smoke result"
      end

      unless Enum.map(history_run.attempts, &{&1.step, &1.status, &1.applied?}) == [
               {"load_account", :completed, true},
               {"load_invoice", :completed, true},
               {"prepare_notification", :completed, true}
             ] do
        raise "unexpected dependency inspection history"
      end

      unless mapped_dependency_input?(history_run) do
        raise "unexpected dependency mapped input"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "dependency recovery smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Runs the dependency-based example workflow through the journal run loop.
  """
  @spec run_journal_run!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_run! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    attrs = %{
      account_id: "acct_journal_demo",
      invoice_id: "inv_journal_demo",
      attempt_id: "attempt_journal_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             Jizoku.start(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           {:ok, inspected_run} <-
             drain_journal_run(started_run.run_id, @journal_run_attempts),
           {:ok, explanation} <- Jizoku.explain_run(started_run.run_id),
           {:ok, graph} <- Jizoku.inspect_run_graph(started_run.run_id),
           {:ok, listed_runs} <-
             Jizoku.list_runs(workflow: MinimalHostApp.Workflows.DependencyRecovery) do
        unless started_run.queue == queue and
                 inspected_run.queue == queue and
                 explanation.queue == queue do
          raise "unexpected journal run queue"
        end

        unless explanation.details.command_count == 1 and
                 get_in(explanation.details, [:latest_command, :signal_type]) == "start_run" and
                 explanation.evidence.command_counts == %{"start_run" => 1} do
          raise "unexpected journal command explanation"
        end

        unless inspected_run.status == :completed do
          raise "unexpected journal run smoke result"
        end

        unless started_run.definition_version == "2026-05-26.dependency-recovery" and
                 inspected_run.definition_version == "2026-05-26.dependency-recovery" and
                 explanation.definition_version == "2026-05-26.dependency-recovery" and
                 graph.definition_version == "2026-05-26.dependency-recovery" and
                 Enum.any?(listed_runs, fn listed ->
                   listed.run_id == started_run.run_id and
                     listed.definition_version == "2026-05-26.dependency-recovery"
                 end) do
          raise "unexpected journal workflow definition version metadata"
        end

        inspected_run
      else
        {:error, reason} ->
          raise "journal run smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs one native continue-as-new cycle and verifies its bounded lineage.
  """
  @spec run_recurring_cursor!() :: %{
          predecessor: Jizoku.ReadModel.Inspection.Snapshot.t(),
          successor: Jizoku.ReadModel.Inspection.Snapshot.t(),
          chain: Jizoku.ReadModel.ContinuationChain.t()
        }
  def run_recurring_cursor! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    with {:ok, started_run} <- WorkflowRuns.start_recurring_cursor(%{cursor: 0}, queue: queue),
         {:ok, successor} <-
           Jizoku.execute_next(
             queue: queue,
             owner_id: "minimal-host-continuation-worker-1"
           ),
         {:ok, completed_successor} <-
           Jizoku.execute_next(
             queue: queue,
             owner_id: "minimal-host-continuation-worker-2"
           ),
         {:ok, predecessor} <- WorkflowRuns.inspect_run(started_run.run_id, queue: queue),
         {:ok, chain} <-
           WorkflowRuns.inspect_continuation_chain(completed_successor.run_id,
             direction: :backward,
             max_hops: 5
           ) do
      expected_run_ids = [completed_successor.run_id, predecessor.run_id]

      unless predecessor.status == :continued and
               completed_successor.status == :completed and
               successor.run_id == completed_successor.run_id and
               successor.input == %{cursor: 1} and
               predecessor.continuation.continued_to.run_id == completed_successor.run_id and
               completed_successor.continuation.continued_from.run_id == predecessor.run_id and
               Enum.map(chain.runs, & &1.run_id) == expected_run_ids and
               chain.truncated? == false do
        raise "unexpected recurring cursor continuation result"
      end

      %{predecessor: predecessor, successor: completed_successor, chain: chain}
    else
      {:error, reason} ->
        raise "recurring cursor continuation smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Schedules executable dynamic work in the host example journal and verifies the
  graph/explanation read models expose it.
  """
  @spec run_dynamic_work_inspection!() :: map()
  def run_dynamic_work_inspection! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             Jizoku.start(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               %{
                 account_id: "acct_dynamic_demo",
                 invoice_id: "inv_dynamic_demo",
                 attempt_id: "attempt_dynamic_demo"
               }
             ),
           {:ok, producer_run} <- Jizoku.execute_next(journal_run_execute_options()),
           :ok <- preview_dynamic_work!(producer_run),
           :ok <- schedule_dynamic_work!(producer_run),
           {:ok, inspected_run} <- drain_journal_run(started_run.run_id, @journal_run_attempts),
           {:ok, graph} <- Jizoku.inspect_run_graph(inspected_run.run_id),
           {:ok, explanation} <- Jizoku.explain_run(inspected_run.run_id) do
        graph_payload = Jizoku.Runs.GraphInspection.to_map(graph)

        unless Enum.any?(
                 graph_payload.dynamic_work,
                 &(&1.dynamic_key == "dynamic_invoice_fanout")
               ) and
                 Enum.any?(
                   graph_payload.dynamic_work_overlays,
                   &(&1.dynamic_key == "dynamic_invoice_fanout" and
                       &1.status == :scheduled and
                       &1.origin_node_id == "load_account" and
                       &1.added_node_ids == ["notify_invoice:inv_dynamic_demo"] and
                       &1.added_edge_ids == [
                         "load_account:dynamic:notify_invoice:inv_dynamic_demo"
                       ] and
                       &1.node_count == 1 and
                       &1.edge_count == 1)
                 ) and
                 Enum.any?(
                   graph_payload.nodes,
                   &(&1.id == "notify_invoice:inv_dynamic_demo" and &1.status == :completed)
                 ) and
                 explanation.details.dynamic_work_count == 1 do
          raise "unexpected dynamic work inspection smoke result"
        end

        graph_payload
      else
        {:error, reason} ->
          raise "dynamic work inspection smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Applies a versioned dependency graph mutation and repairs a failed dispatch append.
  """
  @spec run_graph_mutation_inspection!() :: map()
  def run_graph_mutation_inspection! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()
    dynamic_queue = queue

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             Jizoku.start(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               %{
                 account_id: "acct_graph_demo",
                 invoice_id: "inv_graph_demo",
                 attempt_id: "attempt_graph_demo"
               }
             ),
           {:ok, _producer_run} <- await_graph_mutation_origin(started_run.run_id, 3),
           {:ok, report} <- apply_smoke_graph_mutation(started_run.run_id, dynamic_queue),
           :ok <- ensure_pending_graph_mutation(started_run.run_id, report),
           {:ok, reconciliation} <- Jizoku.reconcile_dynamic_graph(started_run.run_id),
           :ok <- ensure_graph_reconciliation(reconciliation, dynamic_queue),
           {:ok, completed_run} <-
             drain_graph_mutation_run(
               started_run.run_id,
               [queue, dynamic_queue],
               @journal_run_attempts * 2
             ),
           {:ok, graph} <- Jizoku.inspect_run_graph(completed_run.run_id),
           :ok <- ensure_completed_graph_mutation(graph) do
        Jizoku.Runs.GraphInspection.to_map(graph)
      else
        {:error, reason} ->
          raise "graph mutation inspection smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs a journal run smoke path after dropping checkpoints.

  The append-only Jido thread log remains the source of truth, so inspection and
  execution must recover from persisted entries when checkpoint accelerators are
  missing.
  """
  @spec run_journal_recovery!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_recovery! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    attrs = %{
      account_id: "acct_journal_recovery_demo",
      invoice_id: "inv_journal_recovery_demo",
      attempt_id: "attempt_journal_recovery_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             Jizoku.start(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           :ok <- delete_journal_checkpoints(started_run.run_id, queue),
           {:ok, recovered_run} <- Jizoku.inspect_run(started_run.run_id),
           {:ok, completed_run} <-
             drain_journal_run(started_run.run_id, @journal_run_attempts) do
        unless recovered_run.run_id == started_run.run_id and recovered_run.queue == queue do
          raise "unexpected recovered journal run"
        end

        unless completed_run.status == :completed do
          raise "unexpected journal recovery smoke result"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal run recovery smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs journal cancellation through the example app's configured Ecto storage.
  """
  @spec run_journal_cancellation!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_cancellation! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    attrs = %{
      account_id: "acct_journal_cancel_demo",
      invoice_id: "inv_journal_cancel_demo",
      attempt_id: "attempt_journal_cancel_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             Jizoku.start(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           {:ok, cancelled_run} <- Jizoku.cancel(started_run.run_id),
           {:ok, inspected_run} <- Jizoku.inspect_run(started_run.run_id),
           {:ok, :none} <- Jizoku.execute_next(journal_run_execute_options()) do
        unless started_run.queue == queue and
                 cancelled_run.queue == queue and
                 inspected_run.queue == queue do
          raise "unexpected journal cancellation queue"
        end

        unless cancelled_run.status == :cancelled and inspected_run.status == :cancelled and
                 cancelled_run.visible_attempts == [] do
          raise "unexpected journal cancellation smoke result"
        end

        unless Enum.map(cancelled_run.command_history, & &1.signal_type) == [
                 "start_run",
                 "cancel_run"
               ] do
          raise "unexpected journal cancellation command history"
        end

        cancelled_run
      else
        {:error, reason} ->
          raise "journal cancellation smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs journal replay through the example app's configured Ecto storage.
  """
  @spec run_journal_replay!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_replay! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()

    {server_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn _attempt -> RuntimeHarness.success_gateway_response("retry_required") end,
        2
      )

    attrs = %{
      account_id: "acct_journal_replay_demo",
      invoice_id: "inv_journal_replay_demo",
      attempt_id: "attempt_journal_replay_demo",
      gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
    }

    try do
      with_journal_runtime_config(queue, fn ->
        with {:ok, started_run} <-
               Jizoku.start(
                 MinimalHostApp.Workflows.PaymentRecovery,
                 :payment_recovery,
                 attrs
               ),
             {:ok, completed_run} <-
               drain_journal_run(started_run.run_id, @journal_run_attempts),
             {:ok, replayed_run} <-
               Jizoku.replay(completed_run.run_id, allow_irreversible: true),
             {:ok, completed_replay} <-
               drain_journal_run(replayed_run.run_id, @journal_run_attempts),
             {:ok, replay_graph} <- Jizoku.inspect_run_graph(completed_replay.run_id) do
          unless completed_run.status == :completed and completed_replay.status == :completed do
            raise "unexpected journal replay smoke result"
          end

          unless replayed_run.replayed_from_run_id == completed_run.run_id and
                   replayed_run.input == attrs do
            raise "unexpected journal replay lineage"
          end

          unless selected_gateway_success_route?(replay_graph) and
                   completed_replay.context.notification.channel == "email" and
                   completed_replay.context.gateway_check.status == "retry_required" do
            raise "unexpected journal replay conditional route"
          end

          completed_replay
        else
          {:error, reason} ->
            raise "journal replay smoke test failed: #{inspect(reason)}"
        end
      end)
    after
      RuntimeHarness.stop_gateway_server(server_pid)
    end
  end

  @doc """
  Starts and replays journal runs through Jizoku command signals.
  """
  @spec run_journal_command_signals!() :: %{
          start: Jizoku.ReadModel.Inspection.Snapshot.t(),
          replay: Jizoku.ReadModel.Inspection.Snapshot.t()
        }
  def run_journal_command_signals! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()
    lifecycle_event = [:jizoku, :runtime, :attempt, :completed]

    attrs = %{
      account_id: "acct_journal_signal_demo",
      invoice_id: "inv_journal_signal_demo",
      attempt_id: "attempt_journal_signal_demo"
    }

    with_smoke_telemetry(lifecycle_event, fn ->
      with_journal_runtime_config(queue, fn ->
        with {:ok, command_trace} <-
               Trace.new_root(causation_id: "minimal-host-app:journal-signal:jido-start"),
             {:ok, jido_start_signal} <-
               Jido.Signal.new(
                 "minimal_host.dependency_recovery.requested",
                 Map.new(attrs, fn {key, value} -> {Atom.to_string(key), value} end),
                 id: "minimal-host-app:journal-signal:jido-start",
                 source: "/minimal_host_app/dependency_recovery",
                 subject: "accounts/#{attrs.account_id}"
               ),
             jido_start_signal =
               %{jido_start_signal | extensions: %{"correlation" => command_trace}},
             {:ok, started_run} <- RuntimeSignals.apply_domain(jido_start_signal),
             {:ok, _first_worker_run} <-
               Jizoku.execute_next(owner_id: "minimal-host-app-signal-worker-a"),
             {:ok, lifecycle_metadata} <-
               await_smoke_telemetry(lifecycle_event, started_run.run_id),
             {:ok, completed_start} <-
               drain_journal_run(
                 started_run.run_id,
                 @journal_run_attempts,
                 owner_id: "minimal-host-app-signal-worker-b"
               ),
             :ok <-
               ensure_journal_signal_trace(
                 completed_start,
                 queue,
                 command_trace,
                 lifecycle_metadata
               ),
             {:ok, replay_signal} <-
               Signal.replay_run(
                 completed_start.run_id,
                 metadata: %{source: "minimal_host_app_smoke"},
                 idempotency_key: "minimal-host-app:journal-signal:replay"
               ),
             {:ok, replayed_run} <- Jizoku.apply_signal(replay_signal),
             {:ok, completed_replay} <-
               drain_journal_run(replayed_run.run_id, @journal_run_attempts) do
          unless completed_start.status == :completed and completed_replay.status == :completed do
            raise "unexpected journal command signal smoke result"
          end

          unless completed_replay.replayed_from_run_id == completed_start.run_id do
            raise "unexpected journal command signal replay lineage"
          end

          case completed_start.command_history do
            [
              %{
                signal_type: "start_run",
                source: "/minimal_host_app/dependency_recovery",
                metadata: %{
                  "jido" => %{
                    "subject" => "accounts/acct_journal_signal_demo",
                    "type" => "minimal_host.dependency_recovery.requested"
                  }
                }
              }
            ] ->
              :ok

            _other ->
              raise "unexpected journal start command signal history"
          end

          case completed_replay.command_history do
            [%{signal_type: "replay_run", metadata: %{source: "minimal_host_app_smoke"}}] ->
              :ok

            _other ->
              raise "unexpected journal replay command signal history"
          end

          %{start: completed_start, replay: completed_replay}
        else
          {:error, reason} ->
            raise "journal command signal smoke test failed: #{inspect(reason)}"
        end
      end)
    end)
  end

  @doc """
  Starts the daily digest cron trigger through the journal runtime.
  """
  @spec run_journal_cron_digest!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_cron_digest! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()
    signal_id = "minimal-host-app:journal:daily_digest:#{System.unique_integer([:positive])}"

    payload =
      Payload.cron(
        MinimalHostApp.Workflows.DailyDigest,
        :daily_digest,
        signal_id: signal_id
      )

    with_journal_runtime_config(queue, fn ->
      with :ok <- Runner.perform(payload),
           {:ok, run_id} <- journal_daily_digest_run_id(queue),
           {:ok, completed_run} <- drain_journal_run(run_id, @journal_run_attempts) do
        unless completed_run.status == :completed and completed_run.trigger == "daily_digest" do
          raise "unexpected journal cron smoke result"
        end

        unless completed_run.context.schedule.signal_id == signal_id do
          raise "unexpected journal cron schedule context"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal cron smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Proves duplicate daily digest cron delivery is fenced by the journal runtime.
  """
  @spec run_journal_cron_duplicate_digest!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_journal_cron_duplicate_digest! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_run_queue()
    signal_id = "minimal-host-app:journal:daily_digest:duplicate"

    payload =
      Payload.cron(
        MinimalHostApp.Workflows.DailyDigest,
        :daily_digest,
        signal_id: signal_id
      )

    with_journal_runtime_config(queue, fn ->
      with :ok <- Runner.perform(payload),
           {:ok, run_id} <- journal_daily_digest_run_id(queue),
           {:ok, {:duplicate_schedule_start, ^run_id}} <-
             Runner.start_cron_trigger(payload["workflow"], payload["trigger"], payload, []),
           {:ok, completed_run} <- drain_journal_run(run_id, @journal_run_attempts) do
        unless completed_run.context.schedule.signal_id == signal_id do
          raise "unexpected journal cron duplicate schedule context"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal cron duplicate smoke test failed: #{inspect(reason)}"

        other ->
          raise "journal cron duplicate smoke test failed: #{inspect(other)}"
      end
    end)
  end

  @spec run_cancellation!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_cancellation! do
    RuntimeHarness.ensure_runtime_started()

    case run_cancellation_smoke() do
      {:ok, cancelled_run} ->
        cancelled_run

      {:error, reason} ->
        raise "cancellation smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_approval!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_manual_approval! do
    with {:ok, run} <- WorkflowRuns.start_manual_approval(%{account_id: "acct_manual_demo"}),
         {:ok, _paused_run} <- await_paused_run(run.run_id, @poll_attempts),
         {:ok, explanation} <- WorkflowRuns.explain_run(run.run_id),
         :ok <- ensure_paused_approval_explanation(explanation),
         {:ok, resumed_run} <-
           WorkflowRuns.approve(
             run.run_id,
             %{actor: "ops_smoke", comment: "approved", metadata: %{ticket: "SMOKE-1"}}
           ),
         :ok <- ensure_resumed(resumed_run),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true),
         :ok <- ensure_manual_approval_audit(history_run) do
      unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
        raise "unexpected manual approval smoke result"
      end

      history_run
    else
      {:error, reason} ->
        raise "manual approval smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_digest!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_manual_digest! do
    attrs = %{channel: "ops-manual", digest_date: Date.utc_today() |> Date.to_iso8601()}

    with {:ok, run} <- WorkflowRuns.start_manual_digest(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
      unless inspected_run.status == :completed and inspected_run.trigger == "manual_digest" do
        raise "unexpected manual digest smoke result"
      end

      unless inspected_run.context.digest_delivery.channel == attrs.channel and
               inspected_run.context.digest_delivery.digest_date == attrs.digest_date do
        raise "unexpected manual digest payload"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "manual digest smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_local_ledger_checkout!() ::
          {Jizoku.ReadModel.Inspection.Snapshot.t(), Jizoku.ReadModel.Inspection.Snapshot.t()}
  def run_local_ledger_checkout! do
    committed_attrs = %{account_id: "acct_local_commit", fail_after_reserve: false}
    rolled_back_attrs = %{account_id: "acct_local_rollback", fail_after_reserve: true}

    with {:ok, committed_run} <- WorkflowRuns.start_local_ledger_checkout(committed_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, committed_terminal_run} <-
           RuntimeHarness.await_terminal_run(committed_run.run_id, attempts: @poll_attempts),
         :ok <- ensure_local_ledger_entries(committed_terminal_run, ["reserve", "capture"]),
         {:ok, rolled_back_run} <- WorkflowRuns.start_local_ledger_checkout(rolled_back_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, rolled_back_terminal_run} <-
           RuntimeHarness.await_terminal_run(rolled_back_run.run_id, attempts: @poll_attempts),
         :ok <- ensure_local_ledger_entries(rolled_back_terminal_run, []) do
      unless committed_terminal_run.status == :completed and
               rolled_back_terminal_run.status == :failed do
        raise "unexpected local ledger smoke result"
      end

      {committed_terminal_run, rolled_back_terminal_run}
    else
      {:error, reason} ->
        raise "local ledger smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Runs the saga checkout example and verifies persisted retry failure history.
  """
  @spec run_saga_checkout!() :: Jizoku.ReadModel.Inspection.Snapshot.t()
  def run_saga_checkout! do
    attrs = %{account_id: "acct_saga_demo", order_id: "ord_saga_demo"}

    with {:ok, run} <- WorkflowRuns.start_saga_checkout(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true),
         :ok <- ensure_saga_failure_history(history_run) do
      unless inspected_run.status == :failed and
               Enum.any?(inspected_run.attempts, &(&1.step == "capture_payment")) do
        raise "unexpected saga checkout smoke result"
      end

      history_run
    else
      {:error, reason} ->
        raise "saga checkout smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Runs a nested workflow where parent and child both retry once.
  """
  @spec run_nested_invite_delivery!() ::
          {Jizoku.ReadModel.Inspection.Snapshot.t(), Jizoku.ReadModel.Inspection.Snapshot.t()}
  def run_nested_invite_delivery! do
    child_queue = "minimal-host-app-nested-child-smoke"

    attrs = %{
      party_id: "party_smoke",
      guest_id: "guest_smoke",
      child_queue: child_queue,
      fail_after_child_start: true,
      fail_child_once: true
    }

    with {:ok, run} <- WorkflowRuns.start_nested_invite_delivery(attrs),
         {:ok, retried_parent} <-
           Jizoku.execute_next(
             owner_id: "minimal-host-app-nested-smoke-parent",
             queue: run.queue
           ),
         {:ok, child_run_id} <-
           ensure_nested_child_link(retried_parent, "invite_guest_smoke", child_queue),
         :ok <- delete_available_journal_checkpoints(retried_parent.run_id, retried_parent.queue),
         :ok <- delete_available_journal_checkpoints(child_run_id, child_queue),
         :ok <-
           ensure_reconstructed_nested_runs(
             retried_parent.run_id,
             child_run_id,
             retried_parent.child_runs,
             child_queue
           ),
         {:ok, completed_parent} <-
           Jizoku.execute_next(
             owner_id: "minimal-host-app-nested-smoke-parent",
             queue: retried_parent.queue
           ),
         {:ok, child_retrying} <-
           Jizoku.execute_next(
             owner_id: "minimal-host-app-nested-smoke-child",
             queue: child_queue
           ),
         :ok <- delete_available_journal_checkpoints(child_retrying.run_id, child_queue),
         :ok <- ensure_reconstructed_nested_child_retry(child_retrying.run_id, child_queue),
         {:ok, completed_child} <-
           Jizoku.execute_next(
             owner_id: "minimal-host-app-nested-smoke-child",
             queue: child_queue
           ),
         {:ok, parent_history} <- WorkflowRuns.inspect_run(run.run_id, include_history: true),
         {:ok, child_history} <-
           WorkflowRuns.inspect_run(completed_child.run_id,
             queue: child_queue,
             include_history: true
           ),
         :ok <- ensure_nested_parent_result(completed_parent, parent_history, child_queue),
         :ok <- ensure_nested_child_result(child_retrying, child_history) do
      {parent_history, child_history}
    else
      {:error, reason} ->
        raise "nested invite delivery smoke test failed: #{inspect(reason)}"
    end
  end

  @spec wait_for_execution() :: :ok
  defp wait_for_execution do
    RuntimeHarness.wait_for_execution()
  end

  defp smoke_cron_signal_id do
    "minimal-host-app:smoke:daily_digest:#{System.unique_integer([:positive])}"
  end

  defp journal_daily_digest_run_id(queue) do
    case Jizoku.list_runs(workflow: MinimalHostApp.Workflows.DailyDigest) do
      {:ok, runs} ->
        runs
        |> Enum.find(&(&1.queue == queue))
        |> case do
          %{run_id: run_id} when is_binary(run_id) -> {:ok, run_id}
          _missing -> {:error, :missing_journal_daily_digest_run}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp mapped_dependency_input?(%Jizoku.ReadModel.Inspection.Snapshot{attempts: attempts})
       when is_list(attempts) do
    Enum.any?(attempts, fn
      %{step: "prepare_notification", input: input} ->
        input == %{
          account_id: "acct_dependency_demo",
          invoice_id: "inv_dependency_demo",
          account_tier: "standard"
        }

      _step_run ->
        false
    end)
  end

  defp mapped_dependency_input?(_run), do: false

  @spec run_cron_digest() :: :ok
  defp run_cron_digest do
    if manual_oban_testing?() do
      # Manual Oban testing disables plugins, so start the real plugin to
      # validate its configuration and then invoke the cron worker explicitly.
      Cron.ensure_started!()

      %Oban.Job{
        args: %{
          "kind" => "cron",
          "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
          "trigger" => "daily_digest",
          "signal_id" => smoke_cron_signal_id()
        }
      }
      |> JizokuWorker.perform()
      |> case do
        :ok -> wait_for_execution()
        {:error, reason} -> raise "manual cron smoke trigger failed: #{inspect(reason)}"
      end
    else
      Cron.evaluate!()
      wait_for_execution()
    end
  end

  @spec run_cancellation_smoke() ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp run_cancellation_smoke do
    with {:ok, run} <- WorkflowRuns.start_cancellable_wait(%{account_id: "acct_demo"}),
         :ok <- wait_for_execution(),
         {:ok, cancelling_run} <- WorkflowRuns.cancel(run.run_id),
         :ok <- ensure_cancelling(cancelling_run),
         {:ok, cancelled_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
      {:ok, cancelled_run}
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @spec ensure_cancelling(Jizoku.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_cancellation_status}
  defp ensure_cancelling(%Jizoku.ReadModel.Inspection.Snapshot{status: :cancelled}), do: :ok

  defp ensure_cancelling(%Jizoku.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_cancellation_status}

  @spec await_paused_run(Ecto.UUID.t(), non_neg_integer()) ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp await_paused_run(_run_id, 0), do: {:error, :timeout}

  defp await_paused_run(run_id, attempts_remaining) when attempts_remaining > 0 do
    :ok = RuntimeHarness.wait_for_execution()
    _result = Jizoku.execute_next(owner_id: "minimal-host-app-manual-smoke")

    case WorkflowRuns.inspect_run(run_id, include_history: true) do
      {:ok, %Jizoku.ReadModel.Inspection.Snapshot{} = run} ->
        case ensure_paused(run) do
          :ok ->
            {:ok, run}

          {:error, _reason} ->
            Process.sleep(50)
            await_paused_run(run_id, attempts_remaining - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec ensure_paused(Jizoku.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_paused_status}
  defp ensure_paused(%Jizoku.ReadModel.Inspection.Snapshot{
         status: :paused,
         manual_state: %{step: "wait_for_approval"}
       }),
       do: :ok

  defp ensure_paused(%Jizoku.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_paused_status}

  @spec ensure_paused_approval_explanation(Jizoku.ReadModel.Explanation.Diagnostic.t()) ::
          :ok | {:error, :unexpected_explanation}
  defp ensure_paused_approval_explanation(%Jizoku.ReadModel.Explanation.Diagnostic{
         status: :paused,
         next_actions: next_actions
       }) do
    if :resolve_manual_step in next_actions do
      :ok
    else
      {:error, :unexpected_explanation}
    end
  end

  defp ensure_paused_approval_explanation(%Jizoku.ReadModel.Explanation.Diagnostic{}),
    do: {:error, :unexpected_explanation}

  @spec ensure_resumed(Jizoku.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_resumed_status}
  defp ensure_resumed(%Jizoku.ReadModel.Inspection.Snapshot{
         status: :running,
         visible_attempts: [%{step: "record_approval"} | _]
       }),
       do: :ok

  defp ensure_resumed(%Jizoku.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_resumed_status}

  @spec drain_journal_run(String.t(), non_neg_integer()) ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, :timeout | term()}
  defp drain_journal_run(_run_id, 0), do: {:error, :timeout}

  defp drain_journal_run(run_id, attempts_remaining) when attempts_remaining > 0 do
    drain_journal_run(run_id, attempts_remaining, journal_run_execute_options())
  end

  defp drain_journal_run(_run_id, 0, _execute_options), do: {:error, :timeout}

  defp drain_journal_run(run_id, attempts_remaining, execute_options)
       when attempts_remaining > 0 and is_list(execute_options) do
    case Jizoku.inspect_run(run_id) do
      {:ok, %Jizoku.ReadModel.Inspection.Snapshot{terminal?: true} = run} ->
        {:ok, run}

      {:ok, %Jizoku.ReadModel.Inspection.Snapshot{}} ->
        case Jizoku.execute_next(execute_options) do
          {:ok, %Jizoku.ReadModel.Inspection.Snapshot{terminal?: true} = run} ->
            {:ok, run}

          {:ok, %Jizoku.ReadModel.Inspection.Snapshot{}} ->
            drain_journal_run(run_id, attempts_remaining - 1, execute_options)

          {:ok, :none} ->
            Process.sleep(50)
            drain_journal_run(run_id, attempts_remaining - 1, execute_options)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp journal_run_execute_options do
    [
      owner_id: "minimal-host-app-smoke"
    ]
  end

  defp with_smoke_telemetry(event, operation) when is_list(event) and is_function(operation, 0) do
    handler_id = {__MODULE__, make_ref()}
    receiver = self()

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn event, measurements, metadata, receiver ->
          send(receiver, {:minimal_host_app_telemetry, event, measurements, metadata})
        end,
        receiver
      )

    try do
      operation.()
    after
      :telemetry.detach(handler_id)
    end
  end

  defp await_smoke_telemetry(event, run_id) do
    receive do
      {:minimal_host_app_telemetry, ^event, %{count: 1}, %{run_id: ^run_id} = metadata} ->
        {:ok, metadata}

      {:minimal_host_app_telemetry, ^event, _measurements, _metadata} ->
        await_smoke_telemetry(event, run_id)
    after
      2_000 -> {:error, :missing_runtime_lifecycle_telemetry}
    end
  end

  defp ensure_journal_signal_trace(completed_run, queue, command_trace, lifecycle_metadata) do
    with {:ok, run_entries} <-
           Journal.load_entries(@journal_run_storage, {:run, completed_run.run_id}),
         {:ok, dispatch_entries} <- Journal.load_entries(@journal_run_storage, {:dispatch, queue}),
         %{data: %{trace: run_trace}} <- Enum.find(run_entries, &(&1.type == :run_started)),
         runnable_key when is_binary(runnable_key) <-
           Map.get(lifecycle_metadata, :runnable_key),
         runnable when is_map(runnable) <- journal_runnable(run_entries, runnable_key),
         runnable_trace when is_map(runnable_trace) <- Map.get(runnable, :trace),
         %{data: %{trace: attempt_trace}} <-
           Enum.find(dispatch_entries, fn entry ->
             entry.type == :attempt_completed and
               Map.get(entry.data, :run_id) == completed_run.run_id and
               Map.get(entry.data, :runnable_key) == runnable_key
           end),
         :ok <-
           validate_journal_signal_trace(
             completed_run,
             command_trace,
             run_trace,
             runnable_trace,
             attempt_trace,
             lifecycle_metadata
           ) do
      :ok
    else
      {:error, reason} -> {:error, reason}
      _missing -> {:error, :unexpected_journal_signal_trace}
    end
  end

  defp journal_runnable(entries, runnable_key) do
    Enum.find_value(entries, fn
      %{type: :runnables_planned, data: %{runnables: runnables}} when is_list(runnables) ->
        Enum.find(runnables, &(Map.get(&1, :runnable_key) == runnable_key))

      _entry ->
        nil
    end)
  end

  defp validate_journal_signal_trace(
         completed_run,
         command_trace,
         run_trace,
         runnable_trace,
         attempt_trace,
         lifecycle_metadata
       ) do
    owners = completed_run.attempts |> Enum.map(& &1.owner_id) |> Enum.uniq()

    cond do
      run_trace != command_trace ->
        {:error, :unexpected_journal_run_trace}

      runnable_trace.trace_id != run_trace.trace_id or
          runnable_trace.parent_span_id != run_trace.span_id ->
        {:error, :unexpected_journal_runnable_trace}

      attempt_trace != runnable_trace ->
        {:error, :unexpected_journal_attempt_trace}

      lifecycle_metadata.trace_id != attempt_trace.trace_id or
          lifecycle_metadata.span_id != attempt_trace.span_id ->
        {:error, :unexpected_journal_telemetry_trace}

      "minimal-host-app-signal-worker-a" not in owners or
          "minimal-host-app-signal-worker-b" not in owners ->
        {:error, :unexpected_journal_worker_handoff}

      true ->
        :ok
    end
  end

  defp schedule_dynamic_work!(%Jizoku.ReadModel.Inspection.Snapshot{} = inspected_run) do
    case dynamic_work_origin(inspected_run) do
      [runnable | _rest] -> schedule_dynamic_work_for_runnable(inspected_run, runnable)
      _missing -> {:error, :missing_dynamic_work_origin}
    end
  end

  defp schedule_dynamic_work_for_runnable(inspected_run, runnable) do
    with {:ok, _snapshot} <-
           WorkflowRuns.schedule_dynamic_work(
             inspected_run.run_id,
             dynamic_work_attrs(runnable),
             action_registry: dynamic_work_action_registry()
           ) do
      :ok
    end
  end

  defp preview_dynamic_work!(%Jizoku.ReadModel.Inspection.Snapshot{} = inspected_run) do
    case dynamic_work_origin(inspected_run) do
      [runnable | _rest] -> preview_dynamic_work_for_runnable(inspected_run, runnable)
      _missing -> {:error, :missing_dynamic_work_origin}
    end
  end

  defp preview_dynamic_work_for_runnable(inspected_run, runnable) do
    with {:ok, preview} <-
           Jizoku.preview_dynamic_work(
             inspected_run.run_id,
             dynamic_work_attrs(runnable),
             action_registry: dynamic_work_action_registry()
           ) do
      preview_payload = Jizoku.Runs.DynamicWorkPreview.to_map(preview)

      expected_node_id = "notify_invoice:inv_dynamic_demo"
      expected_edge_id = "#{runnable.step}:dynamic:#{expected_node_id}"

      if Map.fetch!(preview_payload, :duplicate?) or
           not Map.fetch!(preview_payload, :recordable?) or
           Map.fetch!(preview_payload, :origin_node_id) != runnable.step or
           Map.fetch!(preview_payload, :added_node_ids) != [expected_node_id] or
           Map.fetch!(preview_payload, :added_edge_ids) != [expected_edge_id] or
           not Enum.empty?(Map.fetch!(preview_payload, :warnings)) or
           not Enum.any?(preview_payload.graph.nodes, &(&1.id == expected_node_id)) do
        {:error, :unexpected_dynamic_work_preview}
      else
        :ok
      end
    end
  end

  defp dynamic_work_attrs(runnable) do
    %{
      dynamic_key: "dynamic_invoice_fanout",
      origin: %{
        runnable_key: Map.fetch!(runnable, :runnable_key),
        step: Map.fetch!(runnable, :step),
        attempt: Map.get(runnable, :attempt_number, 1)
      },
      reason: :host_fanout_preview,
      nodes: [
        %{
          id: "notify_invoice:inv_dynamic_demo",
          action: "payment.notify_customer",
          input: %{
            invoice: %{id: "inv_dynamic_demo"},
            gateway_check: %{status: "dynamic_fanout"}
          },
          metadata: %{invoice_id: "inv_dynamic_demo", channel: "email"}
        }
      ],
      metadata: %{source: "minimal_host_app_smoke"}
    }
  end

  defp dynamic_work_origin(%Jizoku.ReadModel.Inspection.Snapshot{attempts: attempts}) do
    Enum.filter(attempts, &Map.get(&1, :applied?))
  end

  defp dynamic_work_action_registry do
    %{
      "payment.notify_customer" => Steps.NotifyCustomer
    }
  end

  defp await_graph_mutation_origin(_run_id, 0) do
    {:error, :missing_graph_mutation_origin}
  end

  defp await_graph_mutation_origin(run_id, attempts_remaining) do
    with {:ok, %Jizoku.ReadModel.Inspection.Snapshot{} = run} <-
           Jizoku.execute_next(journal_run_execute_options()) do
      if Enum.any?(run.attempts, &(&1.step == "load_account" and &1.applied?)) do
        {:ok, run}
      else
        await_graph_mutation_origin(run_id, attempts_remaining - 1)
      end
    end
  end

  defp apply_smoke_graph_mutation(run_id, dynamic_queue) do
    failing_storage =
      {FaultInjectingStorage,
       delegate: @journal_run_storage,
       fail_append_thread_id: Journal.thread_id({:dispatch, dynamic_queue})}

    Jizoku.apply_graph_mutation(run_id, graph_mutation_attrs(dynamic_queue),
      runtime: :journal,
      journal_storage: failing_storage,
      limits: graph_mutation_limits(),
      action_registry: graph_mutation_action_registry()
    )
  end

  defp graph_mutation_attrs(dynamic_queue) do
    %{
      mutation_id: "minimal-host-graph-mutation",
      expected_version: 0,
      origin: "load_account",
      additions: [
        graph_mutation_node("dynamic-chain", dynamic_queue),
        graph_mutation_node("dynamic-parallel", dynamic_queue),
        graph_mutation_node("dynamic-join", dynamic_queue),
        %{kind: :edge, id: "load-account-chain", from: "load_account", to: "dynamic-chain"},
        %{
          kind: :edge,
          id: "load-account-parallel",
          from: "load_account",
          to: "dynamic-parallel"
        },
        %{kind: :edge, id: "chain-join", from: "dynamic-chain", to: "dynamic-join"},
        %{kind: :edge, id: "parallel-join", from: "dynamic-parallel", to: "dynamic-join"}
      ],
      removals: []
    }
  end

  defp graph_mutation_node(id, queue) do
    %{
      kind: :node,
      id: id,
      action: "digest.record_delivery",
      input: %{channel: "email", digest_date: "2026-07-18"},
      queue: queue
    }
  end

  defp graph_mutation_limits do
    %{
      max_nodes_per_mutation: 10,
      max_edges_per_mutation: 10,
      max_active_nodes_per_run: 10,
      max_active_edges_per_run: 10
    }
  end

  defp graph_mutation_action_registry do
    %{
      "digest.record_delivery" => Steps.RecordDigestDelivery
    }
  end

  defp ensure_pending_graph_mutation(run_id, report) do
    with :committed_needs_reconciliation <- report.status,
         :required <- report.reconciliation,
         {:ok, snapshot} <- Jizoku.inspect_run(run_id),
         {:ok, graph} <- Jizoku.inspect_run_graph(run_id) do
      payload = Jizoku.Runs.GraphInspection.to_map(graph)

      if snapshot.graph_version == 1 and
           snapshot.ready_node_ids == ["dynamic-chain", "dynamic-parallel"] and
           snapshot.blocked_node_ids == ["dynamic-join"] and
           snapshot.reconciliation_status == :required and
           payload.reconciliation_status == :required do
        :ok
      else
        {:error, :unexpected_pending_graph_mutation}
      end
    else
      _unexpected -> {:error, :unexpected_pending_graph_mutation}
    end
  end

  defp ensure_graph_reconciliation(reconciliation, dynamic_queue) do
    if reconciliation.status == :reconciled and
         reconciliation.repaired_queue_ids == [dynamic_queue] and
         reconciliation.scheduled_node_ids == ["dynamic-chain", "dynamic-parallel"] do
      :ok
    else
      {:error, :unexpected_graph_reconciliation}
    end
  end

  defp drain_graph_mutation_run(_run_id, _queues, 0) do
    {:error, :timeout}
  end

  defp drain_graph_mutation_run(run_id, queues, attempts_remaining) do
    case Jizoku.inspect_run(run_id) do
      {:ok, %Jizoku.ReadModel.Inspection.Snapshot{terminal?: true} = run} ->
        {:ok, run}

      {:ok, %Jizoku.ReadModel.Inspection.Snapshot{}} ->
        Enum.each(queues, fn queue ->
          _result =
            Jizoku.execute_next(
              owner_id: "minimal-host-app-graph-mutation-smoke",
              queue: queue
            )
        end)

        drain_graph_mutation_run(run_id, queues, attempts_remaining - 1)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_completed_graph_mutation(graph) do
    payload = Jizoku.Runs.GraphInspection.to_map(graph)

    if payload.terminal? and payload.graph_version == 1 and
         payload.active_node_ids == ["dynamic-chain", "dynamic-join", "dynamic-parallel"] and
         payload.ready_node_ids == [] and payload.blocked_node_ids == [] and
         payload.reconciliation_status == :completed and
         Enum.map(payload.mutation_history, & &1.mutation_id) == [
           "minimal-host-graph-mutation"
         ] do
      :ok
    else
      {:error, :unexpected_completed_graph_mutation}
    end
  end

  defp journal_run_queue do
    "#{@journal_run_queue_prefix}-#{System.unique_integer([:positive])}"
  end

  defp delete_journal_checkpoints(run_id, queue) when is_binary(run_id) and is_binary(queue) do
    [
      Journal.thread_id({:run, run_id}),
      Journal.thread_id({:dispatch, queue})
    ]
    |> Enum.each(fn thread_id ->
      {:ok, _checkpoint} =
        JournalStorage.get_checkpoint({"jizoku", :checkpoint, thread_id}, repo: Repo)

      :ok = JournalStorage.delete_checkpoint({"jizoku", :checkpoint, thread_id}, repo: Repo)
    end)

    :ok
  end

  defp delete_available_journal_checkpoints(run_id, queue)
       when is_binary(run_id) and is_binary(queue) do
    [
      Journal.thread_id({:run, run_id}),
      Journal.thread_id({:dispatch, queue})
    ]
    |> Enum.each(fn thread_id ->
      :ok = JournalStorage.delete_checkpoint({"jizoku", :checkpoint, thread_id}, repo: Repo)
    end)

    :ok
  end

  defp with_journal_runtime_config(queue, fun) when is_binary(queue) and is_function(fun, 0) do
    original_config = Application.get_all_env(:jizoku)

    try do
      Application.put_env(:jizoku, :runtime, :journal)
      Application.put_env(:jizoku, :read_model, :read_model)
      Application.put_env(:jizoku, :journal_storage, @journal_run_storage)
      Application.put_env(:jizoku, :queue, queue)

      fun.()
    after
      :jizoku
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:jizoku, &1))

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(:jizoku, key, value)
      end)
    end
  end

  @spec ensure_manual_approval_audit(Jizoku.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_manual_approval_audit}
  defp ensure_manual_approval_audit(%Jizoku.ReadModel.Inspection.Snapshot{
         context: %{approval: %{status: "approved", actor: "ops_smoke"}}
       }) do
    :ok
  end

  defp ensure_manual_approval_audit(%Jizoku.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_manual_approval_audit}

  @spec ensure_saga_failure_history(Jizoku.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_saga_compensation}
  defp ensure_saga_failure_history(%Jizoku.ReadModel.Inspection.Snapshot{attempts: attempts})
       when is_list(attempts) do
    expected_steps = [
      {"reserve_inventory", :completed, true, 1},
      {"authorize_payment", :completed, true, 1},
      {"capture_payment", :failed, false, 1},
      {"capture_payment", :failed, false, 2},
      {"compensate:authorize_payment", :completed, true, 1},
      {"compensate:reserve_inventory", :completed, true, 1}
    ]

    if Enum.map(attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) ==
         expected_steps do
      :ok
    else
      {:error, :unexpected_saga_compensation}
    end
  end

  defp ensure_saga_failure_history(%Jizoku.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_saga_compensation}

  defp ensure_nested_child_link(
         %Jizoku.ReadModel.Inspection.Snapshot{run_id: parent_run_id, child_runs: child_runs},
         child_key,
         child_queue
       ) do
    case child_runs do
      [%{child_key: ^child_key, child_run_id: child_run_id}] ->
        with {:ok, child_run} <- WorkflowRuns.inspect_run(child_run_id, queue: child_queue),
             {:ok, graph} <- Jizoku.inspect_run_graph(parent_run_id),
             :ok <- ensure_nested_graph_child_link(graph, child_run_id, child_key) do
          if child_run.status == :running do
            {:ok, child_run_id}
          else
            {:error, :unexpected_nested_child_status}
          end
        end

      _other ->
        {:error, :unexpected_nested_child_runs}
    end
  end

  defp ensure_nested_graph_child_link(graph, child_run_id, child_key) do
    graph_map = Jizoku.Runs.GraphInspection.to_map(graph)

    case Map.fetch!(graph_map, :child_links) do
      [
        %{
          from: "start_nested_invite",
          to: ^child_run_id,
          type: :child_run,
          status: :linked,
          child_key: ^child_key
        }
      ] ->
        :ok

      _other ->
        {:error, :unexpected_nested_graph_child_link}
    end
  end

  defp ensure_reconstructed_nested_runs(parent_run_id, child_run_id, child_runs, child_queue) do
    with {:ok, child_key} <- nested_child_key(child_runs),
         {:ok, parent_run} <- WorkflowRuns.inspect_run(parent_run_id),
         {:ok, child_run} <- WorkflowRuns.inspect_run(child_run_id, queue: child_queue),
         {:ok, graph} <- Jizoku.inspect_run_graph(parent_run_id),
         :ok <- ensure_nested_graph_child_link(graph, child_run_id, child_key) do
      cond do
        parent_run.child_runs != child_runs ->
          {:error, :unexpected_reconstructed_nested_child_runs}

        child_run.status != :running ->
          {:error, :unexpected_reconstructed_nested_child_status}

        child_run.parent_run.run_id != parent_run_id ->
          {:error, :unexpected_reconstructed_nested_parent_link}

        true ->
          :ok
      end
    end
  end

  defp nested_child_key([%{child_key: child_key}]) when is_binary(child_key), do: {:ok, child_key}
  defp nested_child_key(_child_runs), do: {:error, :unexpected_nested_child_runs}

  defp ensure_reconstructed_nested_child_retry(child_run_id, child_queue) do
    with {:ok, child_run} <- WorkflowRuns.inspect_run(child_run_id, queue: child_queue) do
      case child_run.visible_attempts do
        [%{step: "deliver_invite", status: :retry_scheduled, attempt_number: 2}] ->
          :ok

        _other ->
          {:error, :unexpected_reconstructed_nested_child_retry}
      end
    end
  end

  defp ensure_nested_parent_result(completed_parent, parent_history, child_queue) do
    cond do
      completed_parent.status != :completed ->
        {:error, :unexpected_nested_parent_status}

      completed_parent.context.invite_child.queue != child_queue ->
        {:error, :unexpected_nested_child_queue}

      completed_parent.context.invite_child.reused_after_retry? != true ->
        {:error, :unexpected_nested_retry_idempotency}

      Enum.map(parent_history.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) !=
          [
            {"start_nested_invite", :failed, false, 1},
            {"start_nested_invite", :completed, true, 2}
          ] ->
        {:error, :unexpected_nested_parent_retry_history}

      true ->
        :ok
    end
  end

  defp ensure_nested_child_result(child_retrying, child_history) do
    cond do
      child_retrying.status != :running ->
        {:error, :unexpected_nested_child_retry_status}

      child_history.status != :completed ->
        {:error, :unexpected_nested_child_status}

      Enum.map(child_history.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) != [
        {"deliver_invite", :failed, false, 1},
        {"deliver_invite", :completed, true, 2}
      ] ->
        {:error, :unexpected_nested_child_retry_history}

      true ->
        :ok
    end
  end

  @spec ensure_local_ledger_entries(Jizoku.ReadModel.Inspection.Snapshot.t(), [String.t()]) ::
          :ok | {:error, :unexpected_local_ledger_entries}
  defp ensure_local_ledger_entries(
         %Jizoku.ReadModel.Inspection.Snapshot{run_id: run_id},
         expected_entries
       ) do
    entries =
      Repo.all(
        from(entry in "local_ledger_entries",
          where: entry.run_id == ^run_id,
          order_by: [asc: entry.id],
          select: entry.entry
        )
      )

    if entries == expected_entries do
      :ok
    else
      {:error, :unexpected_local_ledger_entries}
    end
  end

  @spec latest_daily_digest_run([Jizoku.ReadModel.Listing.Summary.t()]) ::
          {:ok, Jizoku.ReadModel.Listing.Summary.t()} | {:error, :missing_daily_digest_run}
  defp latest_daily_digest_run(runs) when is_list(runs) do
    case Enum.max_by(runs, & &1.indexed_at) do
      %Jizoku.ReadModel.Listing.Summary{} = run -> {:ok, run}
      _other -> {:error, :missing_daily_digest_run}
    end
  rescue
    Enum.EmptyError -> {:error, :missing_daily_digest_run}
  end

  @spec await_daily_digest_run(MapSet.t(Ecto.UUID.t()), non_neg_integer()) ::
          {:ok, Jizoku.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp await_daily_digest_run(_existing_run_ids, 0), do: {:error, :missing_daily_digest_run}

  defp await_daily_digest_run(existing_run_ids, attempts_remaining) when attempts_remaining > 0 do
    :ok = wait_for_execution()

    case WorkflowRuns.list_daily_digest_runs() do
      {:ok, []} ->
        Process.sleep(50)
        await_daily_digest_run(existing_run_ids, attempts_remaining - 1)

      {:ok, runs} ->
        new_runs =
          Enum.reject(runs, fn run -> MapSet.member?(existing_run_ids, run.run_id) end)

        with {:ok, run} <- latest_daily_digest_run(new_runs) do
          RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)
        else
          {:error, :missing_daily_digest_run} ->
            Process.sleep(50)
            await_daily_digest_run(existing_run_ids, attempts_remaining - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp daily_digest_run_ids do
    case WorkflowRuns.list_daily_digest_runs() do
      {:ok, runs} -> MapSet.new(runs, & &1.run_id)
      {:error, _reason} -> MapSet.new()
    end
  end

  defp manual_oban_testing? do
    case Application.fetch_env(:minimal_host_app, Oban) do
      {:ok, config} -> Keyword.get(config, :testing) == :manual
      :error -> false
    end
  end

  defp reset_runtime_state! do
    Repo.delete_all("jizoku_journal_entries")
    Repo.delete_all("jizoku_journal_checkpoints")
    Repo.delete_all("jizoku_journal_threads")
    Repo.delete_all("local_ledger_entries")
    Repo.delete_all("oban_jobs")
    Repo.delete_all("oban_peers")
    :ok
  end
end
