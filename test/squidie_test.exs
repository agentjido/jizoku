defmodule SquidieTest do
  use Squidie.DataCase, async: false

  import ExUnit.CaptureLog

  alias Squidie.Executor.Payload
  alias Squidie.ReadModel.Explanation.Diagnostic
  alias Squidie.ReadModel.Inspection.Snapshot
  alias Squidie.ReadModel.Listing.Summary
  alias Squidie.Runtime.DispatchAgent
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.Journal
  alias Squidie.Runtime.Journal.Executor
  alias Squidie.Runtime.Journal.SignalInterpreter
  alias Squidie.Runtime.Runner
  alias Squidie.Runtime.Signal
  alias Squidie.Workflow.Definition

  defp execute_journal_next(opts), do: Executor.execute_next(opts)

  @spec record_saga_compensation(String.t(), atom()) :: :ok
  def record_saga_compensation(order_id, compensation) do
    key = saga_compensation_log_key(order_id)
    :persistent_term.put(key, [compensation | :persistent_term.get(key, [])])
  end

  @spec saga_compensation_log_key(String.t()) :: {atom(), String.t()}
  def saga_compensation_log_key(order_id), do: {:squidie_test_saga_compensations, order_id}

  defmodule CommitThenFailStorage do
    @behaviour Jido.Storage

    @impl Jido.Storage
    def get_checkpoint(_key, _opts), do: :not_found

    @impl Jido.Storage
    def put_checkpoint(_key, _data, _opts), do: :ok

    @impl Jido.Storage
    def delete_checkpoint(_key, _opts), do: :ok

    @impl Jido.Storage
    def load_thread("squidie:dispatch:" <> _queue, _opts), do: :not_found
    def load_thread(_thread_id, _opts), do: {:error, :load_failed}

    @impl Jido.Storage
    def append_thread(thread_id, entries, _opts) do
      thread =
        [id: thread_id]
        |> Jido.Thread.new()
        |> Jido.Thread.append(entries)

      {:ok, thread}
    end

    @impl Jido.Storage
    def delete_thread(_thread_id, _opts), do: :ok
  end

  defmodule FaultInjectingStorage do
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
      if thread_id == Keyword.get(opts, :fail_load_thread_id) do
        {:error, :load_failed}
      else
        {adapter, delegate_opts} = delegate(opts)
        adapter.load_thread(thread_id, delegate_opts)
      end
    end

    @impl Jido.Storage
    def append_thread(thread_id, entries, opts) do
      cond do
        thread_id == Keyword.get(opts, :conflict_thread_id) and
            Keyword.get(opts, :commit_before_conflict?) ->
          {adapter, delegate_opts} = delegate(opts)

          case adapter.append_thread(
                 thread_id,
                 entries,
                 Keyword.merge(delegate_opts, append_opts(opts))
               ) do
            {:ok, _thread} -> {:error, :conflict}
            {:error, _reason} = error -> error
          end

        thread_id == Keyword.get(opts, :conflict_thread_id) ->
          {:error, :conflict}

        thread_id == Keyword.get(opts, :fail_append_thread_id) ->
          {:error, :append_failed}

        true ->
          {adapter, delegate_opts} = delegate(opts)

          adapter.append_thread(
            thread_id,
            entries,
            Keyword.merge(delegate_opts, append_opts(opts))
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

    defp append_opts(opts), do: Keyword.take(opts, [:expected_rev])
  end

  defmodule InvoiceReminderWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_invoice, InvoiceReminderWorkflow.LoadInvoice
      step :send_email, InvoiceReminderWorkflow.SendEmail, retry: [max_attempts: 3]

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule PaymentRecoveryWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :gateway_recovery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, PaymentRecoveryWorkflow.CheckGateway, retry: [max_attempts: 2]
      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule DeferredContinuationWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :checkout do
        manual()

        payload do
          field :order_id, :string
        end
      end

      step :check_gateway, DeferredContinuationWorkflow.CheckGateway,
        retry: [max_attempts: 2],
        deadline: [within: 60_000, due_soon: 20_000, escalation: :diagnostic]

      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule DeferredContinuationWorkflow.CheckGateway do
    use Squidie.Step,
      name: "deferred_gateway_check",
      input_schema: [order_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(%{order_id: order_id}, context) do
      key = {__MODULE__, context.run_id}
      calls = :persistent_term.get(key, 0)
      :persistent_term.put(key, calls + 1)

      if calls == 0 do
        {:defer, %{code: "gateway_pending", order_id: order_id}, schedule_in: 30}
      else
        {:ok, %{gateway: %{status: "ready", calls: calls + 1}}}
      end
    end
  end

  defmodule DeferredDependencyWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :checkout do
        manual()

        payload do
          field :order_id, :string
          field :invoice_id, :string
        end
      end

      step :check_gateway, DeferredDependencyWorkflow.CheckGateway
      step :load_invoice, DeferredDependencyWorkflow.LoadInvoice

      step :send_receipt, DeferredDependencyWorkflow.SendReceipt,
        after: [:check_gateway, :load_invoice]
    end
  end

  defmodule DeferredDependencyWorkflow.CheckGateway do
    use Squidie.Step,
      name: "deferred_dependency_gateway_check",
      input_schema: [order_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(%{order_id: order_id}, context) do
      key = {__MODULE__, context.run_id}
      calls = :persistent_term.get(key, 0)
      :persistent_term.put(key, calls + 1)

      if calls == 0 do
        {:defer, %{code: "gateway_pending", order_id: order_id}, schedule_in: 30}
      else
        {:ok, %{gateway: %{order_id: order_id, status: "ready"}}}
      end
    end
  end

  defmodule DeferredDependencyWorkflow.LoadInvoice do
    use Squidie.Step,
      name: "deferred_dependency_load_invoice",
      input_schema: [invoice_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(%{invoice_id: invoice_id}, _context) do
      {:ok, %{invoice: %{id: invoice_id, status: "open"}}}
    end
  end

  defmodule DeferredDependencyWorkflow.SendReceipt do
    use Squidie.Step,
      name: "deferred_dependency_send_receipt",
      input_schema: [
        gateway: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Squidie.Step
    def run(%{gateway: gateway, invoice: invoice}, _context) do
      {:ok,
       %{
         receipt: %{
           order_id: gateway.order_id,
           invoice_id: invoice.id,
           gateway_status: gateway.status
         }
       }}
    end
  end

  defmodule DeferredDependencyFailureWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :checkout do
        manual()

        payload do
          field :order_id, :string
          field :invoice_id, :string
        end
      end

      step :check_gateway, DeferredDependencyWorkflow.CheckGateway
      step :load_invoice, DeferredDependencyFailureWorkflow.LoadInvoice

      step :send_receipt, DeferredDependencyWorkflow.SendReceipt,
        after: [:check_gateway, :load_invoice]
    end
  end

  defmodule DeferredDependencyFailureWorkflow.LoadInvoice do
    use Squidie.Step,
      name: "deferred_dependency_fail_invoice",
      input_schema: [invoice_id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(%{invoice_id: invoice_id}, _context) do
      {:error,
       %{
         code: "invoice_unavailable",
         message: "invoice unavailable",
         retryable?: false,
         invoice_id: invoice_id
       }}
    end
  end

  defmodule BillingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :billing do
        manual()

        payload do
          field :payment_id, :string
        end
      end

      step :charge_card, BillingWorkflow.ChargeCard
      step :send_receipt, BillingWorkflow.SendReceipt

      transition :charge_card, on: :ok, to: :send_receipt
      transition :send_receipt, on: :ok, to: :complete
    end
  end

  defmodule VersionedPaymentRecoveryWorkflow do
    use Squidie.Workflow

    workflow do
      version "2026-05-26.payment-recovery-v2"

      trigger :gateway_recovery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :check_gateway, PaymentRecoveryWorkflow.CheckGateway, retry: [max_attempts: 2]
      transition :check_gateway, on: :ok, to: :complete
    end
  end

  defmodule ChildDigestWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :deliver_digest do
        manual()

        payload do
          field :subscription_id, :string
        end
      end

      step :deliver_digest, ChildDigestWorkflow.DeliverDigest
      transition :deliver_digest, on: :ok, to: :complete
    end
  end

  defmodule RepoTransactionWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :repo_transaction do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :record_event, RepoTransactionWorkflow.RecordEvent, transaction: :repo
      transition :record_event, on: :ok, to: :complete
    end
  end

  defmodule RepoTransactionWorkflow.RecordEvent do
    use Squidie.Step,
      name: :record_event,
      description: "Records one transactional event",
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: [event: [type: :string, required: true]]

    @impl Squidie.Step
    def run(%{account_id: account_id}, %Squidie.Step.Context{run_id: run_id}) do
      now = NaiveDateTime.utc_now(:second)

      Squidie.Test.Repo.insert_all("transactional_events", [
        %{
          run_id: Ecto.UUID.dump!(run_id),
          account_id: account_id,
          event: "recorded",
          inserted_at: now,
          updated_at: now
        }
      ])

      {:ok, %{event: "recorded"}}
    end
  end

  defmodule ChildDigestWorkflow.DeliverDigest do
    use Squidie.Step,
      name: :deliver_digest,
      input_schema: [subscription_id: [type: :string, required: true]],
      output_schema: [delivered: [type: :map, required: true]]

    @impl Squidie.Step
    def run(%{subscription_id: subscription_id}, _context) do
      {:ok, %{delivered: %{subscription_id: subscription_id}}}
    end
  end

  defmodule ChildDigestWorkflow.FlakyDeliverDigest do
    use Squidie.Step,
      name: :flaky_deliver_digest,
      input_schema: [subscription_id: [type: :string, required: true]],
      output_schema: [delivered: [type: :map, required: true]]

    @impl Squidie.Step
    def run(%{subscription_id: subscription_id}, %Squidie.Step.Context{attempt: 1}) do
      {:retry, %{message: "temporary digest failure", subscription_id: subscription_id}}
    end

    def run(%{subscription_id: subscription_id}, %Squidie.Step.Context{attempt: 2}) do
      {:ok, %{delivered: %{subscription_id: subscription_id}}}
    end
  end

  defmodule DynamicElixirAdapters do
    @spec deliver(map(), Squidie.Step.Context.t()) :: {:ok, map()}
    def deliver(params, _context), do: {:ok, %{delivered: params}}
  end

  defmodule JournalConditionalWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :conditional_route do
        manual()

        payload do
          field :account_id, :string
          field :decision, :string
        end
      end

      step :classify, JournalConditionalWorkflow.Classify
      step :auto_approve, JournalConditionalWorkflow.AutoApprove
      step :manual_review, JournalConditionalWorkflow.ManualReview

      transition :classify,
        on: :ok,
        to: :auto_approve,
        condition: [path: [:routing, :decision], equals: "auto"]

      transition :classify, on: :ok, to: :manual_review
      transition :auto_approve, on: :ok, to: :complete
      transition :manual_review, on: :ok, to: :complete
    end
  end

  defmodule JournalAccumulatedConditionalWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :accumulated_conditional_route do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_profile, JournalAccumulatedConditionalWorkflow.LoadProfile

      step :classify, JournalAccumulatedConditionalWorkflow.Classify,
        input: [account_id: [:account_id]]

      step :auto_approve, JournalAccumulatedConditionalWorkflow.AutoApprove
      step :manual_review, JournalAccumulatedConditionalWorkflow.ManualReview

      transition :load_profile, on: :ok, to: :classify

      transition :classify,
        on: :ok,
        to: :auto_approve,
        condition: [path: [:profile, :tier], equals: "trusted"]

      transition :classify, on: :ok, to: :manual_review
      transition :auto_approve, on: :ok, to: :complete
      transition :manual_review, on: :ok, to: :complete
    end
  end

  defmodule JournalFailureWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_failure do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalFailureWorkflow.FailGateway
      transition :fail_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalErrorTransitionWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_error_transition do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalErrorTransitionWorkflow.FailGateway
      step :notify_failure, JournalErrorTransitionWorkflow.NotifyFailure

      transition :fail_gateway, on: :ok, to: :complete
      transition :fail_gateway, on: :error, to: :notify_failure
      transition :notify_failure, on: :ok, to: :complete
    end
  end

  defmodule JournalConditionalErrorCompleteWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_error_complete do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :fail_gateway, JournalConditionalErrorCompleteWorkflow.FailGateway

      transition :fail_gateway, on: :ok, to: :complete

      transition :fail_gateway,
        on: :error,
        to: :complete,
        condition: [path: [:account_id], equals: "acct_123"]
    end
  end

  defmodule JournalRetryWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_retry do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :retry_gateway, JournalRetryWorkflow.RetryGateway,
        retry: [max_attempts: 2],
        deadline: [within: 60_000, due_soon: 20_000, escalation: :diagnostic]

      transition :retry_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalRetryThenCompleteWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_retry_then_complete do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :retry_gateway, JournalRetryThenCompleteWorkflow.RetryGateway, retry: [max_attempts: 2]
      transition :retry_gateway, on: :ok, to: :complete
    end
  end

  defmodule JournalSecretFailureWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_secret_failure do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :leak_secret, JournalSecretFailureWorkflow.LeakSecret
      transition :leak_secret, on: :ok, to: :complete
    end
  end

  defmodule JournalExceptionWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_exception do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :decode_payload, JournalExceptionWorkflow.DecodePayload
      transition :decode_payload, on: :ok, to: :complete
    end
  end

  defmodule JournalConflictWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_conflict do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :write_conflict, JournalConflictWorkflow.WriteConflict
      transition :write_conflict, on: :ok, to: :complete
    end
  end

  defmodule JournalDependencyWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_dependency do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyWorkflow.LoadAccount
      step :load_invoice, JournalDependencyWorkflow.LoadInvoice
      step :send_email, JournalDependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule JournalDependencyWaitWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_dependency_wait do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyWorkflow.LoadAccount
      step :wait_for_settlement, :wait, duration: 2_000, after: [:load_account]
      step :load_invoice, JournalDependencyWorkflow.LoadInvoice

      step :send_email, JournalDependencyWorkflow.SendEmail,
        after: [:wait_for_settlement, :load_invoice]
    end
  end

  defmodule JournalRootWaitWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_root_wait do
        manual()

        payload do
          field :invoice_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 2_000
      step :z_load_invoice, JournalDependencyWorkflow.LoadInvoice

      step :send_email, JournalRootWaitWorkflow.SendEmail,
        after: [:wait_for_settlement, :z_load_invoice]
    end
  end

  defmodule JournalDependencyFailureWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_dependency_failure do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, JournalDependencyFailureWorkflow.LoadAccount
      step :load_invoice, JournalDependencyFailureWorkflow.LoadInvoice

      step :send_email, JournalDependencyFailureWorkflow.SendEmail,
        after: [:load_account, :load_invoice]
    end
  end

  defmodule JournalMissingPathWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_missing_path do
        manual()

        payload do
          field :draft, :map
        end
      end

      step :load_review_context, JournalMissingPathWorkflow.LoadReviewContext

      step :record_review, JournalMissingPathWorkflow.RecordReview,
        input: [drafts: [:draft, :drafts]]

      transition :load_review_context, on: :ok, to: :record_review
      transition :record_review, on: :ok, to: :complete
    end
  end

  defmodule ReorderedWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :send_email, ReorderedWorkflow.SendEmail
      step :load_invoice, ReorderedWorkflow.LoadInvoice

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule IrreversibleWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :payment_capture do
        manual()

        payload do
          field(:account_id, :string)
        end
      end

      step(:load_account, IrreversibleWorkflow.LoadAccount)
      step(:capture_payment, IrreversibleWorkflow.CapturePayment, irreversible: true)

      transition(:load_account, on: :ok, to: :capture_payment)
      transition(:capture_payment, on: :ok, to: :complete)
    end
  end

  defmodule InvoiceReminderWorkflow.LoadInvoice do
    use Jido.Action,
      name: "load_invoice",
      description: "Loads invoice details",
      schema: [
        account_id: [type: :string, required: true],
        invoice_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id, invoice_id: invoice_id}, _context) do
      {:ok,
       %{
         account: %{id: account_id},
         invoice: %{id: invoice_id, status: "open"}
       }}
    end
  end

  defmodule InvoiceReminderWorkflow.SendEmail do
    use Jido.Action,
      name: "send_email",
      description: "Sends an invoice reminder email",
      schema: [
        account: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account, invoice: invoice}, _context) do
      {:ok,
       %{
         delivery: %{
           account_id: account.id,
           invoice_id: invoice.id,
           channel: "email"
         }
       }}
    end
  end

  defmodule PaymentRecoveryWorkflow.CheckGateway do
    use Jido.Action,
      name: "check_gateway",
      description: "Checks payment gateway status",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      if hook = :persistent_term.get(:journal_gateway_run_hook, nil) do
        hook.()
      end

      {:ok, %{gateway_check: %{account_id: account_id, status: "healthy"}}}
    end
  end

  defmodule BillingWorkflow.ChargeCard do
    use Jido.Action,
      name: "charge_card",
      description: "Charges a stored card",
      schema: [
        payment_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{payment_id: payment_id}, _context) do
      {:ok, %{payment: %{id: payment_id, status: "charged"}}}
    end
  end

  defmodule BillingWorkflow.SendReceipt do
    use Jido.Action,
      name: "send_receipt",
      description: "Sends a payment receipt",
      schema: [
        payment: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{payment: payment}, _context) do
      {:ok, %{receipt: %{payment_id: payment.id, status: "sent"}}}
    end
  end

  defmodule JournalConditionalWorkflow.Classify do
    use Jido.Action,
      name: "classify",
      description: "Classifies a conditional journal route",
      schema: [
        account_id: [type: :string, required: true],
        decision: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{decision: decision}, _context) do
      {:ok, %{routing: %{decision: decision}}}
    end
  end

  defmodule JournalConditionalWorkflow.AutoApprove do
    use Jido.Action,
      name: "auto_approve",
      description: "Records automatic approval",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "auto"}}}
  end

  defmodule JournalConditionalWorkflow.ManualReview do
    use Jido.Action,
      name: "manual_review",
      description: "Records manual review",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "manual"}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.LoadProfile do
    use Jido.Action,
      name: "load_profile",
      description: "Loads account profile data used by a later branch",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{profile: %{account_id: account_id, tier: "trusted"}}}
    end
  end

  defmodule JournalAccumulatedConditionalWorkflow.Classify do
    use Jido.Action,
      name: "classify_accumulated",
      description: "Returns no profile data so routing must use accumulated context",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{routing: %{checked?: true}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.AutoApprove do
    use Jido.Action,
      name: "auto_approve_accumulated",
      description: "Records automatic approval",
      schema: [profile: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "auto"}}}
  end

  defmodule JournalAccumulatedConditionalWorkflow.ManualReview do
    use Jido.Action,
      name: "manual_review_accumulated",
      description: "Records manual review",
      schema: [routing: [type: :map, required: true]]

    @impl Jido.Action
    def run(_input, _context), do: {:ok, %{approval: %{mode: "manual"}}}
  end

  defmodule JournalFailureWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway",
      description: "Fails payment gateway status checks",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalErrorTransitionWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway_for_error_transition",
      description: "Fails nonretryably so the workflow follows its error transition",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, context) do
      if hook = :persistent_term.get(:journal_error_transition_conflict_hook, nil) do
        hook.(context)
      end

      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalConditionalErrorCompleteWorkflow.FailGateway do
    use Jido.Action,
      name: "fail_gateway",
      description: "Fails and routes to terminal completion",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_params, context) do
      if hook = :persistent_term.get(:journal_conditional_error_complete_conflict_hook, nil) do
        hook.(context)
      end

      {:error, %{message: "gateway timeout", code: "gateway_timeout", retryable?: false}}
    end
  end

  defmodule JournalErrorTransitionWorkflow.NotifyFailure do
    use Jido.Action,
      name: "notify_failure_for_error_transition",
      description: "Records that the error transition ran",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:ok, %{failure_notification: %{channel: "email"}}}
    end
  end

  defmodule JournalRetryWorkflow.RetryGateway do
    use Jido.Action,
      name: "retry_gateway",
      description: "Fails retryably",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, context) do
      if hook = :persistent_term.get(:journal_retry_failure_conflict_hook, nil) do
        hook.(context)
      end

      {:error,
       %{
         code: "gateway_timeout",
         message: "gateway timeout",
         retryable?: true,
         account_id: account_id
       }}
    end
  end

  defmodule JournalRetryThenCompleteWorkflow.RetryGateway do
    use Squidie.Step,
      name: :retry_gateway_then_complete,
      description: "Fails once before completing",
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        gateway: [type: :string, required: true]
      ]

    @impl Squidie.Step
    def run(%{account_id: _account_id}, %Squidie.Step.Context{attempt: 1}) do
      {:retry, %{message: "gateway timeout"}}
    end

    def run(%{account_id: _account_id}, %Squidie.Step.Context{attempt: 2}) do
      {:ok, %{gateway: "ok"}}
    end
  end

  defmodule JournalCompensationWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :checkout do
        manual()

        payload do
          field :order_id, :string
        end
      end

      step :reserve_inventory, JournalCompensationWorkflow.ReserveInventory,
        compensate: JournalCompensationWorkflow.ReleaseInventory

      step :capture_payment, JournalCompensationWorkflow.CapturePayment

      transition :reserve_inventory, on: :ok, to: :capture_payment
      transition :capture_payment, on: :ok, to: :complete
    end
  end

  defmodule JournalCompensationWorkflow.ReserveInventory do
    use Squidie.Step,
      name: :reserve_inventory,
      description: "Reserves inventory for a checkout"

    @impl Squidie.Step
    def run(%{order_id: order_id}, _context) do
      {:ok, %{inventory_reservation: %{order_id: order_id, status: "reserved"}}}
    end
  end

  defmodule JournalCompensationWorkflow.ReleaseInventory do
    use Squidie.Step,
      name: :release_inventory,
      description: "Releases a completed inventory reservation"

    @impl Squidie.Step
    def run(%{step: %{output: %{inventory_reservation: reservation}}}, _context) do
      {:ok, %{released_inventory: Map.put(reservation, :status, "released")}}
    end
  end

  defmodule JournalCompensationWorkflow.CapturePayment do
    use Squidie.Step,
      name: :capture_payment,
      description: "Captures payment for a checkout"

    @impl Squidie.Step
    def run(_input, _context) do
      {:error, %{message: "capture declined", code: "capture_declined"}}
    end
  end

  defmodule JournalSagaRollbackWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :checkout do
        manual()

        payload do
          field :order_id, :string
        end
      end

      step :reserve_inventory, JournalSagaRollbackWorkflow.ReserveInventory,
        compensate: JournalSagaRollbackWorkflow.ReleaseInventory

      step :authorize_payment, JournalSagaRollbackWorkflow.AuthorizePayment,
        compensate: JournalSagaRollbackWorkflow.VoidPaymentAuthorization

      step :capture_payment, JournalSagaRollbackWorkflow.CapturePayment

      transition :reserve_inventory, on: :ok, to: :authorize_payment
      transition :authorize_payment, on: :ok, to: :capture_payment
      transition :capture_payment, on: :ok, to: :complete
    end
  end

  defmodule JournalSagaRollbackWorkflow.ReserveInventory do
    use Squidie.Step,
      name: :reserve_inventory,
      description: "Reserves inventory for a saga rollback test"

    @impl Squidie.Step
    def run(%{order_id: order_id}, _context) do
      {:ok, %{inventory_reservation: %{order_id: order_id, status: "reserved"}}}
    end
  end

  defmodule JournalSagaRollbackWorkflow.AuthorizePayment do
    use Squidie.Step,
      name: :authorize_payment,
      description: "Authorizes payment for a saga rollback test"

    @impl Squidie.Step
    def run(%{order_id: order_id}, _context) do
      {:ok, %{payment_authorization: %{order_id: order_id, status: "authorized"}}}
    end
  end

  defmodule JournalSagaRollbackWorkflow.ReleaseInventory do
    use Squidie.Step,
      name: :release_inventory,
      description: "Releases inventory during saga rollback"

    @impl Squidie.Step
    def run(
          %{step: %{output: %{inventory_reservation: %{order_id: order_id} = reservation}}},
          _context
        ) do
      SquidieTest.record_saga_compensation(order_id, :release_inventory)
      {:ok, %{released_inventory: Map.put(reservation, :status, "released")}}
    end
  end

  defmodule JournalSagaRollbackWorkflow.VoidPaymentAuthorization do
    use Squidie.Step,
      name: :void_payment_authorization,
      description: "Voids payment authorization during saga rollback"

    @impl Squidie.Step
    def run(
          %{step: %{output: %{payment_authorization: %{order_id: order_id} = authorization}}},
          _context
        ) do
      SquidieTest.record_saga_compensation(order_id, :void_payment_authorization)
      {:ok, %{voided_payment_authorization: Map.put(authorization, :status, "voided")}}
    end
  end

  defmodule JournalSagaRollbackWorkflow.CapturePayment do
    use Squidie.Step,
      name: :capture_payment,
      description: "Fails after compensatable steps complete"

    @impl Squidie.Step
    def run(_input, _context) do
      {:error, %{message: "capture declined", code: "capture_declined", retryable?: false}}
    end
  end

  defmodule JournalSecretFailureWorkflow.LeakSecret do
    use Jido.Action,
      name: "leak_secret",
      description: "Returns a secret-bearing error",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(_params, _context) do
      {:error,
       %{
         code: "token=super-secret-token",
         message: "token=super-secret-token",
         retryable?: false,
         validation_errors: %{authorization: "Bearer super-secret-token"}
       }}
    end
  end

  defmodule JournalExceptionWorkflow.DecodePayload do
    use Squidie.Step,
      name: :decode_payload,
      description: "Raises a third-party exception"

    @impl Squidie.Step
    def run(_params, _context) do
      Jason.decode!("[secret-token")
    end
  end

  defmodule JournalConflictWorkflow.WriteConflict do
    use Jido.Action,
      name: "write_conflict",
      description: "Runs a test hook before returning",
      schema: [
        account_id: [type: :string, required: true]
      ]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      if hook = :persistent_term.get(:journal_run_conflict_hook, nil) do
        hook.()
      end

      {:ok, %{conflict_probe: %{account_id: account_id, status: "written"}}}
    end
  end

  defmodule JournalDependencyWorkflow.LoadAccount do
    use Jido.Action,
      name: "journal_load_account",
      description: "Loads account for journal dependency workflow",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule JournalDependencyWorkflow.LoadInvoice do
    use Jido.Action,
      name: "journal_load_invoice",
      description: "Loads invoice for journal dependency workflow",
      schema: [invoice_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{invoice_id: invoice_id}, _context) do
      if hook = :persistent_term.get(:journal_dependency_invoice_hook, nil) do
        hook.()
      end

      {:ok, %{invoice: %{id: invoice_id, status: "open"}}}
    end
  end

  defmodule JournalDependencyWorkflow.SendEmail do
    use Jido.Action,
      name: "journal_send_dependency_email",
      description: "Sends dependency email",
      schema: [
        account: [type: :map, required: true],
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{account: account, invoice: invoice}, _context) do
      {:ok,
       %{
         delivery: %{
           account_id: account.id,
           invoice_id: invoice.id,
           channel: "email"
         }
       }}
    end
  end

  defmodule JournalRootWaitWorkflow.SendEmail do
    use Jido.Action,
      name: "journal_send_root_wait_email",
      description: "Sends dependency email after a root wait",
      schema: [
        invoice: [type: :map, required: true]
      ]

    @impl Jido.Action
    def run(%{invoice: invoice}, _context) do
      {:ok, %{delivery: %{invoice_id: invoice.id, channel: "email"}}}
    end
  end

  defmodule JournalDependencyFailureWorkflow.LoadAccount do
    use Jido.Action,
      name: "journal_dependency_fail_account",
      description: "Fails account loading for journal dependency workflow",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:error,
       %{
         code: "account_unavailable",
         message: "account unavailable",
         retryable?: false,
         account_id: account_id
       }}
    end
  end

  defmodule JournalDependencyFailureWorkflow.LoadInvoice do
    defdelegate run(params, context), to: JournalDependencyWorkflow.LoadInvoice
  end

  defmodule JournalDependencyFailureWorkflow.SendEmail do
    defdelegate run(params, context), to: JournalDependencyWorkflow.SendEmail
  end

  defmodule JournalMissingPathWorkflow.LoadReviewContext do
    use Jido.Action,
      name: "journal_missing_path_load_context",
      description: "Returns a partial nested context",
      schema: [draft: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{draft: draft}, _context), do: {:ok, %{draft: draft}}
  end

  defmodule JournalMissingPathWorkflow.RecordReview do
    use Jido.Action,
      name: "journal_missing_path_record_review",
      description: "Should not execute when successor mapped input is missing",
      schema: [drafts: [type: {:list, :map}, required: true]]

    @impl Jido.Action
    def run(_params, _context) do
      raise "record_review should not execute when successor mapped input is missing"
    end
  end

  defmodule ReorderedWorkflow.LoadInvoice do
    defdelegate run(params, context), to: InvoiceReminderWorkflow.LoadInvoice
  end

  defmodule IrreversibleWorkflow.LoadAccount do
    use Jido.Action,
      name: "load_account",
      description: "Loads account details",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(%{account_id: account_id}, _context) do
      {:ok, %{account: %{id: account_id}}}
    end
  end

  defmodule IrreversibleWorkflow.CapturePayment do
    use Jido.Action,
      name: "capture_payment",
      description: "Captures a payment",
      schema: [account: [type: :map, required: true]]

    @impl Jido.Action
    def run(%{account: account}, _context) do
      {:ok, %{payment: %{account_id: account.id, status: "captured"}}}
    end
  end

  defmodule ReorderedWorkflow.SendEmail do
    defdelegate run(params, context), to: InvoiceReminderWorkflow.SendEmail
  end

  defmodule WorkflowWithPayloadDefaults do
    use Squidie.Workflow

    workflow do
      trigger :invoice_delivery do
        manual()

        payload do
          field :team_id, :string, default: "backend"
          field :prompt_date, :string, default: {:today, :iso8601}
          field :invoice_id, :string
        end
      end

      step :deliver_invoice, WorkflowWithPayloadDefaults.DeliverInvoice
      transition :deliver_invoice, on: :ok, to: :complete
    end
  end

  defmodule DailyStandupWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :daily_standup do
        cron "@reboot", timezone: "Etc/UTC"

        payload do
          field :team_id, :string, default: "backend"
          field :prompt_date, :string, default: {:today, :iso8601}
        end
      end

      step :announce_prompt, :log, message: "posting daily standup"
      transition :announce_prompt, on: :ok, to: :complete
    end
  end

  defmodule ManualAndScheduledDigestWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual_digest do
        manual()

        payload do
          field :chat_id, :integer
        end
      end

      trigger :scheduled_digest do
        cron "@reboot", timezone: "Etc/UTC"

        payload do
          field :window_start_at, :string, default: {:today, :iso8601}
        end
      end

      step :announce_prompt, :log, message: "posting digest", level: :warning
      transition :announce_prompt, on: :ok, to: :complete
    end
  end

  defmodule ScheduledContextWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC"
      end

      step :capture_schedule, ScheduledContextWorkflow.CaptureSchedule
      transition :capture_schedule, on: :ok, to: :complete
    end
  end

  defmodule IdempotentScheduledContextWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC", idempotency: :return_existing_run

        payload do
          field :signal_id, :string, required: false
          field :intended_window, :map, required: false
        end
      end

      step :capture_schedule, ScheduledContextWorkflow.CaptureSchedule
      transition :capture_schedule, on: :ok, to: :complete
    end
  end

  defmodule SkipDuplicateScheduleClobberWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC", idempotency: :skip_duplicate
      end

      step :clobber_schedule, SkipDuplicateScheduleClobberWorkflow.ClobberSchedule
      transition :clobber_schedule, on: :ok, to: :complete
    end
  end

  defmodule SkipDuplicateScheduleClobberWorkflow.ClobberSchedule do
    use Jido.Action,
      name: "clobber_schedule",
      description: "Returns an accidental reserved schedule output",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:ok, %{schedule: %{idempotency: :return_existing_run}, digest_delivery: %{ok: true}}}
    end
  end

  defmodule AnotherScheduledContextWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :scheduled_capture do
        cron "@hourly", timezone: "Etc/UTC"
      end

      step :capture_schedule, ScheduledContextWorkflow.CaptureSchedule
      transition :capture_schedule, on: :ok, to: :complete
    end
  end

  defmodule ScheduledContextWorkflow.CaptureSchedule do
    use Squidie.Step,
      name: :capture_schedule,
      output_schema: [schedule_seen: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, context) do
      {:ok, %{schedule_seen: Map.fetch!(context.state, :schedule)}}
    end
  end

  defmodule NativeAttemptContextWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :capture_attempt_context do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :capture_context, NativeAttemptContextWorkflow.CaptureContext
      transition :capture_context, on: :ok, to: :complete
    end
  end

  defmodule NativeAttemptContextWorkflow.CaptureContext do
    use Squidie.Step,
      name: :capture_context,
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: [captured: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, context) do
      {:ok,
       %{
         captured: %{
           idempotency_key: context.idempotency_key,
           claim_id: context.claim_id,
           claim_token_present?: Map.has_key?(Map.from_struct(context), :claim_token)
         }
       }}
    end
  end

  defmodule RawActionAttemptContextWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :capture_attempt_context do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :capture_context, RawActionAttemptContextWorkflow.CaptureContext
      transition :capture_context, on: :ok, to: :complete
    end
  end

  defmodule RawActionAttemptContextWorkflow.CaptureContext do
    use Jido.Action,
      name: "capture_raw_action_attempt_context",
      description: "Captures raw action attempt context",
      schema: [account_id: [type: :string, required: true]]

    @impl Jido.Action
    def run(_input, context) do
      {:ok,
       %{
         captured: %{
           idempotency_key: Map.fetch!(context, :idempotency_key),
           claim_id: Map.fetch!(context, :claim_id),
           claim_token_present?: Map.has_key?(context, :claim_token)
         }
       }}
    end
  end

  defmodule PauseWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_approval, :pause

      step :record_delivery, :log,
        message: "delivery recorded",
        level: :info,
        input: [account_id: [:account_id]]

      transition :wait_for_approval, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule WaitWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :wait_for_settlement, :wait, duration: 2_000
      step :record_settlement, :log, message: "settlement recorded", level: :info

      transition :wait_for_settlement, on: :ok, to: :record_settlement
      transition :record_settlement, on: :ok, to: :complete
    end
  end

  defmodule ApprovalWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      approval_step :wait_for_review, output: :approval

      step :record_approval, :log,
        message: "approval recorded",
        level: :info,
        deadline: [within: 30_000, due_soon: 10_000, escalation: :diagnostic]

      step :record_rejection, :log, message: "rejection recorded", level: :warning

      transition :wait_for_review, on: :ok, to: :record_approval
      transition :wait_for_review, on: :error, to: :record_rejection
      transition :record_approval, on: :ok, to: :complete
      transition :record_rejection, on: :ok, to: :complete
    end
  end

  test "configures an application supervisor" do
    assert Application.spec(:squidie, :mod) == {Squidie.Application, []}
  end

  test "loads the public entrypoint module" do
    assert Code.ensure_loaded?(Squidie)
  end

  describe "config/1" do
    test "returns the validated host app contract with defaults" do
      assert {:ok, config} = Squidie.config(repo: Squidie.Test.Repo)

      assert config.repo == Squidie.Test.Repo
      refute Map.has_key?(config, :executor)
      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Squidie.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: Squidie.Test.Repo]
      assert config.queue == "default"
    end

    test "ignores retired runtime keys" do
      assert {:ok, config} =
               Squidie.config(
                 repo: Squidie.Test.Repo,
                 executor: String
               )

      assert config.repo == Squidie.Test.Repo
      refute Map.has_key?(config, :executor)
      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Squidie.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: Squidie.Test.Repo]
    end

    test "allows host applications to configure journal runtime defaults" do
      journal_storage = {Jido.Storage.ETS, table: :squidie_config_test}

      overrides = [
        repo: Squidie.Test.Repo,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: journal_storage,
        queue: :configured_queue
      ]

      assert {:ok, config} = Squidie.config(overrides)

      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Jido.Storage.ETS
      assert config.journal_storage.opts == [table: :squidie_config_test]
      assert config.queue == "configured_queue"
    end

    test "allows explicit journal storage without a configured repo" do
      journal_storage = {Jido.Storage.ETS, table: :squidie_config_no_repo_test}
      original_repo = Application.get_env(:squidie, :repo)

      on_exit(fn ->
        if is_nil(original_repo) do
          Application.delete_env(:squidie, :repo)
        else
          Application.put_env(:squidie, :repo, original_repo)
        end
      end)

      Application.delete_env(:squidie, :repo)

      assert {:ok, config} =
               Squidie.config(
                 runtime: :journal,
                 read_model: :read_model,
                 journal_storage: journal_storage,
                 queue: :configured_queue
               )

      assert config.repo == nil
      assert config.runtime == :journal
      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Jido.Storage.ETS
      assert config.journal_storage.opts == [table: :squidie_config_no_repo_test]
      assert config.queue == "configured_queue"
    end

    test "infers Ecto journal storage from the configured repo when runtime uses the journal" do
      required = [
        repo: Squidie.Test.Repo
      ]

      assert {:ok, config} = Squidie.config(Keyword.put(required, :runtime, :journal))

      assert config.runtime == :journal
      assert config.journal_storage.adapter == Squidie.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: Squidie.Test.Repo]
    end

    test "infers Ecto journal storage from the configured repo when read model uses the journal" do
      required = [
        repo: Squidie.Test.Repo
      ]

      assert {:ok, config} = Squidie.config(Keyword.put(required, :read_model, :read_model))

      assert config.read_model == :read_model
      assert config.journal_storage.adapter == Squidie.Runtime.Journal.Storage.Ecto
      assert config.journal_storage.opts == [repo: Squidie.Test.Repo]
    end

    test "rejects explicit nil journal storage when configured runtime or read model uses the journal" do
      required = [
        repo: Squidie.Test.Repo,
        journal_storage: nil
      ]

      assert {:error, {:missing_config, [:journal_storage]}} =
               Squidie.config(Keyword.put(required, :runtime, :journal))

      assert {:error, {:missing_config, [:journal_storage]}} =
               Squidie.config(Keyword.put(required, :read_model, :read_model))
    end

    test "rejects unsupported runtime configuration" do
      assert {:error, {:invalid_config, [runtime: :unsupported]}} =
               Squidie.config(
                 repo: Squidie.Test.Repo,
                 runtime: :unsupported
               )
    end

    test "rejects unsupported read model configuration" do
      assert {:error, {:invalid_config, [read_model: :unsupported]}} =
               Squidie.config(
                 repo: Squidie.Test.Repo,
                 read_model: :unsupported
               )
    end

    test "redacts invalid queue settings in config errors" do
      secret_queue = %{claim_token: "super-secret-token"}

      assert {:error, {:invalid_config, [queue: :invalid]} = reason} =
               Squidie.config(
                 repo: Squidie.Test.Repo,
                 queue: secret_queue
               )

      refute inspect(reason) =~ "super-secret-token"

      assert_raise ArgumentError, ~r/queue=:invalid/, fn ->
        Squidie.config!(
          repo: Squidie.Test.Repo,
          queue: secret_queue
        )
      end
    end

    test "reports missing required configuration keys" do
      original_repo = Application.get_env(:squidie, :repo)

      on_exit(fn ->
        Application.put_env(:squidie, :repo, original_repo)
      end)

      Application.delete_env(:squidie, :repo)

      assert {:error, {:missing_config, [:repo]}} = Squidie.config()

      assert_raise ArgumentError,
                   ~r/config :squidie, repo: .*journal_storage:/,
                   fn ->
                     Squidie.config!()
                   end
    end

    test "reports invalid repo configuration separately from missing repo config" do
      assert {:error, {:invalid_config, [repo: :invalid]}} =
               Squidie.config(repo: "not_a_repo")
    end

    test "journal-only configuration still rejects unsupported runtimes" do
      assert {:error, {:invalid_config, [runtime: :unsupported]}} =
               Squidie.config(repo: Squidie.Test.Repo, runtime: :unsupported)
    end
  end

  describe "journal-only runtime payloads" do
    test "runner rejects non-cron payload kinds as invalid payloads" do
      assert {:error, {:invalid_runtime_payload, %{"kind" => "step", "run_id" => _run_id}}} =
               Runner.perform(%{
                 "kind" => "step",
                 "run_id" => Ecto.UUID.generate(),
                 "step" => "charge_card"
               })

      assert {:error,
              {:invalid_runtime_payload, %{"kind" => "compensation", "run_id" => _run_id}}} =
               Runner.perform(%{
                 "kind" => "compensation",
                 "run_id" => Ecto.UUID.generate()
               })
    end

    test "runtime payloads expose cron trigger delivery only" do
      assert Code.ensure_loaded?(Payload)
      refute function_exported?(Payload, :step, 2)
      refute function_exported?(Payload, :compensation, 1)
      assert function_exported?(Payload, :cron, 2)
      assert function_exported?(Payload, :cron, 3)
      assert Squidie.Executor.required_callbacks() == [enqueue_cron: 4]
    end

    test "list_runs/2 returns an empty journal catalog when no runs exist" do
      assert {:ok, []} =
               Squidie.list_runs([], repo: Repo)
    end

    test "cancel/2 returns not found through the journal default" do
      assert {:error, :not_found} =
               Squidie.cancel(Ecto.UUID.generate(), repo: Repo)
    end

    test "replay/2 returns not found through the journal default" do
      assert {:error, :not_found} =
               Squidie.replay(Ecto.UUID.generate(), repo: Repo)
    end

    test "inspect_run_timeline/2 returns chronological redaction-safe operator events" do
      storage = {Jido.Storage.ETS, table: :squidie_public_timeline_test}
      queue = "public-timeline-test"
      started_at = ~U[2026-05-15 00:00:00Z]
      executed_at = ~U[2026-05-15 00:00:10Z]

      put_squidie_config(
        repo: Repo,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: storage,
        queue: queue
      )

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(BillingWorkflow, :billing, %{payment_id: "pay_secret_123"},
                 now: started_at
               )

      assert {:ok, %Snapshot{} = _completed_step} =
               Squidie.execute_next(owner_id: "timeline-worker", now: executed_at)

      assert {:ok, timeline} = Squidie.inspect_run_timeline(started.run_id, now: executed_at)

      assert timeline.run_id == started.run_id
      assert timeline.workflow == "Elixir.SquidieTest.BillingWorkflow"

      assert [
               :command_received,
               :run_started,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed,
               :runnable_applied,
               :attempt_scheduled
               | _remaining_event_types
             ] = Enum.map(timeline.events, & &1.type)

      assert Enum.all?(timeline.events, &match?(%DateTime{}, &1.occurred_at))
      refute inspect(timeline.events) =~ "pay_secret_123"
      refute inspect(timeline.events) =~ "charged"
    end

    test "cron starts run through the journal default and expose schedule context" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_context_test}
      queue = "journal-cron-context-test"
      started_at = ~U[2026-05-15 00:00:00Z]
      visible_at = ~U[2026-05-15 00:00:10Z]

      put_squidie_config(
        repo: Repo,
        runtime: :journal,
        read_model: :read_model,
        journal_storage: storage,
        queue: queue
      )

      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          signal_id: "journal_signal_123",
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok = Runner.perform(payload, now: started_at)

      assert {:ok, [%Summary{} = summary]} = Squidie.list_runs([])

      assert {:ok, %Snapshot{} = started} =
               Squidie.inspect_run(summary.run_id, now: started_at)

      assert started.trigger == "scheduled_capture"
      assert started.context.schedule.signal_id == "journal_signal_123"
      assert started.context.schedule.trigger_name == "scheduled_capture"
      assert started.context.schedule.intended_window.start_at == "2026-05-15T09:00:00Z"

      assert [
               %{
                 signal_type: "start_cron",
                 idempotency_key: "journal_signal_123",
                 payload: %{
                   workflow: "Elixir.SquidieTest.ScheduledContextWorkflow",
                   trigger: "scheduled_capture",
                   input: %{}
                 }
               }
             ] = started.command_history

      assert {:ok, %Snapshot{} = completed} =
               Squidie.execute_next(
                 owner_id: "journal-cron-test",
                 now: visible_at
               )

      assert completed.terminal_status == :completed
      assert completed.context.schedule_seen == completed.context.schedule
    end

    test "apply_signal/2 starts cron-triggered journal runs from command signals" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_signal_test}
      queue = "journal-cron-signal-test"
      started_at = ~U[2026-05-15 00:00:00Z]

      assert {:ok, %Signal{} = signal} =
               Signal.start_cron(
                 ScheduledContextWorkflow,
                 :scheduled_capture,
                 %{},
                 metadata: %{source: "signal_interpreter"},
                 occurred_at: started_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert started.trigger == "scheduled_capture"
      assert started.queue == queue

      assert [
               %{
                 signal_type: "start_cron",
                 metadata: %{source: "signal_interpreter"},
                 payload: %{
                   workflow: "Elixir.SquidieTest.ScheduledContextWorkflow",
                   trigger: "scheduled_capture",
                   input: %{}
                 }
               }
             ] = started.command_history
    end

    test "idempotent cron starts reuse one journal run for duplicate schedule delivery" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_idempotency_test}
      queue = "journal-cron-idempotency-test"
      started_at = ~U[2026-05-15 00:00:00Z]

      payload =
        Payload.cron(
          IdempotentScheduledContextWorkflow,
          :scheduled_capture,
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: started_at
               )

      assert :ok =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: DateTime.add(started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert summary.workflow == Atom.to_string(IdempotentScheduledContextWorkflow)

      assert {:ok, %Snapshot{} = started} =
               Squidie.inspect_run(summary.run_id,
                 runtime: :journal,
                 read_model: :read_model,
                 journal_storage: storage,
                 queue: queue,
                 now: started_at
               )

      assert [
               %{
                 signal_type: "start_cron",
                 idempotency_key: idempotency_key
               }
             ] = started.command_history

      assert idempotency_key == started.context.schedule.idempotency_key
    end

    test "duplicate journal cron starts survive queue changes for the same schedule identity" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_duplicate_queue_change_test}
      started_at = ~U[2026-05-15 00:00:00Z]

      payload =
        Payload.cron(
          IdempotentScheduledContextWorkflow,
          :scheduled_capture,
          signal_id: "journal_queue_change_signal_123"
        )

      assert :ok =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: "journal-cron-original-queue",
                 now: started_at
               )

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: storage,
                 queue: "journal-cron-original-queue"
               )

      assert {:ok, {:duplicate_schedule_start, duplicate_run_id}} =
               Runner.start_cron_trigger(payload["workflow"], payload["trigger"], payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: "journal-cron-new-queue",
                 now: DateTime.add(started_at, 1, :second)
               )

      assert duplicate_run_id == summary.run_id
    end

    test "duplicate journal cron starts survive current workflow definition drift" do
      storage =
        {Jido.Storage.ETS, table: :squidie_journal_cron_duplicate_definition_drift_test}

      started_at = ~U[2026-05-15 00:00:00Z]
      workflow = DynamicJournalCronDefinitionDrift

      on_exit(fn -> unload_dynamic_workflow(workflow) end)

      compile_dynamic_cron_workflow(workflow, :with_idempotent_schedule)

      payload =
        Payload.cron(workflow, :scheduled_capture,
          signal_id: "journal_definition_drift_signal_123"
        )

      opts = [
        runtime: :journal,
        journal_storage: storage,
        queue: "journal-cron-definition-drift-test"
      ]

      assert :ok = Runner.perform(payload, Keyword.put(opts, :now, started_at))
      assert {:ok, [%Summary{} = summary]} = Squidie.list_runs([], opts)

      compile_dynamic_cron_workflow(workflow, :without_scheduled_trigger)

      assert {:ok, {:duplicate_schedule_start, duplicate_run_id}} =
               Runner.start_cron_trigger(
                 payload["workflow"],
                 payload["trigger"],
                 payload,
                 Keyword.put(opts, :now, DateTime.add(started_at, 1, :second))
               )

      assert duplicate_run_id == summary.run_id
    end

    test "duplicate journal cron starts derive identity after workflow definition drift" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_duplicate_derived_drift_test}
      started_at = ~U[2026-05-15 00:00:00Z]
      workflow = DynamicJournalCronDerivedDrift

      on_exit(fn -> unload_dynamic_workflow(workflow) end)

      compile_dynamic_cron_workflow(workflow, :with_idempotent_schedule)

      payload =
        Payload.cron(workflow, :scheduled_capture,
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      opts = [
        runtime: :journal,
        journal_storage: storage,
        queue: "journal-cron-derived-drift-test"
      ]

      assert :ok = Runner.perform(payload, Keyword.put(opts, :now, started_at))
      assert {:ok, [%Summary{} = summary]} = Squidie.list_runs([], opts)

      compile_dynamic_cron_workflow(workflow, :without_scheduled_trigger)

      assert {:ok, {:duplicate_schedule_start, duplicate_run_id}} =
               Runner.start_cron_trigger(
                 payload["workflow"],
                 payload["trigger"],
                 payload,
                 Keyword.put(opts, :now, DateTime.add(started_at, 1, :second))
               )

      assert duplicate_run_id == summary.run_id
    end

    test "journal cron duplicate classification ignores step output schedule keys" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_schedule_clobber_test}
      queue = "journal-cron-schedule-clobber-test"
      started_at = ~U[2026-05-15 00:00:00Z]
      visible_at = ~U[2026-05-15 00:00:10Z]

      payload =
        Payload.cron(
          SkipDuplicateScheduleClobberWorkflow,
          :scheduled_capture,
          signal_id: "journal_schedule_clobber_signal_123"
        )

      opts = [
        runtime: :journal,
        journal_storage: storage,
        queue: queue
      ]

      assert :ok = Runner.perform(payload, Keyword.put(opts, :now, started_at))

      assert {:ok, %Snapshot{} = completed} =
               execute_journal_next(
                 opts
                 |> Keyword.put(:owner_id, "journal-cron-schedule-clobber")
                 |> Keyword.put(:now, visible_at)
                 |> Keyword.put(:finished_at, visible_at)
               )

      assert completed.context.schedule.idempotency == :skip_duplicate
      assert completed.context.schedule.idempotency_key == "journal_schedule_clobber_signal_123"
      assert completed.context.digest_delivery.ok == true

      assert {:ok, {:skipped_schedule_start, skipped_run_id}} =
               Runner.start_cron_trigger(
                 payload["workflow"],
                 payload["trigger"],
                 payload,
                 Keyword.put(opts, :now, DateTime.add(started_at, 1, :second))
               )

      assert skipped_run_id == completed.run_id
    end

    test "replay/2 preserves journal cron schedule context" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_replay_context_test}
      queue = "journal-cron-replay-context-test"
      started_at = ~U[2026-05-15 00:00:00Z]
      visible_at = ~U[2026-05-15 00:00:10Z]

      payload =
        Payload.cron(
          ScheduledContextWorkflow,
          :scheduled_capture,
          signal_id: "journal_replay_signal_123",
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: started_at
               )

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert {:ok, %Snapshot{} = completed} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "journal-cron-replay-source",
                 now: visible_at,
                 finished_at: visible_at
               )

      assert completed.context.schedule.signal_id == "journal_replay_signal_123"
      assert completed.context.schedule_seen == completed.context.schedule

      assert {:ok, %Snapshot{} = replayed} =
               Squidie.replay(summary.run_id,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: DateTime.add(started_at, 1, :second)
               )

      assert replayed.context.schedule == completed.context.schedule

      assert {:ok, %Snapshot{} = completed_replay} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "journal-cron-replay-target",
                 now: DateTime.add(visible_at, 1, :second),
                 finished_at: DateTime.add(visible_at, 1, :second)
               )

      assert completed_replay.context.schedule_seen == completed.context.schedule
    end

    test "replay/2 removes schedule idempotency identity from journal cron context" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_replay_idempotency_test}
      queue = "journal-cron-replay-idempotency-test"
      started_at = ~U[2026-05-15 00:00:00Z]

      payload =
        Payload.cron(
          IdempotentScheduledContextWorkflow,
          :scheduled_capture,
          signal_id: "journal_replay_idempotency_signal_123",
          intended_window: %{
            start_at: "2026-05-15T09:00:00Z",
            end_at: "2026-05-15T10:00:00Z"
          }
        )

      assert :ok =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: started_at
               )

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert {:ok, %Snapshot{} = source} =
               Squidie.inspect_run(summary.run_id,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert source.context.schedule.idempotency == :return_existing_run
      assert source.context.schedule.idempotency_key == "journal_replay_idempotency_signal_123"

      assert {:ok, %Snapshot{} = replayed} =
               Squidie.replay(summary.run_id,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: DateTime.add(started_at, 1, :second)
               )

      assert replayed.context.schedule.signal_id == "journal_replay_idempotency_signal_123"
      refute Map.has_key?(replayed.context.schedule, :idempotency)
      refute Map.has_key?(replayed.context.schedule, :idempotency_key)
    end

    test "journal cron starts reject malformed schedule idempotency keys" do
      assert {:error, {:invalid_option, {:schedule_idempotency_key, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 IdempotentScheduledContextWorkflow,
                 :scheduled_capture,
                 %{},
                 %{schedule: %{idempotency_key: 123}},
                 runtime: :journal,
                 journal_storage: {Jido.Storage.ETS, table: :squidie_journal_cron_bad_key_test},
                 queue: "journal-cron-bad-key-test"
               )

      assert {:ok, %Signal{} = signal} =
               Signal.start_cron(IdempotentScheduledContextWorkflow, :scheduled_capture, %{})

      assert {:error, {:invalid_option, {:schedule_idempotency_key, :invalid}}} =
               SignalInterpreter.apply(signal,
                 journal_storage: {Jido.Storage.ETS, table: :squidie_journal_cron_empty_key_test},
                 queue: "journal-cron-empty-key-test",
                 initial_context: %{schedule: %{idempotency_key: ""}}
               )
    end

    test "journal cron starts return structured option errors" do
      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ScheduledContextWorkflow,
                 :scheduled_capture,
                 %{},
                 %{schedule: %{idempotency_key: "valid-key"}},
                 runtime: :journal,
                 journal_storage: {Jido.Storage.ETS, table: :squidie_journal_cron_bad_queue_test},
                 queue: ""
               )
    end

    test "malformed journal cron scheduler metadata does not create a run" do
      storage = {Jido.Storage.ETS, table: :squidie_journal_cron_invalid_metadata_test}
      queue = "journal-cron-invalid-metadata-test"

      payload =
        ScheduledContextWorkflow
        |> Payload.cron(:scheduled_capture)
        |> Map.put("signal_id", 123)

      assert {:error, {:invalid_schedule_signal_id, 123}} =
               Runner.perform(payload,
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )

      assert {:ok, []} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue
               )
    end
  end

  defp compile_dynamic_cron_workflow(module, variant) do
    compiler_options = Code.compiler_options()
    Code.compiler_options(ignore_module_conflict: true)

    try do
      Code.compile_string(dynamic_cron_workflow_source(module, variant))
    after
      Code.compiler_options(compiler_options)
    end
  end

  defp dynamic_cron_workflow_source(module, :with_idempotent_schedule) do
    """
    defmodule #{inspect(module)} do
      use Squidie.Workflow

      workflow do
        trigger :scheduled_capture do
          cron "@hourly", timezone: "Etc/UTC", idempotency: :return_existing_run
        end

        step :capture_schedule, SquidieTest.ScheduledContextWorkflow.CaptureSchedule
        transition :capture_schedule, on: :ok, to: :complete
      end
    end
    """
  end

  defp dynamic_cron_workflow_source(module, :without_scheduled_trigger) do
    """
    defmodule #{inspect(module)} do
      use Squidie.Workflow

      workflow do
        trigger :manual_capture do
          manual()
        end

        step :capture_schedule, SquidieTest.ScheduledContextWorkflow.CaptureSchedule
        transition :capture_schedule, on: :ok, to: :complete
      end
    end
    """
  end

  defp unload_dynamic_workflow(module) do
    :code.purge(module)
    :code.delete(module)
  end

  describe "read model" do
    @read_model_storage {Jido.Storage.ETS, table: :squidie_read_model_squidie_test}
    @read_model_run_id "run_123"
    @read_model_workflow Atom.to_string(BillingWorkflow)
    @read_model_queue "default"
    @read_model_runnable_key "run_123:charge_card:1"
    @read_model_idempotency_key "run_123:charge_card:payment_456"
    @read_model_started_at ~U[2026-05-15 00:00:00Z]
    @read_model_visible_at ~U[2026-05-15 00:00:10Z]

    setup do
      cleanup_read_model_storage()
      on_exit(&cleanup_read_model_storage/0)
    end

    test "inspect_run/2 can read from the read model" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      append_read_model_dispatch_entries([read_model_attempt_scheduled()])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert snapshot.run_id == @read_model_run_id
      assert snapshot.workflow == @read_model_workflow
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible

      assert [%{runnable_key: @read_model_runnable_key, status: :available}] =
               snapshot.visible_attempts
    end

    test "start/3 appends journal start and dispatch facts" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible

      assert [%{runnable_key: runnable_key, step: "check_gateway", status: :available}] =
               snapshot.visible_attempts

      assert [
               %{type: :run_signal_received, data: signal_receipt},
               %{type: :run_started},
               %{type: :runnables_planned}
             ] = raw_run_entries(snapshot.run_id, @read_model_storage)

      assert signal_receipt.signal_type == "start_run"

      assert signal_receipt.payload == %{
               workflow: Atom.to_string(PaymentRecoveryWorkflow),
               trigger: "gateway_recovery",
               input: %{account_id: "acct_123"}
             }

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert graph.source == :read_model
      assert graph.current_node_id == "check_gateway"
      assert graph.current_node_ids == ["check_gateway"]
      assert nodes["check_gateway"].status == :pending
      assert nodes["check_gateway"].attempts == []
      assert edges["check_gateway:ok:complete"].status == :pending

      assert {:ok, run_entries} =
               load_read_model_run_entries(snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned]

      assert [%{runnable_key: ^runnable_key, step: "check_gateway"}] =
               run_entries
               |> Enum.at(-1)
               |> Map.fetch!(:data)
               |> Map.fetch!(:runnables)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]
      assert [%{runnable_key: ^runnable_key, step: "check_gateway"}] = snapshot.visible_attempts

      assert {:ok, run_index_projection} =
               Journal.rebuild_run_index_projection(
                 @read_model_storage,
                 Atom.to_string(PaymentRecoveryWorkflow)
               )

      assert Squidie.Runtime.RunIndexProjection.run_ids(run_index_projection) == [
               snapshot.run_id
             ]
    end

    test "inspect, graph, and explanation expose durable compensation policy" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 JournalCompensationWorkflow,
                 :checkout,
                 %{order_id: "order_compensation_policy"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      expected_recovery = %{
        irreversible?: false,
        compensatable?: true,
        replay: :allowed,
        recovery: :automatic,
        compensation: %{
          callback: Atom.to_string(JournalCompensationWorkflow.ReleaseInventory),
          status: :available
        }
      }

      assert [
               %{
                 step: "reserve_inventory",
                 status: :available,
                 recovery: ^expected_recovery
               }
             ] = snapshot.visible_attempts

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert nodes["reserve_inventory"].recovery == expected_recovery

      assert {:ok, %Diagnostic{} = diagnostic} =
               Squidie.explain_run(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert diagnostic.evidence.recovery_policies == %{
               "reserve_inventory" => expected_recovery
             }
    end

    test "terminal failure compensates completed saga steps in reverse completion order" do
      order_id = "order_saga_rollback"

      :persistent_term.put(saga_compensation_log_key(order_id), [])
      on_exit(fn -> :persistent_term.erase(saga_compensation_log_key(order_id)) end)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 JournalSagaRollbackWorkflow,
                 :checkout,
                 %{order_id: order_id},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{step: "reserve_inventory"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{step: "authorize_payment"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert [%{step: "capture_payment"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 4, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 5, :second)
               )

      refute snapshot.terminal?
      assert [%{step: "compensate:authorize_payment"}] = snapshot.visible_attempts

      assert [%{dynamic_work: %{kind: :compensation, origin_step: "authorize_payment"}}] =
               Enum.filter(
                 snapshot.planned_runnables,
                 &(&1.step == "compensate:authorize_payment")
               )

      assert {:ok, graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["compensate:authorize_payment"].status == :pending
      assert graph_nodes["compensate:authorize_payment"].current?

      assert {:ok, diagnostic} =
               Squidie.explain_run(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert diagnostic.evidence.recovery_policies["compensate:authorize_payment"] == %{
               irreversible?: false,
               compensatable?: false,
               replay: :manual_review_required,
               recovery: :manual_intervention
             }

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 6, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 7, :second)
               )

      refute snapshot.terminal?
      assert [%{step: "compensate:reserve_inventory"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 8, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 9, :second)
               )

      assert snapshot.terminal?
      assert snapshot.terminal_status == :failed

      assert order_id
             |> saga_compensation_log_key()
             |> :persistent_term.get()
             |> Enum.reverse() == [
               :void_payment_authorization,
               :release_inventory
             ]

      assert %{
               released_inventory: %{status: "released"},
               voided_payment_authorization: %{status: "voided"}
             } = snapshot.context
    end

    test "execute_next/1 recovers a failed saga attempt by planning compensation" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalSagaRollbackWorkflow,
                 :checkout,
                 %{order_id: "order_saga_recovery"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "authorize_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "capture_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: DateTime.add(@read_model_visible_at, 4, :second)
               )

      assert attempt.step == "capture_payment"

      assert {:ok, %{}} =
               DispatchAgent.fail(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{code: "capture_declined", message: "capture declined", retryable?: false},
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert {:ok, %Snapshot{reason: :waiting_for_dispatch}} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 6, :second)
               )

      refute recovered_snapshot.terminal?
      assert [%{step: "compensate:authorize_payment"}] = recovered_snapshot.visible_attempts

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnables_planned
             ]
    end

    test "graph and explanation surface compensation before dispatch scheduling recovers" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalSagaRollbackWorkflow,
                 :checkout,
                 %{order_id: "order_saga_pending_dispatch"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "authorize_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "capture_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      compensation_key = "#{started_snapshot.run_id}:compensate:authorize_payment:1"

      compensation_recovery = %{
        "irreversible?" => false,
        "compensatable?" => false,
        "replay" => "manual_review_required",
        "recovery" => "manual_intervention"
      }

      compensation_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: compensation_key,
        idempotency_key: compensation_key,
        attempt_number: 1,
        queue: @read_model_queue,
        step: "compensate:authorize_payment",
        input: %{
          source: %{run_id: started_snapshot.run_id},
          step: %{name: :authorize_payment, output: %{}}
        },
        recovery: compensation_recovery,
        visible_at: DateTime.add(@read_model_visible_at, 5, :second),
        dynamic?: true,
        dynamic_work: %{kind: :compensation, origin_step: "authorize_payment"}
      }

      assert {:ok, entry} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: started_snapshot.run_id,
                 runnables: [compensation_runnable],
                 occurred_at: DateTime.add(@read_model_visible_at, 5, :second)
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [entry])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert [%{step: "compensate:authorize_payment", recovery: recovery}] =
               snapshot.pending_dispatches

      assert recovery == %{
               irreversible?: false,
               compensatable?: false,
               replay: :manual_review_required,
               recovery: :manual_intervention
             }

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["compensate:authorize_payment"].current?
      assert graph_nodes["compensate:authorize_payment"].status == :pending
      assert graph_nodes["compensate:authorize_payment"].recovery == recovery

      assert {:ok, diagnostic} =
               Squidie.explain_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert diagnostic.evidence.recovery_policies["compensate:authorize_payment"] == recovery
    end

    test "execute_next/1 persists deferred continuation without consuming retry budget" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_deferred"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase(
          {DeferredContinuationWorkflow.CheckGateway, started_snapshot.run_id}
        )
      end)

      finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(finished_at, 30, :second)

      assert {:ok, %Snapshot{} = deferred_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: @read_model_visible_at,
                 finished_at: finished_at
               )

      refute deferred_snapshot.terminal?
      assert deferred_snapshot.reason == :deferred_continuation
      assert deferred_snapshot.next_visible_at == deferred_visible_at
      assert deferred_snapshot.visible_attempts == []

      assert [
               %{
                 step: "check_gateway",
                 attempt_number: 1,
                 visible_at: ^deferred_visible_at,
                 deadline: %{status: :on_time, due_at: deferred_due_at},
                 deferred: %{
                   reason: %{code: "gateway_pending", order_id: "order_deferred"},
                   from_runnable_key: original_runnable_key
                 }
               } = deferred_attempt
             ] = deferred_snapshot.scheduled_attempts

      assert DateTime.compare(
               deferred_due_at,
               DateTime.add(@read_model_started_at, 60, :second)
             ) == :eq

      assert original_runnable_key == "#{started_snapshot.run_id}:check_gateway:1"
      assert deferred_attempt.runnable_key == "#{original_runnable_key}:deferred"

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: finished_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["check_gateway"].current?
      assert graph_nodes["check_gateway"].status == :deferred

      assert {:ok, diagnostic} =
               Squidie.explain_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: finished_at
               )

      assert diagnostic.reason == :deferred_continuation
      assert diagnostic.next_actions == [:wait_until_attempt_visible]
      assert diagnostic.details.next_visible_at == deferred_visible_at

      assert {:ok, ready_graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_visible_at
               )

      ready_graph_nodes = Map.new(ready_graph.nodes, &{&1.id, &1})
      assert ready_graph_nodes["check_gateway"].current?
      assert ready_graph_nodes["check_gateway"].status == :pending

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: deferred_visible_at,
                 finished_at: DateTime.add(deferred_visible_at, 1, :second)
               )

      assert completed_snapshot.terminal?
      assert completed_snapshot.terminal_status == :completed

      assert %{gateway: %{status: "ready", calls: 2}} =
               completed_snapshot.context
    end

    test "execute_next/1 recovers deferred continuation after dispatch completion" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_dispatch_deferred"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase(
          {DeferredContinuationWorkflow.CheckGateway, started_snapshot.run_id}
        )
      end)

      finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(finished_at, 30, :second)

      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok,
              %{
                agent: claimed_agent,
                attempt: claimed_attempt,
                claim_id: claim_id,
                claim_token: claim_token
              }} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 now: @read_model_visible_at,
                 claim_id: "deferred_completion_claim",
                 claim_token: "deferred_completion_token"
               )

      assert {:ok, %{attempt: completed_attempt}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 claimed_attempt.runnable_key,
                 claim_id,
                 claim_token,
                 %{},
                 now: finished_at,
                 execution_opts: [
                   defer: %{
                     reason: %{code: "gateway_pending", order_id: "order_dispatch_deferred"}
                   },
                   schedule_in: 30
                 ]
               )

      assert completed_attempt.execution_opts[:defer].reason.code == "gateway_pending"

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-recovery-worker",
                 now: DateTime.add(finished_at, 1, :second)
               )

      assert recovered_snapshot.reason == :deferred_continuation
      assert recovered_snapshot.next_visible_at == deferred_visible_at

      assert [
               %{
                 step: "check_gateway",
                 attempt_number: 1,
                 visible_at: ^deferred_visible_at,
                 deferred: %{
                   reason: %{
                     code: "gateway_pending",
                     order_id: "order_dispatch_deferred"
                   }
                 }
               }
             ] = recovered_snapshot.scheduled_attempts
    end

    test "execute_next/1 waits for deferred dependency continuations before joins unlock" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredDependencyWorkflow,
                 :checkout,
                 %{order_id: "order_dependency_deferred", invoice_id: "inv_dependency_deferred"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredDependencyWorkflow.CheckGateway, started_snapshot.run_id})
      end)

      finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(finished_at, 30, :second)

      assert {:ok, %Snapshot{} = after_gateway} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-worker-1",
                 now: @read_model_visible_at,
                 finished_at: finished_at
               )

      assert Enum.map(after_gateway.visible_attempts, & &1.step) == ["load_invoice"]
      assert [%{step: "check_gateway", deferred: %{}}] = after_gateway.scheduled_attempts

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-worker-2",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert after_invoice.status == :running
      assert after_invoice.visible_attempts == []
      refute Enum.any?(after_invoice.pending_dispatches, &(&1.step == "send_receipt"))
      refute Enum.any?(after_invoice.scheduled_attempts, &(&1.step == "send_receipt"))

      assert [%{step: "check_gateway", visible_at: ^deferred_visible_at}] =
               after_invoice.scheduled_attempts

      assert {:ok, %Snapshot{} = after_continuation} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-worker-3",
                 now: deferred_visible_at,
                 finished_at: DateTime.add(deferred_visible_at, 1, :second)
               )

      assert [
               %{
                 step: "send_receipt",
                 input: %{
                   gateway: %{status: "ready"},
                   invoice: %{id: "inv_dependency_deferred", status: "open"}
                 }
               }
             ] = after_continuation.visible_attempts
    end

    test "cancel/2 prevents scheduled deferred continuations from running later" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_deferred_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase(
          {DeferredContinuationWorkflow.CheckGateway, started_snapshot.run_id}
        )
      end)

      deferred_finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(deferred_finished_at, 30, :second)

      assert {:ok, %Snapshot{reason: :deferred_continuation}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-cancel-worker-1",
                 now: @read_model_visible_at,
                 finished_at: deferred_finished_at
               )

      assert {:ok, %Snapshot{} = cancelled_snapshot} =
               Squidie.cancel(started_snapshot.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(deferred_finished_at, 1, :second)
               )

      assert cancelled_snapshot.status == :cancelled
      assert cancelled_snapshot.visible_attempts == []

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-cancel-worker-2",
                 now: deferred_visible_at,
                 finished_at: DateTime.add(deferred_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = still_cancelled} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_visible_at
               )

      assert still_cancelled.status == :cancelled
      assert still_cancelled.visible_attempts == []
      assert still_cancelled.context == %{}

      assert :persistent_term.get(
               {DeferredContinuationWorkflow.CheckGateway, started_snapshot.run_id}
             ) == 1
    end

    test "replay/2 starts fresh work instead of copying deferred continuation state" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_deferred_replay"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredContinuationWorkflow.CheckGateway, source.run_id})
      end)

      assert {:ok, %Snapshot{reason: :deferred_continuation}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-replay-source",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = replay} =
               Squidie.replay(source.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredContinuationWorkflow.CheckGateway, replay.run_id})
      end)

      assert replay.run_id != source.run_id
      assert replay.replayed_from_run_id == source.run_id
      assert replay.input == %{order_id: "order_deferred_replay"}
      assert [%{step: "check_gateway", attempt_number: 1}] = replay.visible_attempts
      assert replay.scheduled_attempts == []

      assert {:ok, %Snapshot{} = replay_deferred} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-replay-target",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert replay_deferred.reason == :deferred_continuation
      assert [%{step: "check_gateway", attempt_number: 1}] = replay_deferred.scheduled_attempts
    end

    test "execute_next/1 treats duplicate deferred completion recovery as idempotent" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_dispatch_deferred_duplicate"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase(
          {DeferredContinuationWorkflow.CheckGateway, started_snapshot.run_id}
        )
      end)

      finished_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok,
              %{
                agent: claimed_agent,
                attempt: claimed_attempt,
                claim_id: claim_id,
                claim_token: claim_token
              }} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 now: @read_model_visible_at,
                 claim_id: "deferred_duplicate_claim",
                 claim_token: "deferred_duplicate_token"
               )

      completion_opts = [
        defer: %{
          reason: %{code: "gateway_pending", order_id: "order_dispatch_deferred_duplicate"}
        },
        schedule_in: 30
      ]

      assert {:ok, %{agent: completed_agent}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 claimed_attempt.runnable_key,
                 claim_id,
                 claim_token,
                 %{},
                 now: finished_at,
                 execution_opts: completion_opts
               )

      assert {:ok, %{agent: ^completed_agent}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 completed_agent,
                 claimed_attempt.runnable_key,
                 claim_id,
                 claim_token,
                 %{},
                 now: finished_at,
                 execution_opts: completion_opts
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_completed)) == 1

      assert {:ok, %Snapshot{reason: :deferred_continuation} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-duplicate-recovery",
                 now: DateTime.add(finished_at, 1, :second)
               )

      assert [%{step: "check_gateway"}] = recovered_snapshot.scheduled_attempts

      assert {:ok, run_entries} = load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.count(run_entries, &(&1.type == :runnables_planned)) == 2

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "deferred-duplicate-recovery-2",
                 now: DateTime.add(finished_at, 2, :second)
               )

      assert {:ok, duplicate_run_entries} = load_read_model_run_entries(started_snapshot.run_id)
      assert duplicate_run_entries == run_entries

      assert {:ok, %Snapshot{} = duplicate_recovery} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(finished_at, 2, :second)
               )

      assert duplicate_recovery.reason == :deferred_continuation
    end

    test "inspection tolerates legacy deferred metadata facts" do
      run_id = Ecto.UUID.generate()
      deferred_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(deferred_at, 30, :second)
      original_runnable_key = "#{run_id}:check_gateway:1"
      deferred_runnable_key = "#{original_runnable_key}:deferred"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_legacy_deferred"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: ^original_runnable_key}] = started_snapshot.visible_attempts

      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok,
              %{
                agent: claimed_agent,
                attempt: %{runnable_key: ^original_runnable_key},
                claim_id: claim_id,
                claim_token: claim_token
              }} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "legacy-deferred",
                 now: @read_model_visible_at,
                 claim_id: "legacy_deferred_claim",
                 claim_token: "legacy_deferred_token"
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 original_runnable_key,
                 claim_id,
                 claim_token,
                 %{},
                 now: deferred_at,
                 execution_opts: [
                   defer: %{reason: "legacy pending"},
                   schedule_in: 30
                 ]
               )

      deferred_runnable = %{
        run_id: run_id,
        runnable_key: deferred_runnable_key,
        idempotency_key: deferred_runnable_key,
        attempt_number: 1,
        queue: @read_model_queue,
        step: "check_gateway",
        input: %{order_id: "order_legacy_deferred"},
        visible_at: deferred_visible_at,
        recovery: %{
          "replay" => "allowed",
          "recovery" => "retryable",
          "irreversible?" => false,
          "compensatable?" => true
        },
        deferred: %{
          "reason" => "legacy pending",
          "from_runnable_key" => original_runnable_key,
          "deferred_at" => deferred_at
        }
      }

      assert {:ok, planned_entry} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [deferred_runnable],
                 occurred_at: deferred_at
               })

      assert {:ok, applied_entry} =
               DispatchProtocol.new_entry(:runnable_applied, %{
                 run_id: run_id,
                 runnable_key: original_runnable_key,
                 result: %{},
                 execution_opts: [
                   defer: %{reason: "legacy pending"},
                   schedule_in: 30
                 ],
                 occurred_at: deferred_at,
                 applied_at: deferred_at
               })

      scheduled_attrs =
        deferred_runnable
        |> Map.delete(:deferred)
        |> Map.put(:occurred_at, deferred_at)

      assert {:ok, scheduled_entry} =
               DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs)

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [applied_entry, planned_entry])

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [scheduled_entry])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      assert snapshot.reason == :deferred_continuation

      assert [
               %{
                 runnable_key: ^deferred_runnable_key,
                 deferred: %{
                   reason: "legacy pending",
                   from_runnable_key: ^original_runnable_key,
                   deferred_at: ^deferred_at
                 }
               }
             ] = snapshot.scheduled_attempts

      assert {:ok, graph} =
               Squidie.inspect_run_graph(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["check_gateway"].status == :deferred

      assert {:ok, diagnostic} =
               Squidie.explain_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      assert diagnostic.reason == :deferred_continuation

      assert [
               %{
                 reason: "legacy pending",
                 from_runnable_key: ^original_runnable_key,
                 deferred_at: ^deferred_at
               }
             ] = diagnostic.details.deferred
    end

    test "inspection tolerates malformed stale deferred facts without mislabeling graph state" do
      run_id = Ecto.UUID.generate()
      deferred_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(deferred_at, 30, :second)
      original_runnable_key = "#{run_id}:check_gateway:1"
      stale_runnable_key = "#{original_runnable_key}:stale-deferred"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredContinuationWorkflow,
                 :checkout,
                 %{order_id: "order_stale_deferred"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: ^original_runnable_key}] = started_snapshot.visible_attempts

      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok,
              %{
                agent: claimed_agent,
                claim_id: claim_id,
                claim_token: claim_token
              }} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "stale-deferred",
                 now: @read_model_visible_at,
                 claim_id: "stale_deferred_claim",
                 claim_token: "stale_deferred_token"
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 original_runnable_key,
                 claim_id,
                 claim_token,
                 %{},
                 now: deferred_at,
                 execution_opts: [
                   defer: %{reason: "stale pending"},
                   schedule_in: 30
                 ]
               )

      stale_runnable = %{
        run_id: run_id,
        runnable_key: stale_runnable_key,
        idempotency_key: stale_runnable_key,
        attempt_number: 1,
        queue: @read_model_queue,
        step: "check_gateway",
        input: %{order_id: "order_stale_deferred"},
        visible_at: deferred_visible_at,
        recovery: %{
          "replay" => "allowed",
          "recovery" => "retryable",
          "irreversible?" => false,
          "compensatable?" => true
        },
        deferred: "malformed deferred metadata"
      }

      assert {:ok, planned_entry} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [stale_runnable],
                 occurred_at: deferred_at
               })

      assert {:ok, applied_entry} =
               DispatchProtocol.new_entry(:runnable_applied, %{
                 run_id: run_id,
                 runnable_key: original_runnable_key,
                 result: %{},
                 execution_opts: [
                   defer: %{reason: "stale pending"},
                   schedule_in: 30
                 ],
                 occurred_at: deferred_at,
                 applied_at: deferred_at
               })

      scheduled_attrs =
        stale_runnable
        |> Map.delete(:deferred)
        |> Map.put(:occurred_at, deferred_at)

      assert {:ok, scheduled_entry} =
               DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs)

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [applied_entry, planned_entry])

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [scheduled_entry])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      assert snapshot.reason == :attempt_scheduled_for_later

      assert [%{runnable_key: ^stale_runnable_key} = stale_attempt] =
               snapshot.scheduled_attempts

      assert Map.get(stale_attempt, :deferred) == nil

      assert {:ok, graph} =
               Squidie.inspect_run_graph(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["check_gateway"].status == :pending

      assert {:ok, diagnostic} =
               Squidie.explain_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_at
               )

      assert diagnostic.reason == :attempt_scheduled_for_later
    end

    test "dependency workflows fail terminally without unlocking joins when a sibling fails during deferral" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredDependencyFailureWorkflow,
                 :checkout,
                 %{order_id: "order_dependency_deferred_failure", invoice_id: "inv_failure"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredDependencyWorkflow.CheckGateway, started_snapshot.run_id})
      end)

      assert {:ok, %Snapshot{} = after_gateway} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-failure-worker-1",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{step: "check_gateway", deferred: %{}}] = after_gateway.scheduled_attempts
      assert Enum.map(after_gateway.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Snapshot{} = failed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-failure-worker-2",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert failed_snapshot.status == :failed
      assert failed_snapshot.terminal?
      refute Enum.any?(failed_snapshot.pending_dispatches, &(&1.step == "send_receipt"))
      refute Enum.any?(failed_snapshot.scheduled_attempts, &(&1.step == "send_receipt"))

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})
      assert graph_nodes["load_invoice"].status == :failed
      assert graph_nodes["send_receipt"].status == :waiting
    end

    test "dependency workflows keep joins locked when cancelled during a deferred dependency" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredDependencyWorkflow,
                 :checkout,
                 %{order_id: "order_dependency_deferred_cancel", invoice_id: "inv_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredDependencyWorkflow.CheckGateway, started_snapshot.run_id})
      end)

      deferred_finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(deferred_finished_at, 30, :second)

      assert {:ok, %Snapshot{} = after_gateway} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-cancel-worker-1",
                 now: @read_model_visible_at,
                 finished_at: deferred_finished_at
               )

      assert [%{step: "check_gateway", deferred: %{}}] = after_gateway.scheduled_attempts
      assert Enum.map(after_gateway.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Snapshot{} = cancelled_snapshot} =
               Squidie.cancel(started_snapshot.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(deferred_finished_at, 1, :second)
               )

      assert cancelled_snapshot.status == :cancelled
      refute Enum.any?(cancelled_snapshot.pending_dispatches, &(&1.step == "send_receipt"))

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-cancel-worker-2",
                 now: deferred_visible_at,
                 finished_at: DateTime.add(deferred_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = still_cancelled} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_visible_at
               )

      assert still_cancelled.status == :cancelled
      refute Enum.any?(still_cancelled.pending_dispatches, &(&1.step == "send_receipt"))
      refute Enum.any?(still_cancelled.scheduled_attempts, &(&1.step == "send_receipt"))
    end

    test "dependency workflows keep joins locked when cancelled after a sibling completed during deferral" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 DeferredDependencyWorkflow,
                 :checkout,
                 %{
                   order_id: "order_dependency_deferred_cancel_after_sibling",
                   invoice_id: "inv_cancel_after_sibling"
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      on_exit(fn ->
        :persistent_term.erase({DeferredDependencyWorkflow.CheckGateway, started_snapshot.run_id})
      end)

      deferred_finished_at = DateTime.add(@read_model_visible_at, 1, :second)
      deferred_visible_at = DateTime.add(deferred_finished_at, 30, :second)

      assert {:ok, %Snapshot{} = after_gateway} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-cancel-after-sibling-worker-1",
                 now: @read_model_visible_at,
                 finished_at: deferred_finished_at
               )

      assert [%{step: "check_gateway", deferred: %{}}] = after_gateway.scheduled_attempts
      assert Enum.map(after_gateway.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-cancel-after-sibling-worker-2",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert after_invoice.status == :running
      assert after_invoice.visible_attempts == []
      refute Enum.any?(after_invoice.pending_dispatches, &(&1.step == "send_receipt"))
      refute Enum.any?(after_invoice.scheduled_attempts, &(&1.step == "send_receipt"))

      assert {:ok, %Snapshot{} = cancelled_snapshot} =
               Squidie.cancel(started_snapshot.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 4, :second)
               )

      assert cancelled_snapshot.status == :cancelled
      refute Enum.any?(cancelled_snapshot.pending_dispatches, &(&1.step == "send_receipt"))

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-cancel-after-sibling-worker-3",
                 now: deferred_visible_at,
                 finished_at: DateTime.add(deferred_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = still_cancelled} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: deferred_visible_at
               )

      assert still_cancelled.status == :cancelled
      refute Enum.any?(still_cancelled.pending_dispatches, &(&1.step == "send_receipt"))
      refute Enum.any?(still_cancelled.scheduled_attempts, &(&1.step == "send_receipt"))

      assert %{invoice: %{id: "inv_cancel_after_sibling", status: "open"}} =
               still_cancelled.context

      refute Map.has_key?(still_cancelled.context, :receipt)
    end

    test "execute_next/1 recovers completed compensation by continuing the undo chain" do
      order_id = "order_saga_compensation_recovery"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalSagaRollbackWorkflow,
                 :checkout,
                 %{order_id: order_id},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "authorize_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "capture_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert {:ok, %Snapshot{visible_attempts: [%{step: "compensate:authorize_payment"}]}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 now: DateTime.add(@read_model_visible_at, 4, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 5, :second)
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: DateTime.add(@read_model_visible_at, 6, :second)
               )

      assert attempt.step == "compensate:authorize_payment"

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{voided_payment_authorization: %{order_id: order_id, status: "voided"}},
                 now: DateTime.add(@read_model_visible_at, 7, :second)
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 8, :second)
               )

      refute recovered_snapshot.terminal?
      assert [%{step: "compensate:reserve_inventory"}] = recovered_snapshot.visible_attempts

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned
             ]
    end

    test "inspect, graph, and explanation expose recorded dynamic work metadata" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded()
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [
               %{
                 dynamic_key: "subscription_digest_fanout",
                 status: :recorded,
                 reason: :runtime_fanout,
                 origin: %{
                   runnable_key: @read_model_runnable_key,
                   step: "charge_card",
                   attempt: 1
                 },
                 nodes: [
                   %{
                     id: "deliver_digest:chat_1",
                     action: "digest.deliver",
                     status: :recorded,
                     metadata: %{chat_id: "chat_1", secret: "[REDACTED]"}
                   }
                 ],
                 edges: [
                   %{
                     id: "charge_card:dynamic:deliver_digest:chat_1",
                     from: "charge_card",
                     to: "deliver_digest:chat_1",
                     type: :dynamic,
                     status: :pending
                   }
                 ],
                 metadata: %{source: "subscription_query"},
                 recorded_at: @read_model_visible_at
               }
             ] = snapshot.dynamic_work

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert nodes["deliver_digest:chat_1"].dynamic? == true
      assert nodes["deliver_digest:chat_1"].origin.step == "charge_card"
      assert nodes["deliver_digest:chat_1"].metadata == %{chat_id: "chat_1", secret: "[REDACTED]"}
      assert edges["charge_card:dynamic:deliver_digest:chat_1"].type == :dynamic
      assert graph.dynamic_work == snapshot.dynamic_work

      assert [
               %{
                 dynamic_key: "subscription_digest_fanout",
                 status: :recorded,
                 reason: :runtime_fanout,
                 origin: %{step: "charge_card"},
                 origin_node_id: "charge_card",
                 added_node_ids: ["deliver_digest:chat_1"],
                 added_edge_ids: ["charge_card:dynamic:deliver_digest:chat_1"],
                 node_count: 1,
                 edge_count: 1,
                 recorded_at: @read_model_visible_at
               }
             ] = graph.dynamic_work_overlays

      graph_payload = Squidie.Runs.GraphInspection.to_map(graph)
      assert [%{dynamic_key: "subscription_digest_fanout"}] = graph_payload.dynamic_work
      assert [%{origin_node_id: "charge_card"}] = graph_payload.dynamic_work_overlays

      assert Enum.any?(
               graph_payload.nodes,
               &match?(%{id: "deliver_digest:chat_1", dynamic?: true}, &1)
             )

      assert {:ok, %Diagnostic{} = diagnostic} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert diagnostic.evidence.dynamic_work == snapshot.dynamic_work
      assert diagnostic.details.dynamic_work_count == 1
    end

    test "record_dynamic_work/3 appends validated inspectable dynamic work" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       metadata: %{chat_id: "chat_1", secret: "redacted"}
                     }
                   ],
                   metadata: %{source: "subscription_query"}
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [
               %{
                 dynamic_key: "subscription_digest_fanout",
                 status: :recorded,
                 reason: :runtime_fanout,
                 origin: %{
                   runnable_key: @read_model_runnable_key,
                   step: "charge_card",
                   attempt: 1
                 },
                 nodes: [
                   %{
                     id: "deliver_digest:chat_1",
                     action: "digest.deliver",
                     status: :recorded,
                     metadata: %{chat_id: "chat_1", secret: "[REDACTED]"}
                   }
                 ],
                 edges: [
                   %{
                     id: "charge_card:dynamic:deliver_digest:chat_1",
                     from: "charge_card",
                     to: "deliver_digest:chat_1",
                     type: :dynamic,
                     status: :pending
                   }
                 ],
                 metadata: %{source: "subscription_query"},
                 recorded_at: @read_model_visible_at
               }
             ] = snapshot.dynamic_work
    end

    test "schedule_dynamic_work/3 records dynamic work and dispatches executable nodes" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key, step: "charge_card", attempt_number: 1}] =
               started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false} = after_charge} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      assert charge_key in after_charge.applied_runnable_keys

      assert {:ok, %Snapshot{} = scheduled} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       input: %{subscription_id: "sub_123"},
                       metadata: %{chat_id: "chat_1"}
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}
               )

      assert scheduled.terminal? == false

      assert [
               %{
                 step: "deliver_digest:chat_1",
                 status: :available,
                 input: %{subscription_id: "sub_123"}
               },
               %{step: "send_receipt", status: :available}
             ] = scheduled.visible_attempts

      assert [
               %{
                 dynamic_key: "subscription_digest_fanout",
                 status: :scheduled,
                 nodes: [%{id: "deliver_digest:chat_1", input: %{subscription_id: "sub_123"}}]
               }
             ] = scheduled.dynamic_work

      assert {:ok, %Snapshot{} = duplicate_schedule} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       input: %{subscription_id: "sub_123"},
                       metadata: %{chat_id: "chat_1"}
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}
               )

      assert Enum.any?(
               duplicate_schedule.visible_attempts,
               &match?(%{step: "deliver_digest:chat_1", status: :available}, &1)
             )

      assert {:ok, %Snapshot{terminal?: false} = after_dynamic} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_dynamic",
                 claim_id: "claim_dynamic",
                 claim_token: "token_dynamic",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_dynamic.context.delivered == %{subscription_id: "sub_123"}

      assert {:ok, %Snapshot{terminal?: true, status: :completed} = completed} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_receipt",
                 claim_id: "claim_receipt",
                 claim_token: "token_receipt",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed.context.receipt == %{payment_id: "pay_123", status: "sent"}

      assert Enum.map(completed.attempts, &{&1.step, &1.status, &1.applied?}) == [
               {"charge_card", :completed, true},
               {"deliver_digest:chat_1", :completed, true},
               {"send_receipt", :completed, true}
             ]

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{status: :completed, dynamic?: true} =
               graph.nodes
               |> Map.new(&{&1.id, &1})
               |> Map.fetch!("deliver_digest:chat_1")

      assert Enum.count(graph.nodes, &(&1.id == "deliver_digest:chat_1")) == 1

      assert {:error, {:unsafe_replay, %{steps: [%{step: "deliver_digest:chat_1"}]}}} =
               Squidie.replay(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "schedule_dynamic_work/3 strips unsafe Elixir adapter metadata before persistence" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key}] = started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      registry = %{
        "elixir.run" => [
          module: Squidie.Step.Elixir,
          action_opts: [
            adapters: %{
              "digest.deliver" => [
                module: DynamicElixirAdapters,
                function: :deliver,
                display_name: {DynamicElixirAdapters, :deliver},
                description: "Deliver digest",
                category: DynamicElixirAdapters,
                enabled?: true
              ]
            }
          ]
        ]
      }

      assert {:ok, %Snapshot{} = scheduled} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_elixir",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:elixir",
                       action: "elixir.run",
                       input: %{adapter: "digest.deliver", params: %{subscription_id: "sub_123"}}
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: registry
               )

      assert [%{dynamic_key: "subscription_digest_elixir"}] = scheduled.dynamic_work

      assert {:ok, %{entries: entries}} =
               Journal.load_thread(@read_model_storage, {:run, started.run_id})

      assert %{runnables: [runnable]} =
               entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> List.last()
               |> Map.fetch!(:data)

      assert %{
               action_opts: [
                 adapters: %{
                   "digest.deliver" => %{description: "Deliver digest", enabled?: true}
                 }
               ]
             } = runnable.dynamic_work

      refute inspect(runnable.dynamic_work.action_opts) =~ inspect(DynamicElixirAdapters)
      refute inspect(runnable.dynamic_work.action_opts) =~ "deliver}"
    end

    test "schedule_dynamic_work/3 retries dynamic nodes with persisted retry metadata" do
      dynamic_queue = "dynamic-work-retry"

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key}] = started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = scheduled} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.flaky_deliver",
                       input: %{subscription_id: "sub_123"},
                       retry: [max_attempts: 2]
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 now: @read_model_visible_at,
                 action_registry: %{
                   "digest.flaky_deliver" => ChildDigestWorkflow.FlakyDeliverDigest
                 }
               )

      assert [%{step: "deliver_digest:chat_1", attempt_number: 1, status: :available}] =
               scheduled.visible_attempts

      assert {:ok, %Snapshot{terminal?: false} = retry_scheduled} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 owner_id: "worker_dynamic_1",
                 claim_id: "claim_dynamic_1",
                 claim_token: "token_dynamic_1",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert Enum.any?(
               retry_scheduled.visible_attempts,
               &match?(
                 %{step: "deliver_digest:chat_1", attempt_number: 2, status: :retry_scheduled},
                 &1
               )
             )

      assert {:ok, %Snapshot{terminal?: false} = after_dynamic_retry} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 owner_id: "worker_dynamic_2",
                 claim_id: "claim_dynamic_2",
                 claim_token: "token_dynamic_2",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert after_dynamic_retry.context.delivered == %{subscription_id: "sub_123"}

      assert Enum.map(
               after_dynamic_retry.attempts,
               &{&1.step, &1.status, &1.applied?, &1.attempt_number}
             ) == [
               {"deliver_digest:chat_1", :failed, false, 1},
               {"deliver_digest:chat_1", :completed, true, 2}
             ]

      assert {:ok, %Snapshot{terminal?: true, status: :completed} = completed} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_receipt",
                 claim_id: "claim_receipt",
                 claim_token: "token_receipt",
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert completed.context.delivered == %{subscription_id: "sub_123"}
      assert completed.context.receipt == %{payment_id: "pay_123", status: "sent"}

      assert Enum.map(completed.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) ==
               [
                 {"charge_card", :completed, true, 1},
                 {"send_receipt", :completed, true, 1}
               ]

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert %{status: :completed, dynamic?: true} =
               graph.nodes
               |> Map.new(&{&1.id, &1})
               |> Map.fetch!("deliver_digest:chat_1")
    end

    test "execute_next/1 recovers dynamic retry progression after durable dispatch failure" do
      dynamic_queue = "dynamic-work-retry-recovery"

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key}] = started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = scheduled} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.flaky_deliver",
                       input: %{subscription_id: "sub_123"},
                       retry: [max_attempts: 2]
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 now: @read_model_visible_at,
                 action_registry: %{
                   "digest.flaky_deliver" => ChildDigestWorkflow.FlakyDeliverDigest
                 }
               )

      assert [%{step: "deliver_digest:chat_1", attempt_number: 1, status: :available}] =
               scheduled.visible_attempts

      conflict_storage =
        {FaultInjectingStorage,
         delegate: @read_model_storage,
         conflict_thread_id: Journal.thread_id({:run, started.run_id})}

      assert {:error, :conflict} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: conflict_storage,
                 queue: dynamic_queue,
                 owner_id: "worker_dynamic_1",
                 claim_id: "claim_dynamic_1",
                 claim_token: "token_dynamic_1",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, dynamic_queue})

      retry_key = "#{started.run_id}:deliver_digest:chat_1:2"

      assert Enum.any?(
               dispatch_entries,
               &match?(
                 %{type: :attempt_failed, data: %{retry_runnable_key: ^retry_key}},
                 &1
               )
             )

      assert {:ok, %Snapshot{terminal?: false} = recovered} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 owner_id: "worker_recovery",
                 claim_id: "claim_recovery",
                 claim_token: "token_recovery",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert Enum.any?(
               recovered.visible_attempts,
               &match?(
                 %{step: "deliver_digest:chat_1", attempt_number: 2, status: :retry_scheduled},
                 &1
               )
             )

      assert {:ok, %Snapshot{terminal?: false} = after_dynamic_retry} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: dynamic_queue,
                 owner_id: "worker_dynamic_2",
                 claim_id: "claim_dynamic_2",
                 claim_token: "token_dynamic_2",
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert after_dynamic_retry.context.delivered == %{subscription_id: "sub_123"}
    end

    test "preview and record dynamic work validate node actions through an action registry" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      attrs = %{
        dynamic_key: "subscription_digest_fanout",
        origin: %{
          runnable_key: @read_model_runnable_key,
          step: "charge_card",
          attempt: 1
        },
        nodes: [%{id: "deliver_digest:chat_1", action: "digest.deliver"}]
      }

      registry = %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}

      assert {:ok, %Squidie.Runs.DynamicWorkPreview{}} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: registry
               )

      assert {:error,
              {:invalid_dynamic_work, {:nodes, {:node, 0, {:action, :unknown_action_key}}}}} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{}
               )

      assert {:error,
              {:invalid_dynamic_work, {:nodes, {:node, 0, {:action, :disabled_action_key}}}}} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{
                   "digest.deliver" => %{
                     module: ChildDigestWorkflow.DeliverDigest,
                     enabled?: false
                   }
                 }
               )

      assert {:error,
              {:invalid_dynamic_work,
               {:nodes, {:node, 0, {:action, :incompatible_action_module}}}}} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => String}
               )

      assert {:error,
              {:invalid_dynamic_work, {:nodes, {:node, 0, {:action, :missing_action_key}}}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{attrs | nodes: [%{id: "deliver_digest:chat_1"}]},
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: registry
               )

      assert {:ok, %Snapshot{dynamic_work: [%{dynamic_key: "subscription_digest_fanout"}]}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: registry
               )

      assert {:error,
              {:invalid_dynamic_work, {:nodes, {:node, 0, {:action, :unknown_action_key}}}}} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{}
               )
    end

    test "schedule_dynamic_work/3 requires an action registry" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:error, {:invalid_dynamic_work, {:action_registry, :required}}} =
               Squidie.schedule_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1", action: "digest.deliver"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{dynamic_work: [], planned_runnable_keys: [@read_model_runnable_key]}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "schedule_dynamic_work/3 requires an applied origin attempt" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:error, {:invalid_dynamic_work, {:origin, :unapplied_runnable}}} =
               Squidie.schedule_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1", action: "digest.deliver"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}
               )
    end

    test "schedule_dynamic_work/3 schedules dispatch after committed run-thread conflicts" do
      dynamic_queue = "dynamic-work-conflict"

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key}] = started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      conflict_storage =
        {FaultInjectingStorage,
         delegate: @read_model_storage,
         conflict_thread_id: Journal.thread_id({:run, started.run_id}),
         commit_before_conflict?: true}

      assert {:ok, %Snapshot{} = scheduled} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       input: %{subscription_id: "sub_123"}
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: conflict_storage,
                 queue: dynamic_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}
               )

      assert Enum.any?(
               scheduled.visible_attempts,
               &match?(
                 %{step: "deliver_digest:chat_1", input: %{subscription_id: "sub_123"}},
                 &1
               )
             )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, dynamic_queue})

      assert Enum.any?(
               dispatch_entries,
               &match?(
                 %{type: :run_queued, data: %{run_id: run_id}} when run_id == started.run_id,
                 &1
               )
             )

      assert Enum.any?(
               dispatch_entries,
               &match?(
                 %{type: :attempt_scheduled, data: %{step: "deliver_digest:chat_1"}},
                 &1
               )
             )
    end

    test "schedule_dynamic_work/3 records the dispatch queue before appending run work" do
      dynamic_queue = "dynamic-work-queue-marker"

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 BillingWorkflow,
                 %{payment_id: "pay_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: charge_key}] = started.visible_attempts

      assert {:ok, %Snapshot{terminal?: false}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_charge",
                 claim_id: "claim_charge",
                 claim_token: "token_charge",
                 now: @read_model_visible_at
               )

      failing_storage =
        {FaultInjectingStorage,
         delegate: @read_model_storage,
         fail_append_thread_id: Journal.thread_id({:run, started.run_id})}

      assert {:error, :append_failed} =
               Squidie.schedule_dynamic_work(
                 started.run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: charge_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       input: %{subscription_id: "sub_123"}
                     }
                   ]
                 },
                 read_model: :read_model,
                 journal_storage: failing_storage,
                 queue: dynamic_queue,
                 now: @read_model_visible_at,
                 action_registry: %{"digest.deliver" => ChildDigestWorkflow.DeliverDigest}
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, dynamic_queue})

      assert Enum.any?(
               dispatch_entries,
               &match?(
                 %{type: :run_queued, data: %{run_id: run_id}} when run_id == started.run_id,
                 &1
               )
             )

      refute Enum.any?(
               dispatch_entries,
               &match?(
                 %{type: :attempt_scheduled, data: %{step: "deliver_digest:chat_1"}},
                 &1
               )
             )
    end

    test "preview_dynamic_work/3 validates inspectable dynamic work without appending" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:ok, %Squidie.Runs.DynamicWorkPreview{} = preview} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       metadata: %{chat_id: "chat_1", secret: "redacted"}
                     }
                   ],
                   metadata: %{source: "subscription_query"}
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{
               run_id: @read_model_run_id,
               duplicate?: false,
               recordable?: true,
               origin_node_id: "charge_card",
               added_node_ids: ["deliver_digest:chat_1"],
               added_edge_ids: ["charge_card:dynamic:deliver_digest:chat_1"],
               warnings: [],
               dynamic_work: %{
                 dynamic_key: "subscription_digest_fanout",
                 recorded_at: @read_model_visible_at,
                 status: :recorded,
                 nodes: [
                   %{
                     id: "deliver_digest:chat_1",
                     metadata: %{chat_id: "chat_1", secret: "[REDACTED]"}
                   }
                 ]
               }
             } = preview

      assert Enum.any?(preview.graph.nodes, &(&1.id == "deliver_digest:chat_1" and &1.dynamic?))

      assert [%{dynamic_key: "subscription_digest_fanout", recorded_at: @read_model_visible_at}] =
               preview.graph.dynamic_work

      assert {:ok, %Snapshot{dynamic_work: [], thread_revisions: %{run: 2}}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "preview_dynamic_work/3 orders graph overlay like durable dynamic work projection" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded(%{
          dynamic_key: "alpha_fanout",
          nodes: [%{id: "deliver_digest:chat_alpha", action: "digest.deliver"}]
        })
      ])

      assert {:ok, %Squidie.Runs.DynamicWorkPreview{} = preview} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "zulu_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_zulu", action: "digest.deliver"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert ["alpha_fanout", "zulu_fanout"] =
               Enum.map(preview.graph.dynamic_work, & &1.dynamic_key)

      assert {:ok, %Snapshot{thread_revisions: %{run: 3}, dynamic_work: [_existing]}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "preview_dynamic_work/3 marks exact duplicate dynamic work without appending" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded()
      ])

      assert {:ok, %Squidie.Runs.DynamicWorkPreview{} = preview} =
               Squidie.preview_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       metadata: %{chat_id: "chat_1", secret: "redacted"}
                     }
                   ],
                   metadata: %{source: "subscription_query"}
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{duplicate?: true, dynamic_work: %{dynamic_key: "subscription_digest_fanout"}} =
               preview

      assert %{
               duplicate?: true,
               recordable?: false,
               origin_node_id: "charge_card",
               added_node_ids: [],
               added_edge_ids: [],
               warnings: [:duplicate_dynamic_work]
             } = preview

      assert %{
               duplicate?: true,
               recordable?: false,
               origin_node_id: "charge_card",
               added_node_ids: [],
               added_edge_ids: [],
               warnings: [:duplicate_dynamic_work]
             } = Squidie.Runs.DynamicWorkPreview.to_map(preview)

      assert {:ok, %Snapshot{thread_revisions: %{run: 3}, dynamic_work: [_existing]}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects malformed dynamic work before appending" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:error, {:invalid_dynamic_work, {:dynamic_key, :invalid}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{dynamic_key: :fanout, origin: %{}, nodes: []},
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_dynamic_work, {:origin, :missing_step}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{runnable_key: @read_model_runnable_key, attempt: 1},
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_dynamic_work, {:nodes, :empty}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: []
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_dynamic_work, {:edges, {:unknown_node, "missing"}}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}],
                   edges: [%{id: "edge_1", from: "charge_card", to: "missing"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_dynamic_work, {:origin, :unknown_runnable}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: "run_123:missing:1",
                     step: "missing",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{dynamic_work: []}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects options and node id collisions" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      attrs = %{
        dynamic_key: "fanout",
        origin: %{
          runnable_key: @read_model_runnable_key,
          step: "charge_card",
          attempt: 1
        },
        nodes: [%{id: "deliver_digest:chat_1"}]
      }

      assert {:error, {:invalid_option, {:option, :journal_stroage}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_stroage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_dynamic_work, {:nodes, {:duplicate_existing_id, "charge_card"}}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{attrs | dynamic_key: "declared_collision", nodes: [%{id: "charge_card"}]},
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{dynamic_work: [%{dynamic_key: "fanout"}]} = first_recording} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 repo: Repo,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{dynamic_work: [%{dynamic_key: "fanout"}]} = second_recording} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert first_recording.thread_revisions.run == second_recording.thread_revisions.run

      assert {:error,
              {:invalid_dynamic_work, {:nodes, {:duplicate_existing_id, "deliver_digest:chat_1"}}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{attrs | dynamic_key: "second_fanout"},
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects stale workflow definitions" do
      append_read_model_run_entries([
        read_model_run_started(%{definition_fingerprint: "stale-definition"}),
        read_model_runnables_planned()
      ])

      assert {:error,
              {:invalid_dynamic_work,
               {:definition,
                %{
                  code: "incompatible_workflow_definition",
                  retryable?: false,
                  persisted_definition_fingerprint: "stale-definition"
                }}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects terminal runs before definition validation" do
      append_read_model_run_entries([
        read_model_run_started(%{definition_fingerprint: "stale-definition"}),
        read_model_runnables_planned(),
        read_model_entry!(:run_terminal, %{
          run_id: @read_model_run_id,
          status: :completed,
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:error, {:invalid_dynamic_work, {:run, :terminal}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects node ids that collide with unplanned declared steps" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 JournalConditionalWorkflow,
                 %{account_id: "acct_123", decision: "auto"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{step: "classify"} = runnable] = snapshot.planned_runnables

      assert {:error, {:invalid_dynamic_work, {:nodes, {:duplicate_existing_id, "auto_approve"}}}} =
               Squidie.record_dynamic_work(
                 snapshot.run_id,
                 %{
                   dynamic_key: "future_collision",
                   origin: %{
                     runnable_key: Map.fetch!(runnable, :runnable_key),
                     step: "classify",
                     attempt: Map.fetch!(runnable, :attempt_number)
                   },
                   nodes: [%{id: "auto_approve"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects terminal runs and append conflicts" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_entry!(:run_terminal, %{
          run_id: @read_model_run_id,
          status: :completed,
          occurred_at: @read_model_visible_at
        })
      ])

      attrs = %{
        dynamic_key: "fanout",
        origin: %{
          runnable_key: @read_model_runnable_key,
          step: "charge_card",
          attempt: 1
        },
        nodes: [%{id: "deliver_digest:chat_1"}]
      }

      assert {:error, {:invalid_dynamic_work, {:run, :terminal}}} =
               Squidie.record_dynamic_work(@read_model_run_id, attrs,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{dynamic_work: []}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      {:ok, definition} = Definition.load(BillingWorkflow)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: "run_conflict",
          workflow: @read_model_workflow,
          definition_version: definition.definition_version,
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: "run_conflict",
          occurred_at: @read_model_visible_at,
          runnables: [
            Map.merge(read_model_planned_runnable(), %{
              run_id: "run_conflict",
              runnable_key: "run_conflict:charge_card:1"
            })
          ]
        })
      ])

      conflict_storage =
        {FaultInjectingStorage,
         delegate: @read_model_storage,
         conflict_thread_id: Journal.thread_id({:run, "run_conflict"})}

      assert {:error, :conflict} =
               Squidie.record_dynamic_work(
                 "run_conflict",
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: "run_conflict:charge_card:1",
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: conflict_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 rejects terminal duplicate deliveries" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded(),
        read_model_entry!(:run_terminal, %{
          run_id: @read_model_run_id,
          status: :completed,
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:error, {:invalid_dynamic_work, {:run, :terminal}}} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "subscription_digest_fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   reason: :runtime_fanout,
                   nodes: [
                     %{
                       id: "deliver_digest:chat_1",
                       action: "digest.deliver",
                       metadata: %{chat_id: "chat_1", secret: "redacted"}
                     }
                   ],
                   metadata: %{source: "subscription_query"}
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "record_dynamic_work/3 treats committed conflict retries as idempotent duplicates" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      conflict_storage =
        {FaultInjectingStorage,
         delegate: @read_model_storage,
         conflict_thread_id: Journal.thread_id({:run, @read_model_run_id}),
         commit_before_conflict?: true}

      assert {:ok, %Snapshot{dynamic_work: [%{dynamic_key: "fanout"}]} = snapshot} =
               Squidie.record_dynamic_work(
                 @read_model_run_id,
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: conflict_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert snapshot.thread_revisions.run > 2
    end

    test "record_dynamic_work/3 rejects missing run ids without creating a run thread" do
      assert {:error, :not_found} =
               Squidie.record_dynamic_work(
                 "missing_run",
                 %{
                   dynamic_key: "fanout",
                   origin: %{
                     runnable_key: @read_model_runnable_key,
                     step: "charge_card",
                     attempt: 1
                   },
                   nodes: [%{id: "deliver_digest:chat_1"}]
                 },
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, :not_found} =
               Squidie.inspect_run("missing_run",
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "graph inspection tolerates stale malformed dynamic work facts" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded(%{
          nodes: [
            %{action: "missing.id"},
            %{id: "deliver_digest:chat_2", action: "digest.deliver"}
          ],
          edges: [
            %{id: "missing_to", from: "charge_card", type: :dynamic},
            %{id: "valid_edge", from: "charge_card", to: "deliver_digest:chat_2", type: :dynamic}
          ]
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [%{nodes: [%{id: "deliver_digest:chat_2"}], edges: [%{id: "valid_edge"}]}] =
               snapshot.dynamic_work

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert Enum.any?(graph.nodes, &(&1.id == "deliver_digest:chat_2"))
      assert Enum.any?(graph.edges, &(&1.id == "valid_edge"))
    end

    test "graph inspection drops inferred dynamic edges when origin is incomplete" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded(%{
          origin: %{runnable_key: @read_model_runnable_key, attempt: 1}
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [%{nodes: [%{id: "deliver_digest:chat_1"}], edges: []}] = snapshot.dynamic_work

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert Enum.any?(graph.nodes, &(&1.id == "deliver_digest:chat_1"))
      refute Enum.any?(graph.edges, &(&1.to == "deliver_digest:chat_1"))
    end

    test "dynamic work facts are idempotent and conflicting duplicates become anomalies" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned(),
        read_model_dynamic_work_recorded(),
        read_model_dynamic_work_recorded(),
        read_model_dynamic_work_recorded(%{
          nodes: [
            %{
              id: "deliver_digest:chat_99",
              action: "digest.deliver",
              metadata: %{chat_id: "chat_99"}
            }
          ]
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [%{dynamic_key: "subscription_digest_fanout"}] = snapshot.dynamic_work

      assert [
               %{
                 entry_type: :dynamic_work_recorded,
                 reason: :conflicting_dynamic_work,
                 run_id: @read_model_run_id
               }
             ] = snapshot.anomalies
    end

    test "apply_signal/2 starts journal runs through a durable signal receipt" do
      assert {:ok, %Signal{} = signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_signal_start"},
                 metadata: %{source: "signal_interpreter"},
                 idempotency_key: "start-signal-1",
                 occurred_at: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible

      assert [
               %{
                 signal_type: "start_run",
                 payload: %{
                   workflow: workflow,
                   trigger: "gateway_recovery",
                   input: %{account_id: "acct_signal_start"}
                 },
                 metadata: %{source: "signal_interpreter"},
                 idempotency_key: "start-signal-1",
                 occurred_at: @read_model_started_at
               }
             ] = snapshot.command_history

      assert workflow == Atom.to_string(PaymentRecoveryWorkflow)

      assert [
               %{type: :run_signal_received},
               %{type: :run_started},
               %{type: :runnables_planned}
             ] = raw_run_entries(snapshot.run_id, @read_model_storage)

      command_history = snapshot.command_history
      run_entries = raw_run_entries(snapshot.run_id, @read_model_storage)

      assert {:ok, %Snapshot{} = duplicate_snapshot} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate_snapshot.run_id == snapshot.run_id
      assert duplicate_snapshot.command_history == command_history
      assert raw_run_entries(snapshot.run_id, @read_model_storage) == run_entries
    end

    test "start/3 starts a default-trigger journal run" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_concise_default"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible
    end

    test "start/4 starts a named-trigger journal run" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_concise_named"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible
    end

    test "start/4 works with explicit journal storage when no repo is configured" do
      original_repo = Application.get_env(:squidie, :repo)

      on_exit(fn ->
        if is_nil(original_repo) do
          Application.delete_env(:squidie, :repo)
        else
          Application.put_env(:squidie, :repo, original_repo)
        end
      end)

      Application.delete_env(:squidie, :repo)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_explicit_storage_no_repo"},
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.queue == @read_model_queue
      assert snapshot.reason == :attempt_visible
    end

    test "start/3 rejects invalid default-trigger payloads" do
      assert {:error, {:invalid_payload, :expected_map}} =
               Squidie.start(PaymentRecoveryWorkflow, :invalid_payload,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )
    end

    test "concise convenience arities use configured runtime defaults" do
      put_squidie_config(
        repo: Repo,
        runtime: :journal,
        journal_storage: @read_model_storage,
        queue: @read_model_queue
      )

      assert {:ok, %Snapshot{} = default_trigger_snapshot} =
               Squidie.start(PaymentRecoveryWorkflow, %{account_id: "acct_concise_start"})

      assert {:ok, %Snapshot{} = named_trigger_snapshot} =
               Squidie.start(PaymentRecoveryWorkflow, :gateway_recovery, %{
                 account_id: "acct_concise_trigger"
               })

      assert default_trigger_snapshot.queue == @read_model_queue
      assert named_trigger_snapshot.queue == @read_model_queue

      missing_run_id = Ecto.UUID.generate()
      attrs = %{actor: "ops_123"}

      assert {:error, :not_found} = Squidie.resume(missing_run_id)
      assert {:error, :not_found} = Squidie.resume(missing_run_id, runtime: :journal)
      assert {:error, :not_found} = Squidie.resume(missing_run_id, attrs)
      assert {:error, :not_found} = Squidie.approve(missing_run_id, attrs)
      assert {:error, :not_found} = Squidie.reject(missing_run_id, attrs)
    end

    test "concise control functions preserve existing public error shapes" do
      missing_run_id = Ecto.UUID.generate()
      malformed_run_id = "not-a-uuid"
      attrs = %{actor: "ops_123"}

      opts = [
        runtime: :journal,
        journal_storage: @read_model_storage,
        queue: @read_model_queue
      ]

      assert {:error, :not_found} = Squidie.resume(missing_run_id, attrs, opts)
      assert {:error, :not_found} = Squidie.approve(missing_run_id, attrs, opts)
      assert {:error, :not_found} = Squidie.reject(missing_run_id, attrs, opts)
      assert {:error, :not_found} = Squidie.cancel(missing_run_id, opts)
      assert {:error, :not_found} = Squidie.replay(missing_run_id, opts)

      assert {:error, :invalid_run_id} = Squidie.resume(malformed_run_id, attrs, opts)
      assert {:error, :invalid_run_id} = Squidie.approve(malformed_run_id, attrs, opts)
      assert {:error, :invalid_run_id} = Squidie.reject(malformed_run_id, attrs, opts)
      assert {:error, :invalid_run_id} = Squidie.cancel(malformed_run_id, opts)
      assert {:error, :invalid_run_id} = Squidie.replay(malformed_run_id, opts)
    end

    test "concise API keeps supported runtime names explicit" do
      refute function_exported?(Squidie, :inspect, 2)
      assert function_exported?(Squidie, :start, 2)
      assert function_exported?(Squidie, :start, 3)
      assert function_exported?(Squidie, :start, 4)
      assert function_exported?(Squidie, :resume, 1)
      assert function_exported?(Squidie, :resume, 2)
      assert function_exported?(Squidie, :resume, 3)
      assert function_exported?(Squidie, :approve, 2)
      assert function_exported?(Squidie, :approve, 3)
      assert function_exported?(Squidie, :reject, 2)
      assert function_exported?(Squidie, :reject, 3)
      assert function_exported?(Squidie, :cancel, 1)
      assert function_exported?(Squidie, :cancel, 2)
      assert function_exported?(Squidie, :replay, 1)
      assert function_exported?(Squidie, :replay, 2)
      assert function_exported?(Squidie, :inspect_run, 2)
      assert function_exported?(Squidie, :inspect_run_graph, 2)
      assert function_exported?(Squidie, :explain_run, 2)
    end

    test "start/3 exposes workflow definition version metadata in read models" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 VersionedPaymentRecoveryWorkflow,
                 %{account_id: "acct_versioned"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.definition_version == "2026-05-26.payment-recovery-v2"

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert summary.run_id == snapshot.run_id
      assert summary.definition_version == "2026-05-26.payment-recovery-v2"

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert graph.definition_version == "2026-05-26.payment-recovery-v2"

      assert %{definition_version: "2026-05-26.payment-recovery-v2"} =
               Squidie.Runs.GraphInspection.to_map(graph)

      assert {:ok, %Diagnostic{} = diagnostic} =
               Squidie.explain_run(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert diagnostic.definition_version == "2026-05-26.payment-recovery-v2"
      assert diagnostic.evidence.definition_version == "2026-05-26.payment-recovery-v2"

      assert {:ok, run_entries} =
               load_read_model_run_entries(snapshot.run_id)

      assert %{definition_version: "2026-05-26.payment-recovery-v2"} =
               run_entries
               |> List.first()
               |> Map.fetch!(:data)
    end

    test "start/3 leaves workflow definition version nil when undeclared" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_unversioned"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert snapshot.definition_version == nil

      assert {:ok, [%Summary{} = summary]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert summary.definition_version == nil

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert graph.definition_version == nil

      assert {:ok, %Diagnostic{} = diagnostic} =
               Squidie.explain_run(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert diagnostic.definition_version == nil
      assert diagnostic.evidence.definition_version == nil
    end

    test "start_child_run/4 starts a deterministic child and links it to the parent" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_parent_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      parent_context =
        step_context(parent,
          step: :check_gateway,
          runnable_key: parent_runnable_key,
          state: %{account_id: "acct_parent_child"}
        )

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 metadata: %{subscription_id: "sub_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert child.run_id != parent.run_id
      assert child.workflow == Atom.to_string(ChildDigestWorkflow)

      assert child.parent_run == %{
               run_id: parent.run_id,
               runnable_key: parent_runnable_key,
               step: "check_gateway",
               attempt: 1,
               child_key: "digest_subscription_1",
               metadata: %{subscription_id: "sub_123"}
             }

      assert {:ok, %Snapshot{} = inspected_parent} =
               Squidie.inspect_run(parent.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      child_run_id = child.run_id
      child_workflow = Atom.to_string(ChildDigestWorkflow)

      assert [
               %{
                 child_run_id: ^child_run_id,
                 child_workflow: ^child_workflow,
                 child_trigger: "deliver_digest",
                 child_key: "digest_subscription_1",
                 origin: %{
                   runnable_key: ^parent_runnable_key,
                   step: "check_gateway",
                   attempt: 1
                 },
                 metadata: %{subscription_id: "sub_123"}
               }
             ] = inspected_parent.child_runs
    end

    test "start_child_run/4 is idempotent for duplicate child keys" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_duplicate_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      parent_context =
        step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key)

      child_opts = [
        child_key: "digest_subscription_1",
        runtime: :journal,
        journal_storage: @read_model_storage,
        queue: @read_model_queue,
        now: @read_model_visible_at
      ]

      assert {:ok, %Snapshot{} = first_child} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_opts
               )

      assert {:ok, %Snapshot{} = duplicate_child} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_opts
               )

      assert duplicate_child.run_id == first_child.run_id

      assert {:error, {:invalid_parent_context, :workflow}} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{parent_context | workflow: RepoTransactionWorkflow},
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_opts
               )

      assert {:ok, parent_entries} =
               load_read_model_run_entries(parent.run_id)

      assert 1 ==
               Enum.count(parent_entries, &(&1.type == :child_run_started))
    end

    test "start_child_run/4 uses the child workflow default trigger" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_default_child_trigger"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 %{subscription_id: "sub_default"},
                 child_key: "digest_subscription_default",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert child.trigger == "deliver_digest"

      assert {:ok, %Snapshot{} = inspected_parent} =
               Squidie.inspect_run(parent.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert [%{child_trigger: "deliver_digest"}] = inspected_parent.child_runs
    end

    test "start_child_run/4 rejects missing child keys and terminal parents" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_terminal_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      parent_context =
        step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key)

      assert {:error, {:invalid_option, {:child_key, :missing}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 %{subscription_id: "sub_123"}
               )

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 %{subscription_id: "sub_123"},
                 [:bad]
               )

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 %{subscription_id: "sub_123"},
                 %{child_key: "digest_subscription_1"}
               )

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{child_key: "digest_subscription_1"}
               )

      assert {:error, {:invalid_option, {:child_key, :missing}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_payload, :expected_map}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 :invalid_payload,
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_trigger, :expected_atom}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 "deliver_digest",
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_payload, :expected_map}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 "deliver_digest",
                 :invalid_payload,
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_payload, :expected_map}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :invalid_payload,
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_option, {:child_key, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: %{token: "super-secret-token"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, reason} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 metadata: %{secret: {:token, "super-secret-token"}},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert reason == {:invalid_option, {:metadata, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, {:invalid_option, {:metadata, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 metadata: :invalid,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_option, {:metadata, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 metadata: %{[] => "invalid"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_option, {:now, :invalid}}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: :invalid
               )

      assert {:error, {:invalid_parent_context, :run_id}} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{parent_context | run_id: nil},
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_parent_context, :origin}} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{parent_context | runnable_key: nil},
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_parent_context, :attempt}} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{parent_context | attempt: "one"},
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:ok, %Snapshot{terminal?: true}} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_parent_run, :terminal}} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "child starter validates direct journal boundary inputs" do
      parent_context = %Squidie.Step.Context{
        run_id: Ecto.UUID.generate(),
        workflow: PaymentRecoveryWorkflow,
        step: :check_gateway,
        attempt: 1,
        runnable_key: "parent_run:check_gateway:1",
        state: %{}
      }

      opts = [
        child_key: "digest_subscription_direct",
        journal_storage: @read_model_storage,
        queue: @read_model_queue
      ]

      assert {:error, {:invalid_payload, :expected_map}} =
               Squidie.Runtime.Journal.ChildStarter.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 :invalid_payload,
                 opts
               )

      assert {:error, {:invalid_parent_context, :expected_step_context}} =
               Squidie.Runtime.Journal.ChildStarter.start_child_run(
                 %{},
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 opts
               )
    end

    test "start_child_run/4 accepts atom child keys and storage-safe metadata" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_atom_child_key"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: :digest_subscription_atom,
                 metadata: %{optional: nil, tags: ["digest", nil]},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert child.parent_run.child_key == "digest_subscription_atom"
      assert child.parent_run.metadata == %{optional: nil, tags: ["digest", nil]}
    end

    test "start_child_run/4 returns storage errors while checking child availability" do
      parent_context = %Squidie.Step.Context{
        run_id: Ecto.UUID.generate(),
        workflow: PaymentRecoveryWorkflow,
        step: :check_gateway,
        attempt: 1,
        runnable_key: "parent_run:check_gateway:1",
        state: %{}
      }

      assert {:error, :load_failed} =
               Squidie.start_child_run(
                 parent_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_storage_error"},
                 child_key: "digest_subscription_storage_error",
                 runtime: :journal,
                 journal_storage: CommitThenFailStorage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "start_child_run/4 repairs a missing parent link for an existing child" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_repair_child_link"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      parent_metadata = %{
        run_id: parent.run_id,
        runnable_key: parent_runnable_key,
        step: "check_gateway",
        attempt: 1,
        child_key: child_key,
        metadata: %{subscription_id: "sub_123"}
      }

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{parent: parent_metadata},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 run_id: child_run_id,
                 now: @read_model_visible_at
               )

      assert {:ok, parent_entries_before} =
               load_read_model_run_entries(parent.run_id)

      refute Enum.any?(parent_entries_before, &(&1.type == :child_run_started))

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 metadata: %{subscription_id: "sub_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = repaired_parent} =
               Squidie.inspect_run(parent.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert [%{child_run_id: ^child_run_id}] = repaired_parent.child_runs
    end

    test "start_child_run/4 rejects stale contexts when parent link exists without child" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_stale_linked_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, link_entry} =
               DispatchProtocol.new_entry(:child_run_started, %{
                 run_id: parent.run_id,
                 child_run_id: child_run_id,
                 child_workflow: Atom.to_string(ChildDigestWorkflow),
                 child_trigger: "deliver_digest",
                 child_key: child_key,
                 origin: %{runnable_key: parent_runnable_key, step: "check_gateway", attempt: 1},
                 occurred_at: @read_model_visible_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [link_entry])

      stale_context =
        step_context(parent,
          step: :check_gateway,
          runnable_key: "#{parent.run_id}:check_gateway:stale"
        )

      assert {:error, {:invalid_parent_context, :runnable_key}} =
               Squidie.start_child_run(
                 stale_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:error, :not_found} = Journal.load_thread(@read_model_storage, {:run, child_run_id})
    end

    test "start_child_run/4 rejects terminal parents after the child link exists" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_linked_then_terminal_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, link_entry} =
               DispatchProtocol.new_entry(:child_run_started, %{
                 run_id: parent.run_id,
                 child_run_id: child_run_id,
                 child_workflow: Atom.to_string(ChildDigestWorkflow),
                 child_trigger: "deliver_digest",
                 child_key: child_key,
                 origin: %{runnable_key: parent_runnable_key, step: "check_gateway", attempt: 1},
                 occurred_at: @read_model_visible_at
               })

      assert {:ok, terminal_entry} =
               DispatchProtocol.new_entry(:run_terminal, %{
                 run_id: parent.run_id,
                 status: :cancelled,
                 occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [link_entry, terminal_entry])

      assert {:error, {:invalid_parent_run, :terminal}} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:error, :not_found} = Journal.load_thread(@read_model_storage, {:run, child_run_id})
    end

    test "start_child_run/4 rejects parent links that reuse a child key for another child" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_reused_child_key"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, link_entry} =
               DispatchProtocol.new_entry(:child_run_started, %{
                 run_id: parent.run_id,
                 child_run_id: Ecto.UUID.generate(),
                 child_workflow: Atom.to_string(ChildDigestWorkflow),
                 child_trigger: "deliver_digest",
                 child_key: child_key,
                 origin: %{runnable_key: parent_runnable_key, step: "check_gateway", attempt: 1},
                 occurred_at: @read_model_visible_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [link_entry])

      assert {:error, :conflict} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, parent_entries} =
               load_read_model_run_entries(parent.run_id)

      assert 1 == Enum.count(parent_entries, &(&1.type == :child_run_started))
    end

    test "start_child_run/4 allows the same child key from different parent steps" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_child_key_steps", invoice_id: "inv_child_key_steps"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [
               %{runnable_key: load_account_key, step: "load_account"},
               %{runnable_key: load_invoice_key, step: "load_invoice"}
             ] = Enum.sort_by(parent.visible_attempts, & &1.step)

      child_opts = [
        child_key: "sync",
        runtime: :journal,
        journal_storage: @read_model_storage,
        queue: @read_model_queue,
        now: @read_model_visible_at
      ]

      assert {:ok, %Snapshot{} = account_child} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{
                   run_id: parent.run_id,
                   workflow: JournalDependencyWorkflow,
                   step: :load_account,
                   attempt: 1,
                   runnable_key: load_account_key,
                   state: %{}
                 },
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_account"},
                 child_opts
               )

      assert {:ok, %Snapshot{} = invoice_child} =
               Squidie.start_child_run(
                 %Squidie.Step.Context{
                   run_id: parent.run_id,
                   workflow: JournalDependencyWorkflow,
                   step: :load_invoice,
                   attempt: 1,
                   runnable_key: load_invoice_key,
                   state: %{}
                 },
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_invoice"},
                 Keyword.put(child_opts, :now, DateTime.add(@read_model_visible_at, 1, :second))
               )

      assert account_child.run_id != invoice_child.run_id
      assert account_child.parent_run.child_key == "sync"
      assert account_child.parent_run.step == "load_account"
      assert invoice_child.parent_run.child_key == "sync"
      assert invoice_child.parent_run.step == "load_invoice"
    end

    test "start_child_run/4 reuses string-keyed persisted parent links" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_string_keyed_child_link"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      link_entry = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :child_run_started,
        thread: {:run, parent.run_id},
        data: %{
          "run_id" => parent.run_id,
          "child_run_id" => child_run_id,
          "child_workflow" => Atom.to_string(ChildDigestWorkflow),
          "child_trigger" => "deliver_digest",
          "child_key" => child_key,
          "origin" => %{
            "runnable_key" => parent_runnable_key,
            "step" => "check_gateway",
            "attempt" => 1
          },
          "metadata" => %{"subscription_id" => "sub_123"}
        },
        occurred_at: @read_model_visible_at
      }

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [link_entry])

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert child.run_id == child_run_id

      assert child.parent_run == %{
               run_id: parent.run_id,
               runnable_key: parent_runnable_key,
               step: "check_gateway",
               attempt: 1,
               child_key: child_key,
               metadata: %{"subscription_id" => "sub_123"}
             }
    end

    test "start_child_run/4 rejects an existing child with matching input but no parent lineage" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_orphaned_child_conflict"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start(
                 ChildDigestWorkflow,
                 %{subscription_id: "sub_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 run_id: child_run_id,
                 now: @read_model_visible_at
               )

      assert {:error, :conflict} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "start_child_run/4 returns conflict when parent link repair keeps conflicting" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_child_link_conflict_retry"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: parent.run_id,
                     runnable_key: parent_runnable_key,
                     step: "check_gateway",
                     attempt: 1,
                     child_key: child_key
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 run_id: child_run_id,
                 now: @read_model_visible_at
               )

      assert {:error, :conflict} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage:
                   {FaultInjectingStorage,
                    delegate: @read_model_storage,
                    conflict_thread_id: "squidie:run:#{parent.run_id}"},
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "start_child_run/4 returns append errors while linking existing children" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_child_link_append_error"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: parent.run_id,
                     runnable_key: parent_runnable_key,
                     step: "check_gateway",
                     attempt: 1,
                     child_key: child_key
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 run_id: child_run_id,
                 now: @read_model_visible_at
               )

      assert {:error, :append_failed} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage:
                   {FaultInjectingStorage,
                    delegate: @read_model_storage,
                    fail_append_thread_id: "squidie:run:#{parent.run_id}"},
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "start_child_run/4 rejects malformed existing child links for the same child" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_malformed_same_child_link"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      malformed_link = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :child_run_started,
        thread: {:run, parent.run_id},
        data: %{child_run_id: child_run_id, child_key: child_key},
        occurred_at: @read_model_visible_at
      }

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [malformed_link])

      assert {:error, :conflict} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "start_child_run/4 ignores malformed origins when checking child key reuse" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_malformed_origin_child_key"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      malformed_origin_link = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :child_run_started,
        thread: {:run, parent.run_id},
        data: %{
          run_id: parent.run_id,
          child_run_id: Ecto.UUID.generate(),
          child_workflow: Atom.to_string(ChildDigestWorkflow),
          child_trigger: "deliver_digest",
          child_key: child_key,
          origin: "legacy-origin"
        },
        occurred_at: @read_model_visible_at
      }

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [malformed_origin_link])

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert child.parent_run.child_key == child_key
    end

    test "cancel/2 rejects parents with linked children that have not started" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_cancel_during_child_start"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, link_entry} =
               DispatchProtocol.new_entry(:child_run_started, %{
                 run_id: parent.run_id,
                 child_run_id: child_run_id,
                 child_workflow: Atom.to_string(ChildDigestWorkflow),
                 child_trigger: "deliver_digest",
                 child_key: child_key,
                 origin: %{runnable_key: parent_runnable_key, step: "check_gateway", attempt: 1},
                 occurred_at: @read_model_visible_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [link_entry])

      assert {:error, {:invalid_transition, :child_starting, :cancelling}} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:error, :not_found} = Journal.load_thread(@read_model_storage, {:run, child_run_id})
    end

    test "cancel/2 allows parents after linked children have started" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_cancel_after_child_started"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_cancel_started"},
                 child_key: "digest_subscription_cancel_started",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, _thread} = Journal.load_thread(@read_model_storage, {:run, child.run_id})

      assert {:ok, %Snapshot{terminal?: true, status: :cancelled}} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "cancel/2 rejects malformed checkpoint child links without raising" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_malformed_child_checkpoint"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, thread} = Journal.load_thread(@read_model_storage, {:run, parent.run_id})

      projection =
        thread.entries
        |> Squidie.Runtime.WorkflowAgent.Projection.rebuild()
        |> Map.put(:child_runs, [
          %{"child_run_id" => child_run_id, "child_key" => child_key}
        ])

      assert :ok =
               Journal.put_checkpoint(
                 @read_model_storage,
                 {:run, parent.run_id},
                 projection,
                 thread.rev,
                 updated_at: @read_model_visible_at
               )

      assert {:error, {:invalid_transition, :child_starting, :cancelling}} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "cancel/2 rejects checkpoint child links without child run ids" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_missing_child_id_checkpoint"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, thread} = Journal.load_thread(@read_model_storage, {:run, parent.run_id})

      projection =
        thread.entries
        |> Squidie.Runtime.WorkflowAgent.Projection.rebuild()
        |> Map.put(:child_runs, [%{child_key: "digest_subscription_1"}])

      assert :ok =
               Journal.put_checkpoint(
                 @read_model_storage,
                 {:run, parent.run_id},
                 projection,
                 thread.rev,
                 updated_at: @read_model_visible_at
               )

      assert {:error, {:invalid_transition, :child_starting, :cancelling}} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "cancel/2 returns storage errors while checking linked children" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_child_load_error_checkpoint"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      child_run_id = Ecto.UUID.generate()

      assert {:ok, thread} = Journal.load_thread(@read_model_storage, {:run, parent.run_id})

      projection =
        thread.entries
        |> Squidie.Runtime.WorkflowAgent.Projection.rebuild()
        |> Map.put(:child_runs, [
          %{child_run_id: child_run_id, child_key: "digest_subscription_1"}
        ])

      assert :ok =
               Journal.put_checkpoint(
                 @read_model_storage,
                 {:run, parent.run_id},
                 projection,
                 thread.rev,
                 updated_at: @read_model_visible_at
               )

      assert {:error, :load_failed} =
               Squidie.cancel(parent.run_id,
                 runtime: :journal,
                 journal_storage:
                   {FaultInjectingStorage,
                    delegate: @read_model_storage,
                    fail_load_thread_id: "squidie:run:#{child_run_id}"},
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "start_run_with_initial_context/5 rejects unsafe parent context" do
      assert {:error, reason} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     token: "super-secret-token",
                     unsafe: {:tuple, "super-secret-token"}
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert reason == {:invalid_initial_context, {:parent, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "start_run_with_initial_context/5 validates malformed parent context shapes" do
      parent_run_id = Ecto.UUID.generate()

      assert {:error, {:invalid_initial_context, {:parent, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{parent: "invalid"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_initial_context, {:parent, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: parent_run_id,
                     runnable_key: "#{parent_run_id}:check_gateway:1",
                     step: "check_gateway",
                     attempt: 1,
                     child_key: "digest_subscription_1",
                     metadata: %{unsafe: {:tuple, "invalid"}}
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_initial_context, {:parent, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: nil,
                     runnable_key: "#{parent_run_id}:check_gateway:1",
                     step: "check_gateway",
                     attempt: 1,
                     child_key: "digest_subscription_1"
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_initial_context, {:parent, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: parent_run_id,
                     runnable_key: "#{parent_run_id}:check_gateway:1",
                     step: "check_gateway",
                     attempt: 1,
                     child_key: "digest_subscription_1",
                     metadata: "invalid"
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_initial_context, {:parent, :invalid}}} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   parent: %{
                     run_id: parent_run_id,
                     runnable_key: "#{parent_run_id}:check_gateway:1",
                     step: "check_gateway",
                     attempt: 1,
                     child_key: "digest_subscription_1",
                     metadata: %{[] => "invalid"}
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "journal starter ignores non-map initial context at the internal boundary" do
      assert {:ok, %Snapshot{} = child} =
               Squidie.Runtime.Journal.Starter.start_run(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_non_map_context"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 initial_context: :invalid,
                 now: @read_model_visible_at
               )

      assert child.parent_run == nil
    end

    test "start_run_with_initial_context/5 canonicalizes parent context" do
      parent_run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 %{
                   "parent" => %{
                     "run_id" => parent_run_id,
                     "runnable_key" => "#{parent_run_id}:check_gateway:1",
                     "step" => "check_gateway",
                     "attempt" => 1,
                     "child_key" => "digest_subscription_1"
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert child.parent_run == %{
               run_id: parent_run_id,
               runnable_key: "#{parent_run_id}:check_gateway:1",
               step: "check_gateway",
               attempt: 1,
               child_key: "digest_subscription_1",
               metadata: %{}
             }

      assert {:ok, %Snapshot{} = child_with_metadata} =
               Squidie.start_run_with_initial_context(
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_456"},
                 %{
                   parent: %{
                     run_id: parent_run_id,
                     runnable_key: "#{parent_run_id}:check_gateway:2",
                     step: "check_gateway",
                     attempt: 2,
                     child_key: "digest_subscription_2",
                     metadata: %{optional: nil, tags: ["digest", nil]}
                   }
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert child_with_metadata.parent_run.metadata == %{optional: nil, tags: ["digest", nil]}
    end

    test "start_child_run/4 rejects conflicting existing children before linking parent" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_conflicting_child"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts
      child_key = "digest_subscription_1"

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent.run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, %Snapshot{run_id: ^child_run_id}} =
               Squidie.start(
                 ChildDigestWorkflow,
                 %{subscription_id: "conflicting_sub"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 run_id: child_run_id,
                 now: @read_model_visible_at
               )

      assert {:error, :conflict} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, parent_entries} =
               load_read_model_run_entries(parent.run_id)

      refute Enum.any?(parent_entries, &(&1.type == :child_run_started))
    end

    test "replay/2 does not copy source child links" do
      assert {:ok, %Snapshot{} = parent} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_replay_child_parent"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: parent_runnable_key}] = parent.visible_attempts

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 step_context(parent, step: :check_gateway, runnable_key: parent_runnable_key),
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: "digest_subscription_1",
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{child_runs: [%{child_run_id: child_run_id}]}} =
               Squidie.inspect_run(parent.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert child_run_id == child.run_id

      assert {:ok, %Snapshot{} = replay} =
               Squidie.replay(parent.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert replay.run_id != parent.run_id
      assert replay.replayed_from_run_id == parent.run_id
      assert replay.child_runs == []
      assert replay.parent_run == nil
    end

    test "start_child_run/4 keeps child identity stable across parent retry runnable keys" do
      parent_run_id = Ecto.UUID.generate()
      first_runnable_key = "#{parent_run_id}:check_gateway:1"
      retry_runnable_key = "#{parent_run_id}:check_gateway:2"
      child_key = "digest_subscription_1"

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: parent_run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 occurred_at: @read_model_started_at
               })

      assert {:ok, runnables_planned} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: parent_run_id,
                 runnables: [
                   journal_start_runnable(parent_run_id),
                   %{
                     journal_start_runnable(parent_run_id)
                     | runnable_key: retry_runnable_key,
                       idempotency_key: retry_runnable_key,
                       attempt_number: 2
                   }
                 ],
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [run_started, runnables_planned])

      first_context =
        %Squidie.Step.Context{
          run_id: parent_run_id,
          workflow: PaymentRecoveryWorkflow,
          step: :check_gateway,
          attempt: 1,
          runnable_key: first_runnable_key,
          state: %{}
        }

      retry_context =
        %Squidie.Step.Context{
          first_context
          | attempt: 2,
            runnable_key: retry_runnable_key
        }

      child_opts = [
        child_key: child_key,
        runtime: :journal,
        journal_storage: @read_model_storage,
        queue: @read_model_queue,
        now: @read_model_visible_at
      ]

      assert {:ok, %Snapshot{} = first_child} =
               Squidie.start_child_run(
                 first_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_opts
               )

      assert {:ok, %Snapshot{} = retry_child} =
               Squidie.start_child_run(
                 retry_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_opts
               )

      assert retry_child.run_id == first_child.run_id

      assert {:ok, %Snapshot{} = inspected_parent} =
               Squidie.inspect_run(parent_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert [%{child_run_id: child_run_id}] = inspected_parent.child_runs
      assert child_run_id == first_child.run_id
    end

    test "start_child_run/4 uses the persisted parent link for retry after linked crash" do
      parent_run_id = Ecto.UUID.generate()
      first_runnable_key = "#{parent_run_id}:check_gateway:1"
      retry_runnable_key = "#{parent_run_id}:check_gateway:2"
      child_key = "digest_subscription_1"

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: parent_run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 occurred_at: @read_model_started_at
               })

      assert {:ok, runnables_planned} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: parent_run_id,
                 runnables: [
                   journal_start_runnable(parent_run_id),
                   %{
                     journal_start_runnable(parent_run_id)
                     | runnable_key: retry_runnable_key,
                       idempotency_key: retry_runnable_key,
                       attempt_number: 2
                   }
                 ],
                 occurred_at: @read_model_started_at
               })

      assert {:ok, child_run_id} =
               Squidie.Runtime.ScheduleIdentity.run_id(
                 Atom.to_string(ChildDigestWorkflow),
                 "deliver_digest",
                 Enum.join([parent_run_id, "check_gateway", child_key], "|")
               )

      assert {:ok, link_entry} =
               DispatchProtocol.new_entry(:child_run_started, %{
                 run_id: parent_run_id,
                 child_run_id: child_run_id,
                 child_workflow: Atom.to_string(ChildDigestWorkflow),
                 child_trigger: "deliver_digest",
                 child_key: child_key,
                 origin: %{runnable_key: first_runnable_key, step: "check_gateway", attempt: 1},
                 metadata: %{subscription_id: "sub_123"},
                 occurred_at: @read_model_visible_at
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [
                 run_started,
                 runnables_planned,
                 link_entry
               ])

      retry_context = %Squidie.Step.Context{
        run_id: parent_run_id,
        workflow: PaymentRecoveryWorkflow,
        step: :check_gateway,
        attempt: 2,
        runnable_key: retry_runnable_key,
        state: %{}
      }

      assert {:ok, %Snapshot{} = child} =
               Squidie.start_child_run(
                 retry_context,
                 ChildDigestWorkflow,
                 :deliver_digest,
                 %{subscription_id: "sub_123"},
                 child_key: child_key,
                 metadata: %{subscription_id: "sub_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert child.parent_run.runnable_key == first_runnable_key
      assert child.parent_run.attempt == 1
    end

    test "list_runs/2 lists journal runs for one workflow newest first" do
      assert {:ok, %Snapshot{} = older_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_older"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = newer_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_newer"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = first, %Summary{} = second]} =
               Squidie.list_runs([workflow: PaymentRecoveryWorkflow],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [first.run_id, second.run_id] == [newer_run.run_id, older_run.run_id]
      assert Enum.all?([first, second], &(&1.workflow == Atom.to_string(PaymentRecoveryWorkflow)))
      assert Enum.all?([first, second], &(&1.queue == @read_model_queue))
      assert Enum.all?([first, second], &(&1.status == :running))
    end

    test "list_runs/2 lists journal runs across workflows from the global catalog" do
      assert {:ok, %Snapshot{} = payment_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_payment"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = approval_run} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_approval"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = first, %Summary{} = second]} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "caller-default-queue",
                 now: @read_model_visible_at
               )

      assert [first.run_id, second.run_id] == [approval_run.run_id, payment_run.run_id]

      assert [first.workflow, second.workflow] == [
               Atom.to_string(ApprovalWorkflow),
               Atom.to_string(PaymentRecoveryWorkflow)
             ]

      assert {:ok, [%Summary{} = filtered]} =
               Squidie.list_runs([workflow: PaymentRecoveryWorkflow],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert filtered.run_id == payment_run.run_id
      assert filtered.workflow == Atom.to_string(PaymentRecoveryWorkflow)
    end

    test "list_runs/2 applies journal status and limit filters after rebuilding snapshots" do
      assert {:ok, %Snapshot{} = completed_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_completed"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "list_runs_worker",
                 claim_id: "list_runs_claim",
                 claim_token: "list_runs_token",
                 now: @read_model_visible_at
               )

      assert {:ok, _running_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_running"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, [%Summary{} = listed_run]} =
               Squidie.list_runs(
                 [workflow: PaymentRecoveryWorkflow, status: :completed, limit: 1],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert listed_run.run_id == completed_run.run_id
      assert listed_run.status == :completed
    end

    test "list_runs/2 uses the queue recorded in each run catalog fact" do
      first_queue = "journal-list-first-queue"
      second_queue = "journal-list-second-queue"

      assert {:ok, %Snapshot{} = first_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_first_queue"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: first_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = second_run} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_second_queue"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: second_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, listed_runs} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "caller-default-queue"
               )

      listed_payment_runs =
        Enum.filter(listed_runs, &(&1.workflow == Atom.to_string(PaymentRecoveryWorkflow)))

      assert [%Summary{} = listed_second, %Summary{} = listed_first | _older_runs] =
               listed_payment_runs

      assert {listed_second.run_id, listed_second.queue} == {second_run.run_id, second_queue}
      assert {listed_first.run_id, listed_first.queue} == {first_run.run_id, first_queue}

      assert {:ok, %Snapshot{} = inspected} =
               Squidie.inspect_run(listed_second.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: listed_second.queue,
                 now: @read_model_visible_at
               )

      assert inspected.run_id == listed_second.run_id
      assert inspected.queue == listed_second.queue
      assert [%{step: "check_gateway", status: :available}] = inspected.visible_attempts

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(listed_second.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: listed_second.queue,
                 now: @read_model_visible_at
               )

      assert graph.run_id == listed_second.run_id
      assert Enum.map(graph.nodes, &{&1.id, &1.status}) == [{"check_gateway", :pending}]
    end

    test "cancel/2 cancels a visible journal run and fences dispatch" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{step: "check_gateway", status: :available}] = started.visible_attempts

      assert {:ok, %Snapshot{} = cancelled} =
               Squidie.cancel(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert cancelled.run_id == started.run_id
      assert cancelled.status == :cancelled
      assert cancelled.terminal?
      assert cancelled.terminal_status == :cancelled
      assert cancelled.visible_attempts == []

      assert [
               %{signal_type: "start_run"},
               %{signal_type: "cancel_run", payload: %{run_id: cancelled_run_id}}
             ] = cancelled.command_history

      assert cancelled_run_id == started.run_id

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )
    end

    test "signal interpreter cancels journal runs through a durable signal receipt" do
      cancelled_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_signal_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      run_id = started.run_id

      assert {:ok, %Signal{} = signal} =
               Signal.cancel_run(run_id,
                 metadata: %{source: "ops_console"},
                 idempotency_key: "cancel-signal-1",
                 occurred_at: cancelled_at
               )

      assert {:ok, %Snapshot{} = cancelled} =
               SignalInterpreter.apply(signal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert cancelled.status == :cancelled

      assert [
               %{signal_type: "start_run"},
               %{
                 signal_type: "cancel_run",
                 payload: %{run_id: ^run_id},
                 metadata: %{source: "ops_console"},
                 idempotency_key: "cancel-signal-1",
                 occurred_at: ^cancelled_at
               }
             ] = cancelled.command_history

      run_entries = raw_run_entries(run_id, @read_model_storage)

      assert Enum.map(run_entries, & &1.type) == [
               :run_signal_received,
               :run_started,
               :runnables_planned,
               :run_signal_received,
               :run_terminal
             ]
    end

    test "apply_signal/2 applies public Squidie control signals" do
      cancelled_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_public_signal_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      run_id = started.run_id

      assert {:ok, %Signal{} = signal} =
               Signal.cancel_run(run_id,
                 metadata: %{source: "public_signal_test"},
                 idempotency_key: "public-cancel-signal-1",
                 occurred_at: cancelled_at
               )

      assert {:ok, %Snapshot{} = cancelled} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert cancelled.status == :cancelled

      command_history_before = cancelled.command_history
      run_entries_before = raw_run_entries(run_id, @read_model_storage)

      assert {:ok, %Snapshot{} = duplicate_cancelled} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate_cancelled.status == :cancelled
      assert duplicate_cancelled.command_history == command_history_before
      assert raw_run_entries(run_id, @read_model_storage) == run_entries_before

      assert [
               %{signal_type: "start_run"},
               %{
                 signal_type: "cancel_run",
                 payload: %{run_id: ^run_id},
                 metadata: %{source: "public_signal_test"},
                 idempotency_key: "public-cancel-signal-1",
                 occurred_at: ^cancelled_at
               }
             ] = cancelled.command_history
    end

    test "public control helpers forward signal metadata and idempotency keys" do
      occurred_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_public_cancel_metadata"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = cancelled} =
               Squidie.cancel(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 metadata: %{source: "ops_api"},
                 idempotency_key: "public-cancel-helper-1",
                 now: occurred_at
               )

      assert [
               %{signal_type: "start_run"},
               %{
                 signal_type: "cancel_run",
                 metadata: %{source: "ops_api"},
                 idempotency_key: "public-cancel-helper-1",
                 occurred_at: ^occurred_at
               }
             ] = cancelled.command_history
    end

    test "apply_signal/2 starts runs from runtime signals idempotently" do
      occurred_at = DateTime.add(@read_model_started_at, 1, :second)

      assert {:ok, %Signal{} = signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_start_signal"},
                 metadata: %{source: "agent_router"},
                 idempotency_key: "start-signal-1",
                 occurred_at: occurred_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert started.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert started.trigger == "gateway_recovery"
      assert started.input == %{account_id: "acct_start_signal"}

      assert [
               %{
                 signal_type: "start_run",
                 metadata: %{source: "agent_router"},
                 idempotency_key: "start-signal-1",
                 occurred_at: ^occurred_at
               }
             ] = started.command_history

      assert {:ok, %Snapshot{} = duplicate} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate.run_id == started.run_id
      assert duplicate.command_history == started.command_history
    end

    test "starter rejects unsupported command signal types before writing history" do
      run_id = Ecto.UUID.generate()
      occurred_at = DateTime.add(@read_model_started_at, 1, :second)

      invalid_command_signal = %Signal{
        type: :cancel_run,
        payload: %{run_id: Ecto.UUID.generate()},
        metadata: %{source: "invalid_command_test"},
        idempotency_key: "invalid-command-start-signal",
        occurred_at: occurred_at
      }

      assert {:error, {:unsupported_command_signal, :cancel_run}} =
               Squidie.Runtime.Journal.Starter.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_invalid_command_signal_type"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: occurred_at,
                 run_id: run_id,
                 command_signal: invalid_command_signal
               )

      assert {:error, :not_found} = Journal.load_entries(@read_model_storage, {:run, run_id})
    end

    test "apply_signal/2 starts default-trigger runtime signals idempotently" do
      assert {:ok, %Signal{} = signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 nil,
                 %{account_id: "acct_default_start_signal"},
                 idempotency_key: "default-start-signal-1",
                 occurred_at: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert started.trigger == "gateway_recovery"

      assert {:ok, %Snapshot{} = duplicate} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate.run_id == started.run_id
      assert duplicate.command_history == started.command_history
    end

    test "apply_signal/2 treats nil and explicit default triggers as the same idempotent start" do
      assert {:ok, %Signal{} = default_signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 nil,
                 %{account_id: "acct_default_start_alias"},
                 idempotency_key: "default-start-signal-alias",
                 occurred_at: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(default_signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:ok, %Signal{} = explicit_signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_default_start_alias"},
                 idempotency_key: "default-start-signal-alias",
                 occurred_at: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:ok, %Snapshot{} = duplicate} =
               Squidie.apply_signal(explicit_signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate.run_id == started.run_id
      assert duplicate.command_history == started.command_history
    end

    test "apply_signal/2 rejects duplicate start idempotency keys with different payloads" do
      assert {:ok, %Signal{} = signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_start_signal_original"},
                 idempotency_key: "start-signal-conflict",
                 occurred_at: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:ok, %Signal{} = conflicting_signal} =
               Signal.start_run(
                 PaymentRecoveryWorkflow,
                 :gateway_recovery,
                 %{account_id: "acct_start_signal_conflict"},
                 idempotency_key: "start-signal-conflict",
                 occurred_at: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert {:error, :conflict} =
               Squidie.apply_signal(conflicting_signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:ok, inspected} =
               Squidie.inspect_run(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 include_history: true
               )

      assert inspected.command_history == started.command_history
    end

    test "apply_signal/2 starts cron runs from runtime signals idempotently" do
      occurred_at = DateTime.add(@read_model_started_at, 2, :second)

      input = %{
        signal_id: "cron-signal-1",
        intended_window: %{
          start_at: "2026-05-15T09:00:00Z",
          end_at: "2026-05-15T10:00:00Z"
        }
      }

      assert {:ok, %Signal{} = signal} =
               Signal.start_cron(
                 IdempotentScheduledContextWorkflow,
                 :scheduled_capture,
                 input,
                 metadata: %{source: "jido_scheduler"},
                 occurred_at: occurred_at
               )

      assert {:ok, %Snapshot{} = started} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert started.trigger == "scheduled_capture"
      assert started.input == input
      assert started.context.schedule.signal_id == "cron-signal-1"
      assert started.context.schedule.idempotency_key == "cron-signal-1"

      assert [
               %{
                 signal_type: "start_cron",
                 metadata: %{source: "jido_scheduler"},
                 idempotency_key: "cron-signal-1",
                 occurred_at: ^occurred_at
               }
             ] = started.command_history

      assert {:ok, %Snapshot{} = duplicate} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate.run_id == started.run_id
      assert duplicate.command_history == started.command_history
    end

    test "apply_signal/2 replays runs from runtime signals" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_replay_signal"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      occurred_at = DateTime.add(@read_model_started_at, 3, :second)

      assert {:ok, %Signal{} = signal} =
               Signal.replay_run(source.run_id,
                 metadata: %{source: "agent_router"},
                 idempotency_key: "replay-signal-1",
                 occurred_at: occurred_at
               )

      assert {:ok, %Snapshot{} = replayed} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert replayed.run_id != source.run_id
      assert replayed.replayed_from_run_id == source.run_id
      assert replayed.input == source.input

      assert [
               %{
                 signal_type: "replay_run",
                 metadata: %{source: "agent_router"},
                 idempotency_key: "replay-signal-1",
                 occurred_at: ^occurred_at,
                 payload: %{run_id: replayed_from_run_id, allow_irreversible: false}
               }
             ] = replayed.command_history

      assert replayed_from_run_id == source.run_id
    end

    test "manual control signals are idempotent for duplicate delivery" do
      manual_signal_cases = [
        {:resume_run, PauseWorkflow, :resume_run, "resume-signal-duplicate-1", :running},
        {:approve_run, ApprovalWorkflow, :approve_run, "approve-signal-duplicate-1", :running},
        {:reject_run, ApprovalWorkflow, :reject_run, "reject-signal-duplicate-1", :running}
      ]

      for {label, workflow, signal_type, idempotency_key, expected_status} <- manual_signal_cases do
        run_id = Ecto.UUID.generate()
        occurred_at = DateTime.add(@read_model_visible_at, 2, :second)

        assert {:ok, %Snapshot{}} =
                 Squidie.start(
                   workflow,
                   %{account_id: "acct_#{label}"},
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   now: @read_model_started_at,
                   run_id: run_id
                 )

        assert {:ok, %Snapshot{status: :paused}} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "journal-#{label}-duplicate-test",
                   claim_id: "claim_#{label}_duplicate",
                   claim_token: "token_#{label}_duplicate",
                   now: @read_model_visible_at,
                   finished_at: @read_model_visible_at
                 )

        attrs = %{actor: "ops_123", comment: "#{label} requested"}

        assert {:ok, %Signal{} = signal} =
                 apply(Signal, signal_type, [
                   run_id,
                   attrs,
                   [
                     metadata: %{source: "ops_console"},
                     idempotency_key: idempotency_key,
                     occurred_at: occurred_at
                   ]
                 ])

        assert {:ok, %Snapshot{} = applied} =
                 SignalInterpreter.apply(signal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue
                 )

        command_history_before = applied.command_history
        run_entries_before = raw_run_entries(run_id, @read_model_storage)

        assert {:ok, %Snapshot{} = duplicate} =
                 SignalInterpreter.apply(signal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue
                 )

        assert duplicate.status == expected_status
        assert duplicate.command_history == command_history_before
        assert raw_run_entries(run_id, @read_model_storage) == run_entries_before

        assert {:ok, %Signal{} = distinct_signal} =
                 apply(Signal, signal_type, [
                   run_id,
                   attrs,
                   [
                     metadata: %{source: "ops_console"},
                     idempotency_key: "#{idempotency_key}-distinct",
                     occurred_at: DateTime.add(occurred_at, 1, :second)
                   ]
                 ])

        assert {:error, {:invalid_transition, _status, :running}} =
                 SignalInterpreter.apply(distinct_signal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue
                 )

        assert raw_run_entries(run_id, @read_model_storage) == run_entries_before
      end
    end

    test "manual control signals without idempotency keys are not repaired as duplicates" do
      run_id = Ecto.UUID.generate()
      occurred_at = DateTime.add(@read_model_visible_at, 2, :second)

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_resume_without_signal_key"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-resume-no-key-test",
                 claim_id: "claim_resume_no_key",
                 claim_token: "token_resume_no_key",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Signal{} = signal} =
               Signal.resume_run(
                 run_id,
                 %{actor: "ops_123", comment: "resume requested"},
                 metadata: %{source: "ops_console"},
                 occurred_at: occurred_at
               )

      assert signal.idempotency_key == nil

      assert {:ok, %Snapshot{status: :running}} =
               SignalInterpreter.apply(signal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:error, {:invalid_transition, _status, :running}} =
               SignalInterpreter.apply(signal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "apply_signal/2 rejects malformed signal application requests" do
      assert {:error, :invalid_signal} = Squidie.apply_signal(%{})

      assert {:ok, %Signal{} = signal} = Signal.cancel_run(Ecto.UUID.generate())

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.apply_signal(signal, :bad_opts)
    end

    test "cancel/2 rejects stale claim completions after journal cancellation" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_stale_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} = DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok,
              %{
                agent: claimed_dispatch_agent,
                attempt: %{runnable_key: runnable_key},
                claim_id: claim_id,
                claim_token: claim_token
              }} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{status: :cancelled}} =
               Squidie.cancel(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:error, :terminal_run} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_dispatch_agent,
                 runnable_key,
                 claim_id,
                 claim_token,
                 %{status: "late"},
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )
    end

    test "cancel/2 fences cancellation between claim and step execution" do
      parent = self()

      on_exit(fn -> :persistent_term.erase(:journal_gateway_run_hook) end)
      :persistent_term.put(:journal_gateway_run_hook, fn -> send(parent, :gateway_step_ran) end)

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_claim_cancel"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      test_after_claim = fn %{run_id: run_id} ->
        send(parent, :after_claim)

        assert {:ok, %Snapshot{status: :cancelled}} =
                 Squidie.cancel(run_id,
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   now: DateTime.add(@read_model_visible_at, 1, :second)
                 )

        :ok
      end

      assert {:ok, %Snapshot{} = cancelled} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 test_after_claim: test_after_claim
               )

      assert cancelled.run_id == started.run_id
      assert cancelled.status == :cancelled
      assert cancelled.visible_attempts == []
      assert_receive :after_claim
      refute_receive :gateway_step_ran
    end

    test "cancel/2 clears journal manual state for paused runs" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_cancel_paused"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :paused} = paused} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert paused.run_id == started.run_id
      assert %{step: "wait_for_review", kind: "approval"} = paused.manual_state

      assert {:ok, %Snapshot{} = cancelled} =
               Squidie.cancel(paused.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert cancelled.status == :cancelled
      assert cancelled.manual_state == nil
      assert cancelled.visible_attempts == []
    end

    test "cancel/2 rejects terminal journal runs" do
      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_cancel_terminal"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_transition, :completed, :cancelling}} =
               Squidie.cancel(started.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "replay/2 creates a fresh journal run from source input" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_replay"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = replay} =
               Squidie.replay(source.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :second)
               )

      assert replay.run_id != source.run_id
      assert replay.replayed_from_run_id == source.run_id
      assert replay.workflow == source.workflow
      assert replay.status == :running
      assert replay.input == %{account_id: "acct_replay"}

      assert [
               %{
                 signal_type: "replay_run",
                 payload: %{run_id: replayed_from_run_id, allow_irreversible: false}
               }
             ] = replay.command_history

      assert replayed_from_run_id == source.run_id

      assert [%{step: "check_gateway", input: %{account_id: "acct_replay"}}] =
               replay.visible_attempts
    end

    test "apply_signal/2 replays journal runs through a durable signal receipt" do
      replayed_at = DateTime.add(@read_model_started_at, 1, :second)

      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_signal_replay"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Signal{} = signal} =
               Signal.replay_run(source.run_id,
                 metadata: %{source: "signal_interpreter"},
                 idempotency_key: "replay-signal-1",
                 occurred_at: replayed_at
               )

      assert {:ok, %Snapshot{} = replay} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert replay.run_id != source.run_id
      assert replay.replayed_from_run_id == source.run_id
      assert replay.input == %{account_id: "acct_signal_replay"}

      assert [
               %{
                 signal_type: "replay_run",
                 payload: %{run_id: source_run_id, allow_irreversible: false},
                 metadata: %{source: "signal_interpreter"},
                 idempotency_key: "replay-signal-1",
                 occurred_at: ^replayed_at
               }
             ] = replay.command_history

      assert source_run_id == source.run_id

      command_history = replay.command_history
      run_entries = raw_run_entries(replay.run_id, @read_model_storage)

      assert {:ok, %Snapshot{} = duplicate_replay} =
               Squidie.apply_signal(signal,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert duplicate_replay.run_id == replay.run_id
      assert duplicate_replay.command_history == command_history
      assert raw_run_entries(replay.run_id, @read_model_storage) == run_entries
    end

    test "replay/2 blocks unsafe journal replays unless explicitly allowed" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 IrreversibleWorkflow,
                 %{account_id: "acct_replay_unsafe"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :running}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:error, {:unsafe_replay, %{steps: [%{step: :capture_payment}]}}} =
               Squidie.replay(source.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert {:ok, %Snapshot{} = replay} =
               Squidie.replay(source.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 allow_irreversible: true,
                 now: DateTime.add(@read_model_started_at, 2, :second)
               )

      assert replay.replayed_from_run_id == source.run_id
      assert [%{step: "load_account"}] = replay.visible_attempts
    end

    test "replay/2 uses persisted journal recovery policy when checking replay safety" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"
      {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      unsafe_runnable =
        Map.merge(
          journal_start_runnable(run_id),
          %{
            runnable_key: runnable_key,
            idempotency_key: runnable_key,
            recovery: %{
              "irreversible?" => false,
              "compensatable?" => false,
              "replay" => "manual_review_required",
              "recovery" => "manual_intervention"
            }
          }
        )

      entries = [
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          trigger: "gateway_recovery",
          input: %{account_id: "acct_persisted_recovery"},
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [unsafe_runnable],
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnable_applied, %{
          run_id: run_id,
          runnable_key: runnable_key,
          result: %{gateway: "ok"},
          occurred_at: @read_model_visible_at
        })
      ]

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)

      assert {:error, {:unsafe_replay, %{steps: [%{step: :check_gateway}]}}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "replay/2 treats completed dispatch attempts as unsafe before run progression" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 IrreversibleWorkflow,
                 %{account_id: "acct_replay_crash_window"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [_load_account] = source.visible_attempts

      runnable_key = "#{source.run_id}:capture_payment:1"

      unsafe_runnable = %{
        run_id: source.run_id,
        runnable_key: runnable_key,
        idempotency_key: runnable_key,
        attempt_number: 1,
        queue: @read_model_queue,
        step: "capture_payment",
        input: %{account_id: "acct_replay_crash_window"},
        recovery: %{
          "irreversible?" => true,
          "compensatable?" => false,
          "replay" => "manual_review_required",
          "recovery" => "manual_intervention"
        },
        visible_at: @read_model_visible_at
      }

      claim_id = Ecto.UUID.generate()
      claim_token = "journal-replay-crash-window-token"

      run_entries = [
        read_model_entry!(:runnables_planned, %{
          run_id: source.run_id,
          runnables: [unsafe_runnable],
          occurred_at: @read_model_visible_at
        })
      ]

      dispatch_entries = [
        read_model_entry!(
          :attempt_scheduled,
          Map.put(unsafe_runnable, :occurred_at, @read_model_visible_at)
        ),
        read_model_entry!(:attempt_claimed, %{
          run_id: source.run_id,
          runnable_key: runnable_key,
          claim_id: claim_id,
          claim_token_hash: claim_token_hash(claim_token),
          owner_id: "journal-replay-crash-window",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: source.run_id,
          runnable_key: runnable_key,
          claim_id: claim_id,
          claim_token_hash: claim_token_hash(claim_token),
          queue: @read_model_queue,
          result: %{account: %{id: "acct_replay_crash_window"}},
          occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
        })
      ]

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, run_entries)
      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, dispatch_entries)

      assert {:error, {:unsafe_replay, %{steps: [%{step: :capture_payment}]}}} =
               Squidie.replay(source.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "execute_next/1 persists recovery policy on retry runnables" do
      assert {:ok, %Snapshot{} = source} =
               Squidie.start(
                 JournalRetryWorkflow,
                 %{account_id: "acct_retry_replay"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = retry_scheduled} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert %{status: :retry_scheduled, runnable_key: retry_runnable_key} =
               Enum.find(retry_scheduled.attempts, &(&1.attempt_number == 2))

      assert {:ok, run_entries} = load_read_model_run_entries(source.run_id)

      retry_runnable =
        Enum.find_value(run_entries, fn
          %{type: :runnables_planned, data: %{runnables: runnables}} ->
            Enum.find(runnables, &(Map.get(&1, :runnable_key) == retry_runnable_key))

          _entry ->
            nil
        end)

      assert retry_runnable.recovery == %{
               "irreversible?" => false,
               "compensatable?" => true,
               "replay" => "allowed",
               "recovery" => "automatic"
             }
    end

    test "replay/2 rejects completed journal runnables without persisted recovery policy" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"
      {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      entries = [
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          trigger: "gateway_recovery",
          input: %{account_id: "acct_missing_recovery"},
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnable_applied, %{
          run_id: run_id,
          runnable_key: runnable_key,
          result: %{gateway: "ok"},
          occurred_at: @read_model_visible_at
        })
      ]

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)

      assert {:error, {:invalid_replay_source, {:missing_recovery, "check_gateway"}}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 allow_irreversible: true,
                 queue: @read_model_queue
               )
    end

    test "replay/2 returns structured errors for malformed source workflow" do
      run_id = Ecto.UUID.generate()

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: 123,
                 trigger: "gateway_recovery",
                 input: %{account_id: "acct_malformed_workflow"},
                 definition_fingerprint: "irrelevant",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [run_started])

      assert {:error, {:invalid_replay_source, :workflow}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "replay/2 returns structured errors for invalid source triggers" do
      run_id = Ecto.UUID.generate()
      {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 trigger: "renamed_trigger",
                 input: %{account_id: "acct_invalid_trigger"},
                 definition_fingerprint: Definition.fingerprint(definition),
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [run_started])

      assert {:error, {:invalid_replay_source, :trigger}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "replay/2 returns structured errors for missing source input" do
      run_id = Ecto.UUID.generate()
      {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 trigger: "gateway_recovery",
                 definition_fingerprint: Definition.fingerprint(definition),
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [run_started])

      assert {:error, {:invalid_replay_source, :missing_input}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "replay/2 returns structured journal errors for missing and malformed run ids" do
      assert {:error, :not_found} =
               Squidie.replay(Ecto.UUID.generate(),
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert {:error, :invalid_run_id} =
               Squidie.replay("not-a-uuid",
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )
    end

    test "replay/2 rejects journal runs with stale workflow definitions" do
      run_id = Ecto.UUID.generate()
      {:ok, current_definition} = Definition.load(VersionedPaymentRecoveryWorkflow)
      current_fingerprint = Definition.fingerprint(current_definition)

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: Atom.to_string(VersionedPaymentRecoveryWorkflow),
                 trigger: "gateway_recovery",
                 input: %{account_id: "acct_stale_definition"},
                 definition_version: "2026-05-25.payment-recovery-v1",
                 definition_fingerprint: "stale-definition",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, runnables_planned} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [journal_start_runnable(run_id)],
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [run_started, runnables_planned])

      assert {:error,
              {:incompatible_workflow_definition, :replay,
               %{
                 persisted_definition_version: "2026-05-25.payment-recovery-v1",
                 persisted_definition_fingerprint: "stale-definition",
                 current_definition_version: "2026-05-26.payment-recovery-v2",
                 current_definition_fingerprint: ^current_fingerprint
               }}} =
               Squidie.replay(run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )
    end

    test "cancel/2 returns structured journal errors for missing and malformed run ids" do
      assert {:error, :not_found} =
               Squidie.cancel(Ecto.UUID.generate(),
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert {:error, :invalid_run_id} =
               Squidie.cancel("not-a-uuid",
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert {:error, {:invalid_option, {:now, :invalid}}} =
               Squidie.cancel(Ecto.UUID.generate(),
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 now: :not_a_datetime
               )
    end

    test "list_runs/2 surfaces malformed journal run catalog facts" do
      workflow = Atom.to_string(PaymentRecoveryWorkflow)

      malformed_entry = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: Ecto.UUID.generate(),
          workflow: workflow
        },
        occurred_at: @read_model_started_at
      }

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [malformed_entry])

      assert {:error, {:run_catalog_anomalies, anomalies}} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert [%{entry_type: :run_cataloged, reason: :malformed_entry, workflow: ^workflow}] =
               anomalies
    end

    test "list_runs/2 surfaces conflicting journal run catalog facts" do
      run_id = Ecto.UUID.generate()
      workflow = Atom.to_string(PaymentRecoveryWorkflow)

      first_entry = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: run_id,
          workflow: workflow,
          queue: "first-queue"
        },
        occurred_at: @read_model_started_at
      }

      second_entry = %Squidie.Runtime.DispatchProtocol.Entry{
        type: :run_cataloged,
        thread: {:run_catalog, "all"},
        data: %{
          run_id: run_id,
          workflow: workflow,
          queue: "second-queue"
        },
        occurred_at: @read_model_started_at
      }

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [first_entry, second_entry])

      assert {:error, {:run_catalog_anomalies, anomalies}} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )

      assert [
               %{
                 entry_type: :run_cataloged,
                 reason: :conflicting_run_catalog,
                 run_id: ^run_id,
                 workflow: ^workflow,
                 queue: "second-queue"
               }
             ] = anomalies
    end

    test "list_runs/2 rejects catalog facts that disagree with the run thread" do
      run_id = Ecto.UUID.generate()
      actual_workflow = Atom.to_string(PaymentRecoveryWorkflow)
      catalog_workflow = Atom.to_string(ApprovalWorkflow)

      assert {:ok, run_started} =
               DispatchProtocol.new_entry(:run_started, %{
                 run_id: run_id,
                 workflow: actual_workflow,
                 occurred_at: @read_model_started_at
               })

      assert {:ok, runnables_planned} =
               DispatchProtocol.new_entry(:runnables_planned, %{
                 run_id: run_id,
                 runnables: [journal_start_runnable(run_id)],
                 occurred_at: @read_model_started_at
               })

      assert {:ok, catalog_entry} =
               DispatchProtocol.new_entry(:run_cataloged, %{
                 run_id: run_id,
                 workflow: catalog_workflow,
                 queue: @read_model_queue,
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} =
               Journal.append_entries(@read_model_storage, [run_started, runnables_planned])

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [catalog_entry])

      assert {:error,
              {:run_catalog_summary_failed, ^run_id,
               {:catalog_workflow_mismatch,
                %{expected: ^catalog_workflow, actual: ^actual_workflow, run_id: ^run_id}}}} =
               Squidie.list_runs([],
                 runtime: :journal,
                 journal_storage: @read_model_storage
               )
    end

    test "start/3 rejects conflicting catalog facts before dispatch visibility" do
      run_id = Ecto.UUID.generate()

      assert {:ok, bad_catalog_entry} =
               DispatchProtocol.new_entry(:run_cataloged, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 queue: "wrong-queue",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [bad_catalog_entry])

      assert {:error, {:journal_start_committed, ^run_id, {:conflicting_run_catalog, ^run_id}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "start/3 rejects conflicting run index facts before dispatch visibility" do
      run_id = Ecto.UUID.generate()

      assert {:ok, bad_index_entry} =
               DispatchProtocol.new_entry(:run_indexed, %{
                 run_id: run_id,
                 workflow: Atom.to_string(PaymentRecoveryWorkflow),
                 queue: "wrong-queue",
                 occurred_at: @read_model_started_at
               })

      assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [bad_index_entry])

      assert {:error, {:journal_start_committed, ^run_id, {:conflicting_run_index, ^run_id}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "configured journal runtime defaults start, inspect, explain, and execute calls" do
      configured_queue = "configured-runtime-test"

      put_squidie_config(
        runtime: :journal,
        read_model: :read_model,
        journal_storage: @read_model_storage,
        queue: configured_queue
      )

      assert {:ok, %Snapshot{} = started} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 now: @read_model_started_at
               )

      assert started.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert started.queue == configured_queue
      assert [%{step: "check_gateway", status: :available}] = started.visible_attempts

      assert {:ok, %Snapshot{} = inspected} =
               Squidie.inspect_run(started.run_id, now: @read_model_started_at)

      assert inspected.run_id == started.run_id
      assert inspected.queue == configured_queue

      assert {:ok, %Diagnostic{} = explanation} =
               Squidie.explain_run(started.run_id, now: @read_model_started_at)

      assert explanation.run_id == started.run_id
      assert explanation.queue == configured_queue
      assert explanation.reason == :attempt_visible

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               Squidie.inspect_run(started.run_id, queue: "../dispatch")

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               Squidie.execute_next(queue: "../dispatch")

      assert {:ok, %Snapshot{} = completed} =
               Squidie.execute_next(
                 owner_id: "configured-runtime-test",
                 now: @read_model_started_at
               )

      assert completed.run_id == started.run_id
      assert completed.terminal?
      assert completed.terminal_status == :completed
    end

    test "journal runtime executes built-in log steps" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 ManualAndScheduledDigestWorkflow,
                 :manual_digest,
                 %{chat_id: 123},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "announce_prompt", status: :available}] = snapshot.visible_attempts

      log =
        capture_log([level: :warning], fn ->
          assert {:ok, %Snapshot{} = completed_snapshot} =
                   execute_journal_next(
                     runtime: :journal,
                     journal_storage: @read_model_storage,
                     queue: @read_model_queue,
                     owner_id: "journal-log-test",
                     now: @read_model_started_at,
                     finished_at: @read_model_visible_at
                   )

          send(self(), {:completed_snapshot, completed_snapshot})
        end)

      assert log =~ "posting digest"
      assert_receive {:completed_snapshot, %Snapshot{} = completed_snapshot}

      assert completed_snapshot.run_id == run_id
      assert completed_snapshot.terminal?
      assert completed_snapshot.terminal_status == :completed
      assert completed_snapshot.visible_attempts == []

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "journal runtime executes built-in wait steps by delaying the successor" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 WaitWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "wait_for_settlement", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert delayed_snapshot.run_id == run_id
      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: @read_model_visible_at
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert [%{data: delayed_attempt}] =
               Enum.filter(
                 dispatch_entries,
                 &(&1.type == :attempt_scheduled and &1.data.step == "record_settlement")
               )

      assert delayed_attempt.visible_at == delayed_at

      refute Enum.any?(
               dispatch_entries,
               &(&1.type == :attempt_claimed and
                   &1.data.runnable_key == delayed_attempt.runnable_key)
             )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-test",
                 now: delayed_at,
                 finished_at: delayed_at
               )

      assert completed_snapshot.run_id == run_id
      assert completed_snapshot.terminal?
      assert completed_snapshot.terminal_status == :completed

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert [_wait_runnable, delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))

      assert delayed_runnable.step == "record_settlement"
      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime executes built-in pause steps into durable manual state" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert [%{step: "wait_for_approval", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = paused_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-test",
                 claim_id: "claim_pause",
                 claim_token: "token_pause",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert paused_snapshot.run_id == run_id
      assert paused_snapshot.status == :paused
      assert paused_snapshot.reason == :manual_intervention_required
      assert paused_snapshot.visible_attempts == []
      assert paused_snapshot.pending_results == []

      assert paused_snapshot.manual_state == %{
               step: "wait_for_approval",
               kind: "pause",
               paused_at: @read_model_visible_at,
               metadata: %{
                 output: %{},
                 target: "record_delivery"
               }
             }

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(run_id,
                 read_model: :read_model,
                 include_history: true,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "wait_for_approval"
      assert graph.current_node_ids == ["wait_for_approval"]
      assert graph_nodes["wait_for_approval"].status == :paused
      assert graph_nodes["wait_for_approval"].current?
      assert graph_nodes["wait_for_approval"].manual_state == paused_snapshot.manual_state

      assert {:error, :not_found} = Squidie.resume(run_id, %{})

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-test",
                 now: @read_model_visible_at
               )

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      assert %{
               data: %{
                 step: "wait_for_approval",
                 kind: "pause",
                 paused_at: @read_model_visible_at,
                 metadata: %{output: %{}, target: "record_delivery"}
               }
             } = Enum.at(run_entries, -1)
    end

    test "journal runtime resumes built-in pause steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      resumed_at = DateTime.add(@read_model_visible_at, 1, :second)
      resumed_at_iso = DateTime.to_iso8601(resumed_at)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_approval", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-test",
                 claim_id: "claim_pause_resolution",
                 claim_token: "token_pause_resolution",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = resumed_snapshot} =
               Squidie.resume(
                 run_id,
                 %{
                   actor: "ops_123",
                   comment: "resume requested",
                   metadata: %{access_token: "secret", reason: "qa"}
                 },
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 idempotency_key: "resume-pause-resolution-1",
                 now: resumed_at
               )

      assert resumed_snapshot.status == :running
      assert resumed_snapshot.reason == :attempt_visible
      assert resumed_snapshot.manual_state == nil
      assert resumed_snapshot.pending_dispatches == []

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = resumed_snapshot.visible_attempts

      assert Enum.any?(resumed_snapshot.command_history, fn
               %{
                 signal_type: "resume_run",
                 actor: "ops_123",
                 comment: "resume requested",
                 payload: %{
                   run_id: command_run_id,
                   attributes: %{actor: "ops_123", comment: "resume requested"}
                 },
                 metadata: %{access_token: "[REDACTED]", reason: "qa"}
               } ->
                 command_run_id == run_id

               _command ->
                 false
             end)

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(run_id,
                 read_model: :read_model,
                 include_history: true,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: resumed_at
               )

      graph_nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "record_delivery"
      assert graph.current_node_ids == ["record_delivery"]
      assert graph_nodes["wait_for_approval"].status == :completed
      assert graph_nodes["wait_for_approval"].manual_state == nil
      assert graph_nodes["record_delivery"].status == :pending
      assert graph_nodes["record_delivery"].current?

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused,
               :manual_step_resolved,
               :runnables_planned
             ]

      assert %{
               type: :manual_step_resolved,
               data: %{
                 step: "wait_for_approval",
                 action: "resumed",
                 result: %{},
                 metadata: %{
                   "event" => "resumed",
                   "actor" => "ops_123",
                   "comment" => "resume requested",
                   "metadata" => %{access_token: "[REDACTED]", reason: "qa"},
                   "at" => ^resumed_at_iso
                 }
               }
             } = Enum.at(run_entries, 4)

      assert {:ok, %Snapshot{} = replayed_resume_snapshot} =
               Squidie.resume(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 idempotency_key: "resume-pause-resolution-1",
                 now: resumed_at
               )

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = replayed_resume_snapshot.visible_attempts

      assert 1 ==
               Enum.count(replayed_resume_snapshot.command_history, fn
                 %{signal_type: "resume_run"} -> true
                 _command -> false
               end)

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-test",
                 claim_id: "claim_record_delivery",
                 claim_token: "token_record_delivery",
                 now: resumed_at,
                 finished_at: resumed_at
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.terminal?

      assert {:ok, replayed_run_entries} =
               load_read_model_run_entries(run_id)

      assert Enum.count(replayed_run_entries, &(&1.type == :manual_step_resolved)) == 1
    end

    test "journal runtime returns structured errors for invalid pause resume requests" do
      assert {:error, :not_found} =
               Squidie.resume(Ecto.UUID.generate(),
                 journal_storage: @read_model_storage
               )

      assert {:error, :not_found} =
               Squidie.resume(Ecto.UUID.generate(), %{}, journal_storage: @read_model_storage)

      assert {:error, :invalid_run_id} =
               Squidie.resume(
                 "not-a-uuid",
                 %{},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, :not_found} =
               Squidie.resume(
                 Ecto.UUID.generate(),
                 %{},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-invalid-pause-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:error, {:invalid_resume, %{actor: :invalid}}} =
               Squidie.resume(
                 run_id,
                 %{actor: ""},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime approves built-in approval steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)
      approved_at_iso = DateTime.to_iso8601(approved_at)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{} = paused_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-test",
                 claim_id: "claim_approval",
                 claim_token: "token_approval",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert paused_snapshot.status == :paused
      assert paused_snapshot.reason == :manual_intervention_required

      assert paused_snapshot.manual_state == %{
               step: "wait_for_review",
               kind: "approval",
               paused_at: @read_model_visible_at,
               metadata: %{
                 ok_target: "record_approval",
                 error_target: "record_rejection",
                 output_key: "approval"
               }
             }

      assert {:ok, %Snapshot{} = approved_snapshot} =
               Squidie.approve(
                 run_id,
                 %{actor: "ops_123", comment: "approved"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: approved_at
               )

      assert approved_snapshot.status == :running
      assert approved_snapshot.reason == :attempt_visible
      assert approved_snapshot.manual_state == nil

      assert [
               %{
                 step: "record_approval",
                 status: :available,
                 deadline: %{status: :on_time, due_at: approval_due_at},
                 input: %{
                   account_id: "acct_123",
                   approval: %{
                     decision: "approved",
                     actor: "ops_123",
                     comment: "approved",
                     decided_at: ^approved_at_iso
                   }
                 }
               }
             ] = approved_snapshot.visible_attempts

      assert DateTime.compare(approval_due_at, DateTime.add(approved_at, 30, :second)) == :eq

      assert Enum.any?(approved_snapshot.command_history, fn
               %{
                 signal_type: "approve_run",
                 actor: "ops_123",
                 comment: "approved",
                 payload: %{run_id: command_run_id}
               } ->
                 command_run_id == run_id

               _command ->
                 false
             end)

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused,
               :manual_step_resolved,
               :runnables_planned
             ]

      assert %{
               type: :manual_step_resolved,
               data: %{
                 step: "wait_for_review",
                 action: "approved",
                 result: %{
                   approval: %{
                     decision: "approved",
                     actor: "ops_123",
                     comment: "approved",
                     decided_at: ^approved_at_iso
                   }
                 },
                 metadata: %{
                   "event" => "approved",
                   "actor" => "ops_123",
                   "comment" => "approved",
                   "at" => ^approved_at_iso
                 }
               }
             } = Enum.at(run_entries, 4)
    end

    test "signal interpreter approves manual steps through a durable signal receipt" do
      run_id = Ecto.UUID.generate()
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_signal_approval"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-signal-approval-test",
                 claim_id: "claim_signal_approval",
                 claim_token: "token_signal_approval",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Signal{} = signal} =
               Signal.approve_run(
                 run_id,
                 %{actor: "ops_123", comment: "approved by signal"},
                 metadata: %{source: "ops_console"},
                 idempotency_key: "approve-signal-1",
                 occurred_at: approved_at
               )

      assert {:ok, %Snapshot{} = approved_snapshot} =
               SignalInterpreter.apply(signal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue
               )

      assert approved_snapshot.status == :running
      assert [%{step: "record_approval"}] = approved_snapshot.visible_attempts

      assert Enum.any?(approved_snapshot.command_history, fn
               %{
                 signal_type: "approve_run",
                 actor: "ops_123",
                 comment: "approved by signal",
                 payload: %{run_id: ^run_id},
                 metadata: %{source: "ops_console"},
                 idempotency_key: "approve-signal-1",
                 occurred_at: ^approved_at
               } ->
                 true

               _command ->
                 false
             end)

      run_entries = raw_run_entries(run_id, @read_model_storage)

      assert Enum.map(run_entries, & &1.type) == [
               :run_signal_received,
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused,
               :run_signal_received,
               :manual_step_resolved,
               :runnables_planned
             ]
    end

    test "journal runtime approves runs with string-keyed persisted definition metadata" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:wait_for_review:1"
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)
      approved_at_iso = DateTime.to_iso8601(approved_at)

      assert {:ok, definition} = Definition.load(ApprovalWorkflow)

      append_read_model_run_entries([
        string_keyed_definition_metadata(
          read_model_entry!(:run_started, %{
            run_id: run_id,
            workflow: Atom.to_string(ApprovalWorkflow),
            definition_fingerprint: Definition.fingerprint(definition),
            occurred_at: @read_model_started_at
          })
        ),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [
            %{
              run_id: run_id,
              runnable_key: runnable_key,
              idempotency_key: runnable_key,
              attempt_number: 1,
              queue: @read_model_queue,
              step: "wait_for_review",
              input: %{account_id: "acct_123"},
              visible_at: @read_model_started_at
            }
          ],
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnable_applied, %{
          run_id: run_id,
          runnable_key: runnable_key,
          result: %{},
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:manual_step_paused, %{
          run_id: run_id,
          step: "wait_for_review",
          kind: "approval",
          metadata: %{
            ok_target: "record_approval",
            error_target: "record_rejection",
            output_key: "approval"
          },
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = approved_snapshot} =
               Squidie.approve(
                 run_id,
                 %{actor: "ops_123", comment: "approved"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: approved_at
               )

      assert approved_snapshot.status == :running

      assert [
               %{
                 step: "record_approval",
                 status: :available,
                 input: %{
                   approval: %{
                     decision: "approved",
                     actor: "ops_123",
                     comment: "approved",
                     decided_at: ^approved_at_iso
                   }
                 }
               }
             ] = approved_snapshot.visible_attempts
    end

    test "journal runtime rejects built-in approval steps through durable manual resolution" do
      run_id = Ecto.UUID.generate()
      rejected_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review", status: :available}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-rejection-test",
                 claim_id: "claim_rejection",
                 claim_token: "token_rejection",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = rejected_snapshot} =
               Squidie.reject(
                 run_id,
                 %{actor: "ops_456", comment: "rejected"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: rejected_at
               )

      assert rejected_snapshot.status == :running

      assert [
               %{
                 step: "record_rejection",
                 status: :available,
                 input: %{approval: %{decision: "rejected", actor: "ops_456"}}
               }
             ] = rejected_snapshot.visible_attempts

      assert Enum.any?(rejected_snapshot.command_history, fn
               %{
                 signal_type: "reject_run",
                 actor: "ops_456",
                 comment: "rejected",
                 payload: %{run_id: command_run_id}
               } ->
                 command_run_id == run_id

               _command ->
                 false
             end)

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1
      assert Enum.at(run_entries, 4).data.action == "rejected"
    end

    test "journal runtime repairs pending dispatch after approval resolution was already committed" do
      run_id = Ecto.UUID.generate()
      approved_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_review"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-resolution-crash-test",
                 claim_id: "claim_approval_resolution_crash",
                 claim_token: "token_approval_resolution_crash",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      result = %{
        approval: %{
          decision: "approved",
          actor: "ops_123",
          decided_at: DateTime.to_iso8601(approved_at)
        }
      }

      successor_runnable = %{
        run_id: run_id,
        runnable_key: "#{run_id}:record_approval:1",
        idempotency_key: "#{run_id}:record_approval:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "record_approval",
        input: Map.merge(%{account_id: "acct_123"}, result),
        visible_at: approved_at
      }

      assert {:ok, approved_signal_receipt} =
               Squidie.Runtime.Journal.CommandReceipt.new(
                 :approve_run,
                 %{
                   run_id: run_id,
                   payload: %{run_id: run_id, attributes: %{actor: "ops_123"}},
                   metadata: %{},
                   idempotency_key: "approve-resolution-repair-1",
                   actor: "ops_123",
                   comment: nil
                 },
                 approved_at
               )

      append_read_model_run_entries([
        approved_signal_receipt,
        read_model_entry!(:manual_step_resolved, %{
          run_id: run_id,
          step: "wait_for_review",
          action: "approved",
          result: result,
          metadata: %{"event" => "approved", "at" => DateTime.to_iso8601(approved_at)},
          occurred_at: approved_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [successor_runnable],
          occurred_at: approved_at
        })
      ])

      assert {:ok, %Snapshot{} = approved_snapshot} =
               Squidie.approve(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 idempotency_key: "approve-resolution-repair-1",
                 now: approved_at
               )

      assert [
               %{
                 step: "record_approval",
                 status: :available,
                 input: %{approval: %{decision: "approved"}}
               }
             ] = approved_snapshot.visible_attempts

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "journal runtime does not approve after terminal transition" do
      run_id = Ecto.UUID.generate()
      terminal_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-terminal-approval-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: run_id,
          status: :cancelled,
          occurred_at: terminal_at
        })
      ])

      assert {:error, {:invalid_transition, :cancelled, :running}} =
               Squidie.approve(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: terminal_at
               )

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime returns structured errors for invalid approval controls" do
      assert {:error, {:invalid_review, %{actor: :required}}} =
               Squidie.approve(Ecto.UUID.generate(), %{}, journal_storage: @read_model_storage)

      assert {:error, :invalid_run_id} =
               Squidie.approve(
                 "not-a-uuid",
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, :not_found} =
               Squidie.reject(
                 Ecto.UUID.generate(),
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:error, {:invalid_review, %{actor: :required}}} =
               Squidie.approve(
                 Ecto.UUID.generate(),
                 %{actor: ""},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )
    end

    test "journal runtime resumes pending dispatch after manual resolution was already committed" do
      run_id = Ecto.UUID.generate()
      resumed_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{step: "wait_for_approval"}] = snapshot.visible_attempts

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-resolution-crash-test",
                 claim_id: "claim_pause_resolution_crash",
                 claim_token: "token_pause_resolution_crash",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      successor_runnable = %{
        run_id: run_id,
        runnable_key: "#{run_id}:record_delivery:1",
        idempotency_key: "#{run_id}:record_delivery:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "record_delivery",
        input: %{account_id: "acct_123"},
        visible_at: resumed_at
      }

      assert {:ok, resume_signal_receipt} =
               Squidie.Runtime.Journal.CommandReceipt.new(
                 :resume_run,
                 %{
                   run_id: run_id,
                   payload: %{run_id: run_id, attributes: %{actor: "ops_123"}},
                   metadata: %{},
                   idempotency_key: "resume-resolution-repair-1",
                   actor: "ops_123",
                   comment: nil
                 },
                 resumed_at
               )

      append_read_model_run_entries([
        resume_signal_receipt,
        read_model_entry!(:manual_step_resolved, %{
          run_id: run_id,
          step: "wait_for_approval",
          action: "resumed",
          result: %{},
          metadata: %{"event" => "resumed", "at" => DateTime.to_iso8601(resumed_at)},
          occurred_at: resumed_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [successor_runnable],
          occurred_at: resumed_at
        })
      ])

      assert {:ok, %Snapshot{} = resumed_snapshot} =
               Squidie.resume(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 idempotency_key: "resume-resolution-repair-1",
                 now: resumed_at
               )

      assert resumed_snapshot.status == :running
      assert resumed_snapshot.reason == :attempt_visible

      assert [
               %{step: "record_delivery", status: :available, input: %{account_id: "acct_123"}}
             ] = resumed_snapshot.visible_attempts

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.count(run_entries, &(&1.type == :manual_step_resolved)) == 1

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "journal runtime does not resolve manual pause after terminal transition" do
      run_id = Ecto.UUID.generate()
      terminal_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-terminal-pause-resolution-test",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: run_id,
          status: :cancelled,
          occurred_at: terminal_at
        })
      ])

      assert {:error, {:invalid_transition, :cancelled, :running}} =
               Squidie.resume(
                 run_id,
                 %{actor: "ops_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: terminal_at
               )

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      refute Enum.any?(run_entries, &(&1.type == :manual_step_resolved))
    end

    test "journal runtime recovers built-in pause manual state after dispatch completion" do
      run_id = Ecto.UUID.generate()
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PauseWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_approval"}] =
               snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "pause_claim",
          claim_token_hash: claim_token_hash("pause_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "pause_claim",
          claim_token_hash: claim_token_hash("pause_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :paused
      assert recovered_snapshot.reason == :manual_intervention_required
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.pending_results == []

      assert recovered_snapshot.manual_state == %{
               step: "wait_for_approval",
               kind: "pause",
               paused_at: @read_model_visible_at,
               metadata: %{
                 output: %{},
                 target: "record_delivery"
               }
             }

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      assert %{data: %{paused_at: @read_model_visible_at}} = Enum.at(run_entries, -1)

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-pause-recovery-test",
                 now: recovery_at
               )

      assert {:ok, replayed_run_entries} =
               load_read_model_run_entries(run_id)

      assert Enum.count(replayed_run_entries, &(&1.type == :manual_step_paused)) == 1
    end

    test "journal runtime recovers built-in approval manual state after dispatch completion" do
      run_id = Ecto.UUID.generate()
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_review"}] =
               snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "approval_claim",
          claim_token_hash: claim_token_hash("approval_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "approval_claim",
          claim_token_hash: claim_token_hash("approval_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-approval-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :paused
      assert recovered_snapshot.reason == :manual_intervention_required
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.pending_results == []

      assert recovered_snapshot.manual_state == %{
               step: "wait_for_review",
               kind: "approval",
               paused_at: @read_model_visible_at,
               metadata: %{
                 ok_target: "record_approval",
                 error_target: "record_rejection",
                 output_key: "approval"
               }
             }

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :manual_step_paused
             ]

      refute Enum.any?(
               run_entries,
               &(&1.type == :runnables_planned and
                   Enum.any?(&1.data.runnables, fn runnable ->
                     runnable.step == "record_approval"
                   end))
             )
    end

    test "journal runtime recovers built-in wait successor delay after dispatch completion" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)
      recovery_at = DateTime.add(@read_model_visible_at, 1, :second)

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 WaitWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [%{runnable_key: runnable_key}] = snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "wait_claim",
          claim_token_hash: claim_token_hash("wait_token"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "wait_claim",
          claim_token_hash: claim_token_hash("wait_token"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "journal-wait-recovery-test",
                 now: recovery_at
               )

      assert recovered_snapshot.run_id == run_id
      assert recovered_snapshot.reason == :attempt_scheduled_for_later
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert [_wait_runnable, delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))

      assert delayed_runnable.step == "record_settlement"
      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime executes dependency-mode wait steps by delaying dependent successors" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 4, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalDependencyWaitWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_account} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-account",
                 claim_id: "claim_account",
                 claim_token: "token_account",
                 now: @read_model_visible_at
               )

      assert after_account.status == :running
      assert Enum.map(after_account.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert Enum.map(after_invoice.visible_attempts, & &1.step) == ["wait_for_settlement"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-wait",
                 claim_id: "claim_wait",
                 claim_token: "token_wait",
                 now: DateTime.add(@read_model_visible_at, 2, :second),
                 finished_at: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-before-delay",
                 now: DateTime.add(@read_model_visible_at, 3, :second)
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-email",
                 claim_id: "claim_email",
                 claim_token: "token_email",
                 now: delayed_at
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_account",
               "load_invoice",
               "wait_for_settlement",
               "send_email"
             ]

      assert [%{step: "send_email", input: send_email_input}] =
               Enum.filter(completed_snapshot.attempts, &(&1.step == "send_email"))

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime recovers dependency wait successor delay after dispatch completion" do
      run_id = Ecto.UUID.generate()
      wait_finished_at = DateTime.add(@read_model_visible_at, 2, :second)
      delayed_at = DateTime.add(wait_finished_at, 2, :second)
      recovery_at = DateTime.add(wait_finished_at, 1, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalDependencyWaitWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-account",
                 claim_id: "claim_account",
                 claim_token: "token_account",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{runnable_key: runnable_key, step: "wait_for_settlement"}] =
               after_invoice.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(wait_finished_at, 30, :second),
          occurred_at: wait_finished_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: wait_finished_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "dependency-wait-recovery",
                 now: recovery_at
               )

      assert recovered_snapshot.reason == :attempt_scheduled_for_later
      assert recovered_snapshot.visible_attempts == []
      assert recovered_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "journal runtime preserves recovered dependency wait delay until later prerequisites finish" do
      run_id = Ecto.UUID.generate()
      wait_finished_at = @read_model_visible_at
      recovery_at = DateTime.add(wait_finished_at, 1, :second)
      delayed_at = DateTime.add(wait_finished_at, 2, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalRootWaitWorkflow,
                 %{invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert [
               %{runnable_key: runnable_key, step: "wait_for_settlement"},
               %{step: "z_load_invoice"}
             ] =
               started_snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(wait_finished_at, 30, :second),
          occurred_at: wait_finished_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_wait",
          claim_token_hash: claim_token_hash("token_wait"),
          queue: @read_model_queue,
          result: %{},
          occurred_at: wait_finished_at
        })
      ])

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-recovery",
                 now: recovery_at
               )

      assert recovered_snapshot.status == :running
      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["z_load_invoice"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: recovery_at
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert [wait_applied] =
               Enum.filter(
                 run_entries,
                 &(&1.type == :runnable_applied and &1.data.runnable_key == runnable_key)
               )

      assert wait_applied.data.applied_at == wait_finished_at
      assert wait_applied.occurred_at == recovery_at
    end

    test "journal runtime preserves a completed dependency wait delay until later prerequisites finish" do
      run_id = Ecto.UUID.generate()
      delayed_at = DateTime.add(@read_model_visible_at, 2, :second)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalRootWaitWorkflow,
                 %{invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "wait_for_settlement",
               "z_load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_wait} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-wait",
                 claim_id: "claim_wait",
                 claim_token: "token_wait",
                 now: @read_model_visible_at,
                 finished_at: @read_model_visible_at
               )

      assert after_wait.status == :running
      assert Enum.map(after_wait.visible_attempts, & &1.step) == ["z_load_invoice"]

      assert {:ok, %Snapshot{} = delayed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-invoice",
                 claim_id: "claim_invoice",
                 claim_token: "token_invoice",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert delayed_snapshot.reason == :attempt_scheduled_for_later
      assert delayed_snapshot.visible_attempts == []
      assert delayed_snapshot.next_visible_at == delayed_at

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "root-wait-email",
                 claim_id: "claim_email",
                 claim_token: "token_email",
                 now: delayed_at
               )

      assert completed_snapshot.status == :completed

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "wait_for_settlement",
               "z_load_invoice",
               "send_email"
             ]

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert [delayed_runnable] =
               run_entries
               |> Enum.filter(&(&1.type == :runnables_planned))
               |> Enum.flat_map(&Map.fetch!(&1.data, :runnables))
               |> Enum.filter(&(&1.step == "send_email"))

      assert delayed_runnable.visible_at == delayed_at
    end

    test "inspect_run_graph/2 identifies claimed journal attempts as the current node" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert [%{runnable_key: runnable_key}] = snapshot.visible_attempts

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_claimed, %{
          run_id: snapshot.run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash("token_1"),
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 30, :second),
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})

      assert graph.current_node_id == "check_gateway"
      assert graph.current_node_ids == ["check_gateway"]
      assert nodes["check_gateway"].status == :running

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_completed, %{
          run_id: snapshot.run_id,
          runnable_key: runnable_key,
          claim_id: "old_claim",
          claim_token_hash: claim_token_hash("stale_token"),
          queue: @read_model_queue,
          result: %{gateway: %{status: "ok"}},
          occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
        })
      ])

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph_with_anomaly} =
               Squidie.inspect_run_graph(snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{source: :dispatch, reason: :stale_claim, entry_type: :attempt_completed}] =
               graph_with_anomaly.anomalies

      refute inspect(graph_with_anomaly.anomalies) =~ "old_claim"
      refute inspect(graph_with_anomaly.anomalies) =~ claim_token_hash("stale_token")
    end

    test "journal runtime start can be rebuilt through inspection after process restart" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = rebuilt_snapshot} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert rebuilt_snapshot.run_id == started_snapshot.run_id
      assert rebuilt_snapshot.workflow == started_snapshot.workflow
      assert rebuilt_snapshot.thread_revisions == started_snapshot.thread_revisions
      assert rebuilt_snapshot.visible_attempts == started_snapshot.visible_attempts
      assert rebuilt_snapshot.pending_dispatches == []
    end

    test "journal runtime start infers Ecto journal storage from the configured repo" do
      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal
               )

      assert snapshot.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert snapshot.status == :running
    end

    test "start/3 rejects unsupported runtime mode" do
      assert {:error, {:invalid_option, {:runtime, :invalid}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :unsupported
               )
    end

    test "start/3 redacts invalid runtime values" do
      assert {:error, reason} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:runtime, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "journal runtime start rejects removed public options" do
      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: String
               )

      assert {:error, {:invalid_option, {:journal_storage, Jido.Storage.File}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: {Jido.Storage.File, []}
               )

      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: {String, path: "/tmp/squidie_storage", token: "redacted"}
               )

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "../dispatch"
               )

      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 run_id: "not-a-uuid"
               )

      assert {:error, reason} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 executor: String,
                 now: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "journal runtime start reports committed run id after post-append failures" do
      run_id = Ecto.UUID.generate()

      assert {:error, {:journal_start_committed, ^run_id, :load_failed}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: CommitThenFailStorage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )
    end

    test "journal runtime start idempotently repairs duplicate caller-provided run ids" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert snapshot.run_id == run_id

      assert {:ok, %Snapshot{} = duplicate_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert duplicate_snapshot.run_id == run_id

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 1
    end

    test "journal runtime start rejects duplicate run ids with conflicting planned work" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :conflict} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )
    end

    test "journal runtime start rejects duplicate run ids with conflicting definition fingerprints" do
      run_id = Ecto.UUID.generate()

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:error, :conflict} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :not_found} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})
    end

    test "journal runtime start repairs a partially appended run thread" do
      run_id = Ecto.UUID.generate()
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 30, :second),
                 run_id: run_id
               )

      assert snapshot.run_id == run_id
      assert snapshot.pending_dispatches == []

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]

      assert {:ok, run_index_projection} =
               Journal.rebuild_run_index_projection(
                 @read_model_storage,
                 Atom.to_string(PaymentRecoveryWorkflow)
               )

      assert Squidie.Runtime.RunIndexProjection.run_ids(run_index_projection) == [run_id]
    end

    test "journal runtime start retries same-queue dispatch append conflicts" do
      warm_read_model_storage()

      results =
        1..8
        |> Task.async_stream(
          fn index ->
            Squidie.start(
              PaymentRecoveryWorkflow,
              %{account_id: "acct_#{index}"},
              runtime: :journal,
              journal_storage: @read_model_storage,
              queue: @read_model_queue,
              now: DateTime.add(@read_model_started_at, index, :second),
              run_id: Ecto.UUID.generate()
            )
          end,
          max_concurrency: 8,
          timeout: 5_000
        )
        |> Enum.map(fn {:ok, result} -> result end)

      assert Enum.all?(results, &match?({:ok, %Snapshot{}}, &1))

      started_run_ids =
        Enum.map(results, fn {:ok, %Snapshot{} = snapshot} -> snapshot.run_id end)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      scheduled_run_ids =
        dispatch_entries
        |> Enum.filter(&(&1.type == :attempt_scheduled))
        |> Enum.map(& &1.data.run_id)
        |> Enum.sort()

      assert scheduled_run_ids == Enum.sort(started_run_ids)
    end

    test "execute_next/1 runs and applies one visible journal attempt" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert executed_snapshot.run_id == started_snapshot.run_id
      assert executed_snapshot.reason == :terminal
      assert executed_snapshot.status == :completed
      assert executed_snapshot.terminal? == true
      assert executed_snapshot.terminal_status == :completed
      assert executed_snapshot.visible_attempts == []
      assert executed_snapshot.pending_results == []
      assert executed_snapshot.applied_runnable_keys == started_snapshot.planned_runnable_keys

      assert [
               %{
                 status: :completed,
                 step: "check_gateway",
                 result: %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 applied?: true
               }
             ] = executed_snapshot.attempts

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]
    end

    test "execute_next/1 heartbeats a long-running claimed attempt without exposing claim tokens" do
      parent = self()
      queue = "executor-heartbeat-#{System.unique_integer([:positive])}"
      now = DateTime.utc_now()

      on_exit(fn -> :persistent_term.erase(:journal_gateway_run_hook) end)

      :persistent_term.put(:journal_gateway_run_hook, fn ->
        send(parent, {:heartbeat_step_started, self()})

        receive do
          :release_heartbeat_step -> :ok
        after
          5_000 -> raise "timed out waiting to release heartbeat step"
        end
      end)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_live_heartbeat"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: now
               )

      task =
        Task.async(fn ->
          execute_journal_next(
            runtime: :journal,
            journal_storage: @read_model_storage,
            queue: queue,
            owner_id: "heartbeat-worker",
            claim_id: "claim_live_heartbeat",
            claim_token: "token_live_heartbeat",
            lease_for: 1,
            heartbeat_interval_ms: 100,
            now: now
          )
        end)

      assert_receive {:heartbeat_step_started, step_pid}, 1_000
      Process.sleep(1_200)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, queue})

      assert Enum.any?(dispatch_entries, &(&1.type == :attempt_heartbeat))

      heartbeat_entries = Enum.filter(dispatch_entries, &(&1.type == :attempt_heartbeat))

      assert Enum.all?(heartbeat_entries, fn entry ->
               Map.has_key?(entry.data, :claim_token_hash) and
                 not Map.has_key?(entry.data, :claim_token)
             end)

      refute inspect(dispatch_entries) =~ "token_live_heartbeat"

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 owner_id: "competing-worker",
                 now: DateTime.utc_now()
               )

      send(step_pid, :release_heartbeat_step)

      assert {:ok, %Snapshot{} = completed_snapshot} = Task.await(task, 5_000)
      assert completed_snapshot.run_id == started_snapshot.run_id
      assert completed_snapshot.status == :completed
    end

    test "execute_next/1 stops heartbeat renewal when the executor process exits" do
      parent = self()
      queue = "executor-heartbeat-exit-#{System.unique_integer([:positive])}"
      now = DateTime.utc_now()

      on_exit(fn -> :persistent_term.erase(:journal_gateway_run_hook) end)

      :persistent_term.put(:journal_gateway_run_hook, fn ->
        send(parent, {:heartbeat_exit_step_started, self()})

        receive do
          :release_heartbeat_exit_step -> :ok
        after
          5_000 -> raise "timed out waiting to release heartbeat exit step"
        end
      end)

      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_heartbeat_executor_exit"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: now
               )

      {executor_pid, executor_ref} =
        spawn_monitor(fn ->
          Process.flag(:trap_exit, true)

          execute_journal_next(
            runtime: :journal,
            journal_storage: @read_model_storage,
            queue: queue,
            owner_id: "heartbeat-exit-worker",
            claim_id: "claim_heartbeat_exit",
            claim_token: "token_heartbeat_exit",
            lease_for: 1,
            heartbeat_interval_ms: 100,
            now: now
          )
        end)

      assert_receive {:heartbeat_exit_step_started, _step_pid}, 1_000
      Process.sleep(250)

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, queue})

      heartbeat_count = Enum.count(dispatch_entries, &(&1.type == :attempt_heartbeat))
      assert heartbeat_count > 0

      Process.exit(executor_pid, :kill)
      assert_receive {:DOWN, ^executor_ref, :process, ^executor_pid, :killed}, 1_000
      Process.sleep(300)

      assert {:ok, dispatch_entries_after_exit} =
               Journal.load_entries(@read_model_storage, {:dispatch, queue})

      assert Enum.count(dispatch_entries_after_exit, &(&1.type == :attempt_heartbeat)) ==
               heartbeat_count

      recovery_task =
        Task.async(fn ->
          execute_journal_next(
            runtime: :journal,
            journal_storage: @read_model_storage,
            queue: queue,
            owner_id: "heartbeat-exit-recovery-worker",
            now: DateTime.add(now, 2, :second)
          )
        end)

      assert_receive {:heartbeat_exit_step_started, recovery_step_pid}, 1_000
      send(recovery_step_pid, :release_heartbeat_exit_step)

      assert {:ok, %Snapshot{} = recovered_snapshot} = Task.await(recovery_task, 5_000)
      assert recovered_snapshot.status == :completed
    end

    test "execute_next/1 fails closed when heartbeat detects a terminal run" do
      parent = self()
      queue = "executor-heartbeat-terminal-#{System.unique_integer([:positive])}"
      now = DateTime.utc_now()

      on_exit(fn -> :persistent_term.erase(:journal_gateway_run_hook) end)

      :persistent_term.put(:journal_gateway_run_hook, fn ->
        send(parent, {:heartbeat_terminal_step_started, self()})

        receive do
          :release_heartbeat_terminal_step -> :ok
        after
          5_000 -> raise "timed out waiting to release heartbeat terminal step"
        end
      end)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_heartbeat_terminal"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: now
               )

      {executor_pid, executor_ref} =
        spawn_monitor(fn ->
          execute_journal_next(
            runtime: :journal,
            journal_storage: @read_model_storage,
            queue: queue,
            owner_id: "heartbeat-terminal-worker",
            claim_id: "claim_heartbeat_terminal",
            claim_token: "token_heartbeat_terminal",
            lease_for: 1,
            heartbeat_interval_ms: 100,
            now: now
          )
        end)

      assert_receive {:heartbeat_terminal_step_started, _step_pid}, 1_000

      assert {:ok, %Snapshot{status: :cancelled}} =
               Squidie.cancel(started_snapshot.run_id,
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: DateTime.utc_now()
               )

      assert_receive {:DOWN, ^executor_ref, :process, ^executor_pid, :killed}, 1_000
    end

    test "execute_next/1 exposes safe attempt metadata to native step context" do
      storage = {Jido.Storage.ETS, table: :squidie_native_attempt_context_test}
      queue = "native-attempt-context-test"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 NativeAttemptContextWorkflow,
                 %{account_id: "acct_native_context"},
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: @read_model_started_at
               )

      assert [runnable_key] = started_snapshot.planned_runnable_keys

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "worker_native_context",
                 claim_id: "claim_native_context",
                 claim_token: "raw-native-context-token",
                 now: @read_model_visible_at
               )

      assert [
               %{
                 result: %{
                   captured: %{
                     idempotency_key: ^runnable_key,
                     claim_id: "claim_native_context",
                     claim_token_present?: false
                   }
                 }
               }
             ] = executed_snapshot.attempts
    end

    test "execute_next/1 exposes safe attempt metadata to raw Jido action context" do
      storage = {Jido.Storage.ETS, table: :squidie_raw_action_attempt_context_test}
      queue = "raw-action-attempt-context-test"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 RawActionAttemptContextWorkflow,
                 %{account_id: "acct_raw_context"},
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: @read_model_started_at
               )

      assert [runnable_key] = started_snapshot.planned_runnable_keys

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "worker_raw_context",
                 claim_id: "claim_raw_context",
                 claim_token: "raw-action-context-token",
                 now: @read_model_visible_at
               )

      assert [
               %{
                 result: %{
                   captured: %{
                     idempotency_key: ^runnable_key,
                     claim_id: "claim_raw_context",
                     claim_token_present?: false
                   }
                 }
               }
             ] = executed_snapshot.attempts
    end

    test "execute_next/1 rolls back repo transaction writes when journal completion aborts" do
      Repo.delete_all("transactional_events")

      queue = "repo-transaction-#{System.unique_integer([:positive])}"
      storage = {Squidie.Runtime.Journal.Storage.Ecto, repo: Repo}

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 RepoTransactionWorkflow,
                 :repo_transaction,
                 %{account_id: "acct_repo_txn"},
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 now: @read_model_started_at
               )

      test_after_transaction_step = fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        {:error, :simulated_crash}
      end

      assert {:error, {:test_after_transaction_step, :simulated_crash}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "repo-txn-worker",
                 lease_for: 1,
                 now: @read_model_visible_at,
                 test_after_transaction_step: test_after_transaction_step
               )

      assert transactional_events(started_snapshot.run_id) == []

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: storage,
                 queue: queue,
                 owner_id: "repo-txn-worker-retry",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed_snapshot.run_id == started_snapshot.run_id
      assert completed_snapshot.status == :completed
      assert transactional_events(started_snapshot.run_id) == ["recorded"]
    end

    test "execute_next/1 heartbeats returned step output until durable completion" do
      parent = self()

      queue = "completion-heartbeat-#{System.unique_integer([:positive])}"
      now = DateTime.utc_now()

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_completion_heartbeat"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: now
               )

      test_before_completion = fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :journal_completion_pending)
        Process.sleep(1_200)
        :ok
      end

      task =
        Task.async(fn ->
          execute_journal_next(
            runtime: :journal,
            journal_storage: @read_model_storage,
            queue: queue,
            owner_id: "completion-heartbeat-worker",
            lease_for: 1,
            heartbeat_interval_ms: 100,
            now: now,
            test_before_completion: test_before_completion
          )
        end)

      assert_receive :journal_completion_pending, 1_000
      Process.sleep(1_100)

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 owner_id: "completion-heartbeat-competitor",
                 now: DateTime.utc_now()
               )

      assert {:ok, %Snapshot{} = completed_snapshot} = Task.await(task, 5_000)
      assert completed_snapshot.run_id == started_snapshot.run_id
      assert completed_snapshot.status == :completed
    end

    test "execute_next/1 fails repo transaction steps closed for non-Ecto journal storage" do
      Repo.delete_all("transactional_events")

      queue = "repo-transaction-unsupported-#{System.unique_integer([:positive])}"

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 RepoTransactionWorkflow,
                 :repo_transaction,
                 %{account_id: "acct_repo_txn_unsupported"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = failed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: queue,
                 owner_id: "repo-txn-worker",
                 now: @read_model_visible_at
               )

      assert failed_snapshot.run_id == started_snapshot.run_id
      assert failed_snapshot.status == :failed
      assert transactional_events(started_snapshot.run_id) == []

      assert [%{status: :failed, error: error}] = failed_snapshot.attempts
      assert error.code == "unsupported_repo_transaction_storage"
      assert error.retryable? == false
    end

    test "execute_next/1 plans and schedules the successor step after a journal completion" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = progressed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert progressed_snapshot.run_id == started_snapshot.run_id
      assert progressed_snapshot.status == :running
      assert progressed_snapshot.reason == :attempt_visible

      assert [%{step: "send_email", status: :available, input: successor_input}] =
               progressed_snapshot.visible_attempts

      assert successor_input == %{
               account_id: "acct_123",
               invoice_id: "inv_456",
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal
      assert completed_snapshot.terminal? == true

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_invoice",
               "send_email"
             ]
    end

    test "execute_next/1 plans the journal successor selected by a transition condition" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalConditionalWorkflow,
                 %{account_id: "acct_123", decision: "auto"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = progressed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert progressed_snapshot.run_id == started_snapshot.run_id
      assert progressed_snapshot.status == :running

      assert [%{step: "auto_approve", status: :available, input: successor_input}] =
               progressed_snapshot.visible_attempts

      assert successor_input.routing == %{decision: "auto"}

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert %{
               transition: %{
                 "from" => "classify",
                 "on" => "ok",
                 "to" => "auto_approve",
                 "condition" => %{"path" => ["routing", "decision"], "equals" => "auto"}
               }
             } =
               run_entries
               |> Enum.find(&(&1.type == :runnable_applied))
               |> then(& &1.data)

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{
               {"classify", "auto_approve"} => :selected,
               {"classify", "manual_review"} => :skipped
             } =
               graph.edges
               |> Enum.filter(&(&1.from == "classify"))
               |> Map.new(&{{&1.from, &1.to}, &1.status})

      auto_edge = Enum.find(graph.edges, &(&1.from == "classify" and &1.to == "auto_approve"))
      assert auto_edge.condition == %{path: [:routing, :decision], equals: "auto"}
    end

    test "execute_next/1 evaluates journal conditions against accumulated context" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalAccumulatedConditionalWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{reason: :attempt_visible}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = branched_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert [%{step: "auto_approve", status: :available, input: successor_input}] =
               branched_snapshot.visible_attempts

      assert successor_input.profile == %{account_id: "acct_123", tier: "trusted"}

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert %{
               {"classify", "auto_approve"} => :selected,
               {"classify", "manual_review"} => :skipped
             } =
               graph.edges
               |> Enum.filter(&(&1.from == "classify"))
               |> Map.new(&{{&1.from, &1.to}, &1.status})
    end

    test "execute_next/1 records selected conditional error transitions to complete" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalConditionalErrorCompleteWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert completed_snapshot.run_id == started_snapshot.run_id
      assert completed_snapshot.status == :completed

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               transition: %{
                 "from" => "fail_gateway",
                 "on" => "error",
                 "to" => "__complete__",
                 "condition" => %{"path" => ["account_id"], "equals" => "acct_123"}
               }
             } =
               run_entries
               |> Enum.find(&(&1.type == :runnable_applied))
               |> then(& &1.data)
    end

    test "execute_next/1 skips conditional error completion after a terminal conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalConditionalErrorCompleteWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      parent = self()

      :persistent_term.put(:journal_conditional_error_complete_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :conditional_error_complete_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:run_terminal, %{
            run_id: run_id,
            status: :cancelled,
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:error, :terminal_run} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :conditional_error_complete_conflict_hook_called
      after
        :persistent_term.erase(:journal_conditional_error_complete_conflict_hook)
      end

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 advances dependency workflows after prerequisites complete" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert Enum.map(started_snapshot.visible_attempts, & &1.step) == [
               "load_account",
               "load_invoice"
             ]

      assert {:ok, %Snapshot{} = after_account} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert after_account.status == :running
      assert Enum.map(after_account.visible_attempts, & &1.step) == ["load_invoice"]

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert graph.source == :read_model
      assert nodes["load_account"].status == :completed
      assert nodes["load_invoice"].status == :pending
      assert nodes["send_email"].status == :waiting
      assert edges["load_account:dependency:send_email"].status == :selected
      assert edges["load_invoice:dependency:send_email"].status == :pending

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert [%{step: "send_email", input: send_email_input}] = after_invoice.visible_attempts

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_3",
                 claim_id: "claim_3",
                 claim_token: "token_3",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert Enum.map(completed_snapshot.attempts, & &1.step) == [
               "load_account",
               "load_invoice",
               "send_email"
             ]
    end

    test "execute_next/1 fails journal runs durably when successor named path input is missing" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalMissingPathWorkflow,
                 %{draft: %{}},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = failed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert failed_snapshot.run_id == started_snapshot.run_id
      assert failed_snapshot.status == :failed
      assert failed_snapshot.reason == :terminal
      assert failed_snapshot.terminal? == true
      assert failed_snapshot.visible_attempts == []

      assert [
               %{
                 step: "load_review_context",
                 status: :completed,
                 result: %{draft: %{}}
               }
             ] = failed_snapshot.attempts

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               error: %{
                 code: "missing_input_path",
                 path: ["draft", "drafts"],
                 target: "drafts",
                 missing_at: ["draft", "drafts"]
               }
             } = Enum.at(run_entries, -1).data

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]
    end

    test "execute_next/1 recovers completed successor mapping failures with terminal error history" do
      run_id = Ecto.UUID.generate()
      assert {:ok, definition} = Definition.load(JournalMissingPathWorkflow)
      runnable = journal_missing_path_runnable(run_id)
      claim_token_hash = claim_token_hash("token_1")

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(JournalMissingPathWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [runnable],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:run_queued, %{
          run_id: run_id,
          queue: @read_model_queue,
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(
          :attempt_scheduled,
          Map.put(runnable, :occurred_at, @read_model_started_at)
        ),
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable.runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash,
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 300, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable.runnable_key,
          claim_id: "claim_1",
          claim_token_hash: claim_token_hash,
          queue: @read_model_queue,
          result: %{draft: %{}},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert [
               %{
                 step: "load_review_context",
                 status: :completed,
                 result: %{draft: %{}}
               }
             ] = snapshot.attempts

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]

      assert %{
               error: %{
                 code: "missing_input_path",
                 path: ["draft", "drafts"],
                 target: "drafts",
                 missing_at: ["draft", "drafts"]
               }
             } = Enum.at(run_entries, -1).data
    end

    test "inspect_run/2 and explain_run/2 surface successor mapping terminal errors" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalMissingPathWorkflow,
                 %{draft: %{}},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = failed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      expected_error = %{
        code: "missing_input_path",
        message: "missing mapped input path",
        path: ["draft", "drafts"],
        target: "drafts",
        retryable?: false,
        missing_at: ["draft", "drafts"]
      }

      assert failed_snapshot.terminal_error == expected_error

      assert {:ok, inspected_snapshot} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert inspected_snapshot.terminal_error == expected_error

      assert {:ok, diagnostic} =
               Squidie.explain_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert %{
               terminal?: true,
               terminal_status: :failed,
               terminal_error: ^expected_error
             } = diagnostic.details

      assert diagnostic.evidence.terminal_error == expected_error
    end

    test "execute_next/1 recomputes dependency progress after concurrent root append conflicts" do
      on_exit(fn -> :persistent_term.erase(:journal_dependency_invoice_hook) end)

      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{attempt: account_attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert account_attempt.step == "load_account"

      :persistent_term.put(:journal_dependency_invoice_hook, fn ->
        account_result = %{account: %{id: "acct_123"}}

        assert {:ok, latest_dispatch_agent} =
                 DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

        assert {:ok, %{}} =
                 DispatchAgent.complete(
                   @read_model_storage,
                   latest_dispatch_agent,
                   account_attempt.runnable_key,
                   "claim_1",
                   "token_1",
                   account_result,
                   now: DateTime.add(@read_model_visible_at, 1, :millisecond)
                 )

        append_read_model_run_entries([
          read_model_entry!(:runnable_applied, %{
            run_id: started_snapshot.run_id,
            runnable_key: account_attempt.runnable_key,
            result: account_result,
            occurred_at: DateTime.add(@read_model_visible_at, 1, :millisecond)
          })
        ])
      end)

      assert {:ok, %Snapshot{} = after_invoice} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert after_invoice.status == :running
      assert [%{step: "send_email", input: send_email_input}] = after_invoice.visible_attempts

      assert send_email_input == %{
               account: %{id: "acct_123"},
               invoice: %{id: "inv_456", status: "open"}
             }

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnable_applied,
               :runnables_planned
             ]
    end

    test "execute_next/1 terminally fails dependency workflows after nonretryable root failure" do
      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 JournalDependencyFailureWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal
      assert snapshot.terminal? == true
      assert snapshot.visible_attempts == []

      assert {:ok, run_entries} =
               load_read_model_run_entries(snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 returns none after the visible journal attempt is already applied" do
      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_completed
             ]
    end

    test "execute_next/1 recovers a completed attempt that crashed before run progression" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :completed_result_pending_apply}} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.status == :completed
      assert recovered_snapshot.reason == :terminal
      assert recovered_snapshot.applied_runnable_keys == started_snapshot.planned_runnable_keys

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "execute_next/1 does not apply completed attempts after the run became terminal" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
                 now: @read_model_visible_at
               )

      append_read_model_run_entries([
        read_model_entry!(:run_terminal, %{
          run_id: started_snapshot.run_id,
          status: :cancelled,
          occurred_at: DateTime.add(@read_model_visible_at, 1, :second)
        })
      ])

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 recovers a failed attempt that crashed before run progression" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.fail(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{code: "gateway_timeout", message: "gateway timeout", retryable?: false},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :waiting_for_dispatch}} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.status == :failed
      assert recovered_snapshot.reason == :terminal

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]
    end

    test "execute_next/1 recovers dispatch scheduling after run progression was committed" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 InvoiceReminderWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      result = %{
        account: %{id: "acct_123"},
        invoice: %{id: "inv_456", status: "open"}
      }

      assert {:ok, %{}} =
               DispatchAgent.complete(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 result,
                 now: @read_model_visible_at
               )

      successor_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:send_email:1",
        idempotency_key: "#{started_snapshot.run_id}:send_email:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "send_email",
        input: result,
        visible_at: @read_model_visible_at
      }

      append_read_model_run_entries([
        read_model_entry!(:runnable_applied, %{
          run_id: started_snapshot.run_id,
          runnable_key: attempt.runnable_key,
          result: result,
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: started_snapshot.run_id,
          runnables: [successor_runnable],
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{reason: :planned_dispatch_pending_schedule}} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert recovered_snapshot.reason == :attempt_visible
      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["send_email"]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.count(dispatch_entries, &(&1.type == :attempt_scheduled)) == 2
    end

    test "execute_next/1 recovers initial dispatch scheduling from a queued run marker" do
      run_id = Ecto.UUID.generate()
      runnable = journal_start_runnable(run_id)
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_dispatch_entries([
        read_model_entry!(:run_queued, %{
          run_id: run_id,
          queue: @read_model_queue,
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Definition.serialize_workflow(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [runnable],
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{reason: :planned_dispatch_pending_schedule}} =
               Squidie.inspect_run(run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert recovered_snapshot.reason == :attempt_visible

      assert [%{runnable_key: runnable_key, step: "check_gateway", status: :available}] =
               recovered_snapshot.visible_attempts

      assert runnable_key == runnable.runnable_key

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [:run_queued, :attempt_scheduled]
    end

    test "execute_next/1 ignores queued run markers for runs planned on another queue" do
      run_id = Ecto.UUID.generate()

      assert {:ok, %Snapshot{run_id: ^run_id}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "default",
                 now: @read_model_started_at,
                 run_id: run_id
               )

      assert {:error, :conflict} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "other",
                 now: DateTime.add(@read_model_started_at, 1, :second),
                 run_id: run_id
               )

      assert {:ok, :none} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: "other",
                 owner_id: "worker_1",
                 now: @read_model_visible_at
               )
    end

    test "execute_next/1 does not repeatedly recover failed attempts after an error transition is planned" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalErrorTransitionWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, dispatch_agent} =
               DispatchAgent.rebuild(@read_model_storage, @read_model_queue)

      assert {:ok, %{agent: claimed_agent, attempt: attempt}} =
               DispatchAgent.claim_next(@read_model_storage, dispatch_agent, "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %{}} =
               DispatchAgent.fail(
                 @read_model_storage,
                 claimed_agent,
                 attempt.runnable_key,
                 "claim_1",
                 "token_1",
                 %{code: "gateway_timeout", message: "gateway timeout", retryable?: false},
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{reason: :attempt_visible} = recovered_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert Enum.map(recovered_snapshot.visible_attempts, & &1.step) == ["notify_failure"]

      assert {:ok, graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert %{status: :selected} =
               Enum.find(
                 graph.edges,
                 &(&1.from == "fail_gateway" and &1.to == "notify_failure")
               )

      assert {:ok, %Snapshot{} = completed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_3",
                 claim_id: "claim_3",
                 claim_token: "token_3",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert completed_snapshot.status == :completed
      assert completed_snapshot.reason == :terminal

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnable_applied,
               :runnables_planned,
               :runnable_applied,
               :run_terminal
             ]
    end

    test "execute_next/1 does not duplicate error transition progression after a run-thread conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalErrorTransitionWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      notify_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:notify_failure:1",
        idempotency_key: "#{started_snapshot.run_id}:notify_failure:1",
        attempt_number: 1,
        queue: @read_model_queue,
        step: "notify_failure",
        input: %{},
        visible_at: @read_model_visible_at
      }

      failed_runnable_key = "#{started_snapshot.run_id}:fail_gateway:1"
      parent = self()

      :persistent_term.put(:journal_error_transition_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :error_transition_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:runnable_applied, %{
            run_id: run_id,
            runnable_key: failed_runnable_key,
            result: %{},
            occurred_at: @read_model_visible_at
          }),
          read_model_entry!(:runnables_planned, %{
            run_id: run_id,
            runnables: [notify_runnable],
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :error_transition_conflict_hook_called
        assert Enum.map(snapshot.visible_attempts, & &1.step) == ["notify_failure"]
      after
        :persistent_term.erase(:journal_error_transition_conflict_hook)
      end

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.count(run_entries, &(&1.type == :runnable_applied)) == 1

      notify_plan_count =
        Enum.count(run_entries, fn
          %{type: :runnables_planned, data: %{runnables: [%{runnable_key: key}]}} ->
            key == notify_runnable.runnable_key

          _entry ->
            false
        end)

      assert notify_plan_count == 1
    end

    test "execute_next/1 fails an incompatible claimed attempt durably" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:missing_gateway:1"
      assert {:ok, definition} = Definition.load(PaymentRecoveryWorkflow)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [
            %{
              run_id: run_id,
              runnable_key: runnable_key,
              idempotency_key: runnable_key,
              attempt_number: 1,
              queue: @read_model_queue,
              step: "missing_gateway",
              input: %{account_id: "acct_123"},
              visible_at: @read_model_started_at
            }
          ],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "missing_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert [
               %{
                 status: :failed,
                 step: "missing_gateway",
                 error: %{
                   message: "journal attempt is incompatible with the current workflow definition"
                 }
               }
             ] = snapshot.attempts

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned, :run_terminal]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 rejects missing workflow definition fingerprints before executing" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed

      assert [%{status: :failed, error: %{code: "incompatible_workflow_definition"}}] =
               snapshot.attempts
    end

    test "execute_next/1 rejects stale workflow definition fingerprints before executing" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"
      {:ok, current_definition} = Definition.load(VersionedPaymentRecoveryWorkflow)
      current_fingerprint = Definition.fingerprint(current_definition)

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(VersionedPaymentRecoveryWorkflow),
          definition_version: "2026-05-25.payment-recovery-v1",
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :failed

      assert [
               %{
                 status: :failed,
                 error: %{
                   code: "incompatible_workflow_definition",
                   persisted_definition_version: "2026-05-25.payment-recovery-v1",
                   persisted_definition_fingerprint: "stale-definition",
                   current_definition_version: "2026-05-26.payment-recovery-v2",
                   current_definition_fingerprint: ^current_fingerprint
                 }
               }
             ] =
               snapshot.attempts
    end

    test "execute_next/1 accepts string-keyed persisted definition metadata" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"
      {:ok, definition} = Definition.load(VersionedPaymentRecoveryWorkflow)

      append_read_model_run_entries([
        string_keyed_definition_metadata(
          read_model_entry!(:run_started, %{
            run_id: run_id,
            workflow: Atom.to_string(VersionedPaymentRecoveryWorkflow),
            definition_version: "2026-05-26.payment-recovery-v2",
            definition_fingerprint: Definition.fingerprint(definition),
            occurred_at: @read_model_started_at
          })
        ),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.status == :completed
      assert snapshot.context.gateway_check == %{account_id: "acct_123", status: "healthy"}
    end

    test "execute_next/1 terminally fails stale completed attempts during recovery" do
      run_id = Ecto.UUID.generate()
      runnable_key = "#{run_id}:check_gateway:1"

      append_read_model_run_entries([
        read_model_entry!(:run_started, %{
          run_id: run_id,
          workflow: Atom.to_string(PaymentRecoveryWorkflow),
          definition_fingerprint: "stale-definition",
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:runnables_planned, %{
          run_id: run_id,
          runnables: [journal_start_runnable(run_id)],
          occurred_at: @read_model_started_at
        })
      ])

      append_read_model_dispatch_entries([
        read_model_entry!(:attempt_scheduled, %{
          run_id: run_id,
          runnable_key: runnable_key,
          idempotency_key: runnable_key,
          attempt_number: 1,
          queue: @read_model_queue,
          step: "check_gateway",
          input: %{account_id: "acct_123"},
          visible_at: @read_model_started_at,
          occurred_at: @read_model_started_at
        }),
        read_model_entry!(:attempt_claimed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: "token_hash_1",
          owner_id: "worker_1",
          queue: @read_model_queue,
          lease_until: DateTime.add(@read_model_visible_at, 300, :second),
          occurred_at: @read_model_visible_at
        }),
        read_model_entry!(:attempt_completed, %{
          run_id: run_id,
          runnable_key: runnable_key,
          claim_id: "claim_1",
          claim_token_hash: "token_hash_1",
          queue: @read_model_queue,
          result: %{gateway_check: %{account_id: "acct_123", status: "healthy"}},
          occurred_at: @read_model_visible_at
        })
      ])

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert snapshot.status == :failed
      assert snapshot.reason == :terminal

      assert {:ok, run_entries} = load_read_model_run_entries(run_id)
      assert Enum.map(run_entries, & &1.type) == [:run_started, :runnables_planned, :run_terminal]
    end

    test "execute_next/1 uses completion time for lease fencing" do
      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:error, :expired_claim} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 lease_for: 1,
                 now: @read_model_visible_at,
                 finished_at: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed
             ]
    end

    test "execute_next/1 retries terminal append after unrelated same-queue writes" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalConflictWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      :persistent_term.put(:journal_run_conflict_hook, fn ->
        assert {:ok, %Snapshot{}} =
                 Squidie.start(
                   PaymentRecoveryWorkflow,
                   %{account_id: "acct_456"},
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   now: DateTime.add(@read_model_started_at, 1, :second),
                   run_id: Ecto.UUID.generate()
                 )
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert snapshot.run_id == started_snapshot.run_id
        assert snapshot.status == :completed
        assert snapshot.reason == :terminal
      after
        :persistent_term.erase(:journal_run_conflict_hook)
      end

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :run_queued,
               :attempt_scheduled,
               :attempt_completed
             ]
    end

    test "execute_next/1 records durable failed-attempt facts" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = executed_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert executed_snapshot.run_id == started_snapshot.run_id
      assert executed_snapshot.reason == :terminal
      assert executed_snapshot.status == :failed
      assert executed_snapshot.terminal? == true
      assert executed_snapshot.terminal_status == :failed
      assert executed_snapshot.applied_runnable_keys == []

      assert [
               %{
                 status: :failed,
                 step: "fail_gateway",
                 error: %{
                   code: "gateway_timeout",
                   message: "gateway timeout",
                   retryable?: false
                 }
               }
             ] = executed_snapshot.attempts

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :run_terminal
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 schedules retry attempts through the journal dispatch projection" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalRetryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{} = snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert snapshot.run_id == started_snapshot.run_id
      assert snapshot.status == :running
      assert snapshot.reason == :attempt_visible
      assert snapshot.terminal? == false

      assert [
               %{status: :failed, step: "retry_gateway", error: %{retryable?: true}},
               %{status: :retry_scheduled, step: "retry_gateway", attempt_number: 2}
             ] = snapshot.attempts

      assert [
               %{
                 status: :retry_scheduled,
                 attempt_number: 2,
                 deadline: %{status: :on_time, due_at: retry_due_at}
               }
             ] = snapshot.visible_attempts

      assert DateTime.compare(
               retry_due_at,
               DateTime.add(@read_model_visible_at, 60, :second)
             ) == :eq

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      nodes = Map.new(graph.nodes, &{&1.id, &1})
      edges = Map.new(graph.edges, &{&1.id, &1})

      assert nodes["retry_gateway"].status == :retrying
      assert nodes["retry_gateway"].attempts == []
      assert edges["retry_gateway:ok:complete"].status == :pending

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph_with_history} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 include_history: true
               )

      nodes_with_history = Map.new(graph_with_history.nodes, &{&1.id, &1})

      assert [
               %{status: :failed, attempt_number: 1},
               %{status: :retry_scheduled, attempt_number: 2}
             ] = nodes_with_history["retry_gateway"].attempts

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed
             ]

      assert %{retry_deadline: %{due_at: ^retry_due_at}} = Enum.at(dispatch_entries, -1).data

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      assert Enum.map(run_entries, & &1.type) == [
               :run_started,
               :runnables_planned,
               :runnables_planned
             ]

      assert {:ok, %Snapshot{} = exhausted_snapshot} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert exhausted_snapshot.status == :failed
      assert exhausted_snapshot.reason == :terminal

      assert Enum.map(exhausted_snapshot.attempts, &{&1.status, &1.attempt_number}) == [
               {:failed, 1},
               {:failed, 2}
             ]

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      assert Enum.map(dispatch_entries, & &1.type) == [
               :run_queued,
               :attempt_scheduled,
               :attempt_claimed,
               :attempt_failed,
               :attempt_claimed,
               :attempt_failed
             ]
    end

    test "execute_next/1 completes a run after a successful retry" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalRetryThenCompleteWorkflow,
                 %{account_id: "acct_retry_success"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :running, terminal?: false} = retry_scheduled} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert [%{status: :retry_scheduled, attempt_number: 2}] =
               retry_scheduled.visible_attempts

      assert {:ok, %Snapshot{status: :completed, terminal?: true} = completed} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_2",
                 claim_id: "claim_2",
                 claim_token: "token_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert completed.run_id == started_snapshot.run_id
      assert completed.context.gateway == "ok"

      assert Enum.map(completed.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) ==
               [
                 {"retry_gateway", :failed, false, 1},
                 {"retry_gateway", :completed, true, 2}
               ]
    end

    test "execute_next/1 does not duplicate retry progression after a run-thread conflict" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalRetryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      retry_runnable = %{
        run_id: started_snapshot.run_id,
        runnable_key: "#{started_snapshot.run_id}:retry_gateway:2",
        idempotency_key: "#{started_snapshot.run_id}:retry_gateway:2",
        attempt_number: 2,
        queue: @read_model_queue,
        step: "retry_gateway",
        input: %{account_id: "acct_123"},
        visible_at: @read_model_visible_at
      }

      parent = self()

      :persistent_term.put(:journal_retry_failure_conflict_hook, fn %{run_id: run_id} ->
        assert run_id == started_snapshot.run_id
        send(parent, :retry_failure_conflict_hook_called)

        append_read_model_run_entries([
          read_model_entry!(:runnables_planned, %{
            run_id: run_id,
            runnables: [retry_runnable],
            occurred_at: @read_model_visible_at
          })
        ])
      end)

      try do
        assert {:ok, %Snapshot{} = snapshot} =
                 execute_journal_next(
                   runtime: :journal,
                   journal_storage: @read_model_storage,
                   queue: @read_model_queue,
                   owner_id: "worker_1",
                   claim_id: "claim_1",
                   claim_token: "token_1",
                   now: @read_model_visible_at
                 )

        assert_receive :retry_failure_conflict_hook_called
        assert [%{status: :retry_scheduled, attempt_number: 2}] = snapshot.visible_attempts
      after
        :persistent_term.erase(:journal_retry_failure_conflict_hook)
      end

      assert {:ok, run_entries} =
               load_read_model_run_entries(started_snapshot.run_id)

      retry_plan_count =
        Enum.count(run_entries, fn
          %{type: :runnables_planned, data: %{runnables: [%{runnable_key: key}]}} ->
            key == retry_runnable.runnable_key

          _entry ->
            false
        end)

      assert retry_plan_count == 1
    end

    test "graph inspection serializes completed runs with details redacted by default" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 PaymentRecoveryWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      payload = Squidie.Runs.GraphInspection.to_map(graph)
      nodes = Map.new(payload.nodes, &{&1.id, &1})
      edges = Map.new(payload.edges, &{&1.id, &1})

      assert payload.workflow == Atom.to_string(PaymentRecoveryWorkflow)
      assert payload.status == :completed
      assert payload.current_node_id == nil
      assert payload.current_node_ids == []
      assert payload.terminal? == true
      assert nodes["check_gateway"].status == :completed
      assert nodes["check_gateway"].current? == false
      assert nodes["check_gateway"].input == nil
      assert nodes["check_gateway"].output == nil
      assert nodes["check_gateway"].error == nil
      assert nodes["check_gateway"].attempts == []
      assert edges["check_gateway:ok:complete"].selected? == true
      assert edges["check_gateway:ok:complete"].skipped? == false
      assert edges["check_gateway:ok:complete"].pending? == false
      assert edges["check_gateway:ok:complete"].blocked? == false

      refute Map.has_key?(payload, :journal_storage)
      refute inspect(payload) =~ "claim_token"
      assert is_binary(Jason.encode!(payload))

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph_with_history} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at,
                 include_history: true
               )

      history_payload = Squidie.Runs.GraphInspection.to_map(graph_with_history)
      history_nodes = Map.new(history_payload.nodes, &{&1.id, &1})

      assert history_nodes["check_gateway"].output == %{
               gateway_check: %{account_id: "acct_123", status: "healthy"}
             }

      assert [%{attempt_number: 1, status: :completed}] =
               history_nodes["check_gateway"].attempts
    end

    test "graph inspection serializes conditional selected and skipped routes" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalConditionalWorkflow,
                 %{account_id: "acct_123", decision: "auto"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :running}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_1",
                 claim_id: "claim_1",
                 claim_token: "token_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = graph} =
               Squidie.inspect_run_graph(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      payload = Squidie.Runs.GraphInspection.to_map(graph)
      edges = Map.new(payload.edges, &{{&1.from, &1.to}, &1})

      assert %{
               status: :selected,
               selected?: true,
               skipped?: false,
               condition: %{path: [:routing, :decision], equals: "auto"}
             } = edges[{"classify", "auto_approve"}]

      assert %{
               status: :skipped,
               selected?: false,
               skipped?: true,
               condition: nil
             } = edges[{"classify", "manual_review"}]

      assert is_binary(Jason.encode!(payload))
    end

    test "graph inspection serializes dependency, paused, retrying, and failed states" do
      assert {:ok, %Snapshot{} = dependency_snapshot} =
               Squidie.start(
                 JournalDependencyWorkflow,
                 %{account_id: "acct_123", invoice_id: "inv_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :running}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_dependency_1",
                 claim_id: "claim_dependency_1",
                 claim_token: "token_dependency_1",
                 now: @read_model_visible_at
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = dependency_graph} =
               Squidie.inspect_run_graph(dependency_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      dependency_payload = Squidie.Runs.GraphInspection.to_map(dependency_graph)
      dependency_edges = Map.new(dependency_payload.edges, &{&1.id, &1})

      assert dependency_payload.current_node_ids == ["load_invoice"]
      assert dependency_edges["load_account:dependency:send_email"].type == :dependency
      assert dependency_edges["load_account:dependency:send_email"].selected? == true
      assert dependency_edges["load_invoice:dependency:send_email"].pending? == true

      assert {:ok, %Snapshot{status: :running}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_dependency_2",
                 claim_id: "claim_dependency_2",
                 claim_token: "token_dependency_2",
                 now: DateTime.add(@read_model_visible_at, 1, :second)
               )

      assert {:ok, %Snapshot{status: :completed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_dependency_3",
                 claim_id: "claim_dependency_3",
                 claim_token: "token_dependency_3",
                 now: DateTime.add(@read_model_visible_at, 2, :second)
               )

      assert {:ok, %Snapshot{} = approval_snapshot} =
               Squidie.start(
                 ApprovalWorkflow,
                 %{account_id: "acct_456"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 1, :minute)
               )

      assert {:ok, %Snapshot{status: :paused}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_approval_1",
                 claim_id: "claim_approval_1",
                 claim_token: "token_approval_1",
                 now: DateTime.add(@read_model_visible_at, 1, :minute)
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = approval_graph} =
               Squidie.inspect_run_graph(approval_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 1, :minute),
                 include_history: true
               )

      approval_payload = Squidie.Runs.GraphInspection.to_map(approval_graph)
      approval_nodes = Map.new(approval_payload.nodes, &{&1.id, &1})

      assert approval_payload.current_node_id == "wait_for_review"
      assert approval_nodes["wait_for_review"].status == :paused
      assert approval_nodes["wait_for_review"].current? == true
      assert approval_nodes["wait_for_review"].manual_state.step == "wait_for_review"

      assert {:ok, %Snapshot{} = retry_snapshot} =
               Squidie.start(
                 JournalRetryWorkflow,
                 %{account_id: "acct_789"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_started_at, 2, :minute)
               )

      assert {:ok, %Snapshot{status: :running}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_retry_1",
                 claim_id: "claim_retry_1",
                 claim_token: "token_retry_1",
                 now: DateTime.add(@read_model_visible_at, 2, :minute)
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = retrying_graph} =
               Squidie.inspect_run_graph(retry_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 2, :minute)
               )

      retrying_payload = Squidie.Runs.GraphInspection.to_map(retrying_graph)
      retrying_nodes = Map.new(retrying_payload.nodes, &{&1.id, &1})

      assert retrying_nodes["retry_gateway"].status == :retrying
      assert retrying_nodes["retry_gateway"].error == nil
      assert retrying_nodes["retry_gateway"].attempts == []

      assert {:ok, %Snapshot{status: :failed}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "worker_retry_2",
                 claim_id: "claim_retry_2",
                 claim_token: "token_retry_2",
                 now: DateTime.add(@read_model_visible_at, 3, :minute)
               )

      assert {:ok, %Squidie.Runs.GraphInspection{} = failed_graph} =
               Squidie.inspect_run_graph(retry_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: DateTime.add(@read_model_visible_at, 3, :minute)
               )

      failed_payload = Squidie.Runs.GraphInspection.to_map(failed_graph)
      failed_nodes = Map.new(failed_payload.nodes, &{&1.id, &1})

      assert failed_payload.status == :failed
      assert failed_payload.terminal? == true
      assert failed_payload.current_node_ids == []
      assert failed_nodes["retry_gateway"].status == :failed
    end

    test "execute_next/1 redacts secret-bearing action errors before persistence" do
      assert {:ok, %Snapshot{}} =
               Squidie.start(
                 JournalSecretFailureWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      {snapshot, log} =
        with_log(fn ->
          assert {:ok, %Snapshot{} = snapshot} =
                   execute_journal_next(
                     runtime: :journal,
                     journal_storage: @read_model_storage,
                     queue: @read_model_queue,
                     owner_id: "worker_1",
                     claim_id: "claim_1",
                     claim_token: "token_1",
                     now: @read_model_visible_at
                   )

          snapshot
        end)

      assert [%{error: error}] = snapshot.attempts

      assert error == %{
               code: "step_error",
               message: "step execution failed",
               retryable?: false
             }

      assert {:ok, dispatch_entries} =
               Journal.load_entries(@read_model_storage, {:dispatch, @read_model_queue})

      failed_entry = Enum.find(dispatch_entries, &(&1.type == :attempt_failed))

      refute log =~ "super-secret-token"
      refute inspect(failed_entry.data.error) =~ "super-secret-token"
      refute inspect(snapshot) =~ "super-secret-token"
    end

    test "execute_next/1 durably fails native steps that raise arbitrary exceptions" do
      assert {:ok, %Snapshot{} = started_snapshot} =
               Squidie.start(
                 JournalExceptionWorkflow,
                 %{account_id: "acct_123"},
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_started_at
               )

      assert {:ok, %Snapshot{status: :failed, terminal?: true}} =
               Squidie.execute_next(
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 owner_id: "exception-worker",
                 now: @read_model_visible_at
               )

      assert {:ok, %Snapshot{status: :failed, terminal?: true} = inspected_snapshot} =
               Squidie.inspect_run(started_snapshot.run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert [%{status: :failed, error: error}] = inspected_snapshot.attempts

      assert %{
               code: "step_exception",
               exception: "Jason.DecodeError",
               message: "step execution failed",
               origin: %{
                 module: "Jason",
                 function: "decode!",
                 arity: 2,
                 line: line
               },
               retryable?: false
             } = error

      assert is_integer(line) and line > 0
      refute inspect(inspected_snapshot) =~ "secret-token"
    end

    test "execute_next/1 rejects malformed option lists without leaking claim tokens" do
      assert {:error, reason} =
               execute_journal_next([{:claim_token, "super-secret-token"}, :not_a_pair])

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "execute_next/1 rejects non-list options without leaking claim tokens" do
      assert {:error, reason} = execute_journal_next(%{claim_token: "super-secret-token"})

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "public execute_next/1 rejects internal runtime controls" do
      for option <- [:claim_id, :claim_token, :finished_at] do
        assert {:error, {:invalid_option, {:option, ^option}}} =
                 Squidie.execute_next(
                   Keyword.put(
                     [
                       runtime: :journal,
                       journal_storage: @read_model_storage
                     ],
                     option,
                     "internal"
                   )
                 )
      end
    end

    test "execute_next/1 redacts invalid option values" do
      secret_value = %{claim_token: "super-secret-token"}

      assert {:error, reason} =
               execute_journal_next(runtime: :journal, finished_at: secret_value)

      assert reason == {:invalid_option, {:finished_at, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 now: secret_value
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 queue: secret_value
               )

      assert reason == {:invalid_option, {:queue, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 owner_id: secret_value
               )

      assert reason == {:invalid_option, {:owner_id, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 heartbeat_interval_ms: secret_value
               )

      assert reason == {:invalid_option, {:heartbeat_interval_ms, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, {:invalid_option, {:heartbeat_interval_ms, :invalid}}} =
               execute_journal_next(
                 runtime: :journal,
                 journal_storage: @read_model_storage,
                 heartbeat_interval_ms: 49
               )
    end

    test "explain_run/2 can read from the read model" do
      append_read_model_run_entries([
        read_model_run_started(),
        read_model_runnables_planned()
      ])

      assert {:ok, %Diagnostic{} = explanation} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: @read_model_queue,
                 now: @read_model_visible_at
               )

      assert explanation.run_id == @read_model_run_id
      assert explanation.workflow == @read_model_workflow
      assert explanation.queue == @read_model_queue
      assert explanation.reason == :planned_dispatch_pending_schedule
      assert explanation.next_actions == [:schedule_pending_dispatch]
    end

    test "read model infers Ecto journal storage from the configured repo" do
      assert {:error, :not_found} =
               Squidie.inspect_run(@read_model_run_id, read_model: :read_model)

      assert {:error, :not_found} =
               Squidie.explain_run(@read_model_run_id, read_model: :read_model)
    end

    test "read model rejects malformed journal storage without leaking options" do
      assert {:error, {:invalid_option, {:journal_storage, Jido.Storage.File}}} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: {Jido.Storage.File, []}
               )

      assert {:error, {:invalid_option, {:journal_storage, String}}} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: {String, path: "/tmp/squidie_storage", token: "redacted"}
               )
    end

    test "returns a structured error for unsupported read models" do
      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               Squidie.inspect_run(@read_model_run_id, read_model: :unknown)

      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               Squidie.explain_run(@read_model_run_id, read_model: :unknown)

      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               Squidie.inspect_run(@read_model_run_id, read_model: :unsupported)

      assert {:error, {:invalid_option, {:read_model, :invalid}}} =
               Squidie.explain_run(@read_model_run_id, read_model: :unsupported)
    end

    test "read model APIs redact invalid read_model values" do
      assert {:error, reason} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:read_model, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: %{claim_token: "super-secret-token"}
               )

      assert reason == {:invalid_option, {:read_model, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "returns a structured error for malformed option lists" do
      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.inspect_run(@read_model_run_id, [:bad])

      assert {:error, {:invalid_option, {:opts, :invalid}}} =
               Squidie.explain_run(@read_model_run_id, [:bad])
    end

    test "read model APIs reject malformed options without leaking claim tokens" do
      assert {:error, reason} =
               Squidie.inspect_run(@read_model_run_id, %{
                 read_model: :read_model,
                 claim_token: "super-secret-token"
               })

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               Squidie.explain_run(@read_model_run_id, [
                 {:read_model, :read_model},
                 {:claim_token, "super-secret-token"},
                 :not_a_pair
               ])

      assert reason == {:invalid_option, {:opts, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end

    test "read model rejects malformed run ids without raising" do
      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               Squidie.inspect_run(123,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               Squidie.explain_run(123,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )
    end

    test "read model rejects storage-unsafe run ids and queues" do
      assert {:error, {:invalid_option, {:run_id, :invalid}}} =
               Squidie.inspect_run("../run",
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert {:error, {:invalid_option, {:queue, :invalid}}} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: "../dispatch"
               )
    end

    test "read model redacts invalid option values" do
      secret_value = %{claim_token: "super-secret-token"}

      assert {:error, reason} =
               Squidie.inspect_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 now: secret_value
               )

      assert reason == {:invalid_option, {:now, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               Squidie.explain_run(secret_value,
                 read_model: :read_model,
                 journal_storage: @read_model_storage
               )

      assert reason == {:invalid_option, {:run_id, :invalid}}
      refute inspect(reason) =~ "super-secret-token"

      assert {:error, reason} =
               Squidie.explain_run(@read_model_run_id,
                 read_model: :read_model,
                 journal_storage: @read_model_storage,
                 queue: secret_value
               )

      assert reason == {:invalid_option, {:queue, :invalid}}
      refute inspect(reason) =~ "super-secret-token"
    end
  end

  defp append_read_model_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)
  end

  defp append_read_model_dispatch_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, entries)
  end

  defp step_context(%Snapshot{} = snapshot, opts) do
    %Squidie.Step.Context{
      run_id: snapshot.run_id,
      workflow: PaymentRecoveryWorkflow,
      step: Keyword.fetch!(opts, :step),
      attempt: Keyword.get(opts, :attempt, 1),
      runnable_key: Keyword.fetch!(opts, :runnable_key),
      state: Keyword.get(opts, :state, %{})
    }
  end

  defp warm_read_model_storage do
    assert {:ok, seed_entry} =
             DispatchProtocol.new_entry(:run_indexed, %{
               run_id: "storage_seed",
               workflow: "StorageSeedWorkflow",
               queue: @read_model_queue,
               occurred_at: @read_model_started_at
             })

    assert {:ok, _thread} = Journal.append_entries(@read_model_storage, [seed_entry])

    assert :ok =
             Journal.put_checkpoint(
               @read_model_storage,
               {:run_index, "StorageSeedWorkflow"},
               Squidie.Runtime.RunIndexProjection.new("StorageSeedWorkflow"),
               1
             )
  end

  defp read_model_run_started(overrides \\ %{}) do
    {:ok, definition} = Definition.load(BillingWorkflow)

    attrs =
      Map.merge(
        %{
          run_id: @read_model_run_id,
          workflow: @read_model_workflow,
          definition_version: definition.definition_version,
          definition_fingerprint: Definition.fingerprint(definition),
          occurred_at: @read_model_started_at
        },
        overrides
      )

    read_model_entry!(:run_started, attrs)
  end

  defp read_model_runnables_planned do
    read_model_entry!(:runnables_planned, %{
      run_id: @read_model_run_id,
      runnables: [read_model_planned_runnable()],
      occurred_at: @read_model_visible_at
    })
  end

  defp read_model_attempt_scheduled do
    read_model_entry!(:attempt_scheduled, read_model_scheduled_attrs())
  end

  defp read_model_dynamic_work_recorded(overrides \\ %{}) do
    attrs =
      Map.merge(
        %{
          run_id: @read_model_run_id,
          dynamic_key: "subscription_digest_fanout",
          origin: %{
            runnable_key: @read_model_runnable_key,
            step: "charge_card",
            attempt: 1
          },
          reason: :runtime_fanout,
          nodes: [
            %{
              id: "deliver_digest:chat_1",
              action: "digest.deliver",
              metadata: %{chat_id: "chat_1", secret: "redacted"}
            }
          ],
          metadata: %{source: "subscription_query"},
          occurred_at: @read_model_visible_at
        },
        overrides
      )

    read_model_entry!(
      :dynamic_work_recorded,
      attrs
    )
  end

  defp read_model_planned_runnable do
    Map.delete(read_model_scheduled_attrs(), :occurred_at)
  end

  defp read_model_scheduled_attrs do
    %{
      run_id: @read_model_run_id,
      runnable_key: @read_model_runnable_key,
      idempotency_key: @read_model_idempotency_key,
      attempt_number: 1,
      queue: @read_model_queue,
      step: "charge_card",
      input: %{"payment_id" => "pay_123"},
      visible_at: @read_model_visible_at,
      occurred_at: @read_model_started_at
    }
  end

  defp read_model_entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp string_keyed_definition_metadata(%{data: data} = entry) do
    data =
      data
      |> Map.put("definition_version", Map.get(data, :definition_version))
      |> Map.put("definition_fingerprint", Map.fetch!(data, :definition_fingerprint))
      |> Map.delete(:definition_version)
      |> Map.delete(:definition_fingerprint)

    %{entry | data: data}
  end

  defp claim_token_hash(token) do
    Base.encode16(:crypto.hash(:sha256, token), case: :lower)
  end

  defp transactional_events(run_id) do
    Repo.all(
      from(event in "transactional_events",
        where: event.run_id == type(^run_id, Ecto.UUID),
        order_by: event.id,
        select: event.event
      )
    )
  end

  defp run_entries(run_id, storage) do
    run_id
    |> raw_run_entries(storage)
    |> Enum.reject(&(&1.type == :run_signal_received))
  end

  defp load_read_model_run_entries(run_id) do
    {:ok, run_entries(run_id, @read_model_storage)}
  end

  defp raw_run_entries(run_id, storage) do
    assert {:ok, entries} = Journal.load_entries(storage, {:run, run_id})
    entries
  end

  defp journal_start_runnable(run_id, account_id \\ "acct_123") do
    %{
      run_id: run_id,
      runnable_key: "#{run_id}:check_gateway:1",
      idempotency_key: "#{run_id}:check_gateway:1",
      attempt_number: 1,
      queue: @read_model_queue,
      step: "check_gateway",
      input: %{account_id: account_id},
      visible_at: @read_model_started_at
    }
  end

  defp journal_missing_path_runnable(run_id) do
    %{
      run_id: run_id,
      runnable_key: "#{run_id}:load_review_context:1",
      idempotency_key: "#{run_id}:load_review_context:1",
      attempt_number: 1,
      queue: @read_model_queue,
      step: "load_review_context",
      input: %{draft: %{}},
      visible_at: @read_model_started_at
    }
  end

  defp read_model_table_name(:checkpoints),
    do: :squidie_read_model_squidie_test_checkpoints

  defp read_model_table_name(:threads),
    do: :squidie_read_model_squidie_test_threads

  defp read_model_table_name(:thread_meta),
    do: :squidie_read_model_squidie_test_thread_meta

  defp cleanup_read_model_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(read_model_table_name(suffix))
    end
  end

  defp delete_table_if_present(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end

  defp put_squidie_config(overrides) do
    original_config = Application.get_all_env(:squidie)

    on_exit(fn ->
      :squidie
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:squidie, &1))

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(:squidie, key, value)
      end)
    end)

    Enum.each(overrides, fn {key, value} ->
      Application.put_env(:squidie, key, value)
    end)
  end
end
