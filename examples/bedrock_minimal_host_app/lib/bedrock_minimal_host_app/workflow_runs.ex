defmodule BedrockMinimalHostApp.WorkflowRuns do
  @moduledoc """
  Application-facing boundary for workflow operations in the example host app.

  A real Phoenix or OTP application would call Squidie from a context or
  service like this one rather than directly from controllers or jobs.
  """

  @type payment_recovery_attrs :: %{
          required(:account_id) => String.t(),
          required(:invoice_id) => String.t(),
          required(:attempt_id) => String.t(),
          required(:gateway_url) => String.t()
        }

  @type cancellable_wait_attrs :: %{
          required(:account_id) => String.t()
        }

  @type retry_verification_attrs :: %{
          required(:attempt_id) => String.t()
        }

  @type dependency_recovery_attrs :: %{
          required(:account_id) => String.t(),
          required(:invoice_id) => String.t(),
          required(:attempt_id) => String.t()
        }

  @type manual_approval_attrs :: %{
          required(:account_id) => String.t()
        }

  @type manual_pause_attrs :: %{
          required(:account_id) => String.t()
        }

  @type manual_digest_attrs :: %{
          required(:channel) => String.t(),
          required(:digest_date) => String.t()
        }

  @type runtime_digest_attrs :: %{
          required(:channel) => String.t(),
          required(:digest_date) => String.t()
        }

  @type saga_checkout_attrs :: %{
          required(:account_id) => String.t(),
          required(:order_id) => String.t()
        }

  @type local_ledger_checkout_attrs :: %{
          required(:account_id) => String.t(),
          optional(:fail_after_reserve) => boolean()
        }

  @type nested_invite_delivery_attrs :: %{
          required(:party_id) => String.t(),
          required(:guest_id) => String.t(),
          required(:child_queue) => String.t(),
          optional(:fail_after_child_start) => boolean(),
          optional(:fail_child_once) => boolean()
        }

  @type run_result :: Squidie.ReadModel.Inspection.Snapshot.t()
  @type explanation_result :: Squidie.ReadModel.Explanation.Diagnostic.t()
  @type listing_result :: Squidie.ReadModel.Listing.Summary.t()

  alias BedrockMinimalHostApp.Steps
  alias Squidie.Runtime.Signal

  @spec start_payment_recovery(payment_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_payment_recovery(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.PaymentRecovery, :payment_recovery, attrs)
  end

  @spec start_cancellable_wait(cancellable_wait_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_cancellable_wait(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.CancellableWait, attrs)
  end

  @spec start_retry_verification(retry_verification_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_retry_verification(attrs) when is_map(attrs) do
    Squidie.start(
      BedrockMinimalHostApp.Workflows.RetryVerification,
      :retry_verification,
      attrs
    )
  end

  @spec start_dependency_recovery(dependency_recovery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_dependency_recovery(attrs) when is_map(attrs) do
    Squidie.start(
      BedrockMinimalHostApp.Workflows.DependencyRecovery,
      :dependency_recovery,
      attrs
    )
  end

  @spec start_manual_approval(manual_approval_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_approval(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.ManualApproval, :manual_approval, attrs)
  end

  @spec start_manual_pause(manual_pause_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_pause(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.ManualPause, :manual_pause, attrs)
  end

  @spec start_manual_digest(manual_digest_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_manual_digest(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.DailyDigest, :manual_digest, attrs)
  end

  @doc """
  Starts a runtime-authored digest workflow through the public spec API.
  """
  @spec start_runtime_digest(runtime_digest_attrs(), keyword()) ::
          {:ok, run_result()} | {:error, term()}
  def start_runtime_digest(attrs, opts \\ []) when is_map(attrs) and is_list(opts) do
    Squidie.start_spec(
      runtime_digest_spec(),
      :manual_digest,
      attrs,
      Keyword.put(opts, :action_registry, runtime_action_registry())
    )
  end

  @doc """
  Starts the saga checkout example that compensates completed side effects.
  """
  @spec start_saga_checkout(saga_checkout_attrs()) :: {:ok, run_result()} | {:error, term()}
  def start_saga_checkout(attrs) when is_map(attrs) do
    Squidie.start(BedrockMinimalHostApp.Workflows.SagaCheckout, :saga_checkout, attrs)
  end

  @doc """
  Starts the local ledger checkout example that uses one host repo transaction.
  """
  @spec start_local_ledger_checkout(local_ledger_checkout_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_local_ledger_checkout(attrs) when is_map(attrs) do
    Squidie.start(
      BedrockMinimalHostApp.Workflows.LocalLedgerCheckout,
      :local_ledger_checkout,
      attrs
    )
  end

  @doc """
  Starts the nested invite delivery example that creates a child workflow run.
  """
  @spec start_nested_invite_delivery(nested_invite_delivery_attrs()) ::
          {:ok, run_result()} | {:error, term()}
  def start_nested_invite_delivery(attrs) when is_map(attrs) do
    Squidie.start(
      BedrockMinimalHostApp.Workflows.NestedInviteDelivery,
      :nested_invite_delivery,
      attrs
    )
  end

  @spec inspect_payment_recovery(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def inspect_payment_recovery(run_id) do
    Squidie.inspect_run(run_id)
  end

  @spec inspect_run(Ecto.UUID.t(), keyword()) :: {:ok, run_result()} | {:error, term()}
  def inspect_run(run_id, opts \\ []) do
    Squidie.inspect_run(run_id, opts)
  end

  @spec explain_run(Ecto.UUID.t()) :: {:ok, explanation_result()} | {:error, term()}
  def explain_run(run_id) do
    Squidie.explain_run(run_id)
  end

  @spec cancel(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def cancel(run_id) do
    with {:ok, signal} <-
           Signal.cancel_run(run_id,
             metadata: %{source: "bedrock_minimal_host_app.workflow_runs"}
           ) do
      Squidie.apply_signal(signal)
    end
  end

  @spec resume(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def resume(run_id), do: resume(run_id, %{})

  @spec resume(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def resume(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.resume_run(run_id, attrs) do
      Squidie.apply_signal(signal)
    end
  end

  @spec approve(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def approve(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.approve_run(run_id, attrs) do
      Squidie.apply_signal(signal)
    end
  end

  @spec reject(Ecto.UUID.t(), map()) :: {:ok, run_result()} | {:error, term()}
  def reject(run_id, attrs) when is_map(attrs) do
    with {:ok, signal} <- Signal.reject_run(run_id, attrs) do
      Squidie.apply_signal(signal)
    end
  end

  @spec replay(Ecto.UUID.t()) :: {:ok, run_result()} | {:error, term()}
  def replay(run_id) do
    Squidie.replay(run_id)
  end

  @spec list_daily_digest_runs() :: {:ok, [listing_result()]} | {:error, term()}
  def list_daily_digest_runs do
    Squidie.list_runs(workflow: BedrockMinimalHostApp.Workflows.DailyDigest)
  end

  defp runtime_action_registry do
    %{
      "digest.record_delivery" => Steps.RecordDigestDelivery
    }
  end

  defp runtime_digest_spec do
    %{
      workflow: BedrockMinimalHostApp.RuntimeAuthoredDigest,
      definition_version: "bedrock-minimal-host-runtime-digest-v1",
      triggers: [
        %{
          name: :manual_digest,
          type: :manual,
          config: %{},
          payload: [
            %{name: :channel, type: :string, opts: []},
            %{name: :digest_date, type: :string, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :channel, type: :string, opts: []},
        %{name: :digest_date, type: :string, opts: []}
      ],
      steps: [
        %{name: :record_digest_delivery, action: "digest.record_delivery", opts: []}
      ],
      transitions: [
        %{from: :record_digest_delivery, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:record_digest_delivery],
      initial_step: :record_digest_delivery,
      entry_step: :record_digest_delivery
    }
  end
end
