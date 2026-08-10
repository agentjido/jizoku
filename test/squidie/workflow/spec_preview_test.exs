defmodule Squidie.Workflow.SpecPreviewTest do
  use ExUnit.Case, async: true

  alias Squidie.Runs.SpecPreview

  defmodule PreviewWorkflow do
  end

  defmodule LoadAccount do
    use Squidie.Step,
      name: "Load account",
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: [id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: raise("preview must not call durable run/2")

    @spec dry_run(map(), map()) :: Squidie.Step.result()
    def dry_run(%{account_id: account_id}, context) do
      {:ok,
       %{
         id: account_id,
         source: :preview,
         step: context.step
       }}
    end
  end

  defmodule SendReceipt do
    use Squidie.Step,
      name: "Send receipt",
      input_schema: [account: [type: :map, required: true]],
      output_schema: [receipt: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: raise("preview must not call durable run/2")

    @spec dry_run(map(), map()) :: Squidie.Step.result()
    def dry_run(%{account: %{id: account_id}}, context) do
      {:ok,
       %{
         receipt: %{
           account_id: account_id,
           preview?: context.preview?
         }
       }}
    end
  end

  defmodule UnsupportedDelivery do
    use Squidie.Step,
      name: "Unsupported delivery",
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: []

    @impl Squidie.Step
    def run(_input, _context), do: raise("preview must not call durable run/2")
  end

  defmodule StrictInput do
    use Squidie.Step,
      name: "Strict input",
      input_schema: [external_id: [type: :string, required: true]],
      output_schema: []

    @impl Squidie.Step
    def run(_input, _context), do: raise("preview must not call durable run/2")

    @spec dry_run(map(), map()) :: Squidie.Step.result()
    def dry_run(_input, _context), do: {:ok, %{}}
  end

  defmodule ContinueInPreview do
    use Squidie.Step,
      name: "Continue in preview",
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: []

    @impl Squidie.Step
    def run(_input, _context), do: raise("preview must not call durable run/2")

    @spec dry_run(map(), map()) :: Squidie.Step.result()
    def dry_run(input, _context) do
      {:continue_as_new, input, key: "preview-next", definition: :current}
    end
  end

  defmodule AllowGuardrail do
    @spec validate_guardrail(map(), map()) :: {:ok, map()}
    def validate_guardrail(_value, context) do
      {:ok, %{placement: context.placement, step: context.step, step_index: context.step_index}}
    end
  end

  defmodule DenyGuardrail do
    @spec validate_guardrail(map(), map()) :: {:error, map()}
    def validate_guardrail(_value, context) do
      {:error,
       %{
         message: "preview value is outside host policy",
         placement: context.placement,
         step: context.step
       }}
    end
  end

  test "executes opted-in dry-run actions and returns structured node output" do
    registry = %{
      "billing.load_account" => [module: LoadAccount, dry_run: true],
      "billing.send_receipt" => [module: SendReceipt, dry_run: true]
    }

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(spec(), %{account_id: "acct_123"}, action_registry: registry)

    assert preview.run_id == nil
    assert preview.workflow == PreviewWorkflow
    assert preview.status == :completed

    assert [
             %{
               id: "load_account",
               action: "billing.load_account",
               status: :completed,
               input: %{account_id: "acct_123"},
               output: %{id: "acct_123", source: :preview, step: :load_account}
             },
             %{
               id: "send_receipt",
               action: "billing.send_receipt",
               status: :completed,
               input: %{account: %{id: "acct_123", source: :preview, step: :load_account}},
               output: %{receipt: %{account_id: "acct_123", preview?: true}}
             }
           ] = preview.nodes

    assert %{
             source: :workflow_spec,
             status: :completed,
             run_id: nil,
             nodes: [%{debug: %{input: %{account_id: "acct_123"}}}, _receipt]
           } = SpecPreview.to_map(preview)
  end

  test "marks actions without registry dry-run opt-in as unsupported without calling run" do
    registry = %{
      "billing.load_account" => [module: UnsupportedDelivery]
    }

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(single_step_spec(), %{account_id: "acct_123"},
               action_registry: registry
             )

    assert preview.status == :blocked

    assert [
             %{
               id: "load_account",
               action: "billing.load_account",
               status: :unsupported,
               error: %{
                 code: :unsupported_preview,
                 message: "step :load_account does not support dry-run preview"
               }
             }
           ] = preview.nodes
  end

  test "returns node validation errors without executing dry-run callbacks" do
    registry = %{
      "billing.load_account" => [module: StrictInput, dry_run: true]
    }

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(single_step_spec(), %{account_id: "acct_123"},
               action_registry: registry
             )

    assert preview.status == :invalid

    assert [
             %{
               id: "load_account",
               status: :validation_error,
               error: %{
                 code: :invalid_action_input,
                 details: %{validation_errors: %{external_id: "input field is required"}}
               }
             }
           ] = preview.nodes
  end

  test "returns guardrail decisions for successful preview nodes" do
    registry = %{
      "billing.load_account" => [module: LoadAccount, dry_run: true]
    }

    guardrail_registry = %{"billing.account_policy" => AllowGuardrail}

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(
               guarded_single_step_spec(
                 input: ["billing.account_policy"],
                 output: ["billing.account_policy"]
               ),
               %{account_id: "acct_123"},
               action_registry: registry,
               guardrail_registry: guardrail_registry
             )

    assert preview.status == :completed

    assert [
             %{
               id: "load_account",
               status: :completed,
               guardrails: [
                 %{key: "billing.account_policy", placement: :input, status: :passed},
                 %{key: "billing.account_policy", placement: :output, status: :passed}
               ],
               debug: %{
                 guardrails: [
                   %{result: %{placement: :input, step: :load_account}},
                   %{result: %{placement: :output, step: :load_account}}
                 ]
               }
             }
           ] = preview.nodes
  end

  test "evaluates preview guardrails with the real step index" do
    registry = %{
      "billing.load_account" => [module: LoadAccount, dry_run: true],
      "billing.send_receipt" => [module: SendReceipt, dry_run: true]
    }

    guardrail_registry = %{"billing.account_policy" => AllowGuardrail}

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(
               guarded_second_step_spec(output: ["billing.account_policy"]),
               %{account_id: "acct_123"},
               action_registry: registry,
               guardrail_registry: guardrail_registry
             )

    assert [
             _load_account,
             %{
               id: "send_receipt",
               status: :completed,
               debug: %{
                 guardrails: [
                   %{result: %{placement: :output, step: :send_receipt, step_index: 1}}
                 ]
               }
             }
           ] = preview.nodes
  end

  test "marks blocking guardrail preview failures as validation errors" do
    registry = %{
      "billing.load_account" => [module: LoadAccount, dry_run: true]
    }

    guardrail_registry = %{"billing.account_policy" => DenyGuardrail}

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(
               guarded_single_step_spec(input: ["billing.account_policy"]),
               %{account_id: "acct_123"},
               action_registry: registry,
               guardrail_registry: guardrail_registry
             )

    assert preview.status == :invalid

    assert [
             %{
               id: "load_account",
               status: :validation_error,
               error: %{
                 code: :guardrail_failed,
                 message: "step :load_account input guardrail \"billing.account_policy\" failed"
               },
               guardrails: [
                 %{key: "billing.account_policy", placement: :input, status: :failed}
               ]
             }
           ] = preview.nodes
  end

  test "rejects missing action registry before previewing runtime-authored actions" do
    assert {:error, {:invalid_option, {:action_registry, :required}}} =
             Squidie.preview_spec(spec(), %{account_id: "acct_123"})
  end

  test "rejects native continue-as-new results in dry-run previews" do
    registry = %{
      "billing.load_account" => [module: ContinueInPreview, dry_run: true]
    }

    assert {:ok, %SpecPreview{} = preview} =
             Squidie.preview_spec(single_step_spec(), %{account_id: "acct_123"},
               action_registry: registry
             )

    assert preview.status == :failed

    assert [
             %{
               status: :failed,
               error: %{
                 message: "native continue-as-new is not supported in dry-run previews",
                 retryable?: false
               }
             }
           ] = preview.nodes
  end

  defp spec do
    %{
      workflow: PreviewWorkflow,
      definition_version: "2026-06-10.preview-test",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :account_id, type: :string, opts: [required: true]}]
        }
      ],
      payload: [%{name: :account_id, type: :string, opts: [required: true]}],
      steps: [
        %{
          name: :load_account,
          action: "billing.load_account",
          opts: [input: [account_id: [:account_id]], output: :account]
        },
        %{
          name: :send_receipt,
          action: "billing.send_receipt",
          opts: [input: [account: [:account]]]
        }
      ],
      transitions: [
        %{from: :load_account, on: :ok, to: :send_receipt},
        %{from: :send_receipt, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_account],
      initial_step: :load_account,
      entry_step: :load_account
    }
  end

  defp single_step_spec do
    %{
      spec()
      | steps: [
          %{
            name: :load_account,
            action: "billing.load_account",
            opts: [input: [account_id: [:account_id]]]
          }
        ],
        transitions: [%{from: :load_account, on: :ok, to: :complete}]
    }
  end

  defp guarded_single_step_spec(guardrails) do
    %{
      single_step_spec()
      | steps: [
          %{
            name: :load_account,
            action: "billing.load_account",
            opts: [input: [account_id: [:account_id]], guardrails: guardrails]
          }
        ]
    }
  end

  defp guarded_second_step_spec(guardrails) do
    %{
      spec()
      | steps: [
          %{
            name: :load_account,
            action: "billing.load_account",
            opts: [input: [account_id: [:account_id]], output: :account]
          },
          %{
            name: :send_receipt,
            action: "billing.send_receipt",
            opts: [input: [account: [:account]], guardrails: guardrails]
          }
        ]
    }
  end
end
