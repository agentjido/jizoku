defmodule Jizoku.WorkflowTest do
  use ExUnit.Case, async: true

  alias __MODULE__.DependencyWorkflow
  alias __MODULE__.InvoiceReminder
  alias __MODULE__.MalformedSchemaStep
  alias __MODULE__.NativeStepContractWorkflow
  alias __MODULE__.NativeStepStructError

  defmodule NativeStepContractWorkflow.LoadAccount do
    use Jizoku.Step,
      name: :load_account,
      description: "Loads account details",
      input_schema: [
        account_id: [type: :string, required: true]
      ],
      output_schema: [
        account: [type: :map, required: true]
      ]

    @impl Jizoku.Step
    def run(_input, _context), do: {:ok, %{account: %{id: "acct_123"}}}
  end

  defmodule NativeStepContractWorkflow do
    use Jizoku.Workflow

    workflow do
      version "2026-05-26.native-step-contract"

      trigger :manual do
        manual()
      end

      step :load_account, NativeStepContractWorkflow.LoadAccount

      transition :load_account, on: :ok, to: :complete
    end
  end

  defmodule MalformedSchemaStep do
    use Jizoku.Step,
      name: :malformed_schema,
      input_schema: [
        account_id: :string
      ]

    @impl Jizoku.Step
    def run(_input, _context), do: {:ok, %{}}
  end

  defmodule NativeStepStructError do
    defexception [:message, :code]
  end

  defmodule NativeContinuationStep do
    use Jizoku.Step,
      name: :continue_cursor,
      output_schema: [required_output: [type: :string, required: true]]

    @impl Jizoku.Step
    def run(_input, _context) do
      {:continue_as_new, %{cursor: "page-42"}, key: "page-42", definition: :current}
    end
  end

  test "exposes a declarative workflow definition" do
    definition = InvoiceReminder.workflow_definition()

    assert definition.triggers == [
             %{
               name: :manual,
               type: :manual,
               config: %{},
               payload: [
                 %{name: :account_id, type: :string, opts: []},
                 %{name: :invoice_id, type: :string, opts: []}
               ]
             }
           ]

    assert definition.payload == [
             %{name: :account_id, type: :string, opts: []},
             %{name: :invoice_id, type: :string, opts: []}
           ]

    assert definition.steps == [
             %{name: :load_invoice, module: InvoiceReminder.LoadInvoice, opts: []},
             %{
               name: :send_email,
               module: InvoiceReminder.SendEmail,
               opts: [retry: [max_attempts: 3]]
             },
             %{name: :record_delivery, module: InvoiceReminder.RecordDelivery, opts: []}
           ]

    assert definition.transitions == [
             %{from: :load_invoice, on: :ok, to: :send_email},
             %{from: :send_email, on: :ok, to: :record_delivery},
             %{from: :record_delivery, on: :ok, to: :complete}
           ]

    assert definition.retries == [
             %{step: :send_email, opts: [max_attempts: 3]}
           ]

    assert definition.definition_version == nil
    assert definition.entry_step == :load_invoice
  end

  test "exposes a declarative workflow definition version" do
    definition = NativeStepContractWorkflow.workflow_definition()

    assert definition.definition_version == "2026-05-26.native-step-contract"

    assert NativeStepContractWorkflow.__workflow__(:definition_version) ==
             "2026-05-26.native-step-contract"
  end

  test "exposes the workflow contract shape" do
    assert InvoiceReminder.__workflow__(:contract) == %{
             required: [:trigger, :step],
             optional: [:transition]
           }
  end

  test "converts a workflow module into a normalized workflow spec" do
    assert {:ok, %Jizoku.Workflow.Spec{} = spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    assert spec.workflow == InvoiceReminder
    assert spec.definition_version == nil
    assert Enum.map(spec.triggers, & &1.name) == [:manual]
    assert Enum.map(spec.steps, & &1.name) == [:load_invoice, :send_email, :record_delivery]

    assert Enum.map(spec.transitions, &{&1.from, &1.on, &1.to}) == [
             {:load_invoice, :ok, :send_email},
             {:send_email, :ok, :record_delivery},
             {:record_delivery, :ok, :complete}
           ]

    assert spec.retries == [%{step: :send_email, opts: [max_attempts: 3]}]
  end

  test "stores trigger and transition declarations as Spark entities" do
    entities = Spark.Dsl.Extension.get_entities(InvoiceReminder, [:workflow])

    assert [
             %Jizoku.Workflow.TriggerSpec{
               definitions: [%Jizoku.Workflow.TriggerDefinitionSpec{}],
               payload: [
                 %Jizoku.Workflow.PayloadSpec{
                   fields: [
                     %Jizoku.Workflow.PayloadFieldSpec{},
                     %Jizoku.Workflow.PayloadFieldSpec{}
                   ]
                 }
               ]
             }
           ] = Enum.filter(entities, &match?(%Jizoku.Workflow.TriggerSpec{}, &1))

    assert [
             %Jizoku.Workflow.StepSpec{},
             %Jizoku.Workflow.StepSpec{},
             %Jizoku.Workflow.StepSpec{}
           ] = Enum.filter(entities, &match?(%Jizoku.Workflow.StepSpec{}, &1))

    assert [
             %Jizoku.Workflow.TransitionSpec{},
             %Jizoku.Workflow.TransitionSpec{},
             %Jizoku.Workflow.TransitionSpec{}
           ] = Enum.filter(entities, &match?(%Jizoku.Workflow.TransitionSpec{}, &1))

    assert Jizoku.Workflow.Info.triggers(InvoiceReminder) == [
             %{
               name: :manual,
               type: :manual,
               config: %{},
               payload: [
                 %{name: :account_id, type: :string, opts: []},
                 %{name: :invoice_id, type: :string, opts: []}
               ]
             }
           ]

    assert Jizoku.Workflow.Info.transitions(InvoiceReminder) == [
             %{from: :load_invoice, on: :ok, to: :send_email},
             %{from: :send_email, on: :ok, to: :record_delivery},
             %{from: :record_delivery, on: :ok, to: :complete}
           ]
  end

  test "converts a workflow version into the normalized workflow spec" do
    assert {:ok, %Jizoku.Workflow.Spec{} = spec} =
             Jizoku.Workflow.to_spec(NativeStepContractWorkflow)

    assert spec.definition_version == "2026-05-26.native-step-contract"
    assert :ok = Jizoku.Workflow.validate_spec(spec)
  end

  test "validates normalized workflow specs without starting a run" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    assert :ok = Jizoku.Workflow.validate_spec(spec)
  end

  test "returns structured validation errors for invalid workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | transitions: [
          %{from: :load_invoice, on: :ok, to: :missing_step}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:transitions, 0, :to],
             code: :unknown_transition_target,
             message: "transition targets unknown step: :missing_step",
             details: %{to: :missing_step}
           } in errors
  end

  test "returns structured validation errors for malformed root workflow specs" do
    assert {:error, {:invalid_workflow_spec, errors}} = Jizoku.Workflow.validate_spec(nil)

    assert %{
             path: [],
             code: :invalid_spec,
             message: "workflow spec must be a map",
             details: %{spec: nil}
           } in errors
  end

  test "returns structured validation errors for malformed workflow spec collections" do
    invalid_spec = %{
      workflow: InvoiceReminder,
      triggers: [],
      payload: [],
      steps: "not a list",
      transitions: [],
      retries: [],
      entry_steps: [],
      initial_step: nil,
      entry_step: nil
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps],
             code: :invalid_collection,
             message: "steps must be a list",
             details: %{field: :steps, value: "not a list"}
           } in errors
  end

  test "returns structured validation errors for missing workflow spec declarations" do
    invalid_spec = %{
      workflow: InvoiceReminder,
      triggers: [],
      payload: [],
      steps: [],
      transitions: [],
      retries: [],
      entry_steps: [],
      initial_step: nil,
      entry_step: nil
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:triggers],
             code: :missing_triggers,
             message: "at least one trigger is required",
             details: %{}
           } in errors

    assert %{
             path: [:steps],
             code: :missing_steps,
             message: "at least one step is required",
             details: %{}
           } in errors
  end

  test "rejects ambiguous atom and string keys in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec =
      spec
      |> Map.from_struct()
      |> Map.put("workflow", "Elixir.System")

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:workflow],
             code: :ambiguous_key,
             message: "workflow cannot be provided with both atom and string keys",
             details: %{key: :workflow}
           } in errors
  end

  test "rejects ambiguous atom and string keys in nested workflow spec data" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | triggers: [
          %{
            name: :scheduled,
            type: :cron,
            config: %{
              "expression" => "bad",
              expression: "* * * * *",
              timezone: "Etc/UTC"
            },
            payload: [
              %{
                "name" => "account_id",
                name: :account_id,
                type: :string,
                opts: []
              }
            ]
          }
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:triggers, 0, :config, :expression],
             code: :ambiguous_key,
             message: "expression cannot be provided with both atom and string keys",
             details: %{key: :expression}
           } in errors

    assert %{
             path: [:triggers, 0, :payload, 0, :name],
             code: :ambiguous_key,
             message: "name cannot be provided with both atom and string keys",
             details: %{key: :name}
           } in errors
  end

  test "returns structured validation errors for duplicate workflow spec steps" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    [step | _rest] = spec.steps
    invalid_spec = %{spec | steps: [step | spec.steps]}

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 1, :name],
             code: :duplicate_step_name,
             message: "duplicate step name: :load_invoice",
             details: %{step: :load_invoice}
           } in errors
  end

  test "rejects string-keyed nested workflow spec records" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | steps: [
          %{"name" => :load_invoice, "module" => InvoiceReminder.LoadInvoice, "opts" => []}
        ],
        transitions: [],
        retries: [],
        entry_steps: []
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 0, :name],
             code: :invalid_step_name,
             message: "step name must be an atom",
             details: %{step: nil}
           } in errors

    assert %{
             path: [:steps, 0, :opts],
             code: :invalid_step_opts,
             message: "step nil opts must be a keyword list",
             details: %{step: nil, opts: nil}
           } in errors
  end

  test "rejects workflow spec steps missing required opts" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | steps: [
          %{name: :load_invoice, module: InvoiceReminder.LoadInvoice},
          %{name: :send_email, module: InvoiceReminder.SendEmail, opts: []},
          %{name: :record_delivery, module: InvoiceReminder.RecordDelivery, opts: []}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 0, :opts],
             code: :invalid_step_opts,
             message: "step :load_invoice opts must be a keyword list",
             details: %{step: :load_invoice, opts: nil}
           } in errors
  end

  test "rejects workflow specs with non-module workflow atoms" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)
    invalid_spec = %{spec | workflow: :not_a_module}

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:workflow],
             code: :invalid_workflow,
             message: "workflow must be a module atom",
             details: %{workflow: :not_a_module}
           } in errors
  end

  test "rejects invalid workflow spec trigger shape" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | triggers: [%{name: :manual, type: :webhook, config: %{}, payload: []}]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:triggers, 0, :type],
             code: :invalid_trigger_type,
             message: "trigger :manual defines unsupported type :webhook",
             details: %{trigger: :manual, type: :webhook}
           } in errors
  end

  test "rejects workflow spec payload that does not match trigger payloads" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    [trigger | _rest] = spec.triggers
    invalid_spec = %{spec | triggers: [%{trigger | payload: [hd(spec.payload)]}], payload: []}

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:payload],
             code: :invalid_payload_contract,
             message: "payload must match trigger payload fields",
             details: %{payload: [], expected: [hd(spec.payload)]}
           } in errors
  end

  test "rejects workflow spec payload fields missing required opts" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)
    invalid_field = Map.delete(hd(spec.payload), :opts)

    invalid_spec = %{
      spec
      | payload: [invalid_field],
        triggers: [
          %{
            hd(spec.triggers)
            | payload: [invalid_field]
          }
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:payload, 0, :opts],
             code: :invalid_payload_field_opts,
             message: "payload field :account_id opts must be a keyword list",
             details: %{field: :account_id, opts: nil}
           } in errors

    assert %{
             path: [:triggers, 0, :payload, 0, :opts],
             code: :invalid_payload_field_opts,
             message: "payload field :account_id opts must be a keyword list",
             details: %{field: :account_id, opts: nil}
           } in errors
  end

  test "rejects conflicting trigger payload fields in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | triggers: [
          %{
            name: :manual,
            type: :manual,
            config: %{},
            payload: [%{name: :account_id, type: :string, opts: []}]
          },
          %{
            name: :scheduled,
            type: :manual,
            config: %{},
            payload: [%{name: :account_id, type: :integer, opts: []}]
          }
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:triggers],
             code: :conflicting_payload_field,
             message:
               "payload field :account_id defines conflicting types across triggers: [:integer, :string]",
             details: %{field: :account_id, types: [:integer, :string]}
           } in errors
  end

  test "rejects invalid workflow spec step mappings and built-in step options" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | steps: [
          %{name: :wait_for_gateway, module: :wait, opts: [duration: 0]},
          %{
            name: :send_email,
            module: InvoiceReminder.SendEmail,
            opts: [input: [], output: "email"]
          }
        ],
        transitions: [
          %{from: :wait_for_gateway, on: :ok, to: :send_email},
          %{from: :send_email, on: :ok, to: :complete}
        ],
        retries: [],
        entry_steps: [:wait_for_gateway],
        initial_step: :wait_for_gateway,
        entry_step: :wait_for_gateway
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 0, :opts, :duration],
             code: :invalid_wait_duration,
             message: "built-in step :wait_for_gateway requires a positive :duration option",
             details: %{step: :wait_for_gateway, duration: 0}
           } in errors

    assert %{
             path: [:steps, 1, :opts, :input],
             code: :invalid_step_input_mapping,
             message: "step :send_email defines an invalid :input mapping",
             details: %{step: :send_email, input: []}
           } in errors

    assert %{
             path: [:steps, 1, :opts, :output],
             code: :invalid_step_output_mapping,
             message: "step :send_email defines an invalid :output mapping",
             details: %{step: :send_email, output: "email"}
           } in errors
  end

  test "accepts named path input mappings in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    path_mapped_spec = %{
      spec
      | steps: [
          %{
            name: :load_invoice,
            module: InvoiceReminder.LoadInvoice,
            opts: [input: [invoice_id: [:trigger, :invoice_id]]]
          },
          %{name: :send_email, module: InvoiceReminder.SendEmail, opts: []},
          %{name: :record_delivery, module: InvoiceReminder.RecordDelivery, opts: []}
        ],
        retries: []
    }

    assert :ok = Jizoku.Workflow.validate_spec(path_mapped_spec)
  end

  test "rejects invalid workflow spec retry options" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)
    invalid_spec = %{spec | retries: [%{step: :send_email, opts: [max_attempts: 0]}]}

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:retries, 0, :opts, :max_attempts],
             code: :invalid_retry_max_attempts,
             message: "retry for :send_email must define a positive :max_attempts",
             details: %{step: :send_email, max_attempts: 0}
           } in errors
  end

  test "rejects workflow spec retries that disagree with step retry options" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_steps =
      Enum.map(spec.steps, fn
        %{name: :send_email} = step -> %{step | opts: [retry: [max_attempts: 0]]}
        step -> step
      end)

    invalid_spec = %{
      spec
      | steps: invalid_steps,
        retries: [%{step: :send_email, opts: [max_attempts: 1]}]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 1, :opts, :retry, :max_attempts],
             code: :invalid_step_retry_max_attempts,
             message: "retry for :send_email must define a positive :max_attempts",
             details: %{step: :send_email, max_attempts: 0}
           } in errors

    assert %{
             path: [:retries],
             code: :invalid_retry_derivation,
             message: "retries must match step retry options",
             details: %{
               retries: [%{step: :send_email, opts: [max_attempts: 1]}],
               expected: [%{step: :send_email, opts: [max_attempts: 0]}]
             }
           } in errors
  end

  test "rejects duplicate and invalid workflow spec transition recovery markers" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | transitions: [
          %{from: :load_invoice, on: :ok, to: :send_email, recovery: :compensation},
          %{from: :load_invoice, on: :ok, to: :record_delivery}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:transitions, 1],
             code: :duplicate_transition,
             message: "duplicate transition declared from :load_invoice on outcome :ok",
             details: %{from: :load_invoice, on: :ok}
           } in errors

    assert %{
             path: [:transitions, 0, :recovery],
             code: :invalid_transition_recovery,
             message:
               "transition from :load_invoice can only define recovery markers for :error outcomes",
             details: %{from: :load_invoice, on: :ok, recovery: :compensation}
           } in errors
  end

  test "rejects transition workflow specs without a root entry step" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | steps: [
          %{name: :load_invoice, module: InvoiceReminder.LoadInvoice, opts: []},
          %{name: :send_email, module: InvoiceReminder.SendEmail, opts: []}
        ],
        transitions: [
          %{from: :load_invoice, on: :ok, to: :send_email},
          %{from: :send_email, on: :ok, to: :load_invoice}
        ],
        retries: [],
        entry_steps: [],
        initial_step: nil,
        entry_step: nil
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:entry_steps],
             code: :missing_entry_steps,
             message: "workflow must define at least one entry step",
             details: %{}
           } in errors
  end

  test "rejects transition workflow specs with disconnected cycles" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | steps: [
          %{name: :load_invoice, module: InvoiceReminder.LoadInvoice, opts: []},
          %{name: :send_email, module: InvoiceReminder.SendEmail, opts: []},
          %{name: :record_delivery, module: InvoiceReminder.RecordDelivery, opts: []}
        ],
        transitions: [
          %{from: :load_invoice, on: :ok, to: :complete},
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :send_email}
        ],
        retries: [],
        entry_steps: [:load_invoice],
        initial_step: :load_invoice,
        entry_step: :load_invoice
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:transitions],
             code: :transition_cycle,
             message: "workflow transition graph must be acyclic",
             details: %{}
           } in errors
  end

  test "rejects invalid workflow spec dependency graphs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(DependencyWorkflow)

    invalid_spec = %{
      spec
      | steps: [
          %{
            name: :load_account,
            module: DependencyWorkflow.LoadAccount,
            opts: [after: [:send_email]]
          },
          %{
            name: :load_invoice,
            module: DependencyWorkflow.LoadInvoice,
            opts: [after: [:missing_step]]
          },
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [after: [:load_account]]
          }
        ],
        transitions: [
          %{from: :load_account, on: :ok, to: :send_email}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:steps, 1, :opts, :after],
             code: :unknown_step_dependency,
             message: "step :load_invoice depends on unknown step :missing_step",
             details: %{step: :load_invoice, dependency: :missing_step}
           } in errors

    assert %{
             path: [:steps],
             code: :dependency_cycle,
             message: "workflow dependency graph must be acyclic",
             details: %{}
           } in errors

    assert %{
             path: [:transitions],
             code: :dependency_transitions,
             message: "dependency-based workflows cannot declare transitions",
             details: %{}
           } in errors
  end

  test "rejects workflow specs with entry metadata that does not match the graph" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    invalid_spec = %{
      spec
      | entry_steps: [:send_email],
        initial_step: :send_email,
        entry_step: :send_email
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(invalid_spec)

    assert %{
             path: [:entry_steps],
             code: :invalid_entry_steps,
             message: "entry_steps must match workflow roots",
             details: %{entry_steps: [:send_email], expected: [:load_invoice]}
           } in errors

    assert %{
             path: [:initial_step],
             code: :invalid_initial_step,
             message: "initial_step must be the first workflow root",
             details: %{initial_step: :send_email, expected: :load_invoice}
           } in errors

    assert %{
             path: [:entry_step],
             code: :invalid_entry_step,
             message: "entry_step must match the transition workflow root",
             details: %{entry_step: :send_email, expected: :load_invoice}
           } in errors
  end

  test "rejects serialized module names in workflow specs without resolving modules" do
    serialized_spec = %{
      workflow: "Elixir.System",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: []
        }
      ],
      payload: [],
      steps: [
        %{name: :load_invoice, module: "Elixir.System", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.validate_spec(serialized_spec)

    assert %{
             path: [:workflow],
             code: :invalid_workflow,
             message: "workflow must be a module atom",
             details: %{workflow: "Elixir.System"}
           } in errors

    assert %{
             path: [:steps, 0, :module],
             code: :invalid_step_module,
             message: "step :load_invoice must use a module atom or built-in step kind",
             details: %{step: :load_invoice, module: "Elixir.System"}
           } in errors
  end

  test "supports dependency-based step declarations with multiple entry steps" do
    definition = DependencyWorkflow.workflow_definition()

    assert definition.steps == [
             %{name: :load_account, module: DependencyWorkflow.LoadAccount, opts: []},
             %{name: :load_invoice, module: DependencyWorkflow.LoadInvoice, opts: []},
             %{
               name: :send_email,
               module: DependencyWorkflow.SendEmail,
               opts: [after: [:load_account, :load_invoice]]
             }
           ]

    assert definition.entry_steps == [:load_account, :load_invoice]
    assert definition.initial_step == :load_account
    assert definition.entry_step == nil
  end

  test "fingerprint canonicalizes unordered dependency declarations" do
    base_definition = %{
      steps: [
        %{
          name: :send_email,
          module: DependencyWorkflow.SendEmail,
          opts: [after: [:load_invoice, :load_account]]
        }
      ],
      transitions: [],
      retries: []
    }

    reordered_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [after: [:load_account, :load_invoice]]
          }
        ]
    }

    assert Jizoku.Workflow.Definition.fingerprint(base_definition) ==
             Jizoku.Workflow.Definition.fingerprint(reordered_definition)
  end

  test "fingerprint treats missing dependency declarations as empty dependencies" do
    base_definition = %{
      steps: [
        %{
          name: :send_email,
          module: DependencyWorkflow.SendEmail,
          opts: []
        }
      ],
      transitions: [],
      retries: []
    }

    explicit_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [after: []]
          }
        ]
    }

    assert Jizoku.Workflow.Definition.fingerprint(base_definition) ==
             Jizoku.Workflow.Definition.fingerprint(explicit_definition)
  end

  test "fingerprint canonicalizes named path input mappings" do
    base_definition = %{
      steps: [
        %{
          name: :send_email,
          module: DependencyWorkflow.SendEmail,
          opts: [
            input: [
              invoice_id: [:invoice, :id],
              account_id: [:account, :id]
            ]
          ]
        }
      ],
      transitions: [],
      retries: []
    }

    reordered_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [
              input: [
                account_id: [:account, :id],
                invoice_id: [:invoice, :id]
              ]
            ]
          }
        ]
    }

    assert Jizoku.Workflow.Definition.fingerprint(base_definition) ==
             Jizoku.Workflow.Definition.fingerprint(reordered_definition)
  end

  test "fingerprint includes step recovery semantics" do
    base_definition = %{
      steps: [
        %{
          name: :send_email,
          module: DependencyWorkflow.SendEmail,
          opts: []
        }
      ],
      transitions: [],
      retries: []
    }

    irreversible_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [irreversible: true]
          }
        ]
    }

    non_compensatable_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [compensatable: false]
          }
        ]
    }

    compensated_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [compensate: DependencyWorkflow.LoadInvoice]
          }
        ]
    }

    changed_compensation_definition = %{
      base_definition
      | steps: [
          %{
            name: :send_email,
            module: DependencyWorkflow.SendEmail,
            opts: [compensate: DependencyWorkflow.LoadAccount]
          }
        ]
    }

    base_fingerprint = Jizoku.Workflow.Definition.fingerprint(base_definition)
    compensated_fingerprint = Jizoku.Workflow.Definition.fingerprint(compensated_definition)

    refute base_fingerprint ==
             Jizoku.Workflow.Definition.fingerprint(irreversible_definition)

    refute base_fingerprint ==
             Jizoku.Workflow.Definition.fingerprint(non_compensatable_definition)

    refute base_fingerprint ==
             compensated_fingerprint

    refute compensated_fingerprint ==
             Jizoku.Workflow.Definition.fingerprint(changed_compensation_definition)
  end

  test "supports explicit step input and output mapping options" do
    module =
      compile_module("""
      defmodule WorkflowWithStepMappings do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :account_id, :string
              field :invoice_id, :string
            end
          end

          step :load_account, WorkflowWithStepMappings.LoadAccount,
            input: [:account_id],
            output: :account

          transition :load_account, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{
               name: :load_account,
               module: Module.safe_concat(module, LoadAccount),
               opts: [input: [:account_id], output: :account]
             }
           ]
  end

  test "supports named path step input mapping options" do
    module =
      compile_module("""
      defmodule WorkflowWithNamedPathStepMappings do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :record_review, WorkflowWithNamedPathStepMappings.RecordReview,
            input: [
              drafts: [:draft, :drafts],
              reviewer: [:review_draft, :reviewer]
            ],
            output: :review

          transition :record_review, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{
               name: :record_review,
               module: Module.safe_concat(module, RecordReview),
               opts: [
                 input: [
                   drafts: [:draft, :drafts],
                   reviewer: [:review_draft, :reviewer]
                 ],
                 output: :review
               ]
             }
           ]
  end

  test "stores native Jizoku step contract metadata in the workflow definition" do
    assert [
             %{
               name: :load_account,
               module: step_module,
               opts: [],
               metadata: %{
                 contract: :jizoku_step,
                 name: :load_account,
                 description: "Loads account details",
                 input_schema: [account_id: [type: :string, required: true]],
                 output_schema: [account: [type: :map, required: true]]
               }
             }
           ] = NativeStepContractWorkflow.workflow_definition().steps

    assert step_module == NativeStepContractWorkflow.LoadAccount

    assert [
             %Jizoku.Workflow.StepSpec{
               metadata: %{
                 contract: :jizoku_step,
                 name: :load_account,
                 input_schema: [account_id: [type: :string, required: true]]
               }
             }
           ] = Jizoku.Workflow.Info.steps(NativeStepContractWorkflow)
  end

  test "resolves native step metadata when a step module is compiled after the workflow" do
    compiled_modules =
      Code.compile_string(
        """
        defmodule WorkflowWithLateNativeStep do
          use Jizoku.Workflow

          workflow do
            trigger :manual do
              manual()
            end

            step :load_account, WorkflowWithLateNativeStep.LoadAccount

            transition :load_account, on: :ok, to: :complete
          end
        end

        defmodule WorkflowWithLateNativeStep.LoadAccount do
          use Jizoku.Step,
            name: :load_account,
            description: "Loads late account details",
            input_schema: [
              account_id: [type: :string, required: true]
            ],
            output_schema: [
              account: [type: :map, required: true]
            ]

          @impl Jizoku.Step
          def run(_input, _context), do: {:ok, %{account: %{id: "acct_late"}}}
        end
        """,
        "test/support/late_native_step_workflow.exs"
      )

    workflow = compiled_module!(compiled_modules, WorkflowWithLateNativeStep)
    step_module = compiled_module!(compiled_modules, WorkflowWithLateNativeStep.LoadAccount)

    assert [
             %{
               name: :load_account,
               module: ^step_module,
               metadata: %{
                 contract: :jizoku_step,
                 description: "Loads late account details",
                 input_schema: [account_id: [type: :string, required: true]]
               }
             }
           ] = workflow.workflow_definition().steps
  end

  test "returns structured errors for malformed native step schemas" do
    assert {:error,
            %{
              message: "native step input schema is invalid",
              retryable?: false
            }} = Jizoku.Step.validate_input(MalformedSchemaStep, %{})
  end

  test "normalizes native step struct errors before adding retry metadata" do
    assert {:error,
            %{
              message: "declined",
              code: "card_declined",
              type: "Jizoku.WorkflowTest.NativeStepStructError",
              retryable?: false
            }} =
             Jizoku.Step.normalize_result(
               {:error, %NativeStepStructError{message: "declined", code: "card_declined"}}
             )
  end

  test "normalizes native step deferred continuation results" do
    assert {:ok, %{},
            [
              defer: %{reason: %{code: "gateway_pending"}},
              schedule_in: 30
            ]} =
             Jizoku.Step.normalize_result({:defer, %{code: "gateway_pending"}, schedule_in: 30})
  end

  test "rejects deferred continuation results without a positive schedule" do
    assert {:error,
            %{
              message:
                "native step deferred continuation requires a positive :schedule_in option",
              details: :missing_schedule_in,
              retryable?: false
            }} = Jizoku.Step.normalize_result({:defer, :pending, []})

    assert {:error,
            %{
              message:
                "native step deferred continuation requires a positive :schedule_in option",
              details: %{schedule_in: 0},
              retryable?: false
            }} = Jizoku.Step.normalize_result({:defer, :pending, schedule_in: 0})
  end

  test "rejects deferred continuation controls in success options" do
    assert {:error,
            %{
              message:
                "native step deferred continuation must use {:defer, reason, opts}; :defer is reserved in success options",
              details: :reserved_success_option,
              retryable?: false
            }} =
             Jizoku.Step.normalize_result(
               {:ok, %{gateway: %{status: "pending"}}, defer: %{reason: :pending}}
             )
  end

  test "normalizes native continue-as-new results" do
    assert {:continue_as_new,
            %{
              input: %{cursor: "page-42"},
              continuation_key: "page-42",
              definition: :current
            }} =
             Jizoku.Step.normalize_result(
               {:continue_as_new, %{cursor: "page-42"}, key: "page-42", definition: :current}
             )
  end

  test "native continue-as-new bypasses current-step output validation" do
    assert {:continue_as_new,
            %{
              input: %{cursor: "page-42"},
              continuation_key: "page-42",
              definition: :current
            }} =
             Jizoku.Step.Action.run(
               %{step: NativeContinuationStep, input: %{}},
               %{
                 run_id: "11111111-1111-5111-8111-111111111111",
                 workflow: NativeStepContractWorkflow,
                 step: :continue_cursor
               }
             )
  end

  test "rejects continue-as-new controls in success options" do
    assert {:error,
            %{
              message:
                "native step continuation must use {:continue_as_new, input, opts}; :continue_as_new is reserved in success options",
              details: :reserved_success_option,
              retryable?: false
            }} =
             Jizoku.Step.normalize_result(
               {:ok, %{cursor: "page-42"}, continue_as_new: %{key: "page-42"}}
             )
  end

  test "rejects malformed native continue-as-new options without raising" do
    malformed_opts = [{:key, "page-42"}, {:definition, :current}, :bad]

    assert {:error,
            %{
              message: "native step continuation options must be a keyword list",
              details: :invalid_options,
              retryable?: false
            }} =
             Jizoku.Step.normalize_result(
               {:continue_as_new, %{cursor: "page-42"}, malformed_opts}
             )
  end

  test "rejects invalid native continue-as-new inputs and options" do
    cases = [
      {{:continue_as_new, "page-42", [key: "page-42", definition: :current]}, :expected_map},
      {{:continue_as_new, %{pid: self()}, [key: "page-42", definition: :current]},
       :storage_unsafe_input},
      {{:continue_as_new, %{}, [definition: :current]}, :missing_key},
      {{:continue_as_new, %{}, [key: "", definition: :current]}, :invalid_key},
      {{:continue_as_new, %{}, [key: "page-42"]}, :missing_definition},
      {{:continue_as_new, %{}, [key: "page-42", definition: :next]}, :invalid_definition},
      {{:continue_as_new, %{}, [key: "page-42", definition: :current, extra: true]},
       {:unknown_options, [:extra]}}
    ]

    for {result, details} <- cases do
      assert {:error, %{details: ^details, retryable?: false}} =
               Jizoku.Step.normalize_result(result)
    end
  end

  test "supports explicit irreversible and non-compensatable step markers" do
    module =
      compile_module("""
      defmodule WorkflowWithRecoveryMarkers do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithRecoveryMarkers.CapturePayment, irreversible: true)
          step(:send_receipt, WorkflowWithRecoveryMarkers.SendReceipt, compensatable: false)

          transition(:capture_payment, on: :ok, to: :send_receipt)
          transition(:send_receipt, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert Jizoku.Workflow.Definition.step_recovery_policy(definition, :capture_payment) ==
             {:ok,
              %{
                irreversible?: true,
                compensatable?: false,
                replay: :manual_review_required,
                recovery: :manual_intervention
              }}

    assert Jizoku.Workflow.Definition.step_recovery_policy(definition, :send_receipt) ==
             {:ok,
              %{
                irreversible?: false,
                compensatable?: false,
                replay: :manual_review_required,
                recovery: :manual_intervention
              }}
  end

  test "supports explicit step compensation callbacks" do
    module =
      compile_module("""
      defmodule WorkflowWithCompensation do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithCompensation.ReserveInventory,
            compensate: WorkflowWithCompensation.ReleaseInventory
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert Jizoku.Workflow.Definition.step_compensation_callback(
             definition,
             :reserve_inventory
           ) ==
             {:ok, Module.safe_concat(module, ReleaseInventory)}
  end

  test "supports repo transaction boundaries on local step groups" do
    module =
      compile_module("""
      defmodule WorkflowWithTransactionalStepGroup do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:write_local_records, WorkflowWithTransactionalStepGroup.WriteLocalRecords,
            transaction: :repo
          )

          transition(:write_local_records, on: :ok, to: :complete)
        end
      end
      """)

    definition = module.workflow_definition()

    assert [
             %{
               name: :write_local_records,
               module: WorkflowWithTransactionalStepGroup.WriteLocalRecords,
               opts: [transaction: :repo]
             }
           ] = definition.steps

    assert Jizoku.Workflow.Definition.step_transaction_boundary(
             definition,
             :write_local_records
           ) == {:ok, :repo}
  end

  test "supports explicit compensation and undo error transition markers" do
    module =
      compile_module("""
      defmodule WorkflowWithFailureRecoveryMarkers do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithFailureRecoveryMarkers.CapturePayment)
          step(:issue_credit, WorkflowWithFailureRecoveryMarkers.IssueCredit)
          step(:reserve_inventory, WorkflowWithFailureRecoveryMarkers.ReserveInventory)
          step(:release_inventory, WorkflowWithFailureRecoveryMarkers.ReleaseInventory)

          transition(:capture_payment, on: :error, to: :issue_credit, recovery: :compensation)
          transition(:issue_credit, on: :ok, to: :reserve_inventory)
          transition(:reserve_inventory, on: :error, to: :release_inventory, recovery: :undo)
          transition(:release_inventory, on: :ok, to: :complete)
        end
      end
      """)

    assert [
             %{
               from: :capture_payment,
               on: :error,
               to: :issue_credit,
               recovery: :compensation
             },
             %{from: :issue_credit, on: :ok, to: :reserve_inventory},
             %{from: :reserve_inventory, on: :error, to: :release_inventory, recovery: :undo},
             %{from: :release_inventory, on: :ok, to: :complete}
           ] = module.workflow_definition().transitions
  end

  test "normalizes persisted irreversible policy as non-compensatable" do
    assert Jizoku.Workflow.Definition.normalize_recovery_policy(%{
             "irreversible?" => true,
             "compensatable?" => true,
             "replay" => "manual_review_required",
             "recovery" => "manual_intervention"
           }) == %{
             irreversible?: true,
             compensatable?: false,
             replay: :manual_review_required,
             recovery: :manual_intervention
           }
  end

  test "normalizes persisted failure recovery decisions" do
    assert Jizoku.Workflow.Definition.normalize_recovery_policy(%{
             "irreversible?" => false,
             "compensatable?" => true,
             "replay" => "allowed",
             "recovery" => "automatic",
             "failure" => %{"strategy" => "undo", "target" => "release_inventory"}
           }) == %{
             irreversible?: false,
             compensatable?: true,
             replay: :allowed,
             recovery: :automatic,
             failure: %{strategy: :undo, target: "release_inventory"}
           }
  end

  test "serializes recovery policy metadata for persisted inspection" do
    callback = InvoiceReminder.LoadInvoice

    assert Jizoku.Workflow.Definition.serialize_recovery_policy(%{
             irreversible?: false,
             compensatable?: true,
             replay: :allowed,
             recovery: :automatic,
             compensation: %{callback: callback, status: :available},
             failure: %{strategy: :undo, target: :complete}
           }) == %{
             "irreversible?" => false,
             "compensatable?" => true,
             "replay" => "allowed",
             "recovery" => "automatic",
             "compensation" => %{
               "callback" => Atom.to_string(callback),
               "status" => "available"
             },
             "failure" => %{
               "strategy" => "undo",
               "target" => "__complete__"
             }
           }
  end

  test "supports introspection of definition segments" do
    assert InvoiceReminder.__workflow__(:steps) == InvoiceReminder.workflow_definition().steps
    assert InvoiceReminder.__workflow__(:payload) == InvoiceReminder.workflow_definition().payload

    assert InvoiceReminder.__workflow__(:triggers) ==
             InvoiceReminder.workflow_definition().triggers

    assert InvoiceReminder.__workflow__(:transitions) ==
             InvoiceReminder.workflow_definition().transitions

    assert InvoiceReminder.__workflow__(:retries) == InvoiceReminder.workflow_definition().retries
    assert InvoiceReminder.__workflow__(:entry_step) == :load_invoice
  end

  test "exposes dependency entry steps for introspection" do
    assert DependencyWorkflow.__workflow__(:entry_steps) == [:load_account, :load_invoice]
    assert DependencyWorkflow.__workflow__(:initial_step) == :load_account
    assert DependencyWorkflow.__workflow__(:entry_step) == nil
  end

  test "waits when the current dependency phase already has a failed sibling" do
    definition = DependencyWorkflow.workflow_definition()

    assert {:wait, [:load_invoice]} =
             Jizoku.Workflow.Definition.dependency_progress(definition, %{
               load_account: :completed,
               load_invoice: :failed
             })
  end

  test "fails when no steps are declared" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutSteps do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :account_id, :string
            end
          end
        end
      end
      """,
      "at least one step is required"
    )
  end

  test "fails when step names are duplicated" do
    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateSteps do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithDuplicateSteps.SendEmail
          step :send_email, WorkflowWithDuplicateSteps.RecordDelivery
        end
      end
      """,
      "duplicate step names: :send_email"
    )
  end

  test "fails when step retry policy is malformed" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetry do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetry.SendEmail, retry: [max_attempts: 0]
        end
      end
      """,
      "retry for :send_email must define a positive :max_attempts"
    )
  end

  test "does not expose retries when no step config defines them" do
    module =
      compile_module("""
      defmodule WorkflowWithoutRetries do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithoutRetries.SendEmail
          transition :send_email, on: :ok, to: :complete
        end
      end
      """)

    assert module.__workflow__(:retries) == []
  end

  test "fails when step retry is not a keyword list" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetryShape do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetryShape.SendEmail, retry: 3
        end
      end
      """,
      "retry for :send_email must define a positive :max_attempts"
    )
  end

  test "fails when retry backoff configuration is invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidRetryBackoff do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidRetryBackoff.SendEmail,
            retry: [max_attempts: 3, backoff: [type: :exponential, min: 0, max: 1_000]]
        end
      end
      """,
      "retry for :send_email defines an invalid :backoff option"
    )
  end

  test "fails when step input mapping is not a non-empty atom list or named path mapping" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepInput do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepInput.SendEmail, input: "account_id"
        end
      end
      """,
      "step :send_email defines an invalid :input mapping"
    )

    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepInputPath do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepInputPath.SendEmail,
            input: [reviewer: []]
        end
      end
      """,
      "step :send_email defines an invalid :input mapping"
    )

    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepInputPathSegment do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepInputPathSegment.SendEmail,
            input: [reviewer: [:review_draft, "reviewer"]]
        end
      end
      """,
      "step :send_email defines an invalid :input mapping"
    )

    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateStepInputPathTargets do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithDuplicateStepInputPathTargets.SendEmail,
            input: [reviewer: [:review_draft, :reviewer], reviewer: [:fallback, :reviewer]]
        end
      end
      """,
      "step :send_email defines an invalid :input mapping"
    )
  end

  test "fails when step output mapping is not an atom" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidStepOutput do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :send_email, WorkflowWithInvalidStepOutput.SendEmail, output: [:delivery]
        end
      end
      """,
      "step :send_email defines an invalid :output mapping"
    )
  end

  test "fails when irreversible and compensatable markers conflict" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingRecoveryMarkers do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithConflictingRecoveryMarkers.CapturePayment,
            irreversible: true,
            compensatable: true
          )

          transition(:capture_payment, on: :ok, to: :complete)
        end
      end
      """,
      "step :capture_payment cannot be both irreversible and compensatable"
    )
  end

  test "fails when a compensation callback is declared for an irreversible step" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingCompensation do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithConflictingCompensation.CapturePayment,
            irreversible: true,
            compensate: WorkflowWithConflictingCompensation.VoidPayment
          )

          transition(:capture_payment, on: :ok, to: :complete)
        end
      end
      """,
      "step :capture_payment cannot declare :compensate when it is irreversible or non-compensatable"
    )
  end

  test "fails when a compensation callback is a built-in step kind" do
    assert_compile_error(
      """
      defmodule WorkflowWithBuiltInCompensation do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithBuiltInCompensation.ReserveInventory,
            compensate: :log
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """,
      "step :reserve_inventory defines an invalid :compensate callback"
    )
  end

  test "fails when a compensation callback is not a module atom" do
    assert_compile_error(
      """
      defmodule WorkflowWithNonModuleCompensation do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:reserve_inventory, WorkflowWithNonModuleCompensation.ReserveInventory,
            compensate: :release_inventory
          )

          transition(:reserve_inventory, on: :ok, to: :complete)
        end
      end
      """,
      "step :reserve_inventory defines an invalid :compensate callback"
    )
  end

  test "does not report recovery marker conflicts for invalid marker shapes" do
    error =
      assert_raise CompileError, fn ->
        Code.compile_string(
          """
          defmodule WorkflowWithInvalidRecoveryMarkerShape do
            use Jizoku.Workflow

            workflow do
              trigger :manual do
                manual()
              end

              step(:capture_payment, WorkflowWithInvalidRecoveryMarkerShape.CapturePayment,
                irreversible: :yes,
                compensatable: true
              )

              transition(:capture_payment, on: :ok, to: :complete)
            end
          end
          """,
          "test/support/invalid_workflow.exs"
        )
      end

    message = Exception.message(error)

    assert String.contains?(
             message,
             "step :capture_payment defines an invalid :irreversible marker"
           )

    refute String.contains?(message, "cannot be both irreversible and compensatable")
  end

  test "fails when transaction boundary markers are invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidTransactionBoundary do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:write_local_records, WorkflowWithInvalidTransactionBoundary.WriteLocalRecords,
            transaction: :database
          )

          transition(:write_local_records, on: :ok, to: :complete)
        end
      end
      """,
      "step :write_local_records defines an invalid :transaction boundary"
    )
  end

  test "fails when built-in steps declare transaction boundaries" do
    assert_compile_error(
      """
      defmodule WorkflowWithBuiltInTransactionBoundary do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:record_message, :log, message: "local work", transaction: :repo)

          transition(:record_message, on: :ok, to: :complete)
        end
      end
      """,
      "built-in step :record_message cannot declare a :transaction boundary"
    )
  end

  test "fails when error transition recovery markers are invalid" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidFailureRecoveryMarker do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithInvalidFailureRecoveryMarker.CapturePayment)
          step(:issue_credit, WorkflowWithInvalidFailureRecoveryMarker.IssueCredit)

          transition(:capture_payment, on: :error, to: :issue_credit, recovery: :rollback)
          transition(:issue_credit, on: :ok, to: :complete)
        end
      end
      """,
      "transition from :capture_payment defines unsupported recovery marker :rollback"
    )
  end

  test "fails when success transitions define recovery markers" do
    assert_compile_error(
      """
      defmodule WorkflowWithSuccessRecoveryMarker do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step(:capture_payment, WorkflowWithSuccessRecoveryMarker.CapturePayment)
          step(:issue_credit, WorkflowWithSuccessRecoveryMarker.IssueCredit)

          transition(:capture_payment, on: :ok, to: :issue_credit, recovery: :undo)
          transition(:issue_credit, on: :ok, to: :complete)
        end
      end
      """,
      "transition from :capture_payment can only define recovery markers for :error outcomes"
    )
  end

  test "fails when a workflow defines multiple entry steps" do
    assert_compile_error(
      """
      defmodule WorkflowWithMultipleEntrySteps do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithMultipleEntrySteps.LoadInvoice
          step :send_email, WorkflowWithMultipleEntrySteps.SendEmail
        end
      end
      """,
      "workflow must define exactly one entry step"
    )
  end

  test "allows multiple entry steps when dependency execution is declared" do
    module =
      compile_module("""
      defmodule WorkflowWithMultipleDependencyRoots do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, WorkflowWithMultipleDependencyRoots.LoadAccount
          step :load_invoice, WorkflowWithMultipleDependencyRoots.LoadInvoice
          step :send_email, WorkflowWithMultipleDependencyRoots.SendEmail,
            after: [:load_account, :load_invoice]
        end
      end
      """)

    assert module.__workflow__(:entry_steps) == [:load_account, :load_invoice]
  end

  test "fails when a workflow defines no entry step" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutEntryStep do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithoutEntryStep.LoadInvoice
          step :send_email, WorkflowWithoutEntryStep.SendEmail

          transition :load_invoice, on: :ok, to: :send_email
          transition :send_email, on: :ok, to: :load_invoice
        end
      end
      """,
      "workflow must define exactly one entry step"
    )
  end

  test "fails when a dependency references an unknown step" do
    assert_compile_error(
      """
      defmodule WorkflowWithUnknownDependency do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithUnknownDependency.LoadInvoice
          step :send_email, WorkflowWithUnknownDependency.SendEmail, after: [:missing_step]
        end
      end
      """,
      "step :send_email depends on unknown step :missing_step"
    )
  end

  test "entry_steps!/2 raises a dependency-specific error when no root steps exist" do
    definition = %{
      steps: [
        %{name: :load_account, opts: [after: [:send_email]]},
        %{name: :send_email, opts: [after: [:load_account]]}
      ],
      transitions: []
    }

    assert_raise CompileError,
                 ~r/dependency-based workflow must define at least one root step/,
                 fn ->
                   Jizoku.Workflow.Validation.entry_steps!(definition, __ENV__)
                 end
  end

  test "fails when dependency declarations contain a cycle" do
    assert_compile_error(
      """
      defmodule WorkflowWithDependencyCycle do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithDependencyCycle.LoadInvoice, after: [:send_email]
          step :send_email, WorkflowWithDependencyCycle.SendEmail, after: [:load_invoice]
        end
      end
      """,
      "workflow dependency graph must be acyclic"
    )
  end

  test "fails when a workflow mixes dependency joins with transitions" do
    assert_compile_error(
      """
      defmodule WorkflowWithMixedProgression do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, WorkflowWithMixedProgression.LoadAccount
          step :load_invoice, WorkflowWithMixedProgression.LoadInvoice
          step :prepare_notification, WorkflowWithMixedProgression.PrepareNotification,
            after: [:load_account, :load_invoice]
          step :record_delivery, WorkflowWithMixedProgression.RecordDelivery

          transition :prepare_notification, on: :ok, to: :record_delivery
          transition :record_delivery, on: :ok, to: :complete
        end
      end
      """,
      "dependency-based workflows cannot declare transitions"
    )
  end

  test "fails when :after is not a list of step atoms" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidAfterShape do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithInvalidAfterShape.LoadInvoice
          step :send_email, WorkflowWithInvalidAfterShape.SendEmail, after: "load_invoice"
        end
      end
      """,
      "step :send_email defines an invalid :after dependency list"
    )
  end

  test "fails when :after is empty" do
    assert_compile_error(
      """
      defmodule WorkflowWithEmptyAfter do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithEmptyAfter.LoadInvoice
          step :send_email, WorkflowWithEmptyAfter.SendEmail, after: []
        end
      end
      """,
      "step :send_email defines an invalid :after dependency list"
    )
  end

  test "fails when no triggers are declared" do
    assert_compile_error(
      """
      defmodule WorkflowWithoutTriggers do
        use Jizoku.Workflow

        workflow do
          step :load_invoice, WorkflowWithoutTriggers.LoadInvoice
        end
      end
      """,
      "at least one trigger is required"
    )
  end

  test "fails when a trigger does not define a type" do
    assert_compile_error(
      """
      defmodule WorkflowWithUntypedTrigger do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            payload do
              field :account_id, :string
            end
          end

          step :load_invoice, WorkflowWithUntypedTrigger.LoadInvoice
        end
      end
      """,
      "trigger :manual must define exactly one type"
    )
  end

  test "fails clearly when a payload field is declared outside a payload block" do
    assert_dsl_error(
      """
      defmodule WorkflowWithTriggerFieldOutsidePayload do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
            field :account_id, :string
          end

          step :load_invoice, WorkflowWithTriggerFieldOutsidePayload.LoadInvoice
        end
      end
      """,
      "field/3 must be declared inside a trigger payload block"
    )
  end

  test "fails clearly when a transition omits a required option" do
    assert_dsl_error(
      """
      defmodule WorkflowWithMissingTransitionTarget do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithMissingTransitionTarget.LoadInvoice
          transition :load_invoice, on: :ok
        end
      end
      """,
      "transition must specify :to option"
    )
  end

  test "supports multiple triggers with independent payload contracts" do
    module =
      compile_module("""
      defmodule WorkflowWithMultipleTriggers do
        use Jizoku.Workflow

        workflow do
          trigger :manual_digest do
            manual()

            payload do
              field :chat_id, :integer
            end
          end

          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC"

            payload do
              field :window_start_at, :string, default: {:today, :iso8601}
            end
          end

          step :load_invoice, WorkflowWithMultipleTriggers.LoadInvoice
          transition :load_invoice, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().triggers == [
             %{
               name: :manual_digest,
               type: :manual,
               config: %{},
               payload: [%{name: :chat_id, type: :integer, opts: []}]
             },
             %{
               name: :scheduled_digest,
               type: :cron,
               config: %{expression: "0 9 * * *", timezone: "UTC"},
               payload: [
                 %{name: :window_start_at, type: :string, opts: [default: {:today, :iso8601}]}
               ]
             }
           ]

    assert module.workflow_definition().payload == [
             %{name: :chat_id, type: :integer, opts: []},
             %{name: :window_start_at, type: :string, opts: [default: {:today, :iso8601}]}
           ]
  end

  test "fails when triggers declare incompatible payload field types" do
    assert_compile_error(
      """
      defmodule WorkflowWithConflictingTriggerPayloads do
        use Jizoku.Workflow

        workflow do
          trigger :manual_digest do
            manual()

            payload do
              field :chat_id, :integer
            end
          end

          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC"

            payload do
              field :chat_id, :string
            end
          end

          step :load_invoice, WorkflowWithConflictingTriggerPayloads.LoadInvoice
          transition :load_invoice, on: :ok, to: :complete
        end
      end
      """,
      "payload field :chat_id defines conflicting types across triggers"
    )
  end

  test "fails when a trigger declares more than one type" do
    assert_compile_error(
      """
      defmodule WorkflowWithMultipleTriggerTypes do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
            cron "0 9 * * *", timezone: "UTC"
          end

          step :load_invoice, WorkflowWithMultipleTriggerTypes.LoadInvoice
        end
      end
      """,
      "trigger :manual must define exactly one type"
    )
  end

  test "exposes cron trigger metadata and payload defaults" do
    module =
      compile_module("""
      defmodule WorkflowWithCronTrigger do
        use Jizoku.Workflow

        workflow do
          trigger :daily_standup do
            cron "0 9 * * 1-5", timezone: "America/Sao_Paulo"

            payload do
              field :team_id, :string, default: "backend"
              field :prompt_date, :string, default: {:today, :iso8601}
            end
          end

          step :load_team_members, WorkflowWithCronTrigger.LoadTeamMembers
          transition :load_team_members, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().triggers == [
             %{
               name: :daily_standup,
               type: :cron,
               config: %{
                 expression: "0 9 * * 1-5",
                 timezone: "America/Sao_Paulo"
               },
               payload: [
                 %{name: :team_id, type: :string, opts: [default: "backend"]},
                 %{name: :prompt_date, type: :string, opts: [default: {:today, :iso8601}]}
               ]
             }
           ]
  end

  test "exposes cron trigger idempotency strategy" do
    module =
      compile_module("""
      defmodule WorkflowWithIdempotentCronTrigger do
        use Jizoku.Workflow

        workflow do
          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC", idempotency: :return_existing_run
          end

          step :load_invoice, WorkflowWithIdempotentCronTrigger.LoadInvoice
        end
      end
      """)

    assert [
             %{
               name: :scheduled_digest,
               type: :cron,
               config: %{
                 expression: "0 9 * * *",
                 timezone: "UTC",
                 idempotency: :return_existing_run
               }
             }
           ] = module.workflow_definition().triggers
  end

  test "fails when a cron trigger declares an invalid idempotency strategy" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidCronIdempotency do
        use Jizoku.Workflow

        workflow do
          trigger :scheduled_digest do
            cron "0 9 * * *", timezone: "UTC", idempotency: :retry
          end

          step :load_invoice, WorkflowWithInvalidCronIdempotency.LoadInvoice
        end
      end
      """,
      "trigger :scheduled_digest defines invalid cron idempotency strategy :retry"
    )
  end

  test "supports declarative built-in steps" do
    module =
      compile_module("""
      defmodule WorkflowWithBuiltInSteps do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :wait_for_settlement, :wait, duration: 250
          step :log_delivery, :log, message: "delivery completed", level: :info

          transition :wait_for_settlement, on: :ok, to: :log_delivery
          transition :log_delivery, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{name: :wait_for_settlement, module: :wait, opts: [duration: 250]},
             %{
               name: :log_delivery,
               module: :log,
               opts: [message: "delivery completed", level: :info]
             }
           ]
  end

  test "fails when a built-in wait step is missing duration" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidWaitStep do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :wait_for_settlement, :wait
        end
      end
      """,
      "built-in step :wait_for_settlement requires a positive :duration option"
    )
  end

  test "fails when a built-in log step is missing message" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidLogStep do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :log_delivery, :log, level: :warning
        end
      end
      """,
      "built-in step :log_delivery requires a non-empty :message option"
    )
  end

  test "fails when a payload default does not match the declared field type" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidPayloadDefault do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :max_attempts, :integer, default: "five"
            end
          end

          step :load_invoice, WorkflowWithInvalidPayloadDefault.LoadInvoice
        end
      end
      """,
      "payload field :max_attempts defines an invalid default for type :integer"
    )
  end

  test "fails when a dynamic payload default does not match the declared field type" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidDynamicPayloadDefault do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()

            payload do
              field :prompt_date, :integer, default: {:today, :iso8601}
            end
          end

          step :load_invoice, WorkflowWithInvalidDynamicPayloadDefault.LoadInvoice
        end
      end
      """,
      "payload field :prompt_date defines an invalid default for type :integer"
    )
  end

  test "fails when a transition declares an unsupported outcome" do
    assert_compile_error(
      """
      defmodule WorkflowWithUnsupportedTransitionOutcome do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_invoice, WorkflowWithUnsupportedTransitionOutcome.LoadInvoice
          transition :load_invoice, on: :unexpected, to: :complete
        end
      end
      """,
      "transition from :load_invoice defines unsupported outcome :unexpected"
    )
  end

  test "supports transitions declared on :error outcomes" do
    module =
      compile_module("""
      defmodule WorkflowWithErrorTransition do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :check_gateway, WorkflowWithErrorTransition.CheckGateway
          step :notify_operator, WorkflowWithErrorTransition.NotifyOperator

          transition :check_gateway, on: :error, to: :notify_operator
          transition :notify_operator, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().transitions == [
             %{from: :check_gateway, on: :error, to: :notify_operator},
             %{from: :notify_operator, on: :ok, to: :complete}
           ]
  end

  test "supports conditional transitions with an unconditional fallback" do
    module =
      compile_module("""
      defmodule WorkflowWithConditionalTransitions do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :classify, WorkflowWithConditionalTransitions.Classify
          step :auto_approve, WorkflowWithConditionalTransitions.AutoApprove
          step :manual_review, WorkflowWithConditionalTransitions.ManualReview

          transition :classify,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:routing, :decision], equals: "auto"]

          transition :classify, on: :ok, to: :manual_review
          transition :auto_approve, on: :ok, to: :complete
          transition :manual_review, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().transitions == [
             %{
               from: :classify,
               on: :ok,
               to: :auto_approve,
               condition: %{path: [:routing, :decision], equals: "auto"}
             },
             %{from: :classify, on: :ok, to: :manual_review},
             %{from: :auto_approve, on: :ok, to: :complete},
             %{from: :manual_review, on: :ok, to: :complete}
           ]

    assert {:ok, spec} = Jizoku.Workflow.to_spec(module)

    assert [
             %{
               from: :classify,
               on: :ok,
               to: :auto_approve,
               condition: %{path: [:routing, :decision], equals: "auto"}
             },
             %{from: :classify, on: :ok, to: :manual_review}
             | _remaining
           ] = spec.transitions

    assert {:ok, %{to: :auto_approve}} =
             Jizoku.Workflow.Definition.transition(
               module.workflow_definition(),
               :classify,
               :ok,
               %{routing: %{decision: "auto"}}
             )

    assert {:ok, %{to: :manual_review}} =
             Jizoku.Workflow.Definition.transition(
               module.workflow_definition(),
               :classify,
               :ok,
               %{routing: %{decision: "manual"}}
             )
  end

  test "supports greater-than conditional transitions with an unconditional fallback" do
    module =
      compile_module("""
      defmodule WorkflowWithNumericConditionalTransitions do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :score_invoice, WorkflowWithNumericConditionalTransitions.ScoreInvoice
          step :escalate_review, WorkflowWithNumericConditionalTransitions.EscalateReview
          step :auto_approve, WorkflowWithNumericConditionalTransitions.AutoApprove

          transition :score_invoice,
            on: :ok,
            to: :escalate_review,
            condition: [path: [:risk, :score], greater_than: 70]

          transition :score_invoice, on: :ok, to: :auto_approve
          transition :escalate_review, on: :ok, to: :complete
          transition :auto_approve, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().transitions == [
             %{
               from: :score_invoice,
               on: :ok,
               to: :escalate_review,
               condition: %{path: [:risk, :score], greater_than: 70}
             },
             %{from: :score_invoice, on: :ok, to: :auto_approve},
             %{from: :escalate_review, on: :ok, to: :complete},
             %{from: :auto_approve, on: :ok, to: :complete}
           ]

    assert {:ok, %{to: :escalate_review}} =
             Jizoku.Workflow.Definition.transition(
               module.workflow_definition(),
               :score_invoice,
               :ok,
               %{risk: %{score: 71}}
             )

    for context <- [%{risk: %{score: 70}}, %{risk: %{score: "high"}}, %{risk: %{}}] do
      assert {:ok, %{to: :auto_approve}} =
               Jizoku.Workflow.Definition.transition(
                 module.workflow_definition(),
                 :score_invoice,
                 :ok,
                 context
               )
    end
  end

  test "supports less-than conditional transitions with an unconditional fallback" do
    module =
      compile_module("""
      defmodule WorkflowWithLessThanConditionalTransitions do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :score_invoice, WorkflowWithLessThanConditionalTransitions.ScoreInvoice
          step :manual_review, WorkflowWithLessThanConditionalTransitions.ManualReview
          step :auto_approve, WorkflowWithLessThanConditionalTransitions.AutoApprove

          transition :score_invoice,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:risk, :score], less_than: 30]

          transition :score_invoice, on: :ok, to: :manual_review
          transition :auto_approve, on: :ok, to: :complete
          transition :manual_review, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().transitions == [
             %{
               from: :score_invoice,
               on: :ok,
               to: :auto_approve,
               condition: %{path: [:risk, :score], less_than: 30}
             },
             %{from: :score_invoice, on: :ok, to: :manual_review},
             %{from: :auto_approve, on: :ok, to: :complete},
             %{from: :manual_review, on: :ok, to: :complete}
           ]

    assert {:ok, %{to: :auto_approve}} =
             Jizoku.Workflow.Definition.transition(
               module.workflow_definition(),
               :score_invoice,
               :ok,
               %{risk: %{score: 29}}
             )

    for context <- [%{risk: %{score: 30}}, %{risk: %{score: "low"}}, %{risk: %{}}] do
      assert {:ok, %{to: :manual_review}} =
               Jizoku.Workflow.Definition.transition(
                 module.workflow_definition(),
                 :score_invoice,
                 :ok,
                 context
               )
    end
  end

  test "accepts JSON round-tripped condition paths in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: %{"path" => ["load_invoice"], "equals" => "daily"}
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert :ok = Jizoku.Workflow.validate_spec(spec)
  end

  test "accepts JSON round-tripped greater-than conditions in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: %{"path" => ["load_invoice"], "greater_than" => 10}
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert :ok = Jizoku.Workflow.validate_spec(spec)
  end

  test "accepts JSON round-tripped less-than conditions in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: %{"path" => ["load_invoice"], "less_than" => 10}
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert :ok = Jizoku.Workflow.validate_spec(spec)
  end

  test "rejects conditions with non-JSON values" do
    assert_raise ArgumentError, "invalid transition condition", fn ->
      compile_module("""
      defmodule WorkflowWithAtomConditionalValue do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :classify, WorkflowWithAtomConditionalValue.Classify
          step :auto_approve, WorkflowWithAtomConditionalValue.AutoApprove

          transition :classify,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:routing, :decision], equals: :auto]
        end
      end
      """)
    end
  end

  test "rejects greater-than conditions with non-numeric values" do
    assert_raise ArgumentError, "invalid transition condition", fn ->
      compile_module("""
      defmodule WorkflowWithNonNumericGreaterThanConditionalValue do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :score_invoice, WorkflowWithNonNumericGreaterThanConditionalValue.ScoreInvoice
          step :escalate_review, WorkflowWithNonNumericGreaterThanConditionalValue.EscalateReview

          transition :score_invoice,
            on: :ok,
            to: :escalate_review,
            condition: [path: [:risk, :score], greater_than: "70"]
        end
      end
      """)
    end
  end

  test "rejects less-than conditions with non-numeric values" do
    assert_raise ArgumentError, "invalid transition condition", fn ->
      compile_module("""
      defmodule WorkflowWithNonNumericLessThanConditionalValue do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :score_invoice, WorkflowWithNonNumericLessThanConditionalValue.ScoreInvoice
          step :auto_approve, WorkflowWithNonNumericLessThanConditionalValue.AutoApprove

          transition :score_invoice,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:risk, :score], less_than: "30"]
        end
      end
      """)
    end
  end

  test "rejects duplicate conditional operators in DSL keyword lists" do
    assert_raise ArgumentError, "invalid transition condition", fn ->
      compile_module("""
      defmodule WorkflowWithDuplicateConditionalOperators do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :score_invoice, WorkflowWithDuplicateConditionalOperators.ScoreInvoice
          step :escalate_review, WorkflowWithDuplicateConditionalOperators.EscalateReview

          transition :score_invoice,
            on: :ok,
            to: :escalate_review,
            condition: [path: [:risk, :score], greater_than: 70, greater_than: 90]
        end
      end
      """)
    end
  end

  test "rejects duplicate condition keys across atom and string list forms" do
    duplicate_conditions = [
      [{:path, ["load_invoice"]}, {"path", ["send_email"]}, {:equals, "daily"}],
      [{:path, ["load_invoice"]}, {:equals, "daily"}, {"equals", "weekly"}],
      [{:path, ["load_invoice"]}, {:greater_than, 10}, {"greater_than", 20}],
      [{:path, ["load_invoice"]}, {:less_than, 10}, {"less_than", 20}],
      [{:path, ["load_invoice"]}, {:equals, "daily"}, {:unknown, true}]
    ]

    for condition <- duplicate_conditions do
      assert {:error, :invalid_condition} =
               Jizoku.Workflow.TransitionCondition.normalize(condition)
    end
  end

  test "returns structured errors for malformed list conditions in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: ["path"]
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} = Jizoku.Workflow.validate_spec(spec)

    assert Enum.any?(errors, fn error ->
             match?(
               %{
                 path: [:transitions, 0, :condition],
                 code: :invalid_transition_condition,
                 message: "transition from :load_invoice defines an invalid condition"
               },
               error
             )
           end)
  end

  test "returns structured errors for duplicate condition keys in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: [
              {:path, ["load_invoice"]},
              {"path", ["send_email"]},
              {:equals, "daily"}
            ]
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} = Jizoku.Workflow.validate_spec(spec)

    assert Enum.any?(errors, fn error ->
             match?(
               %{
                 path: [:transitions, 0, :condition],
                 code: :invalid_transition_condition,
                 message: "transition from :load_invoice defines an invalid condition"
               },
               error
             )
           end)
  end

  test "returns structured errors for conditions with multiple operators in workflow specs" do
    assert {:ok, spec} = Jizoku.Workflow.to_spec(InvoiceReminder)

    spec = %{
      spec
      | transitions: [
          %{
            from: :load_invoice,
            on: :ok,
            to: :send_email,
            condition: %{"path" => ["load_invoice"], "greater_than" => 10, "less_than" => 20}
          },
          %{from: :send_email, on: :ok, to: :record_delivery},
          %{from: :record_delivery, on: :ok, to: :complete}
        ]
    }

    assert {:error, {:invalid_workflow_spec, errors}} = Jizoku.Workflow.validate_spec(spec)

    assert Enum.any?(errors, fn error ->
             match?(
               %{
                 path: [:transitions, 0, :condition],
                 code: :invalid_transition_condition,
                 message: "transition from :load_invoice defines an invalid condition"
               },
               error
             )
           end)
  end

  test "evaluates conditional transitions before the fallback even when fallback is declared first" do
    module =
      compile_module("""
      defmodule WorkflowWithEarlyConditionalFallback do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :classify, WorkflowWithEarlyConditionalFallback.Classify
          step :auto_approve, WorkflowWithEarlyConditionalFallback.AutoApprove
          step :manual_review, WorkflowWithEarlyConditionalFallback.ManualReview

          transition :classify, on: :ok, to: :manual_review

          transition :classify,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:routing, :decision], equals: "auto"]

          transition :auto_approve, on: :ok, to: :complete
          transition :manual_review, on: :ok, to: :complete
        end
      end
      """)

    assert {:ok, %{to: :auto_approve}} =
             Jizoku.Workflow.Definition.transition(
               module.workflow_definition(),
               :classify,
               :ok,
               %{routing: %{decision: "auto"}}
             )
  end

  test "rejects duplicate conditional transitions for the same outcome" do
    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateConditionalTransitions do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :classify, WorkflowWithDuplicateConditionalTransitions.Classify
          step :auto_approve, WorkflowWithDuplicateConditionalTransitions.AutoApprove
          step :fast_track, WorkflowWithDuplicateConditionalTransitions.FastTrack

          transition :classify,
            on: :ok,
            to: :auto_approve,
            condition: [path: [:routing, :decision], equals: "auto"]

          transition :classify,
            on: :ok,
            to: :fast_track,
            condition: [path: [:routing, :decision], equals: "auto"]
        end
      end
      """,
      "duplicate transition declared from :classify on outcome :ok"
    )
  end

  test "rejects conditional transitions from manual built-in steps" do
    assert_compile_error(
      """
      defmodule WorkflowWithConditionalApprovalTransition do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          approval_step :wait_for_review
          step :record_approval, WorkflowWithConditionalApprovalTransition.RecordApproval
          step :record_rejection, WorkflowWithConditionalApprovalTransition.RecordRejection

          transition :wait_for_review,
            on: :ok,
            to: :record_approval,
            condition: [path: [:approval, :decision], equals: "approved"]

          transition :wait_for_review, on: :error, to: :record_rejection
          transition :record_approval, on: :ok, to: :complete
          transition :record_rejection, on: :ok, to: :complete
        end
      end
      """,
      "transition from built-in manual step :wait_for_review cannot define a condition"
    )
  end

  test "supports first-class approval step declarations" do
    module =
      compile_module("""
      defmodule WorkflowWithApprovalStep do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          approval_step :wait_for_review, output: :approval
          step :record_approval, WorkflowWithApprovalStep.RecordApproval
          step :record_rejection, WorkflowWithApprovalStep.RecordRejection

          transition :wait_for_review, on: :ok, to: :record_approval
          transition :wait_for_review, on: :error, to: :record_rejection
          transition :record_approval, on: :ok, to: :complete
          transition :record_rejection, on: :ok, to: :complete
        end
      end
      """)

    assert module.workflow_definition().steps == [
             %{name: :wait_for_review, module: :approval, opts: [output: :approval]},
             %{
               name: :record_approval,
               module: Module.safe_concat(module, RecordApproval),
               opts: []
             },
             %{
               name: :record_rejection,
               module: Module.safe_concat(module, RecordRejection),
               opts: []
             }
           ]
  end

  test "supports step deadline declarations" do
    module =
      compile_module("""
      defmodule WorkflowWithStepDeadlines do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :charge_card, WorkflowWithStepDeadlines.ChargeCard,
            deadline: [within: 5_000, due_soon: 1_000, escalation: :diagnostic]

          approval_step :wait_for_review,
            output: :approval,
            deadline: [within: 60_000, due_soon: 10_000, escalation: :operator_action]

          step :record_approval, WorkflowWithStepDeadlines.RecordApproval
          step :record_rejection, WorkflowWithStepDeadlines.RecordRejection

          transition :charge_card, on: :ok, to: :wait_for_review
          transition :wait_for_review, on: :ok, to: :record_approval
          transition :wait_for_review, on: :error, to: :record_rejection
          transition :record_approval, on: :ok, to: :complete
          transition :record_rejection, on: :ok, to: :complete
        end
      end
      """)

    assert [
             %{name: :charge_card, opts: [deadline: charge_deadline]},
             %{name: :wait_for_review, opts: [output: :approval, deadline: review_deadline]},
             _record_approval,
             _record_rejection
           ] = module.workflow_definition().steps

    assert charge_deadline == [within: 5_000, due_soon: 1_000, escalation: :diagnostic]
    assert review_deadline == [within: 60_000, due_soon: 10_000, escalation: :operator_action]
  end

  test "rejects invalid step deadline declarations" do
    assert_compile_error(
      """
      defmodule WorkflowWithInvalidDeadline do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :charge_card, WorkflowWithInvalidDeadline.ChargeCard,
            deadline: [within: 0]

          transition :charge_card, on: :ok, to: :complete
        end
      end
      """,
      "step :charge_card defines an invalid :deadline policy"
    )
  end

  test "rejects built-in :pause steps in dependency-based workflows" do
    assert_compile_error(
      """
      defmodule DependencyWorkflowWithPause do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, DependencyWorkflowWithPause.LoadAccount
          step :wait_for_approval, :pause, after: [:load_account]
        end
      end
      """,
      "dependency-based workflows cannot declare built-in :pause steps"
    )
  end

  test "rejects approval steps in dependency-based workflows" do
    assert_compile_error(
      """
      defmodule DependencyWorkflowWithApproval do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :load_account, DependencyWorkflowWithApproval.LoadAccount
          approval_step :wait_for_review, after: [:load_account]
        end
      end
      """,
      "dependency-based workflows cannot declare built-in :approval steps"
    )
  end

  test "requires approval steps to declare both :ok and :error transitions" do
    assert_compile_error(
      """
      defmodule WorkflowWithIncompleteApprovalRouting do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          approval_step :wait_for_review
          step :record_approval, WorkflowWithIncompleteApprovalRouting.RecordApproval

          transition :wait_for_review, on: :ok, to: :record_approval
          transition :record_approval, on: :ok, to: :complete
        end
      end
      """,
      "approval step :wait_for_review must define both :ok and :error transitions"
    )
  end

  test "fails when duplicate transitions are declared for the same outcome" do
    assert_compile_error(
      """
      defmodule WorkflowWithDuplicateTransitions do
        use Jizoku.Workflow

        workflow do
          trigger :manual do
            manual()
          end

          step :check_gateway, WorkflowWithDuplicateTransitions.CheckGateway
          step :notify_operator, WorkflowWithDuplicateTransitions.NotifyOperator
          step :record_failure, WorkflowWithDuplicateTransitions.RecordFailure

          transition :check_gateway, on: :error, to: :notify_operator
          transition :check_gateway, on: :error, to: :record_failure
        end
      end
      """,
      "duplicate transition declared from :check_gateway on outcome :error"
    )
  end

  defp assert_compile_error(source, message) do
    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/support/invalid_workflow.exs")
      end

    assert String.contains?(Exception.message(error), message)
  end

  defp assert_dsl_error(source, message) do
    error =
      assert_raise Spark.Error.DslError, fn ->
        Code.compile_string(source, "test/support/invalid_workflow.exs")
      end

    assert String.contains?(Exception.message(error), message)
  end

  defp compile_module(source) do
    [{module, _bytecode}] = Code.compile_string(source, "test/support/valid_workflow.exs")
    module
  end

  defp compiled_module!(compiled_modules, module) do
    compiled_module =
      Enum.find_value(compiled_modules, fn
        {^module, _bytecode} -> module
        _other -> nil
      end)

    case compiled_module do
      nil -> flunk("expected #{inspect(module)} to compile")
      module -> module
    end
  end

  defmodule InvoiceReminder do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_invoice, InvoiceReminder.LoadInvoice
      step :send_email, InvoiceReminder.SendEmail, retry: [max_attempts: 3]
      step :record_delivery, InvoiceReminder.RecordDelivery

      transition :load_invoice, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :record_delivery
      transition :record_delivery, on: :ok, to: :complete
    end
  end

  defmodule DependencyWorkflow do
    use Jizoku.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, DependencyWorkflow.LoadAccount
      step :load_invoice, DependencyWorkflow.LoadInvoice
      step :send_email, DependencyWorkflow.SendEmail, after: [:load_account, :load_invoice]
    end
  end
end
