defmodule Squidie.Workflow.EditorSpecTest do
  use ExUnit.Case, async: true

  alias Squidie.Workflow.EditorSpec

  defmodule LoadInvoice do
    use Squidie.Step,
      name: :load_invoice,
      input_schema: [
        invoice_id: [type: :string, required: true]
      ],
      output_schema: [
        invoice: [type: :map, required: true]
      ]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{invoice: %{id: "inv_123"}}}
  end

  defmodule SendReminder do
    use Squidie.Step,
      name: :send_reminder,
      input_schema: [
        invoice: [type: :map, required: true]
      ],
      output_schema: [
        sent?: [type: :boolean, required: true]
      ]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{sent?: true}}
  end

  defmodule PaymentRecovery do
    use Squidie.Workflow

    workflow do
      version "2026-05-26.payment-recovery"

      trigger :manual do
        manual()

        payload do
          field :invoice_id, :string
          field :digest_date, :string, default: {:today, :iso8601}
        end
      end

      step :load_invoice, LoadInvoice, input: [:invoice_id], output: :invoice
      step :send_reminder, SendReminder, input: [:invoice], output: :reminder

      transition :load_invoice, on: :ok, to: :send_reminder
      transition :send_reminder, on: :ok, to: :complete
      transition :send_reminder, on: :error, to: :complete, recovery: :compensation
    end
  end

  describe "JSON-safe editor specs" do
    test "round-trips a representative workflow spec through JSON and previews its graph" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      round_tripped =
        spec
        |> EditorSpec.to_map()
        |> Jason.encode!()
        |> Jason.decode!()

      assert :ok = EditorSpec.validate_map(round_tripped)

      assert [
               %{"metadata" => %{"description" => nil}},
               %{"metadata" => %{"output_schema" => %{"sent?" => %{"required" => true}}}}
             ] = round_tripped["steps"]

      assert [
               %{"name" => "invoice_id"},
               %{"name" => "digest_date", "opts" => %{"default" => ["today", "iso8601"]}}
             ] = round_tripped["payload"]

      assert {:ok, graph} = EditorSpec.preview_graph(round_tripped)
      assert {:ok, direct_graph} = EditorSpec.preview_graph(spec)

      assert %{
               "source" => "workflow_spec",
               "status" => "draft",
               "workflow" => workflow,
               "definition_version" => "2026-05-26.payment-recovery",
               "nodes" => [
                 %{"id" => "load_invoice", "status" => "draft"},
                 %{"id" => "send_reminder", "status" => "draft"}
               ],
               "edges" => [
                 %{
                   "id" => "load_invoice:ok:send_reminder",
                   "from" => "load_invoice",
                   "to" => "send_reminder",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "ok"
                 },
                 %{
                   "id" => "send_reminder:ok:complete",
                   "from" => "send_reminder",
                   "to" => "complete",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "ok"
                 },
                 %{
                   "id" => "send_reminder:error:complete",
                   "from" => "send_reminder",
                   "to" => "complete",
                   "type" => "transition",
                   "status" => "pending",
                   "outcome" => "error",
                   "recovery" => "compensation"
                 }
               ]
             } = graph

      assert workflow =~ "PaymentRecovery"
      assert direct_graph["edges"] == graph["edges"]
    end

    test "round-trips editor metadata through JSON, preview, and diff without changing runtime graph" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_metadata = %{
        nodes: %{
          load_invoice: %{
            position: %{x: 120, y: 80},
            group: "billing",
            note: "confirm the invoice payload before reminding"
          },
          send_reminder: %{position: %{x: 360, y: 80}, group: "billing"}
        },
        groups: [%{id: "billing", label: "Billing"}],
        notes: [%{id: "n1", text: "kept for visual editor users only"}]
      }

      editor_map =
        spec
        |> Map.from_struct()
        |> Map.put(:editor, editor_metadata)
        |> EditorSpec.to_map()

      round_tripped =
        editor_map
        |> Jason.encode!()
        |> Jason.decode!()

      source_map = Map.delete(round_tripped, "editor")

      assert :ok = EditorSpec.validate_map(round_tripped)
      assert {:ok, source_graph} = EditorSpec.preview_graph(source_map)
      assert {:ok, graph} = EditorSpec.preview_graph(round_tripped)

      assert source_graph["editor"] == %{}
      assert graph["editor"] == round_tripped["editor"]
      assert Map.delete(graph, "editor") == Map.delete(source_graph, "editor")

      assert {:ok, diff} = EditorSpec.diff(source_map, round_tripped)

      assert diff["editor"] == %{
               "changed?" => true,
               "before" => %{},
               "after" => round_tripped["editor"]
             }

      assert diff["summary"]["editor_changed"] == true
      assert diff["summary"]["nodes_changed"] == 0
      assert diff["summary"]["edges_changed"] == 0
    end

    test "rejects runtime-owned fields inside editor metadata" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("editor", %{"run_id" => "run_123"})

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert_error(errors, [:editor, :run_id], :runtime_owned_field)
    end

    test "rejects non-map editor metadata" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("editor", ["not", "metadata"])

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert %{
               path: [:editor],
               code: :invalid_editor_metadata,
               message: "editor metadata must be a map",
               details: %{type: "non_object"}
             } in errors
    end

    test "rejects non JSON-safe values inside editor metadata" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("editor", %{"notes" => [%{"text" => fn -> :ok end}]})

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert_error(errors, [:editor, :notes, 0, :text], :unsupported_json_value)
    end

    test "previews dependency graphs from JSON-safe maps without explicit transitions" do
      editor_map =
        EditorSpec.to_map(%{
          123 => :ignored,
          workflow: :demo_workflow,
          definition_version: "draft",
          triggers: [],
          payload: [],
          retries: [],
          entry_steps: [:extract],
          initial_step: :extract,
          entry_step: :extract,
          steps: [
            %{name: :extract, action: LoadInvoice, opts: []},
            %{
              name: :transform,
              metadata: %{action: :transform_invoice},
              opts: [after: [:extract]]
            },
            %{name: :load, opts: [after: [:extract, :transform]]},
            %{name: :archive, opts: [after: :load]}
          ],
          transitions: []
        })

      assert :ok = EditorSpec.validate_map(editor_map)

      assert {:ok, graph} = EditorSpec.preview_graph(editor_map)

      assert %{
               "workflow" => "demo_workflow",
               "definition_version" => "draft",
               "nodes" => [
                 %{"id" => "extract", "action" => action},
                 %{"id" => "transform", "action" => "transform_invoice"},
                 %{"id" => "load"},
                 %{"id" => "archive"}
               ],
               "edges" => [
                 %{
                   "id" => "extract:dependency:transform",
                   "from" => "extract",
                   "to" => "transform",
                   "type" => "dependency",
                   "status" => "pending",
                   "selected?" => false,
                   "skipped?" => false,
                   "pending?" => true,
                   "blocked?" => false,
                   "outcome" => nil,
                   "condition" => nil,
                   "recovery" => nil
                 },
                 %{
                   "id" => "extract:dependency:load",
                   "from" => "extract",
                   "to" => "load",
                   "type" => "dependency",
                   "status" => "pending"
                 },
                 %{
                   "id" => "transform:dependency:load",
                   "from" => "transform",
                   "to" => "load",
                   "type" => "dependency",
                   "status" => "pending"
                 }
               ]
             } = graph

      assert action =~ "LoadInvoice"
      refute Map.has_key?(editor_map, "123")
    end

    test "validates editor action keys against a host registry before previewing" do
      editor_map =
        EditorSpec.to_map(%{
          workflow: :runtime_invoice_reminder,
          definition_version: "draft",
          triggers: [],
          payload: [],
          retries: [],
          entry_steps: [:load_invoice],
          initial_step: :load_invoice,
          entry_step: :load_invoice,
          steps: [
            %{name: :load_invoice, action: "billing.load_invoice", opts: []},
            %{
              name: :send_reminder,
              action: "billing.send_reminder",
              opts: [after: [:load_invoice]]
            }
          ],
          transitions: []
        })

      registry = %{
        "billing.load_invoice" => LoadInvoice,
        "billing.send_reminder" => SendReminder
      }

      assert :ok = EditorSpec.validate_map(editor_map, action_registry: registry)
      assert {:ok, graph} = EditorSpec.preview_graph(editor_map, action_registry: registry)

      assert [
               %{"id" => "load_invoice", "action" => "billing.load_invoice"},
               %{"id" => "send_reminder", "action" => "billing.send_reminder"}
             ] = graph["nodes"]
    end

    test "validates JSON stringified atom action keys against trusted atom registry keys" do
      editor_map =
        EditorSpec.to_map(%{
          workflow: :runtime_invoice_reminder,
          definition_version: "draft",
          triggers: [],
          payload: [],
          retries: [],
          entry_steps: [:load_invoice],
          initial_step: :load_invoice,
          entry_step: :load_invoice,
          steps: [
            %{name: :load_invoice, action: :billing_load_invoice, opts: []}
          ],
          transitions: []
        })

      assert :ok =
               EditorSpec.validate_map(editor_map,
                 action_registry: [billing_load_invoice: LoadInvoice]
               )

      assert {:ok, graph} =
               EditorSpec.preview_graph(editor_map,
                 action_registry: [billing_load_invoice: LoadInvoice]
               )

      assert [%{"id" => "load_invoice", "action" => "billing_load_invoice"}] =
               graph["nodes"]
    end

    test "previews spec structs with action registry validation" do
      spec = %Squidie.Workflow.Spec{
        workflow: __MODULE__.RuntimeInvoiceReminder,
        definition_version: "draft",
        triggers: [],
        payload: [],
        retries: [],
        entry_steps: [:load_invoice],
        initial_step: :load_invoice,
        entry_step: :load_invoice,
        steps: [
          %{name: :load_invoice, action: "billing.load_invoice", opts: []}
        ],
        transitions: []
      }

      assert {:ok, graph} =
               EditorSpec.preview_graph(spec,
                 action_registry: %{"billing.load_invoice" => LoadInvoice}
               )

      assert [%{"id" => "load_invoice", "action" => "billing.load_invoice"}] =
               graph["nodes"]
    end

    test "keeps metadata-only actions as preview display data with registry options" do
      editor_map =
        EditorSpec.to_map(%{
          workflow: :demo_workflow,
          definition_version: "draft",
          triggers: [],
          payload: [],
          retries: [],
          entry_steps: [:transform],
          initial_step: :transform,
          entry_step: :transform,
          steps: [
            %{name: :transform, metadata: %{action: :transform_invoice}, opts: []}
          ],
          transitions: []
        })

      assert :ok = EditorSpec.validate_map(editor_map, action_registry: %{})
      assert {:ok, graph} = EditorSpec.preview_graph(editor_map, action_registry: %{})

      assert [%{"id" => "transform", "action" => "transform_invoice"}] =
               graph["nodes"]
    end

    test "reports registry validation errors at editor action paths" do
      editor_map =
        EditorSpec.to_map(%{
          workflow: :runtime_invoice_reminder,
          definition_version: "draft",
          triggers: [],
          payload: [],
          retries: [],
          entry_steps: [:load_invoice],
          initial_step: :load_invoice,
          entry_step: :load_invoice,
          steps: [
            %{name: :load_invoice, action: "billing.load_invoice", opts: []},
            %{name: :send_reminder, action: "billing.send_reminder", opts: []},
            %{name: :archive, action: "billing.archive", opts: []},
            %{name: :load_receipt, action: "", opts: []}
          ],
          transitions: []
        })

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map,
                 action_registry: %{
                   "billing.load_invoice" => [module: LoadInvoice, enabled?: false],
                   "billing.send_reminder" => String
                 }
               )

      assert_error(errors, [:steps, 0, :action], :disabled_action_key)
      assert_error(errors, [:steps, 1, :action], :incompatible_action_module)
      assert_error(errors, [:steps, 2, :action], :unknown_action_key)
      assert_error(errors, [:steps, 3, :action], :invalid_action_key)

      assert {:error, {:invalid_workflow_editor_spec, ^errors}} =
               EditorSpec.preview_graph(editor_map,
                 action_registry: %{
                   "billing.load_invoice" => [module: LoadInvoice, enabled?: false],
                   "billing.send_reminder" => String
                 }
               )
    end

    test "keeps conditional transition edge ids unique with stable preview edge keys" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("transitions", [
          %{
            "from" => "send_reminder",
            "on" => "ok",
            "to" => "complete",
            "condition" => %{"field" => "invoice.status", "equals" => "paid"}
          },
          %{
            "from" => "send_reminder",
            "on" => "ok",
            "to" => "complete",
            "condition" => %{"field" => "invoice.status", "equals" => "past_due"}
          }
        ])

      assert :ok = EditorSpec.validate_map(editor_map)
      assert {:ok, graph} = EditorSpec.preview_graph(editor_map)

      edge_ids = Enum.map(graph["edges"], & &1["id"])

      assert edge_ids == Enum.uniq(edge_ids)
      assert Enum.all?(edge_ids, &String.contains?(&1, ":condition:"))
      refute Enum.any?(edge_ids, &String.ends_with?(&1, ":condition:0"))

      assert Enum.all?(graph["edges"], fn edge ->
               match?(
                 %{
                   "selected?" => false,
                   "skipped?" => false,
                   "pending?" => true,
                   "blocked?" => false,
                   "outcome" => "ok",
                   "recovery" => nil
                 },
                 edge
               )
             end)

      assert Enum.all?(graph["edges"], &Map.has_key?(&1, "condition"))

      reordered_editor_map =
        Map.update!(editor_map, "transitions", &Enum.reverse/1)

      assert {:ok, diff} = EditorSpec.diff(editor_map, reordered_editor_map)

      assert diff["summary"] == %{
               "nodes_added" => 0,
               "nodes_removed" => 0,
               "nodes_changed" => 0,
               "edges_added" => 0,
               "edges_removed" => 0,
               "edges_changed" => 0,
               "editor_changed" => false
             }
    end

    test "diffs editor drafts against a source workflow spec" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.update!("steps", fn steps ->
          steps
          |> Enum.map(fn
            %{"name" => "send_reminder"} = step ->
              Map.put(step, "action", "billing.send_updated_reminder")

            step ->
              step
          end)
          |> Kernel.++([
            %{
              "name" => "archive_invoice",
              "action" => "billing.archive_invoice",
              "opts" => %{"after" => ["send_reminder"]}
            }
          ])
        end)
        |> Map.update!("transitions", fn transitions ->
          transitions
          |> Enum.reject(&(&1["from"] == "send_reminder" and &1["on"] == "error"))
          |> Kernel.++([
            %{"from" => "archive_invoice", "on" => "ok", "to" => "complete"}
          ])
        end)

      registry = %{
        "billing.send_updated_reminder" => SendReminder,
        "billing.archive_invoice" => SendReminder
      }

      assert {:ok, diff} = EditorSpec.diff(spec, editor_map, action_registry: registry)

      assert %{
               "source" => "workflow_spec",
               "status" => "draft_diff",
               "summary" => %{
                 "nodes_added" => 1,
                 "nodes_removed" => 0,
                 "nodes_changed" => 1,
                 "edges_added" => 1,
                 "edges_removed" => 1,
                 "edges_changed" => 0
               },
               "nodes" => %{
                 "added" => [%{"id" => "archive_invoice"}],
                 "removed" => [],
                 "changed" => [
                   %{
                     "id" => "send_reminder",
                     "before" => %{"id" => "send_reminder"},
                     "after" => %{
                       "id" => "send_reminder",
                       "action" => "billing.send_updated_reminder"
                     }
                   }
                 ]
               },
               "edges" => %{
                 "added" => [%{"id" => "archive_invoice:ok:complete"}],
                 "removed" => [%{"id" => "send_reminder:error:complete"}],
                 "changed" => []
               }
             } = diff
    end

    test "diff validates editor drafts before comparing" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("transitions", [
          %{"from" => "send_reminder", "on" => "ok", "to" => "missing_step"}
        ])

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.diff(spec, editor_map)

      assert_error(errors, [:transitions, 0, :to], :unknown_transition_target)
    end

    test "rejects duplicate editor node and edge identities before diffing" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      duplicate_steps =
        spec
        |> EditorSpec.to_map()
        |> Map.update!("steps", fn [first | _rest] = steps ->
          Enum.reverse([first | Enum.reverse(steps)])
        end)

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.diff(spec, duplicate_steps)

      assert_error(errors, [:steps, 2, :name], :duplicate_step_name)

      duplicate_edges =
        spec
        |> EditorSpec.to_map()
        |> Map.update!("transitions", fn [first | _rest] -> [first, first] end)

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.diff(spec, duplicate_edges)

      assert_error(errors, [:transitions, 1], :duplicate_edge_id)
    end

    test "rejects runtime-owned fields before previewing editor data" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("definition_fingerprint", "sha256:abc")

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert %{
               path: [:definition_fingerprint],
               code: :runtime_owned_field,
               message: "definition_fingerprint is runtime-owned and cannot be edited",
               details: %{field: "definition_fingerprint"}
             } in errors

      assert {:error, {:invalid_workflow_editor_spec, ^errors}} =
               EditorSpec.preview_graph(editor_map)
    end

    test "returns stable field paths for invalid graph references" do
      assert {:ok, spec} = Squidie.Workflow.to_spec(PaymentRecovery)

      editor_map =
        spec
        |> EditorSpec.to_map()
        |> Map.put("transitions", [
          %{"from" => "load_invoice", "on" => "ok", "to" => "missing_step"}
        ])

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      assert %{
               path: [:transitions, 0, :to],
               code: :unknown_transition_target,
               message: "transition targets unknown step: missing_step",
               details: %{to: "missing_step"}
             } in errors
    end

    test "rejects non-map editor input before validation or preview" do
      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map("not a spec")

      assert_error(errors, [], :invalid_editor_spec)

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.preview_graph(["not", "a", "map"])

      assert_error(errors, [], :invalid_editor_spec)
    end

    test "reports stable validation errors for malformed editor maps" do
      editor_map =
        %{
          "workflow" => "Demo",
          "definition_version" => "draft",
          "triggers" => :invalid,
          "payload" => :invalid,
          "steps" => [%{"name" => ""}, :not_a_step],
          "transitions" => [%{"from" => "missing_source", "on" => "retry", "to" => 123}],
          "retries" => :invalid,
          "entry_steps" => ["missing_entry", 123],
          "initial_step" => "missing_initial",
          "entry_step" => "missing_entry",
          run_id: "run_123",
          status: "running",
          terminal_status: "failed",
          current_node_id: "load_invoice",
          current_node_ids: ["load_invoice"],
          fingerprint: "sha256:runtime",
          spec_fingerprint: "sha256:spec",
          journal: [],
          audit_history: [],
          attempts: [],
          dispatches: [],
          history: []
        }

      assert {:error, {:invalid_workflow_editor_spec, errors}} =
               EditorSpec.validate_map(editor_map)

      for field <- [
            :run_id,
            :status,
            :terminal_status,
            :current_node_id,
            :current_node_ids,
            :fingerprint,
            :spec_fingerprint,
            :journal,
            :audit_history,
            :attempts,
            :dispatches,
            :history
          ] do
        assert_error(errors, [field], :runtime_owned_field)
      end

      assert_error(errors, [:triggers], :invalid_collection)
      assert_error(errors, [:payload], :invalid_collection)
      assert_error(errors, [:retries], :invalid_collection)
      assert_error(errors, [:steps, 0, :name], :invalid_step_name)
      assert_error(errors, [:steps, 1, :name], :invalid_step_name)
      assert_error(errors, [:transitions, 0, :from], :unknown_transition_source)
      assert_error(errors, [:transitions, 0, :to], :unknown_transition_target)
      assert_error(errors, [:transitions, 0, :on], :invalid_transition_outcome)
      assert_error(errors, [:entry_steps, 0], :unknown_entry_step)
      assert_error(errors, [:entry_steps, 1], :unknown_entry_step)
      assert_error(errors, [:initial_step], :unknown_step_reference)
      assert_error(errors, [:entry_step], :unknown_step_reference)
    end
  end

  defp assert_error(errors, path, code) do
    assert Enum.any?(errors, &match?(%{path: ^path, code: ^code}, &1))
  end
end
