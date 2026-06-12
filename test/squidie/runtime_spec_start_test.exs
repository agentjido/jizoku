defmodule Squidie.RuntimeSpecStartTest do
  use ExUnit.Case, async: false

  defmodule LoadInvoice do
    use Squidie.Step, name: :load_invoice

    @impl Squidie.Step
    def run(input, _context), do: {:ok, %{id: input.invoice_id}}
  end

  defmodule SendReminder do
    use Squidie.Step, name: :send_reminder

    @impl Squidie.Step
    def run(input, _context), do: {:ok, %{sent_invoice_id: input.invoice.id}}
  end

  defmodule HandleGuardrailFailure do
    use Squidie.Step, name: :handle_guardrail_failure

    @impl Squidie.Step
    def run(input, _context), do: {:ok, %{handled_guardrail_failure: input.invoice_id}}
  end

  defmodule AllowGuardrail do
    @spec validate_guardrail(map(), map()) :: {:ok, map()}
    def validate_guardrail(_value, context) do
      {:ok, %{placement: context.placement, step: context.step}}
    end
  end

  defmodule DenyGuardrail do
    @spec validate_guardrail(map(), map()) :: {:error, map()}
    def validate_guardrail(_value, context) do
      {:error,
       %{
         message: "invoice is outside host policy",
         placement: context.placement,
         step: context.step
       }}
    end
  end

  defmodule ElixirAdapters do
    @spec load_invoice(map(), Squidie.Step.Context.t()) :: {:ok, map()}
    def load_invoice(%{"invoice_id" => invoice_id}, _context) do
      {:ok, %{invoice: %{id: invoice_id}}}
    end

    def load_invoice(%{invoice_id: invoice_id}, _context) do
      {:ok, %{invoice: %{id: invoice_id}}}
    end

    def load_invoice(params, _context) do
      {:ok, %{invoice: %{id: Map.fetch!(params, "invoice_id")}}}
    end
  end

  @storage {Jido.Storage.ETS, table: :squidie_runtime_spec_start_test}
  @run_id "00000000-0000-4000-8000-000000000254"
  @missing_action_run_id "00000000-0000-4000-8000-000000000255"
  @http_action_run_id "00000000-0000-4000-8000-000000000358"
  @invalid_http_action_run_id "00000000-0000-4000-8000-000000000359"
  @elixir_action_run_id "00000000-0000-4000-8000-000000000360"
  @invalid_elixir_action_run_id "00000000-0000-4000-8000-000000000361"
  @guardrail_blocked_run_id "00000000-0000-4000-8000-000000000362"
  @guardrail_routed_run_id "00000000-0000-4000-8000-000000000363"

  test "starts and executes a validated runtime-authored spec" do
    registry = action_registry()
    spec = runtime_invoice_spec()

    assert :ok = Squidie.Workflow.validate_spec(spec, action_registry: registry)

    assert {:ok, snapshot} =
             Squidie.start_spec(spec, :manual, %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: @run_id
             )

    assert snapshot.run_id == @run_id
    assert snapshot.workflow == Atom.to_string(__MODULE__.RuntimeInvoiceReminder)
    assert snapshot.trigger == "manual"
    assert snapshot.definition_version == "runtime-spec-v1"

    assert {:ok, first} =
             Squidie.execute_next(journal_storage: @storage, owner_id: "worker")

    assert first.status in [:running, :waiting_for_dispatch]
    assert "send_reminder" in Enum.map(first.planned_runnables, &Map.fetch!(&1, :step))

    assert {:ok, completed} =
             Squidie.execute_next(journal_storage: @storage, owner_id: "worker")

    assert completed.status == :completed
    assert completed.terminal?

    assert {:ok, inspected} = Squidie.inspect_run(@run_id, journal_storage: @storage)
    assert inspected.status == :completed

    assert {:ok, graph} =
             Squidie.inspect_run_graph(@run_id,
               journal_storage: @storage,
               include_history: true
             )

    assert Enum.map(graph.nodes, & &1.id) == ["load_invoice", "send_reminder"]

    assert Enum.any?(
             graph.edges,
             &match?(%{from: "load_invoice", to: "send_reminder", type: :transition}, &1)
           )
  end

  test "starts specs whose top-level fields were decoded with string keys" do
    registry = action_registry()
    run_id = "00000000-0000-4000-8000-000000000257"

    spec = Map.new(runtime_invoice_spec(), fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:ok, snapshot} =
             Squidie.start_spec(spec, :manual, %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: run_id
             )

    assert snapshot.run_id == run_id
    assert snapshot.workflow == Atom.to_string(__MODULE__.RuntimeInvoiceReminder)
    assert [%{step: "load_invoice", status: :available}] = snapshot.visible_attempts
  end

  test "rejects unknown action keys before creating a run" do
    assert {:error, {:invalid_workflow_spec, errors}} =
             Squidie.start_spec(runtime_invoice_spec(), %{invoice_id: "inv_123"},
               action_registry: %{},
               journal_storage: @storage,
               run_id: @missing_action_run_id
             )

    assert Enum.any?(errors, &(&1.code == :unknown_action_key))

    assert {:error, :not_found} =
             Squidie.inspect_run(@missing_action_run_id, journal_storage: @storage)
  end

  test "blocks runtime-authored run start when an input guardrail fails" do
    assert {:error, {:invalid_workflow_spec, errors}} =
             Squidie.start_spec(
               runtime_guardrail_spec(input: [[key: "billing.invoice_policy"]]),
               %{invoice_id: "inv_123"},
               action_registry: action_registry(),
               guardrail_registry: %{"billing.invoice_policy" => DenyGuardrail},
               journal_storage: @storage,
               run_id: @guardrail_blocked_run_id
             )

    assert %{
             path: [:steps, 0, :opts, :guardrails, :input, 0],
             code: :guardrail_failed,
             message: "step :load_invoice input guardrail \"billing.invoice_policy\" failed",
             details: %{
               step: :load_invoice,
               guardrail: "billing.invoice_policy",
               placement: :input,
               policy: :block_run_start,
               result: %{
                 message: "invoice is outside host policy",
                 placement: :input,
                 step: :load_invoice
               }
             }
           } in errors

    assert {:error, :not_found} =
             Squidie.inspect_run(@guardrail_blocked_run_id, journal_storage: @storage)
  end

  test "routes runtime-authored guardrail failures through explicit error policy" do
    assert {:ok, snapshot} =
             Squidie.start_spec(
               runtime_guardrail_spec(
                 output: [[key: "billing.invoice_policy", policy: :route_error]]
               ),
               %{invoice_id: "inv_123"},
               action_registry: action_registry(),
               guardrail_registry: %{"billing.invoice_policy" => DenyGuardrail},
               journal_storage: @storage,
               run_id: @guardrail_routed_run_id
             )

    assert [
             %{
               step: "load_invoice",
               guardrails: [
                 %{key: "billing.invoice_policy", placement: :output, policy: :route_error}
               ]
             }
           ] = snapshot.planned_runnables

    assert {:ok, routed} =
             Squidie.execute_next(
               journal_storage: @storage,
               owner_id: "guardrail-worker",
               guardrail_registry: %{"billing.invoice_policy" => DenyGuardrail}
             )

    assert "handle_guardrail_failure" in Enum.map(
             routed.planned_runnables,
             &Map.fetch!(&1, :step)
           )

    assert [
             %{
               step: "load_invoice",
               error: %{
                 code: "guardrail_failed",
                 message: "step :load_invoice output guardrail \"billing.invoice_policy\" failed"
               }
             }
             | _other_attempts
           ] = routed.attempts

    assert Enum.any?(
             routed.guardrails,
             &match?(
               %{
                 key: "billing.invoice_policy",
                 placement: :output,
                 policy: :route_error,
                 status: :failed
               },
               &1
             )
           )

    assert {:ok, explanation} =
             Squidie.explain_run(@guardrail_routed_run_id, journal_storage: @storage)

    assert Enum.any?(
             explanation.evidence.guardrails,
             &match?(
               %{
                 key: "billing.invoice_policy",
                 placement: :output,
                 policy: :route_error,
                 status: :failed
               },
               &1
             )
           )

    assert {:ok, completed} =
             Squidie.execute_next(
               journal_storage: @storage,
               owner_id: "guardrail-worker",
               guardrail_registry: %{"billing.invoice_policy" => DenyGuardrail}
             )

    assert completed.status == :completed
    assert completed.context.handled_guardrail_failure == "inv_123"
  end

  test "rejects invalid named triggers without raising" do
    assert {:error, {:invalid_trigger, "manual"}} =
             Squidie.start_spec(runtime_invoice_spec(), "manual", %{invoice_id: "inv_123"},
               action_registry: action_registry(),
               journal_storage: @storage
             )
  end

  test "rejects runtime-authored spec replays explicitly" do
    registry = action_registry()
    run_id = "00000000-0000-4000-8000-000000000256"

    assert {:ok, _snapshot} =
             Squidie.start_spec(runtime_invoice_spec(), %{invoice_id: "inv_123"},
               action_registry: registry,
               journal_storage: @storage,
               run_id: run_id
             )

    assert {:error, {:invalid_replay_source, :runtime_spec}} =
             Squidie.replay(run_id, journal_storage: @storage)
  end

  test "executes an approved HTTP action key from a runtime-authored spec" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "GET", "/invoices/inv_123", fn conn ->
      Plug.Conn.resp(conn, 200, "paid")
    end)

    assert {:ok, snapshot} =
             Squidie.start_spec(
               runtime_http_spec(),
               :manual,
               %{
                 request: %{
                   method: "GET",
                   url_template: "http://127.0.0.1:#{bypass.port}/invoices/{{ invoice_id }}",
                   bindings: %{invoice_id: "inv_123"}
                 }
               },
               action_registry: http_action_registry(["127.0.0.1"], persist_response_body?: true),
               journal_storage: @storage,
               run_id: @http_action_run_id
             )

    assert [%{step: "fetch_invoice", status: :available}] = snapshot.visible_attempts

    assert {:ok, completed} =
             Squidie.execute_next(journal_storage: @storage, owner_id: "http-worker")

    assert completed.status == :completed
    assert completed.context.http_response.status == 200
    assert completed.context.http_response.body == "paid"
  end

  test "rejects invalid HTTP action config before creating a runtime-authored run" do
    assert {:error, {:invalid_workflow_spec, errors}} =
             Squidie.start_spec(
               runtime_http_spec(),
               :manual,
               %{
                 request: %{
                   method: "GET",
                   url: "https://metadata.internal/latest"
                 }
               },
               action_registry: http_action_registry(["api.example.test"]),
               journal_storage: @storage,
               run_id: @invalid_http_action_run_id
             )

    assert %{
             path: [:steps, 0, :input],
             code: :invalid_action_input,
             message: "step :fetch_invoice action input is invalid",
             details: %{
               step: :fetch_invoice,
               validation_errors: %{url: "host is not allowed"}
             }
           } in errors

    assert {:error, :not_found} =
             Squidie.inspect_run(@invalid_http_action_run_id, journal_storage: @storage)
  end

  test "executes an approved Elixir action adapter from a runtime-authored spec" do
    assert {:ok, snapshot} =
             Squidie.start_spec(
               runtime_elixir_spec(),
               :manual,
               %{
                 adapter: "billing.load_invoice",
                 params: %{"invoice_id" => "inv_123"}
               },
               action_registry: elixir_action_registry(),
               journal_storage: @storage,
               run_id: @elixir_action_run_id
             )

    assert [%{step: "load_invoice", status: :available}] = snapshot.visible_attempts
    assert_persisted_elixir_action_opts_are_safe(@elixir_action_run_id)

    assert {:ok, completed} =
             Squidie.execute_next(
               journal_storage: @storage,
               owner_id: "elixir-worker",
               action_registry: elixir_action_registry()
             )

    assert completed.status == :completed
    assert completed.context.result.invoice.id == "inv_123"
  end

  test "rejects invalid Elixir action adapters before creating a runtime-authored run" do
    assert {:error, {:invalid_workflow_spec, errors}} =
             Squidie.start_spec(
               runtime_elixir_spec(),
               :manual,
               %{
                 adapter: "billing.unknown",
                 params: %{"invoice_id" => "inv_123"}
               },
               action_registry: elixir_action_registry(),
               journal_storage: @storage,
               run_id: @invalid_elixir_action_run_id
             )

    assert %{
             path: [:steps, 0, :input],
             code: :invalid_action_input,
             message: "step :load_invoice action input is invalid",
             details: %{
               step: :load_invoice,
               validation_errors: %{adapter: "adapter is not approved"}
             }
           } in errors

    assert {:error, :not_found} =
             Squidie.inspect_run(@invalid_elixir_action_run_id, journal_storage: @storage)
  end

  defp action_registry do
    %{
      "billing.load_invoice" => LoadInvoice,
      "billing.send_reminder" => SendReminder,
      "billing.handle_guardrail_failure" => HandleGuardrailFailure
    }
  end

  defp http_action_registry(allowed_hosts, opts \\ []) do
    %{
      "http.request" => [
        module: Squidie.Step.HTTP,
        action_opts: Keyword.put(opts, :allowed_hosts, allowed_hosts)
      ]
    }
  end

  defp elixir_action_registry do
    %{
      "elixir.run" => [
        module: Squidie.Step.Elixir,
        action_opts: [
          adapters: %{
            "billing.load_invoice" => {ElixirAdapters, :load_invoice}
          }
        ]
      ]
    }
  end

  defp runtime_invoice_spec do
    %{
      workflow: __MODULE__.RuntimeInvoiceReminder,
      definition_version: "runtime-spec-v1",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :invoice_id, type: :string, opts: []}]
        }
      ],
      payload: [%{name: :invoice_id, type: :string, opts: []}],
      steps: [
        %{name: :load_invoice, action: "billing.load_invoice", opts: [output: :invoice]},
        %{name: :send_reminder, action: "billing.send_reminder", opts: [input: [:invoice]]}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :send_reminder},
        %{from: :send_reminder, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }
  end

  defp runtime_guardrail_spec(guardrails) do
    %{
      runtime_invoice_spec()
      | steps: [
          %{
            name: :load_invoice,
            action: "billing.load_invoice",
            opts: [output: :invoice, guardrails: guardrails]
          },
          %{
            name: :handle_guardrail_failure,
            action: "billing.handle_guardrail_failure",
            opts: [input: [:invoice_id]]
          }
        ],
        transitions: [
          %{from: :load_invoice, on: :ok, to: :complete},
          %{from: :load_invoice, on: :error, to: :handle_guardrail_failure},
          %{from: :handle_guardrail_failure, on: :ok, to: :complete}
        ]
    }
  end

  defp runtime_http_spec do
    %{
      workflow: __MODULE__.RuntimeHTTPWorkflow,
      definition_version: "runtime-http-v1",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [%{name: :request, type: :map, opts: []}]
        }
      ],
      payload: [%{name: :request, type: :map, opts: []}],
      steps: [
        %{name: :fetch_invoice, action: "http.request", opts: []}
      ],
      transitions: [
        %{from: :fetch_invoice, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:fetch_invoice],
      initial_step: :fetch_invoice,
      entry_step: :fetch_invoice
    }
  end

  defp runtime_elixir_spec do
    %{
      workflow: __MODULE__.RuntimeElixirWorkflow,
      definition_version: "runtime-elixir-v1",
      triggers: [
        %{
          name: :manual,
          type: :manual,
          config: %{},
          payload: [
            %{name: :adapter, type: :string, opts: []},
            %{name: :params, type: :map, opts: []}
          ]
        }
      ],
      payload: [
        %{name: :adapter, type: :string, opts: []},
        %{name: :params, type: :map, opts: []}
      ],
      steps: [
        %{name: :load_invoice, action: "elixir.run", opts: []}
      ],
      transitions: [
        %{from: :load_invoice, on: :ok, to: :complete}
      ],
      retries: [],
      entry_steps: [:load_invoice],
      initial_step: :load_invoice,
      entry_step: :load_invoice
    }
  end

  defp assert_persisted_elixir_action_opts_are_safe(run_id) do
    assert {:ok, %{entries: entries}} =
             Squidie.Runtime.Journal.load_thread(@storage, {:run, run_id})

    assert %{definition_spec: %{steps: [step]}} =
             Enum.find_value(entries, fn
               %{type: :run_started, data: data} -> data
               _entry -> nil
             end)

    assert [
             action_opts: [
               adapters: %{
                 "billing.load_invoice" => %{}
               }
             ]
           ] = step.opts

    refute inspect(step.opts) =~ inspect(ElixirAdapters)
  end
end
