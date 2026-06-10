defmodule Squidie.Step.ElixirTest do
  use ExUnit.Case, async: true

  alias Squidie.Workflow.ActionRegistry

  defmodule HostAdapters do
    @spec load_invoice(map(), Squidie.Step.Context.t()) :: {:ok, map()}
    def load_invoice(params, context) do
      {:ok,
       %{
         invoice: %{
           id: Map.fetch!(params, "invoice_id"),
           run_id: context.run_id,
           step: context.step
         }
       }}
    end

    @spec map_only(map()) :: {:ok, map()}
    def map_only(params), do: {:ok, %{echo: params}}

    @spec retry(map(), Squidie.Step.Context.t()) :: {:retry, map()}
    def retry(_params, _context), do: {:retry, %{message: "gateway unavailable"}}

    @spec fail(map(), Squidie.Step.Context.t()) :: {:error, map()}
    def fail(_params, _context), do: {:error, %{message: "invoice not found"}}

    @spec bad_output(map(), Squidie.Step.Context.t()) :: {:ok, String.t()}
    def bad_output(_params, _context), do: {:ok, "bad"}

    @spec bad_return(map(), Squidie.Step.Context.t()) :: :ok
    def bad_return(_params, _context), do: :ok

    @spec execution_opts(map(), Squidie.Step.Context.t()) :: {:ok, map(), keyword()}
    def execution_opts(_params, _context), do: {:ok, %{done?: true}, schedule_in: 10}

    @spec raise_error(map(), Squidie.Step.Context.t()) :: no_return()
    def raise_error(_params, _context), do: raise(ArgumentError, "boom secret")
  end

  defmodule ModuleAdapter do
    @spec run(map(), Squidie.Step.Context.t()) :: {:ok, map()}
    def run(params, _context), do: {:ok, %{loaded: Map.fetch!(params, "invoice_id")}}
  end

  describe "run/2" do
    test "invokes a host-approved module/function adapter by stable key" do
      assert {:ok, %{result: result}} =
               Squidie.Step.Elixir.run(
                 %{
                   adapter: "billing.load_invoice",
                   params: %{"invoice_id" => "inv_123"}
                 },
                 context(
                   adapters: %{
                     "billing.load_invoice" => [
                       module: HostAdapters,
                       function: :load_invoice
                     ]
                   }
                 )
               )

      assert result.invoice.id == "inv_123"
      assert result.invoice.run_id == "00000000-0000-4000-8000-000000000359"
      assert result.invoice.step == :elixir_action
    end

    test "invokes a host-approved module adapter with run/2" do
      assert {:ok, %{result: result}} =
               Squidie.Step.Elixir.run(
                 %{
                   adapter: "billing.module_adapter",
                   params: %{"invoice_id" => "inv_123"}
                 },
                 context(adapters: %{"billing.module_adapter" => ModuleAdapter})
               )

      assert result.loaded == "inv_123"
    end

    test "supports one-arity approved functions without passing runtime context" do
      assert {:ok, %{result: result}} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.map_only", params: %{"invoice_id" => "inv_123"}},
                 context(adapters: %{"billing.map_only" => {HostAdapters, :map_only}})
               )

      assert result.echo == %{"invoice_id" => "inv_123"}
    end

    test "supports atom adapter keys from trusted host-side specs" do
      assert {:ok, %{result: result}} =
               Squidie.Step.Elixir.run(
                 %{adapter: :load_invoice, params: %{"invoice_id" => "inv_123"}},
                 context(adapters: [load_invoice: ModuleAdapter])
               )

      assert result.loaded == "inv_123"
    end

    test "rejects missing host-owned adapter policy" do
      assert {:error, error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.load_invoice", params: %{"invoice_id" => "inv_123"}},
                 context()
               )

      assert error == %{
               message: "Elixir action validation failed",
               validation_errors: %{adapters: "adapters policy is required"},
               retryable?: false
             }
    end

    test "rejects invalid params and validation opts" do
      assert {:error, params_error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.load_invoice", params: "bad"},
                 context(adapters: %{"billing.load_invoice" => ModuleAdapter})
               )

      assert params_error.validation_errors == %{params: "params must be a map"}

      assert {:error, opts_error} =
               Squidie.Step.Elixir.validate_action_input(
                 %{adapter: "billing.load_invoice", params: %{}},
                 "bad"
               )

      assert opts_error.validation_errors == %{opts: "validation options must be a keyword list"}
    end

    test "rejects unknown and disabled adapter keys" do
      assert {:error, unknown} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.unknown", params: %{}},
                 context(adapters: %{"billing.load_invoice" => ModuleAdapter})
               )

      assert unknown.validation_errors == %{adapter: "adapter is not approved"}

      assert {:error, disabled} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.load_invoice", params: %{}},
                 context(
                   adapters: %{
                     "billing.load_invoice" => [
                       module: ModuleAdapter,
                       enabled?: false
                     ]
                   }
                 )
               )

      assert disabled.validation_errors == %{adapter: "adapter is disabled"}

      assert {:error, malformed_enabled} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.load_invoice", params: %{}},
                 context(
                   adapters: %{
                     "billing.load_invoice" => [
                       module: ModuleAdapter,
                       enabled?: "false"
                     ]
                   }
                 )
               )

      assert malformed_enabled.validation_errors == %{adapter: "adapter definition is invalid"}
    end

    test "rejects malformed adapter definitions without raising" do
      assert {:error, error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.malformed", params: %{}},
                 context(adapters: %{"billing.malformed" => ["not", "a", "keyword"]})
               )

      assert error.validation_errors == %{adapter: "adapter definition is invalid"}

      assert {:error, keyword_error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.keyword", params: %{}},
                 context(
                   adapters: %{
                     "billing.keyword" => [
                       module: "Billing.Actions",
                       function: "load_invoice"
                     ]
                   }
                 )
               )

      assert keyword_error.validation_errors == %{adapter: "adapter definition is invalid"}

      assert {:error, map_error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.map", params: %{}},
                 context(
                   adapters: %{
                     "billing.map" => %{
                       "module" => "Billing.Actions",
                       "function" => "load_invoice"
                     }
                   }
                 )
               )

      assert map_error.validation_errors == %{adapter: "adapter definition is invalid"}
    end

    test "accepts string-keyed adapter metadata entries" do
      assert {:ok, %{result: result}} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.load_invoice", params: %{"invoice_id" => "inv_123"}},
                 context(
                   adapters: %{
                     "billing.load_invoice" => %{
                       "module" => ModuleAdapter,
                       "function" => :run,
                       "enabled?" => true
                     }
                   }
                 )
               )

      assert result.loaded == "inv_123"
    end

    test "rejects runtime-authored module or function fields" do
      assert {:error, error} =
               Squidie.Step.Elixir.run(
                 %{
                   adapter: "billing.load_invoice",
                   params: %{},
                   module: "MyApp.Secret",
                   function: "run"
                 },
                 context(adapters: %{"billing.load_invoice" => ModuleAdapter})
               )

      assert error.validation_errors == %{
               input: "unsupported Elixir action input fields: function, module"
             }
    end

    test "returns retry and error tuples from host adapters as structured workflow errors" do
      adapters = %{
        "billing.retry" => {HostAdapters, :retry},
        "billing.fail" => {HostAdapters, :fail}
      }

      assert {:retry, retry_error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.retry", params: %{}},
                 context(adapters: adapters)
               )

      assert retry_error == %{
               message: "gateway unavailable",
               kind: :elixir_action,
               adapter: "billing.retry",
               retryable?: true
             }

      assert {:error, error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.fail", params: %{}},
                 context(adapters: adapters)
               )

      assert error == %{
               message: "invoice not found",
               kind: :elixir_action,
               adapter: "billing.fail",
               retryable?: false
             }
    end

    test "rejects non-map output and invalid adapter returns" do
      adapters = %{
        "billing.bad_output" => {HostAdapters, :bad_output},
        "billing.bad_return" => {HostAdapters, :bad_return},
        "billing.execution_opts" => {HostAdapters, :execution_opts}
      }

      assert {:error, bad_output} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.bad_output", params: %{}},
                 context(adapters: adapters)
               )

      assert bad_output.validation_errors == %{output: "adapter output must be a map"}

      assert {:error, bad_return} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.bad_return", params: %{}},
                 context(adapters: adapters)
               )

      assert bad_return.message == "Elixir action returned an invalid result"
      assert bad_return.retryable? == false

      assert {:error, execution_opts} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.execution_opts", params: %{}},
                 context(adapters: adapters)
               )

      assert execution_opts.validation_errors == %{
               output: "adapter execution opts are not supported"
             }
    end

    test "converts raised exceptions into structured errors without leaking messages" do
      assert {:error, error} =
               Squidie.Step.Elixir.run(
                 %{adapter: "billing.raise", params: %{}},
                 context(adapters: %{"billing.raise" => {HostAdapters, :raise_error}})
               )

      assert error == %{
               message: "Elixir action execution failed",
               kind: :elixir_action,
               adapter: "billing.raise",
               exception: "Elixir.ArgumentError",
               retryable?: false
             }

      refute inspect(error) =~ "boom secret"
    end
  end

  describe "validate_action_input/2" do
    test "validates planned runtime-spec input against host-owned adapter opts" do
      opts = [adapters: %{"billing.load_invoice" => ModuleAdapter}]

      assert :ok =
               Squidie.Step.Elixir.validate_action_input(
                 %{adapter: "billing.load_invoice", params: %{"invoice_id" => "inv_123"}},
                 opts
               )

      assert {:error, error} =
               Squidie.Step.Elixir.validate_action_input(
                 %{adapter: "billing.unknown", params: %{}},
                 opts
               )

      assert error.validation_errors == %{adapter: "adapter is not approved"}
    end
  end

  describe "persisted_action_opts/1" do
    test "strips executable adapter definitions before runtime spec persistence" do
      assert [
               adapters: %{
                 "billing.load_invoice" => %{
                   display_name: "Load invoice",
                   enabled?: true
                 },
                 "billing.reprice" => %{}
               }
             ] =
               Squidie.Step.Elixir.persisted_action_opts(
                 adapters: %{
                   "billing.load_invoice" => [
                     module: HostAdapters,
                     function: :load_invoice,
                     display_name: "Load invoice",
                     description: {HostAdapters, :load_invoice},
                     category: HostAdapters,
                     enabled?: true
                   ],
                   "billing.reprice" => ModuleAdapter
                 }
               )
    end

    test "persists only safe metadata from keyword and string-keyed entries" do
      assert [
               adapters: %{
                 load_invoice: %{description: "Loads invoices"}
               }
             ] =
               Squidie.Step.Elixir.persisted_action_opts(
                 adapters: [
                   load_invoice: [
                     module: HostAdapters,
                     function: :load_invoice,
                     description: "Loads invoices"
                   ]
                 ]
               )

      assert [
               adapters: %{
                 "billing.reprice" => %{
                   category: "Billing",
                   enabled?: false
                 }
               }
             ] =
               Squidie.Step.Elixir.persisted_action_opts(
                 adapters: %{
                   "billing.reprice" => %{
                     "module" => ModuleAdapter,
                     "category" => "Billing",
                     "enabled?" => false,
                     "display_name" => {ModuleAdapter, :run}
                   }
                 }
               )
    end
  end

  describe "action registry catalog" do
    test "exposes the Elixir action as an editor-safe approved action" do
      registry = %{
        "elixir.run" => [
          module: Squidie.Step.Elixir,
          category: "Elixir",
          input_contract: %{
            adapter: %{type: :string, required?: true, enum: ["billing.load_invoice"]},
            params: %{type: :map, required?: true}
          },
          action_opts: [adapters: %{"billing.load_invoice" => ModuleAdapter}]
        ]
      }

      assert :ok = ActionRegistry.validate_action("elixir.run", registry)
      assert {:ok, [entry]} = ActionRegistry.catalog(registry)

      assert entry.key == "elixir.run"
      assert entry.display_name == "Elixir action"
      assert entry.category == "Elixir"
      assert entry.input_contract["adapter"]["enum"] == ["billing.load_invoice"]
      assert entry.input_contract["adapter"]["required?"] == true
      assert entry.input_contract["params"]["required?"] == true
      assert entry.output_contract["result"]["required"] == true
      refute inspect(entry) =~ inspect(ModuleAdapter)
    end
  end

  defp context(opts \\ []) do
    %Squidie.Step.Context{
      run_id: "00000000-0000-4000-8000-000000000359",
      workflow: __MODULE__.RuntimeWorkflow,
      step: :elixir_action,
      attempt: 1,
      step_opts: [action_opts: opts],
      state: %{}
    }
  end
end
