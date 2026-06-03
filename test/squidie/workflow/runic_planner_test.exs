defmodule Squidie.Workflow.RunicPlannerTest do
  use ExUnit.Case, async: true

  alias Squidie.Workflow.Info
  alias Squidie.Workflow.RunicPlanner

  defmodule LoadAccount do
    use Squidie.Step,
      name: :load_account,
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: [account: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{account: %{id: "acct_123"}}}
  end

  defmodule LoadInvoice do
    use Squidie.Step,
      name: :load_invoice,
      input_schema: [invoice_id: [type: :string, required: true]],
      output_schema: [invoice: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{invoice: %{id: "inv_123"}}}
  end

  defmodule SendEmail do
    use Squidie.Step,
      name: :send_email,
      input_schema: [
        account_id: [type: :string, required: false],
        account: [type: :map, required: true],
        invoice: [type: :map, required: false]
      ],
      output_schema: [delivery: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{delivery: %{status: "sent"}}}
  end

  defmodule LookupAccount do
    use Squidie.Step,
      name: :lookup_account,
      input_schema: [account_id: [type: :string, required: true]],
      output_schema: [id: [type: :string, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{id: "acct_123"}}
  end

  defmodule CheckGateway do
    use Squidie.Step,
      name: :check_gateway,
      output_schema: [status: [type: :string, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{status: "available"}}
  end

  defmodule IssueCredit do
    use Squidie.Step,
      name: :issue_credit,
      output_schema: [credit: [type: :map, required: true]]

    @impl Squidie.Step
    def run(_input, _context), do: {:ok, %{credit: %{status: "issued"}}}
  end

  defmodule LinearWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
        end
      end

      step :load_account, LoadAccount
      step :send_email, SendEmail

      transition :load_account, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule BranchingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :load_account, LoadAccount
      step :load_invoice, LoadInvoice
      step :send_email, SendEmail, after: [:load_account, :load_invoice]
    end
  end

  defmodule MappedWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :invoice_id, :string
        end
      end

      step :lookup_account, LookupAccount,
        input: [:account_id],
        output: :account

      step :send_email, SendEmail, input: [:account_id, :invoice_id, :account]

      transition :lookup_account, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule PathMappedWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()

        payload do
          field :account_id, :string
          field :review, :map
        end
      end

      step :load_account, LoadAccount,
        input: [:account_id],
        output: :draft

      step :send_email, SendEmail,
        input: [
          account: [:draft, :account],
          reviewer_id: [:review, :reviewer_id]
        ]

      transition :load_account, on: :ok, to: :send_email
      transition :send_email, on: :ok, to: :complete
    end
  end

  defmodule ErrorRoutingWorkflow do
    use Squidie.Workflow

    workflow do
      trigger :manual do
        manual()
      end

      step :check_gateway, CheckGateway
      step :send_email, SendEmail
      step :issue_credit, IssueCredit

      transition :check_gateway, on: :ok, to: :send_email
      transition :check_gateway, on: :error, to: :issue_credit, recovery: :compensation
      transition :send_email, on: :ok, to: :complete
      transition :issue_credit, on: :ok, to: :complete
    end
  end

  test "exposes a normalized workflow spec for planner persistence" do
    spec = Info.spec(LinearWorkflow)

    assert spec.workflow == LinearWorkflow
    assert spec.entry_steps == [:load_account]

    assert spec.triggers == [
             %{
               name: :manual,
               type: :manual,
               config: %{},
               payload: [%{name: :account_id, type: :string, opts: []}]
             }
           ]

    assert Enum.map(spec.steps, &Map.take(&1, [:name, :module, :opts])) == [
             %{name: :load_account, module: LoadAccount, opts: []},
             %{name: :send_email, module: SendEmail, opts: []}
           ]

    assert spec.transitions == [
             %{from: :load_account, on: :ok, to: :send_email},
             %{from: :send_email, on: :ok, to: :complete}
           ]

    rebuilt_spec = :erlang.binary_to_term(:erlang.term_to_binary(spec))

    assert rebuilt_spec == spec

    assert {:ok, planner} = RunicPlanner.new(rebuilt_spec)
    assert {:ok, _planned, [%{step: :load_account}]} = RunicPlanner.plan(planner, %{})
  end

  test "plans a linear workflow through Runic without executing step actions" do
    {:ok, planner} = RunicPlanner.new(LinearWorkflow)
    assert %Runic.Workflow{} = planner.runic_workflow

    {:ok, planned, [load_account]} =
      RunicPlanner.plan(planner, %{account_id: "acct_123"})

    assert load_account.step == :load_account
    assert load_account.metadata.contract == :squidie_step

    {:ok, after_load} =
      RunicPlanner.apply_result(planned, load_account, {:ok, %{account: %{id: "acct_123"}}})

    {:ok, _planned, [send_email]} = RunicPlanner.plan(after_load)

    assert send_email.step == :send_email

    assert send_email.input == %{
             account_id: "acct_123",
             account: %{id: "acct_123"}
           }
  end

  test "plans mapped step inputs from payload and mapped outputs" do
    {:ok, planner} = RunicPlanner.new(MappedWorkflow)

    {:ok, planned, [lookup_account]} =
      RunicPlanner.plan(planner, %{account_id: "acct_123", invoice_id: "inv_123"})

    assert lookup_account.step == :lookup_account
    assert lookup_account.input == %{account_id: "acct_123"}

    {:ok, planned_with_account} =
      RunicPlanner.apply_result(planned, lookup_account, {:ok, %{id: "acct_123"}})

    {:ok, _planned, [send_email]} = RunicPlanner.plan(planned_with_account)

    assert send_email.step == :send_email

    assert send_email.input == %{
             account_id: "acct_123",
             invoice_id: "inv_123",
             account: %{id: "acct_123"}
           }
  end

  test "plans named path mapped step inputs" do
    {:ok, planner} = RunicPlanner.new(PathMappedWorkflow)

    {:ok, planned, [load_account]} =
      RunicPlanner.plan(planner, %{
        account_id: "acct_123",
        review: %{reviewer_id: "user_123"}
      })

    assert load_account.input == %{account_id: "acct_123"}

    {:ok, planned_with_account} =
      RunicPlanner.apply_result(planned, load_account, {:ok, %{account: %{id: "acct_123"}}})

    {:ok, _planned, [send_email]} = RunicPlanner.plan(planned_with_account)

    assert send_email.input == %{
             account: %{id: "acct_123"},
             reviewer_id: "user_123"
           }
  end

  test "returns structured errors when named path mapped planner input is missing" do
    {:ok, planner} = RunicPlanner.new(PathMappedWorkflow)

    {:ok, planned, [load_account]} =
      RunicPlanner.plan(planner, %{account_id: "acct_123"})

    {:ok, planned_with_account} =
      RunicPlanner.apply_result(planned, load_account, {:ok, %{account: %{id: "acct_123"}}})

    assert {:error,
            {:missing_input_path,
             %{target: :reviewer_id, path: [:review, :reviewer_id], missing_at: [:review]}}} =
             RunicPlanner.plan(planned_with_account)
  end

  test "routes successful and failed outcomes to their declared transition targets" do
    {:ok, planner} = RunicPlanner.new(ErrorRoutingWorkflow)
    {:ok, planned, [check_gateway]} = RunicPlanner.plan(planner, %{})

    assert check_gateway.step == :check_gateway

    {:ok, success_planned} =
      RunicPlanner.apply_result(planned, check_gateway, {:ok, %{status: "available"}})

    assert {:ok, _planned, [%{step: :send_email}]} = RunicPlanner.plan(success_planned)

    {:ok, failure_planner} = RunicPlanner.new(ErrorRoutingWorkflow)
    {:ok, failure_plan, [failure_check_gateway]} = RunicPlanner.plan(failure_planner, %{})

    {:ok, failure_planned} =
      RunicPlanner.apply_result(
        failure_plan,
        failure_check_gateway,
        {:error, %{message: "gateway timeout"}}
      )

    assert {:ok, _planned, [%{step: :issue_credit}]} = RunicPlanner.plan(failure_planned)
  end

  test "plans dependency branches and unlocks joins after completed runnable results" do
    {:ok, planner} = RunicPlanner.new(BranchingWorkflow)

    {:ok, planned, runnables} =
      RunicPlanner.plan(planner, %{account_id: "acct_123", invoice_id: "inv_123"})

    assert Enum.map(runnables, & &1.step) |> Enum.sort() == [:load_account, :load_invoice]

    [load_account, load_invoice] = Enum.sort_by(runnables, & &1.step)

    {:ok, planned_with_account} =
      RunicPlanner.apply_result(planned, load_account, {:ok, %{account: %{id: "acct_123"}}})

    {:ok, planned_with_invoice} =
      RunicPlanner.apply_result(
        planned_with_account,
        load_invoice,
        {:ok, %{invoice: %{id: "inv_123"}}}
      )

    {:ok, _planned, [send_email]} = RunicPlanner.plan(planned_with_invoice)

    assert send_email.step == :send_email

    assert send_email.input == %{
             account_id: "acct_123",
             invoice_id: "inv_123",
             account: %{id: "acct_123"},
             invoice: %{id: "inv_123"}
           }
  end
end
