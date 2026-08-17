defmodule Jizoku.Workflow.CompatibilityTest do
  use ExUnit.Case, async: true

  alias Jizoku.Workflow.Compatibility.Difference
  alias Jizoku.Workflow.Compatibility.Result

  defmodule StepV1 do
    use Jizoku.Step, name: "compatibility_step_v1", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 1}}
    end
  end

  defmodule StepV2 do
    use Jizoku.Step, name: "compatibility_step_v2", input_schema: []

    @impl Jizoku.Step
    def run(_input, _context) do
      {:ok, %{version: 2}}
    end
  end

  test "version labels and additive triggers stay compatible" do
    old = base_spec()

    new =
      old
      |> Map.put(:definition_version, "v2")
      |> Map.update!(:triggers, fn triggers ->
        [%{name: :api, type: :manual, config: %{}, payload: payload()} | triggers]
      end)

    assert {:ok, %Result{category: :compatible, differences: differences}} =
             Jizoku.Workflow.compatibility(old, new)

    assert Enum.map(differences, & &1.kind) == [
             :definition_version_changed,
             :trigger_added
           ]

    assert Enum.all?(differences, &match?(%Difference{category: :compatible}, &1))
  end

  test "optional payload additions stay compatible across the workflow and trigger contracts" do
    old = base_spec()

    new =
      add_payload_field(old, %{name: :note, type: :string, opts: [required: false]})

    assert {:ok, %Result{category: :compatible, differences: differences}} =
             Jizoku.Workflow.compatibility(old, new)

    assert Enum.map(differences, &{&1.category, &1.kind, &1.path}) == [
             {:compatible, :payload_field_added, [:payload, "note"]},
             {:compatible, :trigger_payload_field_added, [:triggers, "manual", :payload, "note"]}
           ]
  end

  test "required payload additions require migration across the workflow and trigger contracts" do
    old = base_spec()
    new = add_payload_field(old, %{name: :region, type: :string, opts: [required: true]})

    assert {:ok, %Result{category: :migration_required, differences: differences}} =
             Jizoku.Workflow.compatibility(old, new)

    assert Enum.map(differences, &{&1.category, &1.kind, &1.path}) == [
             {:migration_required, :payload_field_added, [:payload, "region"]},
             {:migration_required, :trigger_payload_field_added,
              [:triggers, "manual", :payload, "region"]}
           ]
  end

  test "execution changes report their exact migration differences" do
    old = base_spec()

    cases = [
      {put_in(old, [:steps, Access.at(0), :module], StepV2), :action_changed,
       [:steps, "process", :action]},
      {put_in(old, [:steps, Access.at(0), :opts], input: [:account_id]), :step_input_changed,
       [:steps, "process", :input]},
      {put_in(old, [:steps, Access.at(0), :opts], output: :result), :step_output_changed,
       [:steps, "process", :output]},
      {old
       |> put_in([:steps, Access.at(0), :opts], retry: [max_attempts: 5])
       |> Map.put(:retries, [%{step: :process, opts: [max_attempts: 5]}]), :step_retry_changed,
       [:steps, "process", :retry]},
      {put_in(old, [:steps, Access.at(0), :opts],
         deadline: [within: 1_000, due_soon: 250, escalation: :diagnostic]
       ), :step_deadline_changed, [:steps, "process", :deadline]},
      {put_in(old, [:steps, Access.at(0), :opts], irreversible: true), :step_recovery_changed,
       [:steps, "process", :recovery]},
      {put_in(old, [:transitions, Access.at(0), :on], :error), :transition_changed,
       [:transitions, 0, :on]},
      {put_in(old, [:triggers, Access.at(0), :config], %{source: "operator"}), :trigger_changed,
       [:triggers, "manual", :config]}
    ]

    Enum.each(cases, fn {changed, expected_kind, expected_path} ->
      assert {:ok, %Result{category: :migration_required, differences: differences}} =
               Jizoku.Workflow.compatibility(old, changed)

      assert Enum.any?(differences, fn difference ->
               difference.category == :migration_required and
                 difference.kind == expected_kind and
                 difference.path == expected_path
             end)
    end)
  end

  test "built-in step options are execution-significant" do
    cases = [
      {built_in_spec(:wait, duration: 1_000), built_in_spec(:wait, duration: 2_000), "duration"},
      {built_in_spec(:log, message: "before", level: :info),
       built_in_spec(:log, message: "after", level: :info), "message"},
      {built_in_spec(:log, message: "same", level: :info),
       built_in_spec(:log, message: "same", level: :warning), "level"}
    ]

    Enum.each(cases, fn {old, new, option_name} ->
      assert {:ok, %Result{category: :migration_required, differences: [difference]}} =
               Jizoku.Workflow.compatibility(old, new)

      assert %Difference{
               category: :migration_required,
               kind: :step_option_changed,
               path: [:steps, "process", :options, ^option_name]
             } = difference
    end)
  end

  test "transition declaration order does not change compatibility" do
    old = two_step_spec()
    reordered = Map.update!(old, :transitions, &Enum.reverse/1)

    assert {:ok, %Result{category: :compatible, differences: []}} =
             Jizoku.Workflow.compatibility(old, reordered)
  end

  test "canonicalizes validated struct values without raising" do
    old = put_in(base_spec(), [:triggers, Access.at(0), :config], %{endpoint: %URI{host: "old"}})
    new = put_in(base_spec(), [:triggers, Access.at(0), :config], %{endpoint: %URI{host: "new"}})

    assert {:ok, %Result{category: :migration_required, differences: differences}} =
             Jizoku.Workflow.compatibility(old, new)

    assert_difference(differences, :migration_required, :trigger_changed, [
      :triggers,
      "manual",
      :config
    ])
  end

  test "removing declared structure is incompatible" do
    old = base_spec()

    without_payload =
      old
      |> Map.put(:payload, [])
      |> Map.update!(:triggers, fn triggers ->
        Enum.map(triggers, &Map.put(&1, :payload, []))
      end)

    changed_specs = [without_payload, Map.update!(old, :triggers, &List.delete_at(&1, 1))]

    Enum.each(changed_specs, fn changed ->
      assert {:ok, %Result{category: :incompatible}} =
               Jizoku.Workflow.compatibility(old, changed)
    end)
  end

  test "graph additions and removals identify steps, transitions, and retries" do
    base = base_spec()
    expanded = two_step_spec()

    assert {:ok, %Result{category: :migration_required, differences: additions}} =
             Jizoku.Workflow.compatibility(base, expanded)

    assert_difference(additions, :migration_required, :step_added, [:steps, "archive"])

    assert_difference(additions, :migration_required, :transition_added, [
      :transitions,
      1
    ])

    assert {:ok, %Result{category: :incompatible, differences: removals}} =
             Jizoku.Workflow.compatibility(expanded, base)

    assert_difference(removals, :incompatible, :step_removed, [:steps, "archive"])
    assert_difference(removals, :incompatible, :transition_removed, [:transitions, 1])

    with_retry =
      base
      |> put_in([:steps, Access.at(0), :opts], retry: [max_attempts: 5])
      |> Map.put(:retries, [%{step: :process, opts: [max_attempts: 5]}])

    assert {:ok, %Result{category: :incompatible, differences: retry_removal}} =
             Jizoku.Workflow.compatibility(with_retry, base)

    assert_difference(retry_removal, :incompatible, :retry_removed, [
      :retries,
      "process"
    ])
  end

  test "differences are deterministic and identify changed fields" do
    old = base_spec()

    new =
      old
      |> put_in([:steps, Access.at(0), :module], StepV2)
      |> put_in([:steps, Access.at(0), :opts], input: [:account_id], output: :result)

    reordered =
      update_in(new, [:steps, Access.at(0)], fn step ->
        step
        |> Map.to_list()
        |> Enum.reverse()
        |> Map.new()
      end)

    assert {:ok, %Result{differences: first}} = Jizoku.Workflow.compatibility(old, new)
    assert {:ok, %Result{differences: second}} = Jizoku.Workflow.compatibility(old, reordered)
    assert first == second

    assert Enum.map(first, & &1.path) == [
             [:steps, "process", :action],
             [:steps, "process", :input],
             [:steps, "process", :output]
           ]
  end

  test "invalid definitions return their validation errors" do
    assert {:error, {:invalid_workflow_spec, [_error | _errors]}} =
             Jizoku.Workflow.compatibility(%{}, base_spec())

    assert {:error, {:invalid_workflow_spec, [_error | _errors]}} =
             Jizoku.Workflow.compatibility(base_spec(), %{})

    assert {:error, {:invalid_workflow_spec, [_error | _errors]}} =
             Jizoku.Workflow.compatibility(42, base_spec())
  end

  test "accepts compiled workflow modules" do
    workflow = Jizoku.TestSupport.LazyWorkflow

    assert {:ok, %Result{category: :compatible, differences: []}} =
             Jizoku.Workflow.compatibility(workflow, workflow)
  end

  test "rejects duplicate identifiers" do
    duplicate_trigger =
      Map.update!(base_spec(), :triggers, fn [first | _rest] = triggers ->
        [first | triggers]
      end)

    assert {:error, {:invalid_workflow_spec, errors}} =
             Jizoku.Workflow.compatibility(duplicate_trigger, base_spec())

    assert Enum.any?(errors, &(&1.code == :duplicate_trigger_name))
  end

  defp base_spec do
    %{
      workflow: __MODULE__,
      definition_version: "v1",
      triggers: [
        %{name: :manual, type: :manual, config: %{}, payload: payload()},
        %{name: :operator, type: :manual, config: %{}, payload: payload()}
      ],
      payload: payload(),
      steps: [%{name: :process, module: StepV1, opts: []}],
      transitions: [%{from: :process, on: :ok, to: :complete}],
      retries: [],
      entry_steps: [:process],
      initial_step: :process,
      entry_step: :process
    }
  end

  defp two_step_spec do
    base_spec()
    |> Map.put(:steps, [
      %{name: :process, module: StepV1, opts: []},
      %{name: :archive, module: StepV1, opts: []}
    ])
    |> Map.put(:transitions, [
      %{from: :process, on: :ok, to: :archive},
      %{from: :archive, on: :ok, to: :complete}
    ])
  end

  defp payload do
    [%{name: :account_id, type: :string, opts: [required: true]}]
  end

  defp add_payload_field(spec, field) do
    spec
    |> Map.update!(:payload, &List.insert_at(&1, -1, field))
    |> update_in(
      [:triggers, Access.at(0), :payload],
      &List.insert_at(&1, -1, field)
    )
  end

  defp built_in_spec(kind, opts) do
    put_in(base_spec(), [:steps, Access.at(0)], %{name: :process, module: kind, opts: opts})
  end

  defp assert_difference(differences, category, kind, path) do
    assert Enum.any?(differences, fn difference ->
             difference.category == category and difference.kind == kind and
               difference.path == path
           end)
  end
end
