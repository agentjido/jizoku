defmodule Jizoku.Runtime.StepInputTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.StepInput

  test "deserializes expected steps from atoms, existing atom names, and nil" do
    assert StepInput.deserialize_expected_step(nil) == {:ok, nil}
    assert StepInput.deserialize_expected_step(:load_invoice) == {:ok, :load_invoice}
    assert StepInput.deserialize_expected_step("load_invoice") == {:ok, :load_invoice}
  end

  test "rejects expected step names that are not existing atoms" do
    step_name = "missing_step_#{System.unique_integer([:positive])}"

    assert StepInput.deserialize_expected_step(step_name) == {:error, {:invalid_step, step_name}}
  end

  test "deserializes expected steps through persisted workflow definitions" do
    definition = %{
      steps: [
        %{name: :load_invoice},
        %{name: :notify_customer}
      ]
    }

    assert StepInput.deserialize_expected_step(nil, definition) == {:ok, nil}

    assert StepInput.deserialize_expected_step(:notify_customer, definition) ==
             {:ok, :notify_customer}

    assert StepInput.deserialize_expected_step("load_invoice", definition) ==
             {:ok, :load_invoice}

    assert StepInput.deserialize_expected_step("unknown-step", definition) ==
             {:error, {:invalid_step, "unknown-step"}}
  end

  test "normalizes nested maps without treating exception structs as enumerables" do
    error = %RuntimeError{message: "boom"}

    assert StepInput.normalize_map_keys(%{"details" => %{"original_exception" => error}}) == %{
             details: %{original_exception: error}
           }
  end

  test "resolves named path input mappings from normalized context" do
    input = %{
      "draft" => %{"drafts" => [%{id: "draft_1"}]},
      review_draft: %{reviewer: %{id: "user_123"}}
    }

    assert StepInput.apply_input_mapping(input,
             drafts: [:draft, :drafts],
             reviewer: [:review_draft, :reviewer]
           ) ==
             {:ok, %{drafts: [%{id: "draft_1"}], reviewer: %{id: "user_123"}}}
  end

  test "returns a structured error when a named path is missing" do
    assert StepInput.apply_input_mapping(%{draft: %{}}, drafts: [:draft, :drafts]) ==
             {:error,
              {:missing_input_path,
               %{target: :drafts, path: [:draft, :drafts], missing_at: [:draft, :drafts]}}}
  end

  test "recognizes and serializes missing input mapping errors" do
    reason =
      {:missing_input_path, %{target: :drafts, path: [:draft, :drafts], missing_at: [:draft]}}

    assert StepInput.input_mapping_error?(reason)
    refute StepInput.input_mapping_error?(:invalid_payload)

    assert StepInput.input_mapping_error_to_map(reason) == %{
             message: "missing mapped input path",
             code: "missing_input_path",
             target: "drafts",
             path: ["draft", "drafts"],
             missing_at: ["draft"],
             retryable?: false
           }
  end
end
