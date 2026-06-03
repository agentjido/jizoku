defmodule Squidie.Runtime.StepInputTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.StepInput

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
end
