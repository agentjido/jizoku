defmodule Jizoku.ToolsTest do
  use ExUnit.Case, async: true

  alias Jizoku.Tools
  alias Jizoku.Tools.Error
  alias Jizoku.Tools.Result

  defmodule SuccessfulAdapter do
    @behaviour Jizoku.Tools.Adapter

    @impl Jizoku.Tools.Adapter
    def invoke(request, context, _opts) do
      {:ok,
       %Result{
         adapter: __MODULE__,
         payload: %{request: request},
         metadata: %{context: context}
       }}
    end
  end

  defmodule InvalidAdapter do
    @behaviour Jizoku.Tools.Adapter

    @impl Jizoku.Tools.Adapter
    def invoke(_request, _context, _opts) do
      {:ok, %{payload: %{}}}
    end
  end

  describe "invoke/4" do
    test "returns normalized tool results from adapters" do
      assert {:ok, %Result{} = result} =
               Tools.invoke(SuccessfulAdapter, %{id: "req_123"}, %{run_id: "run_123"})

      assert result.adapter == SuccessfulAdapter
      assert result.payload == %{request: %{id: "req_123"}}
      assert result.metadata == %{context: %{run_id: "run_123"}}
    end

    test "normalizes invalid adapter responses into tool errors" do
      assert {:error, %Error{} = error} =
               Tools.invoke(InvalidAdapter, %{id: "req_123"}, %{run_id: "run_123"})

      assert error.adapter == InvalidAdapter
      assert error.kind == :adapter_contract
      assert error.retryable? == false
    end
  end

  describe "Error" do
    test "serializes normalized tool errors for step error payloads" do
      error =
        Error.new(
          adapter: SuccessfulAdapter,
          kind: :transport,
          message: "adapter failed",
          details: %{reason: ":closed"},
          retryable?: true
        )

      assert Error.to_map(error) == %{
               adapter: "Jizoku.ToolsTest.SuccessfulAdapter",
               kind: :transport,
               message: "adapter failed",
               details: %{reason: ":closed"},
               retryable?: true
             }
    end
  end
end
