defmodule Jizoku.Runtime.TraceTest do
  use ExUnit.Case, async: true

  alias Jizoku.Runtime.Trace

  @trace_id "4bf92f3577b34da6a3ce929d0e0e4736"
  @span_id "00f067aa0ba902b7"

  test "creates valid root and child trace maps" do
    assert {:ok, root} = Trace.new_root(tracestate: "vendor=value")
    assert Trace.valid?(root)
    assert byte_size(root.trace_id) == 32
    assert byte_size(root.span_id) == 16
    assert root.tracestate == "vendor=value"

    assert {:ok, child} = Trace.child_of(root, "signal-123")
    assert child.trace_id == root.trace_id
    assert child.span_id != root.span_id
    assert child.parent_span_id == root.span_id
    assert child.causation_id == "signal-123"
    assert child.tracestate == root.tracestate
  end

  test "normalizes string-key traces to version-tolerant atom-key maps" do
    assert {:ok,
            %{
              trace_id: @trace_id,
              span_id: @span_id,
              parent_span_id: "b7ad6b7169203331",
              causation_id: "signal-123",
              tracestate: "vendor=value"
            }} =
             Trace.normalize(%{
               "trace_id" => @trace_id,
               "span_id" => @span_id,
               "parent_span_id" => "b7ad6b7169203331",
               "causation_id" => "signal-123",
               "tracestate" => "vendor=value"
             })
  end

  test "rejects malformed, all-zero, ambiguous, and unbounded trace data safely" do
    invalid = [
      {%{trace_id: String.upcase(@trace_id), span_id: @span_id}, {:trace_id, :invalid}},
      {%{trace_id: String.duplicate("0", 32), span_id: @span_id}, {:trace_id, :invalid}},
      {%{trace_id: @trace_id, span_id: String.duplicate("0", 16)}, {:span_id, :invalid}},
      {%{trace_id: @trace_id, span_id: @span_id, parent_span_id: "bad"},
       {:parent_span_id, :invalid}},
      {%{trace_id: @trace_id, span_id: @span_id, causation_id: String.duplicate("x", 256)},
       {:causation_id, :too_long}},
      {%{trace_id: @trace_id, span_id: @span_id, tracestate: String.duplicate("x", 513)},
       {:tracestate, :too_long}},
      {%{"trace_id" => String.duplicate("1", 32), trace_id: @trace_id, span_id: @span_id},
       {:trace_id, :ambiguous}},
      {%{trace_id: @trace_id, span_id: @span_id, secret: "do-not-echo"}, {:keys, :unsupported}}
    ]

    for {trace, reason} <- invalid do
      assert {:error, {:invalid_trace, ^reason}} = Trace.normalize(trace)
    end

    refute inspect(Trace.normalize(%{trace_id: "do-not-echo", span_id: @span_id})) =~
             "do-not-echo"
  end

  test "rejects invalid constructor options without raising" do
    assert {:error, {:invalid_trace, {:options, :expected_keyword}}} = Trace.new_root(%{})

    assert {:error, {:invalid_trace, {:causation_id, :expected_non_empty_string}}} =
             Trace.new_root(causation_id: "")

    assert {:error, {:invalid_trace, {:trace, :invalid}}} =
             Trace.child_of(%{trace_id: "bad", span_id: @span_id}, "signal-123")
  end
end
