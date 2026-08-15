defmodule Squidie.Runtime.Jido.ResultEnvelopeTest do
  use ExUnit.Case, async: true

  alias Squidie.Runtime.Jido.ResultEnvelope

  test "decodes only a fingerprinted envelope with the dedicated completion encoding" do
    envelope =
      ResultEnvelope.wrap_run_instruction(
        %{accepted: true},
        %{"dynamic_work" => %{dynamic_key: "one"}, "runnables" => [%{step: "one"}]}
      )

    encoding = ResultEnvelope.completion_encoding()

    assert {:ok, {:run_instruction, %{accepted: true}, _plan}} =
             ResultEnvelope.decode(envelope, encoding)

    changed_output =
      put_in(
        envelope,
        ["__squidie_jido_result__", "output"],
        %{accepted: false, secret: "output-secret"}
      )

    changed_plan =
      put_in(
        envelope,
        ["__squidie_jido_result__", "plan", "runnables"],
        [%{step: "changed", secret: "plan-secret"}]
      )

    for changed <- [changed_output, changed_plan] do
      assert {:error, :malformed_jido_result_envelope} =
               ResultEnvelope.decode(changed, encoding)

      assert ResultEnvelope.public_result(changed, encoding) == nil
    end
  end

  test "unmarked application output remains ordinary regardless of its keys" do
    output = %{
      "__squidie_jido_result__" => %{
        "effect" => "run_instruction",
        "version" => 1
      }
    }

    assert {:ok, {:ordinary, ^output}} = ResultEnvelope.decode(output, nil)
    assert ResultEnvelope.public_result(output, nil) == output

    unsupported = %{"effect" => "run_instruction", "version" => 2}
    assert ResultEnvelope.native_effect?(unsupported)

    assert {:error, :malformed_jido_result_envelope} =
             ResultEnvelope.decode(output, unsupported)

    assert ResultEnvelope.public_result(output, unsupported) == nil
  end
end
