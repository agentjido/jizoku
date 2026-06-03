defmodule BedrockMinimalHostApp.Steps.StartNestedInvite do
  @moduledoc """
  Example parent workflow step that starts a child workflow.

  The retry path is driven by durable attempt number, so the example keeps the
  same behavior after VM restart and does not depend on process-local state.
  """

  use Squidie.Step,
    name: :start_nested_invite,
    description: "Starts an invite delivery child workflow",
    input_schema: [
      party_id: [type: :string, required: true],
      guest_id: [type: :string, required: true],
      child_queue: [type: :string, required: true],
      fail_after_child_start: [type: :boolean, required: false],
      fail_child_once: [type: :boolean, required: false]
    ],
    output_schema: [
      invite_child: [type: :map, required: true]
    ]

  @impl true
  @spec run(map(), Squidie.Step.Context.t()) ::
          {:ok, map()} | {:retry, map()} | {:error, term()}
  def run(
        %{party_id: party_id, guest_id: guest_id, child_queue: child_queue} = input,
        %Squidie.Step.Context{attempt: attempt} = context
      )
      when is_binary(child_queue) do
    child_key = "invite_#{guest_id}"

    with {:ok, child_run} <-
           Squidie.start_child_run(
             context,
             BedrockMinimalHostApp.Workflows.InviteDelivery,
             %{
               party_id: party_id,
               guest_id: guest_id,
               fail_child_once: Map.get(input, :fail_child_once, false)
             },
             child_key: child_key,
             metadata: %{guest_id: guest_id},
             queue: child_queue
           ) do
      maybe_retry_or_complete(input, child_run, child_key, child_queue, attempt)
    end
  end

  defp maybe_retry_or_complete(input, child_run, child_key, child_queue, attempt) do
    if Map.get(input, :fail_after_child_start, false) and attempt == 1 do
      {:retry, %{message: "retry after child start", child_run_id: child_run.run_id}}
    else
      {:ok,
       %{
         invite_child: %{
           run_id: child_run.run_id,
           child_key: child_key,
           queue: child_queue,
           reused_after_retry?: reused_after_retry?(child_run, attempt)
         }
       }}
    end
  end

  defp reused_after_retry?(
         %Squidie.ReadModel.Inspection.Snapshot{parent_run: parent_run},
         attempt
       ) do
    attempt > 1 and is_map(parent_run) and Map.get(parent_run, :attempt) == 1
  end
end
