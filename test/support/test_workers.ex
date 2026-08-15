defmodule Jizoku.Test.StepWorker do
  @moduledoc false

  alias Jizoku.Runtime.Runner

  @spec perform(%{args: map()}) :: term()
  def perform(%{args: %{"kind" => _kind} = args}) do
    Runner.perform(args)
  end

  def perform(%{args: args}) do
    {:error, {:invalid_job_args, args}}
  end
end

defmodule Jizoku.Test.CronTriggerWorker do
  @moduledoc false

  alias Jizoku.Runtime.Runner

  @spec perform(%{args: map()}) :: term()
  def perform(%{args: %{"kind" => "cron"} = args}) do
    Runner.perform(args)
  end

  def perform(%{args: %{"workflow" => workflow_name, "trigger" => trigger_name}})
      when is_binary(workflow_name) and is_binary(trigger_name) do
    Runner.start_cron_trigger(workflow_name, trigger_name)
  end

  def perform(%{args: args}) do
    {:error, {:invalid_job_args, args}}
  end
end
