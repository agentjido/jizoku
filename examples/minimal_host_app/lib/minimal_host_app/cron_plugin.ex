defmodule MinimalHostApp.CronPlugin do
  @moduledoc """
  Host-owned Oban cron plugin for the example app's Jizoku workflows.
  """

  @behaviour Oban.Plugin

  use Supervisor

  alias MinimalHostApp.JizokuDeliveryAdapter
  alias MinimalHostApp.Workers.JizokuWorker
  alias Jizoku.Executor.Payload
  alias Jizoku.Workflow.Definition, as: WorkflowDefinition

  @type option ::
          Oban.Plugin.option()
          | {:workflows, [module()]}

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts), do: super(opts)

  @impl Oban.Plugin
  @spec start_link([option()]) :: Supervisor.on_start()
  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl Oban.Plugin
  @spec validate([option()]) :: :ok | {:error, String.t()}
  def validate(opts) do
    workflows = Keyword.get(opts, :workflows)

    cond do
      not is_list(workflows) or workflows == [] ->
        {:error, "expected :workflows to be a non-empty list"}

      not Enum.all?(workflows, &is_atom/1) ->
        {:error, "expected :workflows to contain only workflow modules"}

      true ->
        validate_workflows(workflows)
    end
  end

  @spec evaluate(Supervisor.supervisor()) :: :ok
  def evaluate(plugin) do
    plugin
    |> Supervisor.which_children()
    |> Enum.each(fn
      {_id, pid, _type, _modules} when is_pid(pid) -> send(pid, :evaluate)
      _other -> :ok
    end)

    :ok
  end

  @impl Supervisor
  def init(opts) do
    conf = Keyword.fetch!(opts, :conf)
    workflows = Keyword.fetch!(opts, :workflows)
    reboot_activation_id = reboot_activation_id()

    children =
      workflows
      |> build_crontabs(JizokuDeliveryAdapter.queue(), reboot_activation_id)
      |> Enum.map(fn {timezone, crontab} ->
        opts = [conf: conf, crontab: crontab, timezone: timezone]
        Supervisor.child_spec({Oban.Plugins.Cron, opts}, id: {:cron, timezone})
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp validate_workflows(workflows) do
    case Enum.reduce_while(workflows, :ok, &validate_workflow/2) do
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_workflow(workflow, :ok) do
    with {:ok, definition} <- WorkflowDefinition.load(workflow) do
      case cron_triggers(definition) do
        [] ->
          {:halt, {:error, "workflow #{inspect(workflow)} must define a cron trigger"}}

        triggers ->
          validate_cron_triggers(workflow, triggers)
      end
    else
      {:error, {:invalid_workflow, _reason}} ->
        {:halt, {:error, "invalid workflow #{inspect(workflow)}"}}
    end
  end

  defp validate_cron_triggers(workflow, triggers) do
    case Enum.reduce_while(triggers, :ok, &validate_cron_trigger(workflow, &1, &2)) do
      :ok -> {:cont, :ok}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp validate_cron_trigger(workflow, trigger, :ok) do
    %{config: %{expression: expression, timezone: timezone}} = trigger

    with {:ok, _payload} <- WorkflowDefinition.resolve_payload(trigger, %{}),
         :ok <- validate_crontab_entry(workflow, trigger, expression, timezone) do
      {:cont, :ok}
    else
      {:error, {:invalid_payload, _details}} ->
        {:halt,
         {:error, "cron workflow #{inspect(workflow)} must resolve its payload from defaults"}}

      {:error, :requires_dynamic_schedule_identity} ->
        {:halt,
         {:error,
          "cron workflow #{inspect(workflow)} must provide dynamic schedule identity for idempotent recurring triggers"}}

      _other ->
        {:halt, {:error, "workflow #{inspect(workflow)} must define one valid cron trigger"}}
    end
  end

  defp validate_crontab_entry(workflow, trigger, expression, timezone) do
    with {:ok, payload} <- cron_payload(workflow, trigger, "validation") do
      opts = [args: payload]

      case Oban.Plugins.Cron.validate(
             crontab: [{expression, JizokuWorker, opts}],
             timezone: timezone
           ) do
        :ok -> :ok
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp build_crontabs(workflows, queue, reboot_activation_id) do
    workflows
    |> Enum.flat_map(&build_entries(&1, queue, reboot_activation_id))
    |> Enum.group_by(fn {timezone, _entry} -> timezone end, fn {_timezone, entry} -> entry end)
    |> Enum.sort_by(fn {timezone, _entries} -> timezone end)
  end

  defp build_entries(workflow, queue, reboot_activation_id) do
    {:ok, definition} = WorkflowDefinition.load(workflow)

    Enum.map(cron_triggers(definition), fn trigger ->
      {:ok, payload} = cron_payload(workflow, trigger, reboot_activation_id)

      entry = {
        trigger.config.expression,
        JizokuWorker,
        [args: payload, queue: queue]
      }

      {trigger.config.timezone, entry}
    end)
  end

  defp cron_triggers(definition) do
    Enum.filter(definition.triggers, &(&1.type == :cron))
  end

  defp cron_payload(
         workflow,
         %{name: trigger_name, config: %{idempotency: idempotency}} = trigger,
         reboot_activation_id
       )
       when idempotency in [:return_existing_run, :skip_duplicate] do
    case trigger.config.expression do
      "@reboot" ->
        {:ok,
         Payload.cron(workflow, trigger_name,
           signal_id: reboot_signal_id(workflow, trigger, reboot_activation_id)
         )}

      _recurring_expression ->
        {:error, :requires_dynamic_schedule_identity}
    end
  end

  defp cron_payload(workflow, %{name: trigger_name}, _reboot_activation_id) do
    {:ok, Payload.cron(workflow, trigger_name)}
  end

  defp reboot_activation_id do
    16
    |> :crypto.strong_rand_bytes()
    |> Base.url_encode64(padding: false)
  end

  defp reboot_signal_id(workflow, trigger, reboot_activation_id) do
    workflow_name = WorkflowDefinition.serialize_workflow(workflow)
    trigger_name = WorkflowDefinition.serialize_trigger(trigger.name)
    "minimal-host-app:reboot:#{reboot_activation_id}:#{workflow_name}:#{trigger_name}"
  end
end
