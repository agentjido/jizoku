defmodule Squidie.Runtime.Journal.EntryBuilder do
  @moduledoc false

  alias Squidie.Runtime.Deadline
  alias Squidie.Runtime.DispatchProtocol
  alias Squidie.Runtime.DispatchProtocol.ActionAttempt
  alias Squidie.Workflow.Definition
  alias Squidie.Workflow.GuardrailRegistry

  @doc """
  Builds a `:runnables_planned` entry and raises on invalid entry data.
  """
  @spec runnables_planned!(String.t(), [map()], DateTime.t()) :: DispatchProtocol.Entry.t()
  def runnables_planned!(run_id, runnables, %DateTime{} = now) do
    entry!(:runnables_planned, runnables_planned_attrs(run_id, runnables, now))
  end

  @doc """
  Builds a `:runnables_planned` entry.
  """
  @spec runnables_planned(String.t(), [map()], DateTime.t()) ::
          {:ok, DispatchProtocol.Entry.t()} | {:error, term()}
  def runnables_planned(run_id, runnables, %DateTime{} = now) do
    DispatchProtocol.new_entry(
      :runnables_planned,
      runnables_planned_attrs(run_id, runnables, now)
    )
  end

  @doc """
  Builds a terminal run entry and raises on invalid entry data.
  """
  @spec run_terminal!(String.t(), atom(), DateTime.t()) :: DispatchProtocol.Entry.t()
  def run_terminal!(run_id, status, %DateTime{} = now) do
    entry!(:run_terminal, run_terminal_attrs(run_id, status, now))
  end

  @doc """
  Builds a failed terminal run entry with an error payload.
  """
  @spec run_terminal!(String.t(), atom(), DateTime.t(), map()) :: DispatchProtocol.Entry.t()
  def run_terminal!(run_id, status, %DateTime{} = now, error) when is_map(error) do
    entry!(:run_terminal, run_terminal_attrs(run_id, status, now, error))
  end

  @doc "Builds a traced terminal run entry and raises on invalid entry data."
  @spec traced_run_terminal!(String.t(), atom(), map() | nil, DateTime.t()) ::
          DispatchProtocol.Entry.t()
  def traced_run_terminal!(run_id, status, trace, %DateTime{} = now) do
    entry!(:run_terminal, traced_run_terminal_attrs(run_id, status, trace, now))
  end

  @doc "Builds a traced failed terminal run entry with an error payload."
  @spec traced_run_terminal!(String.t(), atom(), map() | nil, DateTime.t(), map()) ::
          DispatchProtocol.Entry.t()
  def traced_run_terminal!(run_id, status, trace, %DateTime{} = now, error)
      when is_map(error) do
    entry!(:run_terminal, traced_run_terminal_attrs(run_id, status, trace, now, error))
  end

  @doc false
  @spec runnable_applied(
          ActionAttempt.t(),
          map(),
          map() | nil,
          DateTime.t(),
          keyword(),
          DateTime.t()
        ) :: {:ok, DispatchProtocol.Entry.t()} | {:error, term()}
  def runnable_applied(
        %ActionAttempt{} = attempt,
        result,
        transition,
        %DateTime{} = now,
        execution_opts,
        %DateTime{} = applied_at
      )
      when is_map(result) and is_list(execution_opts) do
    DispatchProtocol.new_entry(:runnable_applied, %{
      run_id: attempt.run_id,
      runnable_key: attempt.runnable_key,
      result: result,
      guardrails: attempt.guardrails,
      execution_opts: execution_opts,
      applied_at: applied_at,
      transition: transition,
      trace: attempt.trace,
      occurred_at: now
    })
  end

  @doc """
  Builds an initial or successor runnable payload for a workflow step.
  """
  @spec runnable(
          Definition.t(),
          String.t(),
          String.t(),
          atom(),
          map(),
          pos_integer(),
          DateTime.t()
        ) ::
          {:ok, map()} | {:error, term()}
  def runnable(definition, run_id, queue, step_name, input, attempt_number, %DateTime{} = now) do
    step = Definition.serialize_step(step_name)
    runnable_key = "#{run_id}:#{step}:#{attempt_number}"

    with {:ok, recovery} <- replay_recovery_policy(definition, step_name),
         {:ok, deadline} <- Deadline.from_definition(definition, step_name, now) do
      runnable =
        put_guardrails(
          runnable_attrs(
            run_id,
            runnable_key,
            attempt_number,
            queue,
            step,
            input,
            recovery,
            now
          ),
          definition,
          step_name
        )

      {:ok, maybe_put(runnable, :deadline, deadline)}
    end
  end

  @doc """
  Builds a retry runnable payload from a failed action attempt.
  """
  @spec retry_runnable(
          Definition.t(),
          atom(),
          Squidie.Runtime.DispatchProtocol.ActionAttempt.t(),
          String.t(),
          pos_integer(),
          String.t(),
          DateTime.t(),
          DateTime.t() | nil
        ) :: {:ok, map()} | {:error, term()}
  def retry_runnable(
        definition,
        step_name,
        attempt,
        runnable_key,
        attempt_number,
        queue,
        %DateTime{} = visible_at,
        deadline
      ) do
    with {:ok, recovery} <- replay_recovery_policy(definition, step_name) do
      runnable =
        put_guardrails(
          runnable_attrs(
            attempt.run_id,
            runnable_key,
            attempt_number,
            queue,
            Definition.serialize_step(step_name),
            attempt.input || %{},
            recovery,
            visible_at
          ),
          definition,
          step_name
        )

      {:ok, maybe_put(runnable, :deadline, deadline)}
    end
  end

  defp replay_recovery_policy(definition, step_name) do
    with {:ok, recovery} <- Definition.step_recovery_policy(definition, step_name) do
      {:ok, Definition.serialize_recovery_policy(recovery)}
    end
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp runnables_planned_attrs(run_id, runnables, %DateTime{} = now) do
    Map.new(run_id: run_id, runnables: runnables, occurred_at: now)
  end

  defp run_terminal_attrs(run_id, status, %DateTime{} = now) do
    Map.new(run_id: run_id, status: status, occurred_at: now)
  end

  defp run_terminal_attrs(run_id, status, %DateTime{} = now, error) when is_map(error) do
    Map.new(run_id: run_id, status: status, error: error, occurred_at: now)
  end

  defp traced_run_terminal_attrs(run_id, status, trace, %DateTime{} = now) do
    Map.new(run_id: run_id, status: status, trace: trace, occurred_at: now)
  end

  defp traced_run_terminal_attrs(run_id, status, trace, %DateTime{} = now, error)
       when is_map(error) do
    Map.new(run_id: run_id, status: status, trace: trace, error: error, occurred_at: now)
  end

  defp runnable_attrs(
         run_id,
         runnable_key,
         attempt_number,
         queue,
         step,
         input,
         recovery,
         %DateTime{} = visible_at
       ) do
    Map.new(
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: attempt_number,
      queue: queue,
      step: step,
      input: input,
      recovery: recovery,
      visible_at: visible_at
    )
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp put_guardrails(runnable, definition, step_name) do
    case Definition.step(definition, step_name) do
      {:ok, step} ->
        case GuardrailRegistry.public_step_guardrails(step) do
          [] -> runnable
          guardrails -> Map.put(runnable, :guardrails, guardrails)
        end

      {:error, _reason} ->
        runnable
    end
  end
end
