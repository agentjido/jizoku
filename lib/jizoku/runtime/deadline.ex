# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Runtime.Deadline do
  @moduledoc false

  alias Jizoku.Workflow.Definition

  @escalation_outcomes [:diagnostic, :operator_action, :workflow_step, :host_callback]
  @escalation_outcome_strings Map.new(@escalation_outcomes, fn outcome ->
                                {Atom.to_string(outcome), outcome}
                              end)
  @deadline_keys %{
    "policy" => :policy,
    "started_at" => :started_at,
    "due_at" => :due_at,
    "due_soon_at" => :due_soon_at,
    "escalated_at" => :escalated_at,
    "within" => :within,
    "due_soon" => :due_soon,
    "escalate_after" => :escalate_after,
    "escalation" => :escalation,
    "outcome" => :outcome,
    "target" => :target
  }

  @type policy :: %{
          required(:within) => pos_integer(),
          optional(:due_soon) => non_neg_integer(),
          optional(:escalate_after) => non_neg_integer(),
          required(:escalation) => %{required(:outcome) => atom(), optional(:target) => term()}
        }

  @doc false
  @spec from_definition(Definition.t(), atom(), DateTime.t()) ::
          {:ok, map() | nil} | {:error, {:invalid_deadline, term()} | {:unknown_step, atom()}}
  def from_definition(definition, step_name, %DateTime{} = started_at) when is_atom(step_name) do
    with {:ok, %{opts: opts}} <- Definition.step(definition, step_name),
         {:ok, policy} <- policy_from_opts(opts) do
      {:ok, from_policy(policy, started_at)}
    end
  end

  @doc false
  @spec policy_from_opts(keyword()) ::
          {:ok, policy() | nil} | {:error, {:invalid_deadline, term()}}
  def policy_from_opts(opts) when is_list(opts) do
    case Keyword.fetch(opts, :deadline) do
      {:ok, deadline} -> normalize_policy(deadline)
      :error -> {:ok, nil}
    end
  end

  @doc false
  @spec normalize_policy(term()) :: {:ok, policy()} | {:error, {:invalid_deadline, term()}}
  def normalize_policy(policy) when is_list(policy) do
    if Keyword.keyword?(policy) do
      policy
      |> Map.new()
      |> normalize_policy()
    else
      {:error, {:invalid_deadline, policy}}
    end
  end

  def normalize_policy(policy) when is_map(policy) do
    policy = Map.new(policy, fn {key, value} -> {normalize_key(key), value} end)

    with {:ok, within} <- positive_integer(policy, :within),
         {:ok, due_soon} <- optional_non_negative_integer(policy, :due_soon),
         {:ok, escalate_after} <- optional_non_negative_integer(policy, :escalate_after),
         :ok <- validate_due_soon(due_soon, within),
         {:ok, escalation} <- normalize_escalation(Map.get(policy, :escalation, :diagnostic)) do
      {:ok,
       %{
         within: within,
         due_soon: due_soon,
         escalate_after: escalate_after,
         escalation: escalation
       }}
    else
      {:error, reason} -> {:error, {:invalid_deadline, reason}}
    end
  end

  def normalize_policy(policy), do: {:error, {:invalid_deadline, policy}}

  @doc false
  @spec from_policy(policy() | nil, DateTime.t()) :: map() | nil
  def from_policy(nil, %DateTime{}), do: nil

  def from_policy(policy, %DateTime{} = started_at) when is_map(policy) do
    due_at = DateTime.add(started_at, Map.fetch!(policy, :within), :millisecond)
    due_soon = Map.get(policy, :due_soon)
    escalate_after = Map.get(policy, :escalate_after)

    compact(%{
      policy: policy,
      started_at: started_at,
      due_at: due_at,
      due_soon_at: maybe_add(due_at, due_soon, -1),
      escalated_at: maybe_add(due_at, escalate_after, 1)
    })
  end

  @doc false
  @spec evaluate(term(), DateTime.t(), keyword()) :: map() | nil
  def evaluate(deadline, now, opts \\ [])

  def evaluate(nil, %DateTime{}, _opts), do: nil

  def evaluate(deadline, %DateTime{} = now, opts) when is_map(deadline) do
    deadline = normalize_deadline_map(deadline)
    due_at = Map.get(deadline, :due_at)

    if match?(%DateTime{}, due_at) do
      deadline
      |> Map.merge(%{
        status: status(deadline, now),
        overdue?: not before?(now, due_at),
        remaining_ms: DateTime.diff(due_at, now, :millisecond)
      })
      |> maybe_put(:step, Keyword.get(opts, :step))
      |> maybe_put(:runnable_key, Keyword.get(opts, :runnable_key))
      |> maybe_put(:escalation, escalation(deadline))
      |> compact()
    else
      nil
    end
  end

  def evaluate(_deadline, %DateTime{}, _opts), do: nil

  @doc false
  @spec most_urgent([map() | nil]) :: map() | nil
  def most_urgent(deadlines) when is_list(deadlines) do
    deadlines
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(&urgency_sort_key/1)
    |> List.first()
  end

  @doc false
  @spec public_summary(term()) :: map() | nil
  def public_summary(deadline) when is_map(deadline) do
    deadline
    |> normalize_deadline_map()
    |> Map.take([
      :status,
      :overdue?,
      :remaining_ms,
      :started_at,
      :due_at,
      :due_soon_at,
      :escalated_at,
      :step,
      :runnable_key
    ])
    |> maybe_put(:escalation, public_escalation(escalation(normalize_deadline_map(deadline))))
    |> compact()
  end

  def public_summary(_deadline), do: nil

  defp normalize_deadline_map(deadline) do
    deadline
    |> Map.new(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.update(:policy, %{}, &normalize_policy_map/1)
  end

  defp normalize_policy_map(policy) when is_map(policy) do
    policy
    |> Map.new(fn {key, value} -> {normalize_key(key), value} end)
    |> Map.update(:escalation, %{outcome: :diagnostic}, fn escalation ->
      case normalize_escalation(escalation) do
        {:ok, normalized} -> normalized
        {:error, _reason} -> %{outcome: :diagnostic}
      end
    end)
  end

  defp normalize_policy_map(_policy), do: %{}

  defp status(deadline, now) do
    cond do
      at_or_after?(now, Map.get(deadline, :escalated_at)) -> :escalated
      at_or_after?(now, Map.get(deadline, :due_at)) -> :overdue
      at_or_after?(now, Map.get(deadline, :due_soon_at)) -> :due_soon
      true -> :on_time
    end
  end

  defp escalation(deadline) do
    deadline
    |> Map.get(:policy, %{})
    |> Map.get(:escalation)
  end

  defp urgency_sort_key(deadline) do
    status_rank =
      case Map.get(deadline, :status) do
        :escalated -> 0
        :overdue -> 1
        :due_soon -> 2
        :on_time -> 3
        _other -> 4
      end

    due_at =
      case Map.get(deadline, :due_at) do
        %DateTime{} = due_at -> DateTime.to_unix(due_at, :microsecond)
        _missing -> 9_223_372_036_854_775_807
      end

    {status_rank, due_at}
  end

  defp positive_integer(policy, key) do
    case Map.get(policy, key) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {key, value}}
    end
  end

  defp optional_non_negative_integer(policy, key) do
    case Map.get(policy, key) do
      nil -> {:ok, nil}
      value when is_integer(value) and value >= 0 -> {:ok, value}
      value -> {:error, {key, value}}
    end
  end

  defp validate_due_soon(nil, _within), do: :ok
  defp validate_due_soon(due_soon, within) when due_soon < within, do: :ok
  defp validate_due_soon(due_soon, _within), do: {:error, {:due_soon, due_soon}}

  defp normalize_escalation(outcome) when outcome in @escalation_outcomes do
    {:ok, %{outcome: outcome}}
  end

  defp normalize_escalation(outcome) when is_binary(outcome) do
    case Map.fetch(@escalation_outcome_strings, outcome) do
      {:ok, outcome} -> {:ok, %{outcome: outcome}}
      :error -> {:error, {:escalation, outcome}}
    end
  end

  defp normalize_escalation(%{} = escalation) do
    escalation = Map.new(escalation, fn {key, value} -> {normalize_key(key), value} end)

    case normalize_escalation(Map.get(escalation, :outcome)) do
      {:ok, %{outcome: outcome}} ->
        {:ok, maybe_put(%{outcome: outcome}, :target, Map.get(escalation, :target))}

      {:error, _reason} ->
        {:error, {:escalation, escalation}}
    end
  end

  defp normalize_escalation(escalation), do: {:error, {:escalation, escalation}}

  defp maybe_add(_datetime, nil, _direction), do: nil

  defp maybe_add(%DateTime{} = datetime, amount, direction),
    do: DateTime.add(datetime, amount * direction, :millisecond)

  defp at_or_after?(%DateTime{} = now, %DateTime{} = deadline) do
    DateTime.compare(now, deadline) in [:eq, :gt]
  end

  defp at_or_after?(%DateTime{}, _deadline), do: false

  defp before?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.compare(left, right) == :lt
  end

  defp normalize_key(key) when is_binary(key) do
    Map.get(@deadline_keys, key, key)
  end

  defp normalize_key(key), do: key

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp public_escalation(%{outcome: outcome}), do: %{outcome: outcome}
  defp public_escalation(_escalation), do: nil

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
