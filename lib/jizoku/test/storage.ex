# credo:disable-for-this-file ExSlop.Check.Readability.DocFalseOnPublicFunction
defmodule Jizoku.Test.Storage do
  @moduledoc false

  use GenServer

  @behaviour Jido.Storage

  alias Jido.Thread
  alias Jizoku.Runtime.Journal.Storage.Metadata

  @type option :: {:server, pid()}

  @spec start_link(pid(), DateTime.t(), map()) :: GenServer.on_start()
  def start_link(owner, %DateTime{} = now, action_stubs \\ %{})
      when is_pid(owner) and is_map(action_stubs) do
    GenServer.start_link(__MODULE__, {owner, now, action_stubs})
  end

  @spec stop(pid()) :: :ok
  def stop(server) when is_pid(server) do
    GenServer.stop(server, :normal)
    :ok
  catch
    :exit, _reason -> :ok
  end

  @spec reserve_start(pid()) ::
          :ok | {:error, :runtime_already_started | :runtime_stopped}
  def reserve_start(server) when is_pid(server) do
    safe_call(server, :reserve_start)
  end

  @spec commit_start(pid(), Ecto.UUID.t()) :: :ok | {:error, term()}
  def commit_start(server, run_id) when is_pid(server) and is_binary(run_id) do
    safe_call(server, {:commit_start, run_id})
  end

  @spec release_start(pid()) :: :ok | {:error, term()}
  def release_start(server) when is_pid(server) do
    safe_call(server, :release_start)
  end

  @spec root_run_id(pid()) :: {:ok, Ecto.UUID.t()} | {:error, :run_not_started | :runtime_stopped}
  def root_run_id(server) when is_pid(server) do
    safe_call(server, :root_run_id)
  end

  @doc false
  @spec now(pid()) :: {:ok, DateTime.t()} | {:error, :runtime_stopped}
  def now(server) when is_pid(server) do
    safe_call(server, :now)
  end

  @doc false
  @spec begin_execution(pid()) ::
          {:ok, DateTime.t(), reference()} | {:error, :runtime_stopped}
  def begin_execution(server) when is_pid(server) do
    safe_call(server, :begin_execution)
  end

  @doc false
  @spec end_execution(pid(), reference()) :: :ok | {:error, term()}
  def end_execution(server, lease_ref) when is_pid(server) and is_reference(lease_ref) do
    safe_call(server, {:end_execution, lease_ref})
  end

  @doc false
  @spec advance_time(pid(), non_neg_integer(), :second | :millisecond | :microsecond) ::
          {:ok, DateTime.t()}
          | {:error, :runtime_busy | :runtime_owner_required | :runtime_stopped}
  def advance_time(server, amount, unit)
      when is_pid(server) and is_integer(amount) and amount >= 0 do
    safe_call(server, {:advance_time, amount, unit})
  end

  @doc false
  @spec delete_checkpoints(pid()) ::
          :ok | {:error, :runtime_busy | :runtime_owner_required | :runtime_stopped}
  def delete_checkpoints(server) when is_pid(server) do
    safe_call(server, :delete_checkpoints)
  end

  @doc false
  @spec inject_append_conflict(pid(), String.t()) ::
          :ok
          | {:error,
             :append_conflict_already_armed
             | :runtime_busy
             | :runtime_owner_required
             | :runtime_stopped}
  def inject_append_conflict(server, thread_id)
      when is_pid(server) and is_binary(thread_id) do
    safe_call(server, {:inject_append_conflict, thread_id})
  end

  @doc false
  @spec restart(pid()) ::
          {:ok, pid()}
          | {:error, :runtime_busy | :runtime_owner_required | :runtime_stopped | term()}
  def restart(server) when is_pid(server) do
    safe_call(server, :restart)
  end

  @doc false
  @spec consume_action_stub(pid(), term(), term(), map()) ::
          {:ok, Jizoku.Step.result()} | {:error, term()}
  def consume_action_stub(server, action_key, invocation_key, call)
      when is_pid(server) and is_map(call) do
    safe_call(server, {:consume_action_stub, action_key, invocation_key, call})
  end

  @doc false
  @spec action_stub_calls(pid(), term()) :: {:ok, [map()]} | {:error, term()}
  def action_stub_calls(server, action_key) when is_pid(server) do
    safe_call(server, {:action_stub_calls, action_key})
  end

  @doc false
  @spec put_append_fault(pid(), String.t(), {:error, term()} | {:raise, Exception.t()}) ::
          :ok | {:error, :runtime_stopped}
  def put_append_fault(server, thread_prefix, action)
      when is_pid(server) and is_binary(thread_prefix) do
    safe_call(server, {:put_append_fault, thread_prefix, action})
  end

  @impl Jido.Storage
  def get_checkpoint(key, opts) do
    with :ok <- validate_durable(key) do
      call(opts, {:get_checkpoint, key})
    end
  end

  @impl Jido.Storage
  def put_checkpoint(key, data, opts) do
    with :ok <- validate_durable(key),
         :ok <- validate_durable(data) do
      call(opts, {:put_checkpoint, key, data})
    end
  end

  @impl Jido.Storage
  def delete_checkpoint(key, opts) do
    with :ok <- validate_durable(key) do
      call(opts, {:delete_checkpoint, key})
    end
  end

  @impl Jido.Storage
  def load_thread(thread_id, opts) do
    call(opts, {:load_thread, thread_id})
  end

  @impl Jido.Storage
  def append_thread(thread_id, entries, opts) do
    with :ok <- validate_durable(entries),
         {:ok, metadata} <- Metadata.normalize(Keyword.get(opts, :metadata, %{})) do
      opts = Keyword.put(opts, :metadata, metadata)

      case call(opts, {:append_thread, thread_id, entries, opts}) do
        {:raise, exception} -> raise exception
        result -> result
      end
    end
  end

  @impl Jido.Storage
  def delete_thread(thread_id, opts) do
    call(opts, {:delete_thread, thread_id})
  end

  @impl GenServer
  def init({owner, %DateTime{} = now, action_stubs}) when is_map(action_stubs) do
    {:ok, initial_state(owner, now, action_stubs)}
  end

  def init({:restore, owner, persisted_state}) when is_pid(owner) and is_map(persisted_state) do
    state =
      owner
      |> initial_state(Map.fetch!(persisted_state, :now), %{})
      |> Map.merge(persisted_state)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:get_checkpoint, key}, _from, state) do
    reply =
      case Map.fetch(state.checkpoints, key) do
        {:ok, data} -> {:ok, data}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:put_checkpoint, key, data}, _from, state) do
    {:reply, :ok, put_in(state, [:checkpoints, key], data)}
  end

  def handle_call({:delete_checkpoint, key}, _from, state) do
    {:reply, :ok, %{state | checkpoints: Map.delete(state.checkpoints, key)}}
  end

  def handle_call({:load_thread, thread_id}, _from, state) do
    reply =
      case Map.fetch(state.threads, thread_id) do
        {:ok, thread} -> {:ok, thread}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:append_thread, thread_id, entries, opts}, _from, state) do
    case consume_append_conflict(state, thread_id) do
      {:conflict, state} ->
        {:reply, {:error, :conflict}, state}

      {:continue, state} ->
        if append_fault?(state.append_fault, thread_id) do
          {_prefix, action} = state.append_fault
          {:reply, action, state}
        else
          append_thread(state, thread_id, entries, opts)
        end
    end
  end

  def handle_call({:delete_thread, thread_id}, _from, state) do
    {:reply, :ok, %{state | threads: Map.delete(state.threads, thread_id)}}
  end

  def handle_call(
        :reserve_start,
        {caller, _tag},
        %{root_run_id: nil, start_reservation: nil} = state
      ) do
    reservation_ref = Process.monitor(caller)
    {:reply, :ok, %{state | start_reservation: {caller, reservation_ref}}}
  end

  def handle_call(
        :reserve_start,
        {caller, _tag},
        %{root_run_id: nil, start_reservation: {previous_caller, previous_ref}} = state
      ) do
    if Process.alive?(previous_caller) do
      {:reply, {:error, :runtime_already_started}, state}
    else
      Process.demonitor(previous_ref, [:flush])
      reservation_ref = Process.monitor(caller)
      {:reply, :ok, %{state | start_reservation: {caller, reservation_ref}}}
    end
  end

  def handle_call(:reserve_start, _from, state) do
    {:reply, {:error, :runtime_already_started}, state}
  end

  def handle_call(
        {:commit_start, run_id},
        {caller, _tag},
        %{start_reservation: {caller, reservation_ref}} = state
      ) do
    Process.demonitor(reservation_ref, [:flush])
    {:reply, :ok, %{state | root_run_id: run_id, start_reservation: nil}}
  end

  def handle_call({:commit_start, _run_id}, _from, state) do
    {:reply, {:error, :start_not_reserved}, state}
  end

  def handle_call(:release_start, {caller, _tag}, %{start_reservation: {caller, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:reply, :ok, %{state | start_reservation: nil}}
  end

  def handle_call(:release_start, _from, state) do
    {:reply, {:error, :start_not_reserved}, state}
  end

  def handle_call(:root_run_id, _from, %{root_run_id: nil} = state) do
    {:reply, {:error, :run_not_started}, state}
  end

  def handle_call(:root_run_id, _from, state) do
    {:reply, {:ok, state.root_run_id}, state}
  end

  def handle_call({:action_stub_calls, _action_key}, _from, %{root_run_id: nil} = state) do
    {:reply, {:error, :run_not_started}, state}
  end

  def handle_call({:action_stub_calls, action_key}, _from, state) do
    case Map.fetch(state.action_stubs, action_key) do
      {:ok, stub} -> {:reply, {:ok, Enum.reverse(stub.calls)}, state}
      :error -> {:reply, {:error, :unknown_action_stub}, state}
    end
  end

  def handle_call({:consume_action_stub, action_key, invocation_key, call}, _from, state) do
    case consume_stub_result(state.action_stubs, action_key, invocation_key, call) do
      {:ok, result, action_stubs} ->
        {:reply, {:ok, result}, %{state | action_stubs: action_stubs}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:now, _from, state) do
    {:reply, {:ok, state.now}, state}
  end

  def handle_call(:begin_execution, {caller, _tag}, state) do
    lease_ref = make_ref()
    monitor_ref = Process.monitor(caller)

    state =
      state
      |> put_in([:execution_leases, lease_ref], {caller, monitor_ref})
      |> put_in([:execution_monitors, monitor_ref], lease_ref)

    {:reply, {:ok, state.now, lease_ref}, state}
  end

  def handle_call({:end_execution, lease_ref}, {caller, _tag}, state) do
    case Map.get(state.execution_leases, lease_ref) do
      {^caller, monitor_ref} ->
        Process.demonitor(monitor_ref, [:flush])

        state = %{
          state
          | execution_leases: Map.delete(state.execution_leases, lease_ref),
            execution_monitors: Map.delete(state.execution_monitors, monitor_ref)
        }

        {:reply, :ok, state}

      _missing_or_unowned ->
        {:reply, {:error, :execution_lease_not_owned}, state}
    end
  end

  def handle_call(
        {:advance_time, amount, unit},
        {owner, _tag},
        %{owner: owner} = state
      ) do
    state = drop_dead_execution_leases(state)

    if map_size(state.execution_leases) == 0 do
      now = DateTime.add(state.now, amount, unit)
      {:reply, {:ok, now}, %{state | now: now}}
    else
      {:reply, {:error, :runtime_busy}, state}
    end
  end

  def handle_call({:advance_time, _amount, _unit}, _from, state) do
    {:reply, {:error, :runtime_owner_required}, state}
  end

  def handle_call(:delete_checkpoints, {owner, _tag}, %{owner: owner} = state) do
    state = drop_dead_execution_leases(state)

    if map_size(state.execution_leases) == 0 do
      {:reply, :ok, %{state | checkpoints: %{}}}
    else
      {:reply, {:error, :runtime_busy}, state}
    end
  end

  def handle_call(:delete_checkpoints, _from, state) do
    {:reply, {:error, :runtime_owner_required}, state}
  end

  def handle_call(
        {:inject_append_conflict, thread_id},
        {owner, _tag},
        %{owner: owner} = state
      ) do
    state = drop_dead_execution_leases(state)

    cond do
      map_size(state.execution_leases) > 0 ->
        {:reply, {:error, :runtime_busy}, state}

      not is_nil(state.next_append_conflict) ->
        {:reply, {:error, :append_conflict_already_armed}, state}

      true ->
        {:reply, :ok, %{state | next_append_conflict: thread_id}}
    end
  end

  def handle_call({:inject_append_conflict, _thread_id}, _from, state) do
    {:reply, {:error, :runtime_owner_required}, state}
  end

  def handle_call(:restart, {owner, _tag}, %{owner: owner} = state) do
    state = drop_dead_execution_leases(state)

    if restart_busy?(state) do
      {:reply, {:error, :runtime_busy}, state}
    else
      case GenServer.start(__MODULE__, {:restore, owner, restart_state(state)}) do
        {:ok, restarted_server} ->
          {:stop, :normal, {:ok, restarted_server}, state}

        {:error, _reason} = error ->
          {:reply, error, state}
      end
    end
  end

  def handle_call(:restart, _from, state) do
    {:reply, {:error, :runtime_owner_required}, state}
  end

  def handle_call({:put_append_fault, thread_prefix, action}, _from, state) do
    {:reply, :ok, %{state | append_fault: {thread_prefix, action}}}
  end

  @impl GenServer
  def handle_info({:DOWN, owner_ref, :process, _owner, _reason}, %{owner_ref: owner_ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info(
        {:DOWN, reservation_ref, :process, caller, _reason},
        %{start_reservation: {caller, reservation_ref}} = state
      ) do
    {:noreply, %{state | start_reservation: nil}}
  end

  def handle_info({:DOWN, monitor_ref, :process, _caller, _reason}, state) do
    case Map.pop(state.execution_monitors, monitor_ref) do
      {nil, _monitors} ->
        {:noreply, state}

      {lease_ref, monitors} ->
        {:noreply,
         %{
           state
           | execution_leases: Map.delete(state.execution_leases, lease_ref),
             execution_monitors: monitors
         }}
    end
  end

  def handle_info(_message, state) do
    {:noreply, state}
  end

  defp append(nil, thread_id, entries, opts) do
    thread =
      Thread.new(
        id: thread_id,
        metadata: Keyword.get(opts, :metadata, %{})
      )

    Thread.append(thread, entries)
  end

  defp append(%Thread{} = thread, _thread_id, entries, _opts) do
    Thread.append(thread, entries)
  end

  defp initial_state(owner, now, action_stubs) do
    %{
      owner: owner,
      owner_ref: Process.monitor(owner),
      now: now,
      execution_leases: %{},
      execution_monitors: %{},
      checkpoints: %{},
      threads: %{},
      root_run_id: nil,
      start_reservation: nil,
      next_append_conflict: nil,
      append_conflict_counts: %{},
      append_fault: nil,
      action_stubs: action_stub_state(action_stubs)
    }
  end

  defp restart_busy?(state) do
    map_size(state.execution_leases) > 0 or
      not is_nil(state.start_reservation)
  end

  defp restart_state(state) do
    Map.take(state, [
      :append_fault,
      :append_conflict_counts,
      :action_stubs,
      :checkpoints,
      :next_append_conflict,
      :now,
      :root_run_id,
      :threads
    ])
  end

  defp action_stub_state(action_stubs) do
    Map.new(action_stubs, fn {action_key, results} ->
      {action_key, %{assignments: %{}, calls: [], remaining: results}}
    end)
  end

  defp consume_stub_result(action_stubs, action_key, invocation_key, call) do
    case Map.fetch(action_stubs, action_key) do
      {:ok, stub} ->
        replay_or_assign_stub_result(action_stubs, action_key, invocation_key, call, stub)

      :error ->
        {:error, :unknown_action_stub}
    end
  end

  defp replay_or_assign_stub_result(action_stubs, action_key, invocation_key, call, stub) do
    case Map.fetch(stub.assignments, invocation_key) do
      {:ok, result} ->
        call = Map.put(call, :replayed?, true)
        stub = %{stub | calls: [call | stub.calls]}
        {:ok, result, Map.put(action_stubs, action_key, stub)}

      :error ->
        assign_stub_result(action_stubs, action_key, invocation_key, call, stub)
    end
  end

  defp assign_stub_result(action_stubs, action_key, invocation_key, call, stub) do
    case stub.remaining do
      [result | remaining] ->
        call = Map.put(call, :replayed?, false)

        stub = %{
          stub
          | assignments: Map.put(stub.assignments, invocation_key, result),
            calls: [call | stub.calls],
            remaining: remaining
        }

        {:ok, result, Map.put(action_stubs, action_key, stub)}

      [] ->
        {:error, :action_stub_sequence_exhausted}
    end
  end

  defp append_thread(state, thread_id, entries, opts) do
    current = Map.get(state.threads, thread_id)
    current_rev = if current, do: current.rev, else: 0

    if Keyword.get(opts, :expected_rev) in [nil, current_rev] do
      thread = append(current, thread_id, entries, opts)
      {:reply, {:ok, thread}, put_in(state, [:threads, thread_id], thread)}
    else
      {:reply, {:error, :conflict}, state}
    end
  end

  defp append_fault?({thread_prefix, _action}, thread_id) do
    String.starts_with?(thread_id, thread_prefix)
  end

  defp append_fault?(nil, _thread_id) do
    false
  end

  defp consume_append_conflict(%{next_append_conflict: thread_id} = state, thread_id) do
    state = %{
      state
      | next_append_conflict: nil,
        append_conflict_counts: Map.update(state.append_conflict_counts, thread_id, 1, &(&1 + 1))
    }

    {:conflict, state}
  end

  defp consume_append_conflict(state, _thread_id) do
    {:continue, state}
  end

  defp drop_dead_execution_leases(state) do
    Enum.reduce(state.execution_leases, state, fn
      {lease_ref, {caller, monitor_ref}}, state ->
        if Process.alive?(caller) do
          state
        else
          Process.demonitor(monitor_ref, [:flush])

          %{
            state
            | execution_leases: Map.delete(state.execution_leases, lease_ref),
              execution_monitors: Map.delete(state.execution_monitors, monitor_ref)
          }
        end
    end)
  end

  defp call(opts, request) do
    case Keyword.get(opts, :server) do
      server when is_pid(server) -> safe_call(server, request)
      _invalid -> {:error, :runtime_stopped}
    end
  end

  defp safe_call(server, request) do
    GenServer.call(server, request)
  catch
    :exit, reason ->
      if stopped_call_exit?(reason) do
        {:error, :runtime_stopped}
      else
        :erlang.raise(:exit, reason, __STACKTRACE__)
      end
  end

  defp stopped_call_exit?({reason, {GenServer, :call, _args}})
       when reason in [:noproc, :normal, :shutdown] do
    true
  end

  defp stopped_call_exit?({{:shutdown, _details}, {GenServer, :call, _args}}) do
    true
  end

  defp stopped_call_exit?(reason) when reason in [:noproc, :normal, :shutdown] do
    true
  end

  defp stopped_call_exit?(_reason) do
    false
  end

  defp validate_durable(term)
       when is_atom(term) or is_binary(term) or is_integer(term) or is_float(term) do
    :ok
  end

  defp validate_durable(term) when is_list(term) do
    validate_many(term)
  end

  defp validate_durable(term) when is_tuple(term) do
    term
    |> Tuple.to_list()
    |> validate_many()
  end

  defp validate_durable(term) when is_map(term) do
    term
    |> Map.to_list()
    |> Enum.reduce_while(:ok, fn {key, value}, :ok ->
      with :ok <- validate_durable(key),
           :ok <- validate_durable(value) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_durable(term) do
    {:error, {:unsupported_term, term}}
  end

  defp validate_many(terms) do
    Enum.reduce_while(terms, :ok, fn term, :ok ->
      case validate_durable(term) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end
end
