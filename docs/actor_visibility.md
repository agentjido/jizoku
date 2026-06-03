# Actor Visibility

## Overview

Actor visibility in Squidie provides a host-controlled authorization boundary that allows deriving less-sensitive views of workflow data for different actors without mutating the durable history. This enables secure multi-tenant access patterns where different users see appropriate levels of detail based on their authorization scope.

## Core Concepts

### Visibility Scopes

Squidie defines three standard visibility scopes, each providing different levels of information access:

#### `:external` (Most Restrictive)
- **Purpose**: Minimal information for external users and public APIs
- **Preserves**: High-level status, current node state, manual task shape
- **Redacts**: All sensitive data including inputs, outputs, errors, metadata, command history, tokens

#### `:operator` (Moderate Access)
- **Purpose**: Operational detail for support staff and monitoring
- **Preserves**: Everything from `:external` plus reason, attempt counts, next visibility time, anomaly count
- **Redacts**: Actual data payloads, sensitive identifiers, secrets

#### `:auditor` (Full Access)
- **Purpose**: Complete unredacted view for audit trails and debugging
- **Preserves**: Everything - no redaction applied
- **Use with caution**: Should be restricted to privileged users only

### Redacted Fields

The following fields are automatically redacted for non-auditor actors:

- `input`, `output`, `result`, `error` - Workflow data payloads
- `payload`, `metadata` - Additional context data
- `command_history` - Full command audit trail
- `attempts`, `attempt_*` - Retry and failure details
- `idempotency_key` - Deduplication identifiers
- `claim_id`, `owner_id` - Ownership information
- `lease_*` - Lease and lock information
- `token`, `secret` - Authentication credentials

## Implementation

### Basic Usage

```elixir
# Get a workflow snapshot
{:ok, snapshot} = Squidie.inspect(run_id)

# Redact for external actor (most restrictive)
external_view = Squidie.ReadModel.Visibility.redact(snapshot, :external)

# Redact for operator actor
operator_view = Squidie.ReadModel.Visibility.redact(snapshot, :operator)

# Get full auditor view (no redaction)
auditor_view = Squidie.ReadModel.Visibility.redact(snapshot, :auditor)
```

### Custom Visibility Policies

Host applications can define custom visibility policies to map actors to scopes:

#### Method 1: Policy Module with Callback

```elixir
defmodule MyApp.VisibilityPolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @impl true
  def visibility_scope(actor, _view) do
    cond do
      actor.role == "admin" -> :auditor
      actor.role == "support" -> :operator
      true -> :external
    end
  end
end

# Usage
redacted_view = Squidie.ReadModel.Visibility.redact(
  snapshot,
  %{role: "support"},
  MyApp.VisibilityPolicy
)
```

#### Method 2: Policy Module with Options

```elixir
defmodule MyApp.ConfigurablePolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @impl true
  def visibility_scope(actor, view, opts) do
    tenant_id = Keyword.get(opts, :tenant_id)

    cond do
      actor.tenant_id == tenant_id and actor.role == "owner" -> :auditor
      actor.role == "operator" -> :operator
      true -> :external
    end
  end
end

# Usage with options
redacted_view = Squidie.ReadModel.Visibility.redact(
  snapshot,
  %{tenant_id: "acme", role: "owner"},
  {MyApp.ConfigurablePolicy, tenant_id: "acme"}
)
```

#### Method 3: Anonymous Function Policy

```elixir
# Define a custom policy function
policy_fn = fn
  %{admin: true}, _view -> :auditor
  %{support: true}, _view -> :operator
  _, _view -> :external
end

# Apply the policy
redacted_view = Squidie.ReadModel.Visibility.redact(
  snapshot,
  %{support: true},
  policy_fn
)
```

## Integration Patterns

### HTTP/API Boundary

```elixir
defmodule MyAppWeb.WorkflowController do
  use MyAppWeb, :controller

  def show(conn, %{"id" => run_id}) do
    # Get current user/actor from session
    actor = get_current_user(conn)

    # Fetch workflow snapshot
    {:ok, snapshot} = Squidie.inspect(run_id)

    # Apply visibility policy based on actor
    redacted_view = Squidie.ReadModel.Visibility.redact(
      snapshot,
      actor,
      MyApp.VisibilityPolicy
    )

    # Return redacted view to client
    json(conn, redacted_view)
  end
end
```

### LiveView Integration

```elixir
defmodule MyAppWeb.WorkflowLive do
  use MyAppWeb, :live_view

  @impl true
  def mount(%{"id" => run_id}, session, socket) do
    actor = get_actor_from_session(session)

    # Subscribe to workflow updates
    Squidie.subscribe(run_id)

    # Get initial snapshot with appropriate visibility
    {:ok, snapshot} = Squidie.inspect(run_id)
    redacted_view = apply_visibility(snapshot, actor)

    {:ok, assign(socket, workflow: redacted_view, actor: actor)}
  end

  @impl true
  def handle_info({:workflow_updated, snapshot}, socket) do
    # Apply same visibility policy to updates
    redacted_view = apply_visibility(snapshot, socket.assigns.actor)
    {:noreply, assign(socket, workflow: redacted_view)}
  end

  defp apply_visibility(snapshot, actor) do
    Squidie.ReadModel.Visibility.redact(
      snapshot,
      actor,
      MyApp.VisibilityPolicy
    )
  end
end
```

### Manual Actions with Actor Tracking

```elixir
defmodule MyApp.WorkflowActions do
  def approve_task(run_id, task_ref, actor) do
    # Actor information is preserved in command history
    Squidie.signal(run_id, {:approve, task_ref}, actor: actor)
  end

  def reject_task(run_id, task_ref, actor, reason) do
    Squidie.signal(
      run_id,
      {:reject, task_ref, reason: reason},
      actor: actor
    )
  end

  def pause_workflow(run_id, actor) do
    Squidie.signal(run_id, :pause, actor: actor)
  end
end
```

## Actor Information in Command History

When actors perform manual actions, their information is captured in the command history:

```elixir
# Command with actor information
{:ok, _} = Squidie.signal(
  run_id,
  {:approve, "review_task"},
  actor: %{
    id: "user_123",
    email: "reviewer@example.com",
    role: "reviewer"
  }
)

# The actor information is stored in command receipts
{:ok, snapshot} = Squidie.inspect(run_id)

# Auditors can see full command history with actors
auditor_view = Squidie.ReadModel.Visibility.redact(snapshot, :auditor)
# auditor_view.command_history includes actor information

# External users cannot see command history
external_view = Squidie.ReadModel.Visibility.redact(snapshot, :external)
# external_view.command_history is nil (redacted)
```

## Security Best Practices

### 1. Apply Host Authorization First

Always verify actor permissions at the host boundary before applying visibility:

```elixir
def show_workflow(conn, %{"id" => run_id}) do
  actor = get_current_user(conn)

  # Host authorization check
  with :ok <- authorize_workflow_access(actor, run_id) do
    {:ok, snapshot} = Squidie.inspect(run_id)

    # Then apply visibility policy
    view = Squidie.ReadModel.Visibility.redact(snapshot, actor, policy())
    json(conn, view)
  else
    {:error, :unauthorized} -> send_resp(conn, 403, "Forbidden")
  end
end
```

### 2. Never Expose Raw Snapshots

Never serialize full snapshots directly to untrusted clients:

```elixir
# WRONG - Exposes sensitive data
def unsafe_endpoint(conn, %{"id" => run_id}) do
  {:ok, snapshot} = Squidie.inspect(run_id)
  json(conn, snapshot)  # DON'T DO THIS!
end

# CORRECT - Always redact first
def safe_endpoint(conn, %{"id" => run_id}) do
  {:ok, snapshot} = Squidie.inspect(run_id)
  actor = get_current_user(conn)
  view = Squidie.ReadModel.Visibility.redact(snapshot, actor, policy())
  json(conn, view)
end
```

### 3. Treat Auditor Scope as Privileged

The `:auditor` scope provides complete access to all data. Restrict it carefully:

```elixir
defmodule MyApp.StrictPolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @impl true
  def visibility_scope(actor, _view) do
    cond do
      # Only system admins get auditor access
      actor.role == "system_admin" and actor.mfa_verified -> :auditor

      # Support staff get operator access
      actor.role in ["support", "operator"] -> :operator

      # Everyone else gets external view
      true -> :external
    end
  end
end
```

### 4. Immutable Durable History

Visibility redaction creates derived views without modifying the durable history:

```elixir
# Original data remains intact in the journal
{:ok, full_snapshot} = Squidie.inspect(run_id)

# Redaction creates a new view, doesn't modify original
external_view = Squidie.ReadModel.Visibility.redact(full_snapshot, :external)

# The journal still contains all original data
# Only privileged processes should access it directly
```

## Testing Visibility Policies

```elixir
defmodule MyApp.VisibilityPolicyTest do
  use ExUnit.Case
  alias Squidie.ReadModel.Visibility

  setup do
    # Create test snapshot with sensitive data
    snapshot = %Squidie.ReadModel.Snapshot{
      run_id: "test_run",
      status: :running,
      input: %{secret: "sensitive_data"},
      output: %{result: "private_result"},
      metadata: %{internal: "metadata"}
    }

    {:ok, snapshot: snapshot}
  end

  test "external actors see redacted view", %{snapshot: snapshot} do
    actor = %{role: "external"}

    redacted = Visibility.redact(snapshot, actor, MyApp.VisibilityPolicy)

    assert redacted.status == :running
    assert redacted.input == nil
    assert redacted.output == nil
    assert redacted.metadata == nil
  end

  test "operators see operational details", %{snapshot: snapshot} do
    actor = %{role: "operator"}

    redacted = Visibility.redact(snapshot, actor, MyApp.VisibilityPolicy)

    assert redacted.status == :running
    assert redacted.input == nil  # Still redacted
    assert redacted.output == nil  # Still redacted
    # But operational fields would be visible
  end

  test "auditors see everything", %{snapshot: snapshot} do
    actor = %{role: "admin"}

    view = Visibility.redact(snapshot, actor, MyApp.VisibilityPolicy)

    assert view.input == %{secret: "sensitive_data"}
    assert view.output == %{result: "private_result"}
    assert view.metadata == %{internal: "metadata"}
  end
end
```

## Common Patterns

### Multi-Tenant Visibility

```elixir
defmodule MyApp.MultiTenantPolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @impl true
  def visibility_scope(actor, view, opts) do
    tenant_id = Keyword.get(opts, :tenant_id)

    cond do
      # Tenant owner sees everything for their workflows
      actor.tenant_id == tenant_id and actor.role == "owner" ->
        :auditor

      # Tenant members see operational details
      actor.tenant_id == tenant_id ->
        :operator

      # Cross-tenant support staff see limited details
      actor.role == "support" ->
        :operator

      # Others see minimal information
      true ->
        :external
    end
  end
end
```

### Role-Based Access Control (RBAC)

```elixir
defmodule MyApp.RBACPolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @role_scopes %{
    "admin" => :auditor,
    "manager" => :auditor,
    "analyst" => :operator,
    "support" => :operator,
    "viewer" => :external,
    "guest" => :external
  }

  @impl true
  def visibility_scope(actor, _view) do
    Map.get(@role_scopes, actor.role, :external)
  end
end
```

### Time-Based Visibility

```elixir
defmodule MyApp.TimeBasedPolicy do
  @behaviour Squidie.ReadModel.Visibility.Policy

  @impl true
  def visibility_scope(actor, view) do
    workflow_age = DateTime.diff(DateTime.utc_now(), view.created_at, :day)

    cond do
      # Admins always see everything
      actor.role == "admin" -> :auditor

      # Recent workflows - more visibility
      workflow_age < 7 and actor.role == "operator" -> :operator

      # Older workflows - restricted visibility
      workflow_age >= 30 -> :external

      # Default
      true -> :external
    end
  end
end
```

## Troubleshooting

### Issue: Sensitive Data Appearing in External Views

**Symptom**: Data that should be redacted is visible to external actors.

**Solution**: Verify your visibility policy is returning the correct scope:

```elixir
# Debug your policy
actor = %{role: "external_user"}
view = %{} # Your view data

scope = MyApp.VisibilityPolicy.visibility_scope(actor, view)
IO.inspect(scope, label: "Visibility scope for actor")

# Should return :external for external users
assert scope == :external
```

### Issue: Command History Not Captured

**Symptom**: Actor information not appearing in audit logs.

**Solution**: Ensure you're passing actor information in signals:

```elixir
# Correct - includes actor
Squidie.signal(run_id, :pause, actor: %{id: "user_123"})

# Incorrect - missing actor
Squidie.signal(run_id, :pause)  # Actor won't be recorded
```

### Issue: Performance with Large Snapshots

**Symptom**: Slow response times when redacting large workflow snapshots.

**Solution**: Consider caching redacted views:

```elixir
defmodule MyApp.CachedVisibility do
  use GenServer

  def get_redacted_view(run_id, actor, policy) do
    cache_key = {run_id, actor_scope(actor), :view}

    case :ets.lookup(:visibility_cache, cache_key) do
      [{^cache_key, cached_view, expiry}] when expiry > now() ->
        cached_view

      _ ->
        {:ok, snapshot} = Squidie.inspect(run_id)
        view = Squidie.ReadModel.Visibility.redact(snapshot, actor, policy)
        cache_view(cache_key, view)
        view
    end
  end
end
```

## See Also

- [Observability Guide](./observability.md) - Data visibility tiers and patterns
- [Host App Integration](./host_app_integration.md#observability) - Integration guidelines
- [Usage Rules](../usage-rules/host-apps.md) - Host application requirements
- Module documentation: `Squidie.ReadModel.Visibility`