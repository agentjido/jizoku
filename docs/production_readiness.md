# Production Readiness

Jizoku provides a supported `0.3.x` journal runtime for embedded host-app
workflows.

Jizoku is ready to adopt inside host applications that can own their worker
placement, queue/leasing strategy, deploy path, and side-effect safety. Start
with a bounded workflow class, prove it in the host app, then expand the surface
as operational confidence grows.

## Feature Readiness Map

| Area | Current stance | Verification or evidence | Host responsibility |
| --- | --- | --- | --- |
| Workflow DSL and normalized specs | Supported | Formatter rules, workflow authoring docs, reference workflows, and test coverage | Keep workflow modules backend-neutral and validate payload contracts |
| Journal-backed starts and execution | Supported | `Jizoku.start/3`, `Jizoku.execute_next/1`, smoke coverage, and example host app | Supervise workers and size execution capacity |
| Postgres-compatible Ecto storage | Supported baseline | Installed Jizoku migration and storage strategy docs | Own database backups, migrations, pooling, and retention policy |
| Dispatch claims and heartbeats | Supported | Journal lease fencing, heartbeat coverage, and the minimal host app multi-node proof | Set unique worker owner ids, claim durations, and heartbeat intervals for real step duration |
| Retries and terminal failures | Supported | Workflow retry policy tests and example smoke paths | Make external side effects idempotent and alert on terminal failures |
| Manual pause and approval controls | Supported | Resume, approve, reject, and restart-boundary coverage | Authorize operators and persist/redact approval metadata safely |
| Cancellation | Supported | Runtime signal and cancellation coverage | Decide which workflows can be cancelled and document terminal-state handling |
| Replay | Supported, scoped | Replay safety gates and irreversible-step checks | Restrict replay to workflows whose side effects are reversible or explicitly reviewed |
| Cron activation | Supported | Cron payload boundary and example scheduler paths | Own scheduler delivery, idempotency keys, and intended-window policy |
| Runtime-authored specs | Supported, scoped | `start_spec/3`, `start_spec/4`, safe action registry, editor spec validation | Maintain an action allowlist; replay is not yet available for these runs |
| Child workflow starts | Supported, scoped | `start_child_run/4` and `/5` with parent lineage and idempotent child identity | Pick stable child keys and inspect parent-child lineage in host tooling |
| Dynamic work | Supported, scoped | Preview, record, schedule, graph overlays, and duplicate-node validation | Keep action keys allowlisted; review replay safety for dynamic steps |
| Continue as new | Supported, gated | Native/public continuation, deterministic recovery, bounded lineage inspection, and minimal-host smoke coverage | Upgrade all queue readers before enabling `continuation_fences`; keep stable continuation keys and explicit successor input |
| Graph inspection and explanations | Supported | Projection-backed `inspect_run_graph/2`, `explain_run/2`, and graph contracts | Redact host-domain inputs, outputs, errors, and metadata before exposing externally |
| Actor-scoped read views | Supported | Visibility policy docs and read-model redaction APIs | Define tenant/user roles and apply visibility policies at host boundaries |
| Bedrock-backed delivery example | Reference integration | Bedrock minimal host app, two-consumer delivery fencing, lease renewal, retry requeue, and dead-letter checks | Configure Bedrock or another backend as host infrastructure; workflow modules stay backend-neutral |
| Soak/load evidence | Bounded verification | `mix example.soak` and example resilience checks | Run host-specific soak/load under expected production traffic and deploy patterns |

## Initial Rollout Guidance

For the first production workflow, keep the scope concrete:

- record the Jizoku version, Elixir/OTP versions, database version, queue
  backend, and storage adapter
- apply Jizoku migrations in staging before production
- choose one workflow class with clear operator value and well-understood side
  effects
- verify that workflow through the host worker and deploy path
- set worker owner ids, worker count, claim duration, and heartbeat interval for
  that workflow class
- fence external side effects with idempotency keys, compensating steps, or
  explicit irreversible boundaries
- expose enough inspection for operators to see run state, terminal failures,
  retry history, and manual actions
- apply host redaction or actor-visibility policy before exposing inspection
  payloads outside trusted operators

## Example Verification Entry Points

The example host app provides the repeatable checks:

```sh
cd examples/minimal_host_app
MIX_ENV=test mix example.smoke
MIX_ENV=test mix test test/multi_node_host_worker_test.exs
MIX_ENV=test mix example.resilience
MIX_ENV=test mix example.soak

cd ../bedrock_minimal_host_app
MIX_ENV=test mix test test/bedrock_multi_node_consumer_test.exs
```

These checks are meant to answer different questions:

- `example.smoke`: does the basic embedded workflow path work?
- `multi_node_host_worker_test.exs`: do independent host-worker identities
  contend, renew, take over, and fence stale or terminal results safely through
  shared durable storage?
- `bedrock_multi_node_consumer_test.exs`: do independently identified Bedrock
  consumers provide exclusive delivery, renew the backend lease, and complete
  safely after renewal changes the stored lease?
- `example.resilience`: do queued, delayed, retrying, and paused-then-resumed runs survive worker and scheduler restart boundaries?
- `example.soak`: does the runtime remain stable under a bounded mix of success, retry, replay, and cancellation traffic?

## Decision Rule

Use Jizoku in production for a bounded workflow class when:

1. the rollout guidance above is satisfied for that workflow class,
2. the example verification paths are green on the selected release baseline,
3. the host app has verified the same workflow through its own worker and deploy
   path.

Expand to more workflow classes as the host app gains evidence around traffic
volume, operator handling, redaction, and side-effect safety.
