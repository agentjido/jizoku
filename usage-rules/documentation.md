# Squidie Documentation Usage Rules

## Manual Structure

- Use `docs/index.md` as the numbered lesson map.
- Use `docs/getting_started.md` as the narrative onboarding guide.
- Keep README concise and point deeper readers to the manual.
- Keep durable reference details in focused docs such as architecture,
  operations, workflow authoring, and host app integration.

## Current Language

- Describe the runtime as Jido-native and journal-backed.
- Describe step execution as pulled through `Squidie.execute_next/1`.
- Describe Bedrock as the recommended reference backend for backend-owned
  leases.
- Describe storage as adapter-shaped and database-agnostic, while explaining
  that not every database is a good production journal store.
- Do not mention removed executor-direction libraries in user-facing docs unless
  a future design issue explicitly reintroduces them.
- Do not document `:runtime_tables` or `:executor` step execution config.

## Examples And Diagrams

- Prefer examples that can be pasted into a host app without hidden setup.
- Keep Mermaid diagrams small and syntax-valid.
- For PR descriptions, include Mermaid diagrams when runtime flow, data flow,
  persistence, integration boundaries, or user-visible behavior changes.
- Do not include local machine paths, usernames, hostnames, or private
  filesystem details in docs, commits, PR descriptions, or generated examples.

## Tone

- Write docs as a manual for users, not as a changelog of internal migration
  slices.
- Explain ownership boundaries directly.
- Name operational tradeoffs: what becomes durable, what remains host-owned,
  and what external systems still need to guarantee.
