# Quality Gates

Squidie uses the standard `mix precommit` gate for required local verification.
The structural quality tools below are part of that gate and also run in CI.

## Commands

Run both checks:

```bash
mix quality_gates
```

Run an individual check:

```bash
mix quality.ex_dna
mix quality.reach
```

`mix quality.ex_dna` runs ExDNA with a zero-clone budget over the default
project source scope:

```bash
mix ex_dna --min-mass 40 --max-clones 0 --format console
```

`mix quality.reach` runs Reach smell checks in strict mode:

```bash
mix reach.check --smells --strict
```

## Tooling Baseline

The current package metadata was verified from Hex before adding these
dependencies:

- `ex_dna`, `~> 1.5`, dev/test only, `runtime: false`
- `reach`, `~> 2.7`, dev/test only, `runtime: false`

Both tools are configured as strict gates. Keep generated HTML, JSON, SARIF, and
cache output out of the repository unless a report is stable enough to review.

## Rollout Policy

`mix quality_gates` must stay green. If either tool reports findings, fix the
cleanup in the same change that introduces or trips the gate, or split the
cleanup into a separate prerequisite PR before tightening the gate.

Use ExDNA suppression comments only for intentional duplication:

```elixir
# ex_dna:disable-for-next-line
```

Reach smell findings are strict failures in this repository. Keep fixes small
and behavior-preserving, with focused tests when a cleanup changes runtime
logic rather than expression shape.
