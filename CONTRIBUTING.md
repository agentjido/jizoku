# Contributing

Thanks for contributing to Squidie.

## Development Setup

Requirements:

- Elixir `~> 1.18`
- Erlang/OTP compatible with the current CI matrix
- Postgres

Clone the repository and install dependencies:

```sh
mix deps.get
```

Prepare the example host app:

```sh
cd examples/minimal_host_app
mix deps.get
mix ecto.create
mix ecto.migrate
```

## Local Verification

Run the root verification gate:

```sh
mix precommit
```

Run the example host app test suite:

```sh
cd examples/minimal_host_app
mix test
MIX_ENV=test mix example.smoke
```

The example host app is part of the development harness. Changes that affect
runtime behavior should keep that smoke path green.

Run coverage when changing tests or coverage-sensitive runtime paths:

```sh
MIX_ENV=test mix coveralls
```

Coverage thresholds are configured in `coveralls.json` and `codecov.yml`.

## Workflow For Changes

1. Start from `main`.
2. Create a short-lived branch for one focused slice.
3. Keep commits small and intentional.
4. Add or update tests with the change.
5. Run the verification steps before opening a pull request.

## Pull Requests

Pull requests should:

- describe the net change
- explain why the change is needed
- stay focused on one reviewable slice
- reference the issues they close when applicable

## Documentation

Documentation changes are welcome and should keep these sources aligned:

- `README.md`
- `docs/`
- example host app guides
- module docs for public or operationally important modules

## Questions And Discussion

If you are unsure about a change, open an issue or draft pull request first so
the direction can be aligned before implementation gets large.
