# Tool Adapters

Squidie exposes a small tool boundary for workflow steps that need to talk
to external systems.

## Contract

Tool adapters implement `Squidie.Tools.Adapter` and are invoked through
`Squidie.Tools.invoke/4`.

```elixir
{:ok, result} =
  Squidie.Tools.invoke(MyApp.Tools.SomeAdapter, request, context)
```

The shared contract is:

- request: a map owned by the adapter
- context: a workflow or step context map
- success: `{:ok, %Squidie.Tools.Result{}}`
- failure: `{:error, %Squidie.Tools.Error{}}`

## Normalized Result

`Squidie.Tools.Result` contains:

- `adapter`: the adapter module
- `payload`: the normalized adapter response
- `metadata`: adapter metadata such as request method or URL

## Normalized Error

`Squidie.Tools.Error` contains:

- `adapter`: the adapter module
- `kind`: normalized error kind
- `message`: stable human-readable message
- `details`: adapter-specific details in a plain map
- `retryable?`: whether the failure is a reasonable candidate for workflow retry

Steps can convert tool errors into plain maps with
`Squidie.Tools.Error.to_map/1` before returning them as workflow step
failures.

## HTTP Adapter

`Squidie.Tools.HTTP` is the first concrete adapter.

Supported request shape:

- `method`
- `url`
- `headers`
- `params`
- `body`
- `json`
- `timeout`

Successful responses are normalized to:

- `status`
- `headers`
- `trailers`
- `body`

HTTP responses with status `>= 400`, transport failures, and timeouts are
normalized into `Squidie.Tools.Error`.

## HTTP Runtime Action

`Squidie.Step.HTTP` wraps the HTTP adapter as a native workflow step for
runtime-authored specs. Hosts expose it through the action registry under a
stable key:

```elixir
registry = %{
  "http.request" => [
    module: Squidie.Step.HTTP,
    category: "HTTP",
    action_opts: [allowed_hosts: ["api.example.test"]],
    credential_requirements: [%{name: "billing_api", required?: true}]
  ]
}
```

The step expects a `request` map with `method` plus either `url` or
`url_template`. Supported request fields are `headers`, `query_params` or
`params`, `body`, `json`, and `timeout`. URLs must not include userinfo or a
query string; use `query_params` for query data. `url_template` placeholders
use `{{ name }}` syntax and are expanded from the `bindings` map.

Use `Squidie.Step.HTTP.validate_request/1` to validate structural request
configuration without policy. Use `validate_request/2` or
`validate_action_input/2` with the same host-owned `action_opts` before
starting a runtime-authored run. The runtime also invokes `validate_action_input/2`
before appending journal facts for planned runtime-spec attempts.

`allowed_hosts` is required in `action_opts` for execution. Credential values
do not belong in the request map; pass host-owned references through
`credential_refs` and let a host wrapper or transport boundary decide how
references become headers. The reusable action rejects common secret-bearing
headers and payload keys rather than persisting them. Raw string bodies require
`allow_body?: true` in `action_opts`.

Successful responses are returned as `%{http_response: response}` with headers
redacted. Response and error bodies are omitted by default; hosts can opt into
bounded body persistence with `persist_response_body?: true` and
`max_body_bytes: ...`. HTTP and transport errors are converted to structured
step errors with redacted details; retryable tool errors return `{:retry, error}`
so normal workflow retry policy remains the only retry scheduler. Redirects are
disabled at the shared HTTP adapter boundary.

## Retry Boundary

The HTTP adapter disables Req's built-in retry loop.

That keeps retry policy in one place:

- adapters report the first failure
- workflow steps declare retry policy
- Squidie appends the next journal dispatch attempt with the resolved retry
  visibility time

This keeps transport behavior predictable and avoids stacking HTTP-client
retries underneath workflow retries.
