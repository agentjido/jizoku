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
    credential_requirements: [%{name: "billing_api", required?: true}]
  ]
}
```

The step expects a `request` map with `method` plus either `url` or
`url_template`. Supported request fields are `headers`, `query_params` or
`params`, `body`, `json`, and `timeout`. `url_template` placeholders use
`{{ name }}` syntax and are expanded from the `bindings` map.

Use `Squidie.Step.HTTP.validate_request/1` to validate request configuration
before starting a runtime-authored run. Use `validate_request/2` with
`allowed_hosts: [...]` when the host needs an allowed-destination policy check.
Credential values do not belong in the request map; pass host-owned references
through `credential_refs` and let the host registry or wrapping step decide how
references become transport headers.

Successful responses are returned as `%{http_response: response}`. HTTP and
transport errors are converted to structured step errors; retryable tool errors
return `{:retry, error}` so normal workflow retry policy remains the only retry
scheduler.

## Retry Boundary

The HTTP adapter disables Req's built-in retry loop.

That keeps retry policy in one place:

- adapters report the first failure
- workflow steps declare retry policy
- Squidie appends the next journal dispatch attempt with the resolved retry
  visibility time

This keeps transport behavior predictable and avoids stacking HTTP-client
retries underneath workflow retries.
