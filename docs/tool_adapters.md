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

## Retry Boundary

The HTTP adapter disables Req's built-in retry loop.

That keeps retry policy in one place:

- adapters report the first failure
- workflow steps declare retry policy
- Squidie appends the next journal dispatch attempt with the resolved retry
  visibility time

This keeps transport behavior predictable and avoids stacking HTTP-client
retries underneath workflow retries.
