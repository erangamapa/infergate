# infergate

A small grpc gateway that sits between my agents and every model they call.

Agents never hold provider keys and never pick a backend. They ask for a route alias like `brief-drafter` or just a tier, and the gateway decides who serves it. That gave me one place to put the things every agent was reimplementing badly.

- fallback chains per route, so the self hosted vLLM model is tried first and a paid model picks up when it is overloaded
- a circuit breaker per backend, a dead pod gets skipped for a cooldown instead of eating a timeout on every call
- per tenant request rate and a daily token budget, so an eval run cannot starve the live agents
- idempotency keys, a retried step returns the same completion instead of paying for a new one
- streaming that only falls back before the first chunk is sent, never mid answer

The contract lives in [proto/infergate/v1/inference.proto](proto/infergate/v1/inference.proto). Generated Go code is committed under `gen/` so consumers do not need protoc.

## Running it

```sh
cp config/gateway.example.yaml config/gateway.yaml
export ANTHROPIC_API_KEY=... OPENAI_API_KEY=...
go run ./cmd/infergate -config config/gateway.yaml
```

Then from another shell

```sh
export INFERGATE_KEY=dev-pulse-agents
go run ./cmd/igctl -list
go run ./cmd/igctl -tier fast "classify this tweet, home or away or none"
go run ./cmd/igctl -model brief-drafter -stream "short brief for arsenal v spurs"
```

The server also registers grpc health and reflection, so `grpcurl` works against it.

## Config

Backends are either `anthropic` (messages API) or `openai` (chat completions, which also covers vLLM). Keys come from env vars named in the config, the config itself only holds sha256 digests of tenant keys so it can live in the deploy repo.

```yaml
routes:
  - alias: brief-drafter
    tier: standard
    backends: [llama-drafter-vllm, sonnet, gpt-4o]
```

The first route declared for a tier is what you get when you ask by tier only. Unknown yaml keys fail at startup on purpose, a typo in `breaker` once silently gave me default thresholds for a week.

## How failures are handled

A 429, 529, 5xx or transport error is retryable and moves to the next backend in the chain. A 4xx is the caller's fault, it would fail the same way everywhere, so it comes straight back as `INVALID_ARGUMENT` and does not count against the backend's breaker.

Budgets are charged after the response because output tokens are only known then. A tenant can overshoot its daily budget by one request, which I am fine with.

| condition | grpc code |
| --- | --- |
| missing or unknown key | `UNAUTHENTICATED` |
| rate or budget exceeded | `RESOURCE_EXHAUSTED` |
| unknown route | `NOT_FOUND` |
| every backend failed or open | `UNAVAILABLE` |

## Tests

```sh
make test
```

No network. Provider clients are tested against `httptest` servers that replay real response shapes, and the service is tested end to end over `bufconn`.

## Regenerating the stubs

```sh
make proto
```

Needs `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc` on the path.
