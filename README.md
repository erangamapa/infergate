# infergate

A small grpc gateway between my agents and the models they call. Agents ask for a route alias or a tier, the gateway picks the backend and falls back down the chain when one is overloaded.

```sh
go run ./cmd/infergate -config config/gateway.example.yaml
```

Contract is in [proto/infergate/v1/inference.proto](proto/infergate/v1/inference.proto).
