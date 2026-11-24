PROTO := proto/infergate/v1/inference.proto

.PHONY: proto test lint build run

proto:
	protoc -I proto \
		--go_out=gen --go_opt=paths=source_relative \
		--go-grpc_out=gen --go-grpc_opt=paths=source_relative \
		$(PROTO)

test:
	go test -race ./...

lint:
	go vet ./...
	test -z "$$(gofmt -l .)"

build:
	CGO_ENABLED=0 go build -o bin/infergate ./cmd/infergate
	CGO_ENABLED=0 go build -o bin/igctl ./cmd/igctl

run:
	go run ./cmd/infergate -config config/gateway.example.yaml
