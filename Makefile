.PHONY: build test test-race vet run

build:
	go build -trimpath -o bin/rpc-proxy ./cmd/rpc-proxy

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

run:
	go run ./cmd/rpc-proxy -config rpc-proxy.yaml
