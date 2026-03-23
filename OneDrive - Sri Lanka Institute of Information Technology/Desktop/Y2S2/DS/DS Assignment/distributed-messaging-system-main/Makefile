.PHONY: build test test-integration proto lint clean

# Build server and client binaries
build:
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client

# Run all unit tests
test:
	go test ./internal/... ./config/... ./pkg/... -v -race

# Run integration tests
test-integration:
	go test ./test/integration/... -v -race -timeout 60s

# Generate gRPC code from proto files
proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		internal/transport/proto/messaging.proto

# Run linter
lint:
	golangci-lint run ./...

# Remove build artifacts
clean:
	rm -rf bin/
	go clean
