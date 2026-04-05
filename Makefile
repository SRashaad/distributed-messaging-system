.PHONY: run build build-dashboard proto

# Module: Build System
# Phase: Foundation
# Purpose: Provides minimal developer commands for the initial project setup.
# Extended in later phases by test, lint, and proto generation targets.

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server

build-dashboard:
	go build -o bin/dashboard ./cmd/dashboard

# Regenerate internal/transport/proto/*.pb.go (requires protoc, protoc-gen-go, protoc-gen-go-grpc on PATH).
proto:
	protoc -I internal/transport/proto --go_out=internal/transport/proto --go_opt=paths=source_relative \
		--go-grpc_out=internal/transport/proto --go-grpc_opt=paths=source_relative \
		internal/transport/proto/messaging.proto
