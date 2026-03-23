.PHONY: run build

# Module: Build System
# Phase: Foundation
# Purpose: Provides minimal developer commands for the initial project setup.
# Extended in later phases by test, lint, and proto generation targets.

run:
	go run ./cmd/server

build:
	go build -o bin/server ./cmd/server
