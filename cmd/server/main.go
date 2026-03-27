// =============================================================================
// Entry Point: Server
// File: cmd/server/main.go
// Responsible Member: All Members (Shared)
// Purpose: Bootstraps one node process by loading config, initializing logging,
//          starting the node runtime, and handling graceful shutdown via signals.
//
// Usage:
//   go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003" --zk "localhost:2181"
//   go run cmd/server/main.go --id node2 --port 5002 --peers "localhost:5001,localhost:5003" --zk "localhost:2181"
//   go run cmd/server/main.go --id node3 --port 5003 --peers "localhost:5001,localhost:5002" --zk "localhost:2181"
// =============================================================================
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"distributed-messaging-system/internal/config"
	"distributed-messaging-system/internal/logger"
	"distributed-messaging-system/internal/node"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		_, _ = os.Stderr.WriteString("failed to load config: " + err.Error() + "\n")
		os.Exit(1)
	}

	log := logger.New(cfg.NodeID)
	log.Info("starting node",
		"node_id", cfg.NodeID,
		"port", cfg.Port,
		"peers", cfg.Peers,
		"zk_servers", cfg.ZookeeperServers,
	)

	n, err := node.New(cfg, log)
	if err != nil {
		log.Error("failed to initialize node", "error", err)
		os.Exit(1)
	}

	// Set up graceful shutdown via OS signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for SIGINT (Ctrl+C) and SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("received shutdown signal", "signal", sig.String())
		n.Stop()
		cancel()
	}()

	// Start the node (blocks until context is cancelled)
	if err := n.Start(ctx); err != nil {
		log.Error("node startup failed", "error", err)
		os.Exit(1)
	}

	log.Info("node exited cleanly")
}
