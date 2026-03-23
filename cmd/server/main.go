// Module: Server Entry
// Phase: Foundation
// Purpose: Bootstraps one node process by loading config, initializing logging,
// and starting the node runtime.
// Extended in later phases by graceful shutdown, signal handling, and runtime wiring.
package main

import (
	"context"
	"os"

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

	log := logger.New("info")
	log.Info("starting node", "node_id", cfg.NodeID, "port", cfg.Port)

	n, err := node.New(cfg, log)
	if err != nil {
		log.Error("failed to initialize node", "error", err)
		os.Exit(1)
	}

	if err := n.Start(context.Background()); err != nil {
		log.Error("node startup failed", "error", err)
		os.Exit(1)
	}

	// TODO: Add signal handling and graceful shutdown in a later phase.
}
