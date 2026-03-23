// Module: Node
// Phase: Foundation
// Purpose: Initializes and runs a node process with transport startup hooks.
// Extended in later phases by consensus, replication, fault tolerance, and time sync.
package node

import (
	"context"

	"distributed-messaging-system/internal/config"
	"distributed-messaging-system/internal/logger"
	"distributed-messaging-system/internal/transport"
)

// Node represents the minimal runtime container for one server instance.
type Node struct {
	cfg       config.Config
	log       *logger.Logger
	transport *transport.Server
}

// New builds a minimal node instance.
// It wires only configuration, logger, and transport foundations.
func New(cfg config.Config, log *logger.Logger) (*Node, error) {
	n := &Node{
		cfg: cfg,
		log: log,
		transport: transport.NewServer(cfg.Port),
	}

	return n, nil
}

// Start brings up foundational services.
// TODO: Connect to peers and start background modules in later phases.
func (n *Node) Start(ctx context.Context) error {
	_ = ctx // TODO: Use context cancellation for graceful lifecycle management.

	n.log.Info("starting transport server", "port", n.cfg.Port)

	if err := n.transport.Start(); err != nil {
		return err
	}

	for _, peer := range n.cfg.Peers {
		n.log.Debug("peer connection placeholder", "peer", peer)
		// TODO: Connect to peer over gRPC client transport.
	}

	return nil
}

// Stop gracefully shuts down foundational services.
func (n *Node) Stop() {
	n.log.Info("stopping transport server")
	n.transport.Stop()
	// TODO: Close peer connections when client transport is introduced.
}
