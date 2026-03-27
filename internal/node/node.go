// =============================================================================
// Module: Node
// Phase: Foundation + Consensus Integration
// Purpose: Initializes and runs a node process with all modules wired together.
//          This is the central orchestrator that connects:
//            - Configuration
//            - Transport (gRPC server + client)
//            - Consensus (ZooKeeper-based leader election)
//            - Fault Tolerance (failure detection + heartbeat + recovery)
//            - Replication (log + quorum)
//            - Time Synchronization (Lamport clock)
//            - Storage (message store)
// =============================================================================
package node

import (
	"context"
	"log"

	"distributed-messaging-system/internal/config"
	"distributed-messaging-system/internal/consensus"
	"distributed-messaging-system/internal/fault"
	"distributed-messaging-system/internal/logger"
	"distributed-messaging-system/internal/replication"
	"distributed-messaging-system/internal/storage"
	"distributed-messaging-system/internal/timesync"
	"distributed-messaging-system/internal/transport"
)

// Node represents the full runtime container for one server instance.
type Node struct {
	cfg       config.Config
	log       *logger.Logger
	transport *transport.Server
	peerClient *transport.PeerClient

	// Core modules
	consensus    *consensus.RaftNode
	detector     *fault.Detector
	recovery     *fault.LogRecovery
	repLog       *replication.InMemoryLog
	repManager   *replication.Manager
	clock        *timesync.LamportClock
	store        *storage.MessageStore

	// Lifecycle
	cancel context.CancelFunc
}

// New builds a fully wired node instance with all modules initialized.
func New(cfg config.Config, log *logger.Logger) (*Node, error) {
	// Initialize all components
	srv := transport.NewServer(cfg.Port)
	peerClient := transport.NewPeerClient()
	repLog := replication.NewInMemoryLog()
	clusterSize := len(cfg.Peers) + 1 // peers + self
	if clusterSize < 1 {
		clusterSize = 1
	}
	repManager := replication.NewManager(repLog, clusterSize)
	clock := &timesync.LamportClock{}
	msgStore := storage.NewMessageStore()
	detector := fault.NewDetector(cfg.HeartbeatTimeout())
	recovery := fault.NewLogRecovery(cfg.NodeID)

	// Create the ZooKeeper-backed consensus module
	raftNode := consensus.NewRaftNode(cfg.NodeID, cfg.ZookeeperServers)

	n := &Node{
		cfg:        cfg,
		log:        log,
		transport:  srv,
		peerClient: peerClient,
		consensus:  raftNode,
		detector:   detector,
		recovery:   recovery,
		repLog:     repLog,
		repManager: repManager,
		clock:      clock,
		store:      msgStore,
	}

	return n, nil
}

// Start brings up all services in the correct order.
func (n *Node) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	n.cancel = cancel

	// 1. Start the gRPC transport server
	n.log.Info("starting transport server", "port", n.cfg.Port)
	if err := n.transport.Start(); err != nil {
		return err
	}

	// 2. Connect to peer nodes
	for _, peer := range n.cfg.Peers {
		n.log.Debug("connecting to peer", "peer", peer)
		if err := n.peerClient.Connect(peer); err != nil {
			n.log.Warn("failed to connect to peer (will retry)", "peer", peer, "error", err)
		}
	}

	// 3. Start the ZooKeeper-based consensus module (leader election)
	n.log.Info("starting consensus module (ZooKeeper)", "zk_servers", n.cfg.ZookeeperServers)
	n.consensus.Start(ctx)

	// 4. Start the failure detector in the background
	go n.detector.StartMonitoring(ctx)

	// 5. Register a failure callback for logging
	// With ZooKeeper, election re-triggering is handled by ZK session events,
	// but we still log peer failures for observability
	n.detector.OnFailure(func(nodeID string) {
		n.log.Warn("peer failure detected", "failed_node", nodeID)
		// ZooKeeper handles re-election automatically via ephemeral node deletion
	})

	n.log.Info("node started successfully",
		"node_id", n.cfg.NodeID,
		"port", n.cfg.Port,
		"peers", n.cfg.Peers,
		"zk_servers", n.cfg.ZookeeperServers,
	)

	// Block until context is cancelled (keeps the node running)
	<-ctx.Done()
	n.log.Info("node shutting down", "node_id", n.cfg.NodeID)
	return nil
}

// Stop gracefully shuts down all services.
func (n *Node) Stop() {
	n.log.Info("stopping node", "node_id", n.cfg.NodeID)

	// Cancel context to stop all background goroutines
	if n.cancel != nil {
		n.cancel()
	}

	// 1. Stop consensus (disconnects from ZooKeeper, removes ephemeral znode)
	if n.consensus != nil {
		n.consensus.Stop()
	}

	// 2. Close peer connections
	if n.peerClient != nil {
		n.peerClient.CloseAll()
	}

	// 3. Stop the gRPC server
	n.transport.Stop()

	n.log.Info("node stopped", "node_id", n.cfg.NodeID)
}

// PublishMessage handles a client publish request by forwarding to consensus.
func (n *Node) PublishMessage(data []byte) (uint64, error) {
	// Tick the Lamport clock
	ts := n.clock.Tick()

	n.log.Debug("publishing message", "timestamp", ts)

	// Forward to the consensus leader for replication
	index, err := n.consensus.ProposeEntry(data)
	if err != nil {
		return 0, err
	}

	// Apply to local store after commit
	replication.ApplyLogToStore(n.store, index, data)

	return index, nil
}

// GetConsensus returns the consensus module for status queries.
func (n *Node) GetConsensus() *consensus.RaftNode {
	return n.consensus
}

// GetStore returns the message store for read operations.
func (n *Node) GetStore() *storage.MessageStore {
	return n.store
}

// PrintStatus logs the current state of the node and its consensus status.
func (n *Node) PrintStatus() {
	status := n.consensus.FormatClusterStatus()
	log.Printf("[node] %s", status)
}
