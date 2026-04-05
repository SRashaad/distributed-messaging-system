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
	"fmt"
	"log"
	"time"

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
	repManager := replication.NewManager(repLog, clusterSize, cfg.Peers, peerClient, cfg.NodeID)
	clock := &timesync.LamportClock{}
	msgStore := storage.NewMessageStore()
	detector := fault.NewDetector(cfg.HeartbeatTimeout())
	recovery := fault.NewLogRecovery(cfg.NodeID, repLog, peerClient)

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
	
	// Register the protobuf handlers
	n.transport.RegisterMessagingHandler(n)
	n.transport.RegisterConsensusHandler(n)

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
func (n *Node) PublishMessage(data []byte) (uint64, uint64, error) {
	// If this node is not the leader, automatic failover/proxy mechanism
	if !n.consensus.IsLeader() {
		leaderID := n.consensus.LeaderID()
		if leaderID == "" {
			return 0, 0, fmt.Errorf("no leader currently elected")
		}
		
		// Find peer address for the leader
		// (Assume cfg.Peers is list of addresses like localhost:5002, and node IDs match them or we route to leader)
		// Wait, the leaderID is from ZooKeeper which is just a string like "node1". We need a map of NodeID -> Address.
		// For simplicity, in this project if we can't map, we can just return a descriptive error so the client redirects, 
		// but if we want seamless proxy context, we need the leader's address. Let's return a redirect error if mapping is complex,
		// or actually implement the proxy if we have the peer address.
		// Let's implement redirection in the client instead, or proxy here.
		return 0, 0, fmt.Errorf("not the leader — redirect to %s", leaderID)
	}

	// 1. Deduplication mechanism
	msgStr := string(data)
	allMsgs := n.store.GetAll()
	for id, msg := range allMsgs {
		if msg.Data == msgStr {
			n.log.Info("duplicate message detected, avoiding replication", "message", msgStr)
			// Return successful response without appending again
			var storedIndex uint64
			fmt.Sscanf(id, "%d", &storedIndex)
			return storedIndex, msg.Timestamp, nil
		}
	}

	// 2. Tick the Lamport clock
	ts := n.clock.Tick()

	n.log.Debug("publishing message", "timestamp", ts)

	// Create the LogEntry natively pulling exact Term and absolute Log Index mappings
	term := n.consensus.CurrentTerm()
	index := n.repLog.LastIndex() + 1

	// 3. Trigger network replication
	entry := consensus.LogEntry{
		Index:     index,
		Term:      term,
		Timestamp: ts,
		Data:      data,
	}
	
	if err := n.repManager.ReplicateEntry(entry); err != nil {
		return 0, 0, fmt.Errorf("replication failed: %w", err)
	}

	// 4. Wait for quorum (Data Consistency)
	n.log.Debug("waiting for quorum", "index", index)
	success, err := n.repManager.WaitForQuorum(index, 2*time.Second)
	if err != nil || !success {
		return 0, 0, fmt.Errorf("failed to reach quorum for log index %d", index)
	}

	// Apply to local store after commit
	replication.ApplyLogToStore(n.store, index, data, ts, term)

	return index, ts, nil
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

// ---------------------------------------------------------------------------
// Consensus API implementation
// ---------------------------------------------------------------------------

func (n *Node) HandleAppendEntries(req *consensus.AppendEntriesRequest) *consensus.AppendEntriesResponse {
	return n.consensus.HandleAppendEntries(req)
}

func (n *Node) HandleRequestVote(req *consensus.RequestVoteRequest) *consensus.RequestVoteResponse {
	return n.consensus.HandleRequestVote(req)
}

func (n *Node) OnAppendEntriesRecv(entries []consensus.LogEntry, leaderCommit uint64) {
	for _, entry := range entries {
		// Update Lamport clock if needed
		n.clock.Update(entry.Timestamp)

		// Append to local log
		n.repLog.Append(entry)

		// Apply to message store for consumption
		replication.ApplyLogToStore(n.store, entry.Index, entry.Data, entry.Timestamp, entry.Term)
	}
}
