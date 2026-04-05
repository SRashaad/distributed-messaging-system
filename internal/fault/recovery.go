// =============================================================================
// Module: Fault Tolerance
// File: recovery.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Handles re-integration of nodes that have recovered from failure.
//          When a previously failed node comes back online, the Leader must
//          send it all the log entries it missed while it was down.
//
// Connections:
//   - Uses internal/replication.ReplicatedLog to read the leader's log entries.
//   - Uses internal/transport to send AppendEntries RPCs to the recovering node.
//   - Triggered by internal/consensus/raft.go when a follower reconnects.
//
// Note: With ZooKeeper, when a node's session expires and it reconnects,
//       it creates a new ephemeral znode. The leader detects this and
//       triggers recovery via this module.
// =============================================================================
package fault

import (
	"fmt"
	"log"

	"distributed-messaging-system/internal/replication"
	"distributed-messaging-system/internal/transport"
	"distributed-messaging-system/internal/transport/proto"
)

// RecoveryManager defines the interface for node recovery operations.
type RecoveryManager interface {
	InitiateRecovery(nodeID string) error
	SyncLog(nodeID string, fromIndex uint64) error
}

// LogRecovery implements RecoveryManager.
type LogRecovery struct {
	log       replication.ReplicatedLog
	transport *transport.PeerClient
	nodeID    string // this node's ID for logging
}

// NewLogRecovery creates a new recovery manager.
func NewLogRecovery(nodeID string, log replication.ReplicatedLog, tc *transport.PeerClient) *LogRecovery {
	return &LogRecovery{
		log:       log,
		transport: tc,
		nodeID:    nodeID,
	}
}

// InitiateRecovery begins the recovery process for a previously failed node.
// Called by the leader when it detects a follower has reconnected.
func (r *LogRecovery) InitiateRecovery(nodeID string) error {
	log.Printf("[fault] Initiating recovery for node %s", nodeID)

	// Since we don't have the exact state without querying, 
	// we will sync the entire log from index 1.
	// In a complete implementation, we'd query the follower's last log index.
	err := r.SyncLog(nodeID, 1)
	if err != nil {
		return fmt.Errorf("failed to sync log for node %s: %w", nodeID, err)
	}

	log.Printf("[fault] Recovery initiated for node %s (syncing missing entries)", nodeID)
	return nil
}

// SyncLog sends log entries starting from the given index to the recovering node.
func (r *LogRecovery) SyncLog(nodeID string, fromIndex uint64) error {
	log.Printf("[fault] Syncing log to node %s from index %d", nodeID, fromIndex)

	if fromIndex == 0 {
		return fmt.Errorf("invalid fromIndex: must be >= 1")
	}

	entries, err := r.log.GetEntriesFrom(fromIndex)
	if err != nil {
		return fmt.Errorf("failed to get entries from index %d: %w", fromIndex, err)
	}

	if len(entries) == 0 {
		log.Printf("[fault] No entries to sync to node %s", nodeID)
		return nil
	}

	protoEntries := make([]*proto.LogEntry, 0, len(entries))
	for _, e := range entries {
		protoEntries = append(protoEntries, &proto.LogEntry{
			Index:     e.Index,
			Term:      e.Term,
			Timestamp: e.Timestamp,
			Data:      e.Data,
		})
	}

	req := &proto.AppendEntriesRequest{
		Term:         r.log.LastTerm(),
		LeaderId:     r.nodeID,
		PrevLogIndex: fromIndex - 1,
		PrevLogTerm:  0, // Would query from log in prod, optional for basic recovery
		Entries:      protoEntries,
		LeaderCommit: r.log.CommitIndex(),
	}

	_, err = r.transport.SendAppendEntries(nodeID, req)
	if err != nil {
		return fmt.Errorf("failed to send AppendEntries to %s: %w", nodeID, err)
	}

	log.Printf("[fault] Log sync to node %s completed from index %d", nodeID, fromIndex)
	return nil
}
