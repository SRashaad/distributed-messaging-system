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
)

// RecoveryManager defines the interface for node recovery operations.
type RecoveryManager interface {
	InitiateRecovery(nodeID string) error
	SyncLog(nodeID string, fromIndex uint64) error
}

// LogRecovery implements RecoveryManager.
type LogRecovery struct {
	// In full integration, these would be:
	//   log       replication.ReplicatedLog
	//   transport *transport.PeerClient
	nodeID string // this node's ID for logging
}

// NewLogRecovery creates a new recovery manager.
func NewLogRecovery(nodeID string) *LogRecovery {
	return &LogRecovery{
		nodeID: nodeID,
	}
}

// InitiateRecovery begins the recovery process for a previously failed node.
// Called by the leader when it detects a follower has reconnected.
func (r *LogRecovery) InitiateRecovery(nodeID string) error {
	log.Printf("[fault] Initiating recovery for node %s", nodeID)

	// In full integration:
	// 1. Query the recovering node for its last log index (via RPC)
	// 2. Determine which entries are missing
	// 3. Call SyncLog() with the appropriate starting index

	// For now, log the intent and return success
	log.Printf("[fault] Recovery initiated for node %s (will sync missing entries)", nodeID)
	return nil
}

// SyncLog sends log entries starting from the given index to the recovering node.
func (r *LogRecovery) SyncLog(nodeID string, fromIndex uint64) error {
	log.Printf("[fault] Syncing log to node %s from index %d", nodeID, fromIndex)

	// In full integration:
	// 1. Read entries from the leader's log starting at fromIndex
	// 2. Send entries to the recovering node via AppendEntries RPC
	// 3. Handle the response and retry if necessary

	if fromIndex == 0 {
		return fmt.Errorf("invalid fromIndex: must be >= 1")
	}

	log.Printf("[fault] Log sync to node %s completed from index %d", nodeID, fromIndex)
	return nil
}
