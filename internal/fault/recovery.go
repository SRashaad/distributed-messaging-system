// =============================================================================
// Module: Fault Tolerance
// File: recovery.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Handles re-integration of nodes that have recovered from failure.
//          When a previously failed node comes back online, the Leader must
//          send it all the log entries it missed while it was down.
//          This ensures the recovered node's log converges with the cluster.
//
// Connections:
//   - Uses internal/replication.ReplicatedLog to read the leader's log entries.
//   - Uses internal/transport to send AppendEntries RPCs to the recovering node.
//   - Triggered by internal/consensus/raft.go when a follower reconnects
//     and its log is behind the leader's.
//
// Recovery flow:
//   1. Recovering node connects and sends its last log index.
//   2. Leader compares with its own log and identifies missing entries.
//   3. Leader sends missing entries via AppendEntries RPCs.
//   4. Recovering node appends entries and confirms consistency.
// =============================================================================
package fault

// RecoveryManager defines the interface for node recovery operations.
type RecoveryManager interface {
	// InitiateRecovery starts the recovery process for a previously failed node.
	InitiateRecovery(nodeID string) error

	// SyncLog sends log entries starting from fromIndex to the recovering node.
	SyncLog(nodeID string, fromIndex uint64) error
}

// LogRecovery implements RecoveryManager.
type LogRecovery struct {
	// TODO: Add fields for:
	//   - replication.ReplicatedLog → to read the leader's log entries
	//   - transport.PeerClient      → to send entries to the recovering node
}

// NewLogRecovery creates a new recovery manager.
//
// TODO: Accept and store references to the replicated log and transport client.
func NewLogRecovery() *LogRecovery {
	return nil
}

// InitiateRecovery begins the recovery process for a previously failed node.
// Called by the leader when it detects a follower has reconnected.
//
// TODO: Implement:
//   1. Query the recovering node for its last log index (via RPC).
//   2. Determine which entries are missing by comparing with the leader's log.
//   3. Call SyncLog() with the appropriate starting index.
//   4. Verify log consistency after synchronization.
func (r *LogRecovery) InitiateRecovery(nodeID string) error {
	return nil
}

// SyncLog sends log entries starting from the given index to the recovering node.
// Uses AppendEntries RPCs to transfer entries in batches.
//
// TODO: Implement:
//   1. Read entries from the leader's log starting at fromIndex.
//   2. Send entries to the recovering node via AppendEntries RPC.
//   3. Handle the response and retry if necessary.
func (r *LogRecovery) SyncLog(nodeID string, fromIndex uint64) error {
	return nil
}
