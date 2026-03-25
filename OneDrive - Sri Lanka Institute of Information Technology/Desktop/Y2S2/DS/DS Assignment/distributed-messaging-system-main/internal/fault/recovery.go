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

import (
	"errors"
	"fmt"

	"distributed-messaging-system/internal/consensus"
	"distributed-messaging-system/internal/replication"
)

// RecoveryManager defines the interface for node recovery operations.
type RecoveryManager interface {
	// InitiateRecovery starts the recovery process for a previously failed node.
	InitiateRecovery(nodeID string) error

	// SyncLog sends log entries starting from fromIndex to the recovering node.
	SyncLog(nodeID string, fromIndex uint64) error
}

// SendEntriesFunc is a function type used by LogRecovery to send a batch of
// log entries to a recovering node via AppendEntries RPC.
// The transport layer injects a concrete implementation.
type SendEntriesFunc func(nodeID string, entries []consensus.LogEntry) error

// QueryLastIndexFunc is a function type used to query a recovering node for
// the index of the last log entry it holds.
type QueryLastIndexFunc func(nodeID string) (uint64, error)

// LogRecovery implements RecoveryManager.
// It reads missing entries from the leader's replicated log and ships them
// to a recovering node in batches.
type LogRecovery struct {
	log            replication.ReplicatedLog // leader's local log (source of truth)
	sendEntries    SendEntriesFunc           // transport-layer function to send entries via RPC
	queryLastIndex QueryLastIndexFunc        // queries a node for its last log index
	batchSize      int                       // max entries per AppendEntries RPC (default 50)
}

// NewLogRecovery creates a new recovery manager.
//
// Parameters:
//   - log:            the leader's replicated log
//   - sendEntries:    closure to send a batch of entries to a peer (from transport layer)
//   - queryLastIndex: closure to ask a recovering peer its last log index
func NewLogRecovery(
	log replication.ReplicatedLog,
	sendEntries SendEntriesFunc,
	queryLastIndex QueryLastIndexFunc,
) *LogRecovery {
	return &LogRecovery{
		log:            log,
		sendEntries:    sendEntries,
		queryLastIndex: queryLastIndex,
		batchSize:      50,
	}
}

// InitiateRecovery begins the recovery process for a previously failed node.
// Called by the leader when it detects a follower has reconnected.
//
// Steps:
//  1. Query the recovering node for its last log index.
//  2. Compare with the leader's log to find the first missing entry.
//  3. Call SyncLog() from that point forward.
//  4. Verify the recovering node's index matches the leader's after sync.
func (r *LogRecovery) InitiateRecovery(nodeID string) error {
	if nodeID == "" {
		return errors.New("recovery: nodeID must not be empty")
	}

	// Step 1 — ask the recovering node where its log ends.
	peerLastIndex, err := r.queryLastIndex(nodeID)
	if err != nil {
		return fmt.Errorf("recovery: failed to query last index from %s: %w", nodeID, err)
	}

	leaderLastIndex := r.log.LastIndex()

	// Nothing to send — the recovering node is already up to date.
	if peerLastIndex >= leaderLastIndex {
		return nil
	}

	// Step 2 & 3 — send everything the peer is missing.
	// +1 because entries are 1-indexed and peerLastIndex is already present.
	fromIndex := peerLastIndex + 1
	if err := r.SyncLog(nodeID, fromIndex); err != nil {
		return fmt.Errorf("recovery: log sync to %s failed: %w", nodeID, err)
	}

	// Step 4 — re-query to verify convergence.
	peerLastIndex, err = r.queryLastIndex(nodeID)
	if err != nil {
		return fmt.Errorf("recovery: post-sync verification query failed for %s: %w", nodeID, err)
	}
	if peerLastIndex != leaderLastIndex {
		return fmt.Errorf(
			"recovery: inconsistency after sync — leader=%d peer=%d",
			leaderLastIndex, peerLastIndex,
		)
	}

	return nil
}

// SyncLog sends log entries starting from fromIndex to the recovering node.
// Entries are delivered in batches of r.batchSize via AppendEntries RPCs.
// Each batch is retried once on transient failure before returning an error.
func (r *LogRecovery) SyncLog(nodeID string, fromIndex uint64) error {
	leaderLastIndex := r.log.LastIndex()

	if fromIndex > leaderLastIndex {
		// Nothing to send.
		return nil
	}

	for start := fromIndex; start <= leaderLastIndex; start += uint64(r.batchSize) {
		// Read one batch from the leader's log.
		entries, err := r.log.GetEntriesFrom(start)
		if err != nil {
			return fmt.Errorf("recovery: failed to read log from index %d: %w", start, err)
		}

		// Cap the batch to batchSize.
		if len(entries) > r.batchSize {
			entries = entries[:r.batchSize]
		}

		if len(entries) == 0 {
			break
		}

		// Send the batch to the recovering node.
		if err := r.sendEntries(nodeID, entries); err != nil {
			// Retry once — transient network hiccup is common during recovery.
			if retryErr := r.sendEntries(nodeID, entries); retryErr != nil {
				return fmt.Errorf(
					"recovery: failed to send entries [%d..%d] to %s after retry: %w",
					start, start+uint64(len(entries))-1, nodeID, retryErr,
				)
			}
		}
	}

	return nil
}
