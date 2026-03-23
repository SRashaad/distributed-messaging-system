// =============================================================================
// Module: Replication
// File: manager.go
// Responsible Member: Senul Mintharu — Data Replication & Consistency
// Purpose: Orchestrates log replication from the Leader to all Followers.
//          When a client publishes a message, the leader appends it to its
//          local log and then uses the ReplicationManager to send the entry
//          to every follower via AppendEntries RPCs.
//
// Connections:
//   - Uses ReplicatedLog (log.go) to append entries locally.
//   - Uses QuorumTracker (quorum.go) to track follower acknowledgments.
//   - Uses internal/transport to send AppendEntries RPCs to peers.
//   - Called by internal/consensus/raft.go ProposeEntry() on the leader.
//
// Replication flow:
//   1. Leader appends entry to local log.
//   2. Leader sends AppendEntries to all followers in parallel.
//   3. Each follower ACK is recorded in the QuorumTracker.
//   4. Once a quorum (majority) acknowledges, the entry is committed.
// =============================================================================
package replication

import (
	"time"

	"distributed-messaging-system/internal/consensus"
)

// ReplicationManager defines the interface for orchestrating log replication.
type ReplicationManager interface {
	// ReplicateEntry appends an entry and sends it to all followers.
	ReplicateEntry(entry consensus.LogEntry) error

	// WaitForQuorum blocks until a majority acknowledges the entry, or times out.
	WaitForQuorum(index uint64, timeout time.Duration) (bool, error)
}

// Manager implements ReplicationManager.
type Manager struct {
	log    ReplicatedLog  // the local replicated log
	quorum *QuorumTracker // tracks follower acknowledgments

	// TODO: Add a transport.PeerClient field for sending AppendEntries RPCs.
	// TODO: Add a list of peer addresses to iterate over during replication.
}

// NewManager creates a new ReplicationManager.
//
// TODO: Initialize with the given log and a QuorumTracker sized for clusterSize.
//       Also accept a transport reference for sending RPCs.
func NewManager(log ReplicatedLog, clusterSize int) *Manager {
	return nil
}

// ReplicateEntry appends an entry to the local log and initiates
// replication to all followers via AppendEntries RPCs.
//
// TODO: Implement:
//   1. Append the entry to the local log.
//   2. For each follower, send an AppendEntries RPC in a goroutine.
//   3. When a follower responds with Success=true, call quorum.RecordAck().
//   4. Return any errors from the local append.
func (m *Manager) ReplicateEntry(entry consensus.LogEntry) error {
	return nil
}

// WaitForQuorum blocks until a quorum (majority) has acknowledged the entry
// at the given index, or the timeout expires.
//
// TODO: Implement:
//   1. Periodically check quorum.HasQuorum(index).
//   2. Return true if quorum is reached before the timeout.
//   3. Return false if the timeout expires without quorum.
func (m *Manager) WaitForQuorum(index uint64, timeout time.Duration) (bool, error) {
	return false, nil
}
